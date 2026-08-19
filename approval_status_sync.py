import os
import sys
import time
import random
import logging
import threading
import requests
import gspread
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.oauth2.service_account import Credentials

API_KEY           = os.environ['API_KEY']
FORM_ID           = os.environ['FORM_ID']
SPREADSHEET_NAME  = os.environ['SPREADSHEET_NAME']
WORKSHEET_NAME    = os.environ['WORKSHEET_NAME_APPROVAL']
GOOGLE_CREDS_FILE = os.environ.get('GOOGLE_CREDS_FILE', 'credentials.json')  # written to disk by the workflow step

JOTFORM_BASE_URL  = "https://pw.jotform.com/API"  # swap to api.jotform.com if non-enterprise
PAGE_SIZE         = 1000  # JotForm's hard max per request is 1000; loop below pages past that
THREAD_PAGE_SIZE  = 1000
REQUEST_DELAY     = 0.15
MAX_WORKERS       = int(os.environ.get('MAX_WORKERS', 20))   # concurrent thread fetches; raise cautiously, lower on 429s
HEADERS = ["Unique ID", "Submission Date", "Status", "Approval Status", "Date"]

# Statuses that will never change again -> safe to skip refetching their thread.
# Expired and Failed submissions can be restarted/resubmitted in JotForm, so
# they are NOT treated as terminal - they get refetched every run until they
# land on Approved or Rejected.
TERMINAL_STATUSES = {"Approved", "Rejected"}

# ─── Retry / resilience config ────────────────────────────────────────────────
MAX_RETRIES        = 4      # per HTTP request, inside jf_get
BASE_BACKOFF       = 1.0    # seconds, doubles each retry (plus jitter)
MAX_BACKOFF        = 20.0
SUBMISSION_RETRIES = 2      # extra whole-submission retries in process_submission
RATE_LIMIT_PERMITS = MAX_WORKERS  # concurrent in-flight requests across all threads

# ─── Sheets write tuning ───────────────────────────────────────────────────────
SHEET_WRITE_CHUNK  = int(os.environ.get('SHEET_WRITE_CHUNK', 2000))  # rows per append_rows call
SHEET_MAX_RETRIES  = 5
SHEET_BASE_BACKOFF = 2.0

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_session = requests.Session()
_rate_limit_sem = threading.Semaphore(RATE_LIMIT_PERMITS)


# ─── JotForm API ─────────────────────────────────────────────────────────────

def _sleep_backoff(attempt: int, retry_after: str = None, base: float = BASE_BACKOFF, cap: float = MAX_BACKOFF):
    if retry_after:
        try:
            time.sleep(float(retry_after))
            return
        except ValueError:
            pass
    delay = min(base * (2 ** attempt), cap)
    delay += random.uniform(0, delay * 0.25)  # jitter
    time.sleep(delay)


def jf_get(endpoint: str, params: dict = None) -> dict:
    """
    GET against the JotForm API with retries + backoff.
    - 429: honors Retry-After header if present, else exponential backoff. Retried.
    - 5xx / connection / timeout errors: retried with exponential backoff.
    - 4xx other than 429 (bad key, not found, etc): fails fast, no retry.
    - JotForm-level error (responseCode != 200): treated as non-retryable,
      since it usually means a real problem (bad form ID, bad params), not
      a transient issue.
    """
    url = f"{JOTFORM_BASE_URL}{endpoint}"
    p = {"apikey": API_KEY}
    p.update(params or {})

    last_exc = None
    for attempt in range(MAX_RETRIES + 1):
        with _rate_limit_sem:
            time.sleep(REQUEST_DELAY)
            try:
                resp = _session.get(url, params=p, timeout=30)
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_exc = exc
                if attempt < MAX_RETRIES:
                    log.warning("  Network error on %s (attempt %d/%d): %s — retrying",
                                endpoint, attempt + 1, MAX_RETRIES + 1, exc)
                    _sleep_backoff(attempt)
                    continue
                raise

        if resp.status_code == 429:
            if attempt < MAX_RETRIES:
                log.warning("  429 rate-limited on %s (attempt %d/%d) — backing off",
                            endpoint, attempt + 1, MAX_RETRIES + 1)
                _sleep_backoff(attempt, retry_after=resp.headers.get("Retry-After"))
                continue
            resp.raise_for_status()

        if 500 <= resp.status_code < 600:
            if attempt < MAX_RETRIES:
                log.warning("  %d server error on %s (attempt %d/%d) — retrying",
                            resp.status_code, endpoint, attempt + 1, MAX_RETRIES + 1)
                _sleep_backoff(attempt)
                continue
            resp.raise_for_status()

        # 4xx other than 429: fail fast, don't burn retries on a bad request.
        resp.raise_for_status()

        data = resp.json()
        if data.get("responseCode") != 200:
            raise RuntimeError(f"JotForm error on {endpoint}: {data}")
        return data

    # Should only get here if we exhausted retries on a connection error.
    raise last_exc or RuntimeError(f"Failed to fetch {endpoint} after {MAX_RETRIES + 1} attempts")


def fetch_all_submissions(form_id: str) -> list[dict]:
    submissions, offset = [], 0
    while True:
        log.info("Fetching submissions offset=%d ...", offset)
        data = jf_get(
            f"/form/{form_id}/submissions",
            params={
                "limit": PAGE_SIZE,
                "offset": offset,
                "orderby": "created_at",
                "direction": "ASC",
                "addWorkflowStatus": 1,
            },
        )
        batch = data.get("content", [])
        submissions.extend(batch)
        total = data["resultSet"]["count"]
        offset += len(batch)
        if offset >= total or not batch:
            break
    log.info("Total submissions: %d", len(submissions))
    return submissions


def fetch_thread(submission_id: str) -> list[dict]:
    """The thread endpoint is paginated - page through it fully."""
    events, offset = [], 0
    while True:
        data = jf_get(
            f"/submission/{submission_id}/thread",
            params={"limit": THREAD_PAGE_SIZE, "offset": offset},
        )
        batch = data.get("content", [])
        events.extend(batch)
        result_set = data.get("resultSet", {})
        total = result_set.get("count", len(batch))
        offset += len(batch)
        if offset >= total or not batch:
            break
    return events


# ─── Parsing ─────────────────────────────────────────────────────────────────

def get_answer(answers: dict, field_name: str) -> str:
    for v in answers.values():
        if v.get("name") == field_name:
            return str(v.get("answer", ""))
    return ""


def parse_unique_id(sub: dict) -> str:
    return get_answer(sub.get("answers", {}), "uniqueId") or sub.get("id", "")


def latest_workflow_instance_id(thread: list[dict]) -> str:
    for event in reversed(thread):
        wfid = event.get("actionDetails", {}).get("workflowInstanceID")
        if wfid:
            return wfid
    return ""


def filter_to_latest_instance(thread: list[dict]) -> list[dict]:
    latest = latest_workflow_instance_id(thread)
    if not latest:
        return thread
    return [e for e in thread if e.get("actionDetails", {}).get("workflowInstanceID") == latest]


def discover_approval_steps(thread: list[dict]) -> list[str]:
    first_seen: dict[str, str] = {}
    for event in thread:
        eid = str(event.get("elementID") or "")
        if not eid:
            continue
        details = event.get("actionDetails", {})
        if details.get("title") != "Approval":
            continue
        ts = event.get("timestamp", "")
        if eid not in first_seen or ts < first_seen[eid]:
            first_seen[eid] = ts
    return sorted(first_seen, key=lambda eid: first_seen[eid])


def _initial_recipients(events: list[dict]) -> str:
    for e in events:
        if e["actionType"] == "MAIL":
            details = e.get("actionDetails", {})
            if details.get("reason") == "START":
                return details.get("to", "")
        if e["actionType"] == "MULTIPLE_APPROVAL_MAIL":
            details = e.get("actionDetails", {})
            results = details.get("emailResults", [])
            if results:
                return ", ".join(r.get("email", "") for r in results if r.get("email"))
            raw = details.get("assigneeEmails")
            if raw:
                try:
                    import json
                    return ", ".join(json.loads(raw).values())
                except Exception:
                    pass
    return ""


DECISION_ACTION_TYPES = {"APPROVE_REJECT", "MULTIPLE_APPROVE_REJECT", "EXPIRE"}


def parse_one_step(events: list[dict]) -> dict:
    acting_email = _initial_recipients(events)

    for e in events:
        if e["actionType"] == "REASSIGN":
            acting_email = e.get("actionDetails", {}).get("newAssigneeEmail", acting_email)

    action_time = ""
    status = "Pending"
    decided = False
    for e in events:
        if e["actionType"] in DECISION_ACTION_TYPES:
            details = e.get("actionDetails", {})
            action_time = e.get("timestamp", "")
            acting_email = details.get("assigneeEmail") or acting_email

            if e["actionType"] == "EXPIRE":
                status = "Expired"
                decided = True
                break

            outcome_type = details.get("type", "")
            if outcome_type == "APPROVE":
                status = "Approved"
            elif outcome_type == "REJECT":
                status = "Rejected"
            else:
                outcome_info = details.get("outcomeInfo", {}) or {}
                cancel_reason = details.get("cancelReason", "")
                status = (
                    details.get("text")
                    or outcome_info.get("text")
                    or outcome_type
                    or (cancel_reason.title() if cancel_reason else "")
                    or f"Unknown (id={details.get('id')})"
                )
            decided = True
            break

    if not decided:
        for e in events:
            if e["actionType"] == "FAIL":
                status = "Failed"
                action_time = e.get("timestamp", "")
                break

    return {"email": acting_email, "action_time": action_time, "status": status}


def compute_walk_status(thread: list[dict]) -> dict:
    thread = filter_to_latest_instance(thread)

    by_elem: dict[str, list[dict]] = {}
    for event in thread:
        eid = str(event.get("elementID") or "")
        by_elem.setdefault(eid, []).append(event)

    step_order = discover_approval_steps(thread)
    if not step_order:
        return {"status": "Pending", "date": ""}

    last_action_time = ""
    for eid in step_order:
        step = parse_one_step(by_elem.get(eid, []))
        if step["status"] == "Rejected":
            return {"status": "Rejected", "date": step["action_time"]}
        if step["status"] == "Failed":
            return {"status": "Failed", "date": step["action_time"]}
        if step["status"] == "Pending":
            return {"status": "Pending", "date": ""}
        if step["status"] != "Approved":
            return {"status": step["status"], "date": step["action_time"]}
        last_action_time = step["action_time"]

    return {"status": "Approved", "date": last_action_time}


def build_row(sub: dict, thread: list[dict]) -> list:
    unique_id = parse_unique_id(sub)
    submission_date = sub.get("created_at", "")
    raw_status = sub.get("status", "")

    walk = compute_walk_status(thread)
    approval_status = walk["status"]
    date = walk["date"] if walk["status"] == "Approved" else ""

    return [unique_id, submission_date, raw_status, approval_status, date]


# ─── Submission processing (parallel + retried) ───────────────────────────────

def process_submission(sub: dict) -> list:
    """
    Fetches the thread and builds the row for one submission.
    Retries the whole operation up to SUBMISSION_RETRIES extra times on
    failure - separate from jf_get's own per-request retries, since this
    covers e.g. a submission whose thread endpoint is transiently 404ing,
    or any parsing edge case tied to a partial/corrupt fetch.
    """
    sub_id = sub.get("id", "")
    last_exc = None
    for attempt in range(SUBMISSION_RETRIES + 1):
        try:
            thread = fetch_thread(sub_id)
            return build_row(sub, thread)
        except Exception as exc:
            last_exc = exc
            if attempt < SUBMISSION_RETRIES:
                log.warning("  Submission %s failed (attempt %d/%d): %s — retrying",
                            sub_id, attempt + 1, SUBMISSION_RETRIES + 1, exc)
                _sleep_backoff(attempt)
            else:
                log.error("  Submission %s failed permanently after %d attempts: %s",
                           sub_id, SUBMISSION_RETRIES + 1, exc)
    raise last_exc


def process_all_submissions(submissions: list[dict]) -> tuple[list[list], list[list]]:
    """
    Returns (fresh_rows, carried_rows_placeholder). Only submissions passed in
    are actually fetched/processed - callers are expected to have already
    filtered out submissions whose prior status is terminal (see run_sync).
    """
    all_rows = []
    failed = []
    total = len(submissions)
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(process_submission, sub): sub for sub in submissions}
        for future in as_completed(futures):
            sub = futures[future]
            done += 1
            try:
                all_rows.append(future.result())
            except Exception as exc:
                failed.append(sub.get("id", ""))
                log.warning("  Skipped %s: %s", sub.get("id", ""), exc)
            if done % 100 == 0 or done == total:
                log.info("Processed %d/%d submissions ...", done, total)

    if failed:
        log.warning("Finished with %d/%d submissions permanently failed: %s",
                     len(failed), total, failed[:50])
    return all_rows


# ─── Google Sheets helpers ────────────────────────────────────────────────────

def _sheets_call_with_retry(fn, *args, **kwargs):
    """
    gspread/Sheets API calls hit their own 429s (default quota is 60
    write requests/min/user) independent of the JotForm side. Wrap every
    write in the same style of retry we use for jf_get.
    """
    last_exc = None
    for attempt in range(SHEET_MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except gspread.exceptions.APIError as exc:
            status = getattr(exc.response, "status_code", None)
            last_exc = exc
            if status in (429, 500, 502, 503) and attempt < SHEET_MAX_RETRIES:
                log.warning("  Sheets API error (status=%s, attempt %d/%d) — backing off",
                            status, attempt + 1, SHEET_MAX_RETRIES + 1)
                _sleep_backoff(attempt, base=SHEET_BASE_BACKOFF, cap=60.0)
                continue
            raise
    raise last_exc


def get_or_create_sheet(client: gspread.Client, spreadsheet_name: str) -> gspread.Worksheet:
    ss = _sheets_call_with_retry(client.open, spreadsheet_name)
    try:
        ws = _sheets_call_with_retry(ss.worksheet, WORKSHEET_NAME)
        log.info("Found existing sheet: '%s'", WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = _sheets_call_with_retry(ss.add_worksheet, title=WORKSHEET_NAME, rows=5000, cols=len(HEADERS) + 2)
        log.info("Created new sheet: '%s'", WORKSHEET_NAME)
    return ws


def setup_headers(ws: gspread.Worksheet):
    if _sheets_call_with_retry(ws.row_values, 1) == HEADERS:
        return
    _sheets_call_with_retry(ws.update, "A1", [HEADERS])
    sheet_id = ws.id
    _sheets_call_with_retry(ws.spreadsheet.batch_update, {"requests": [
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0, "endRowIndex": 1,
                    "startColumnIndex": 0, "endColumnIndex": len(HEADERS),
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.122, "green": 0.306, "blue": 0.475},
                        "textFormat": {
                            "bold": True,
                            "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                            "fontSize": 10,
                        },
                        "horizontalAlignment": "CENTER",
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
            }
        },
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        },
    ]})
    log.info("Headers written and formatted.")


def setup_conditional_formatting(ws: gspread.Worksheet):
    sheet_id = ws.id
    col = HEADERS.index("Approval Status")

    rules = []
    for val, r, g, b in [
        ("Approved", 0.714, 0.843, 0.659),
        ("Pending", 1.0, 0.878, 0.698),
        ("Rejected", 0.918, 0.600, 0.600),
        ("Failed", 0.6, 0.2, 0.2),
        ("Expired", 0.7, 0.7, 0.7),
    ]:
        rules.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "startColumnIndex": col,
                        "endColumnIndex": col + 1,
                    }],
                    "booleanRule": {
                        "condition": {
                            "type": "TEXT_EQ",
                            "values": [{"userEnteredValue": val}],
                        },
                        "format": {"backgroundColor": {"red": r, "green": g, "blue": b}},
                    },
                },
                "index": 0,
            }
        })

    _sheets_call_with_retry(ws.spreadsheet.batch_update, {"requests": rules})
    log.info("Conditional formatting applied.")


def read_existing_rows(ws: gspread.Worksheet) -> dict[str, list]:
    """
    Load whatever is currently in the sheet, keyed by Unique ID, so we can
    skip refetching submissions that are already in a terminal state and
    only ask JotForm about ones that are new or still Pending.
    """
    values = _sheets_call_with_retry(ws.get_all_values)
    if not values or values[0] != HEADERS:
        return {}
    existing = {}
    for row in values[1:]:
        if not row or not row[0]:
            continue
        # pad in case a row is short
        row = row + [""] * (len(HEADERS) - len(row))
        existing[row[0]] = row[:len(HEADERS)]
    log.info("Loaded %d existing rows from sheet for incremental diff.", len(existing))
    return existing


def write_all_rows_chunked(ws: gspread.Worksheet, rows: list[list]):
    """
    Writes in bounded chunks instead of one massive append_rows call, so:
    - we don't risk hitting request size limits at 30k+ rows
    - a failure partway through doesn't lose everything already written
    - each chunk gets its own retry via _sheets_call_with_retry
    """
    if not rows:
        log.info("No rows to write.")
        return
    total = len(rows)
    written = 0
    for i in range(0, total, SHEET_WRITE_CHUNK):
        chunk = rows[i:i + SHEET_WRITE_CHUNK]
        _sheets_call_with_retry(ws.append_rows, chunk, value_input_option="USER_ENTERED")
        written += len(chunk)
        log.info("Wrote rows %d-%d of %d ...", i + 1, written, total)
    log.info("Wrote %d rows total.", written)


def clear_sheet_body(ws: gspread.Worksheet):
    total_rows = ws.row_count
    if total_rows > 1:
        _sheets_call_with_retry(ws.batch_clear, [f"A2:{gspread.utils.rowcol_to_a1(total_rows, len(HEADERS))}"])
    log.info("Cleared existing data rows (kept header).")


# ─── Core sync (shared by CLI + Cloud Function/Run entry points) ─────────────

def run_sync() -> dict:
    if not os.path.exists(GOOGLE_CREDS_FILE):
        raise RuntimeError(f"Creds file not found: {GOOGLE_CREDS_FILE}")

    log.info("Connecting to Google Sheets ...")
    creds = Credentials.from_service_account_file(GOOGLE_CREDS_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    ws = get_or_create_sheet(client, SPREADSHEET_NAME)

    is_new = _sheets_call_with_retry(ws.row_values, 1) != HEADERS
    setup_headers(ws)
    if is_new:
        setup_conditional_formatting(ws)

    # ── Incremental diff: only fetch threads for submissions that are new, or
    # were last seen as Pending/Expired/Failed (any of which can still change -
    # a workflow can be restarted after expiry/failure). Anything already
    # Approved/Rejected is copied straight over without hitting JotForm.
    existing = read_existing_rows(ws)

    submissions = fetch_all_submissions(FORM_ID)
    to_fetch = []
    carried_rows = {}
    for sub in submissions:
        uid = parse_unique_id(sub)
        prior = existing.get(uid)
        if prior and prior[3] in TERMINAL_STATUSES:
            carried_rows[uid] = prior
        else:
            to_fetch.append(sub)

    log.info("Submissions: %d total, %d carried over (Approved/Rejected), %d to fetch fresh.",
              len(submissions), len(carried_rows), len(to_fetch))

    fresh_rows = process_all_submissions(to_fetch) if to_fetch else []
    for row in fresh_rows:
        carried_rows[row[0]] = row

    # Preserve original submission order for readability.
    ordered_ids = [parse_unique_id(s) for s in submissions]
    all_rows = [carried_rows[uid] for uid in ordered_ids if uid in carried_rows]

    clear_sheet_body(ws)
    log.info("Writing %d rows ...", len(all_rows))
    write_all_rows_chunked(ws, all_rows)

    log.info("Done! Sheet: %s", SPREADSHEET_NAME)
    return {
        "status": "ok",
        "rows_written": len(all_rows),
        "fetched_fresh": len(to_fetch),
        "carried_over": len(carried_rows) - len(fresh_rows),
    }


def sync_http(request):
    """
    HTTP entry point. NOTE: if this is deployed as a request/response Cloud
    Function, the platform's request timeout still applies (Gen1 max 9 min,
    Gen2/Cloud Run max ~60 min) - at 30k+ submissions and growing, prefer
    deploying this as a Cloud Run Job (or Gen2 function with an extended
    timeout) triggered by Cloud Scheduler, rather than a synchronous
    request/response function. That removes the "server stops responding
    mid-run" failure mode entirely, since there's no client waiting on an
    HTTP connection.
    """
    try:
        result = run_sync()
        return (result, 200)
    except Exception as exc:
        log.exception("Sync failed")
        return ({"status": "error", "message": str(exc)}, 500)


def main():
    result = run_sync()
    print(result)


if __name__ == "__main__":
    main()