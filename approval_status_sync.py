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
WORKSHEET_NAME    = os.environ['WORKSHEET_NAME_TDR']
GOOGLE_CREDS_FILE = os.environ.get('GOOGLE_CREDS_FILE', 'credentials.json')

JOTFORM_BASE_URL  = "https://pw.jotform.com/API"  # swap to api.jotform.com if non-enterprise
PAGE_SIZE         = 1000  # JotForm's hard max per request is 1000; loop below pages past that
THREAD_PAGE_SIZE  = 1000
REQUEST_DELAY     = 0.15
MAX_WORKERS       = 12    # concurrent submission-thread fetches; lower this if you hit 429s
HEADERS = ["Unique ID", "Submission Date", "Status", "Approval Status", "Date"]

# ─── Retry / resilience config ────────────────────────────────────────────────
MAX_RETRIES        = 4      # per HTTP request, inside jf_get
BASE_BACKOFF       = 1.0    # seconds, doubles each retry (plus jitter)
MAX_BACKOFF        = 20.0
SUBMISSION_RETRIES = 2      # extra whole-submission retries in process_submission
RATE_LIMIT_PERMITS = MAX_WORKERS  # concurrent in-flight requests across all threads

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

def _sleep_backoff(attempt: int, retry_after: str = None):
    if retry_after:
        try:
            time.sleep(float(retry_after))
            return
        except ValueError:
            pass
    delay = min(BASE_BACKOFF * (2 ** attempt), MAX_BACKOFF)
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


def process_all_submissions(submissions: list[dict]) -> list[list]:
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
            if done % 25 == 0 or done == total:
                log.info("Processed %d/%d submissions ...", done, total)

    if failed:
        log.warning("Finished with %d/%d submissions permanently failed: %s",
                     len(failed), total, failed)
    return all_rows


# ─── Google Sheets helpers ────────────────────────────────────────────────────

def get_or_create_sheet(client: gspread.Client, spreadsheet_name: str) -> gspread.Worksheet:
    ss = client.open(spreadsheet_name)
    try:
        ws = ss.worksheet(WORKSHEET_NAME)
        log.info("Found existing sheet: '%s'", WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=WORKSHEET_NAME, rows=5000, cols=len(HEADERS) + 2)
        log.info("Created new sheet: '%s'", WORKSHEET_NAME)
    return ws


def setup_headers(ws: gspread.Worksheet):
    if ws.row_values(1) == HEADERS:
        return
    ws.update("A1", [HEADERS])
    sheet_id = ws.id
    ws.spreadsheet.batch_update({"requests": [
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

    ws.spreadsheet.batch_update({"requests": rules})
    log.info("Conditional formatting applied.")


def clear_sheet_body(ws: gspread.Worksheet):
    total_rows = ws.row_count
    if total_rows > 1:
        ws.batch_clear([f"A2:{gspread.utils.rowcol_to_a1(total_rows, len(HEADERS))}"])
    log.info("Cleared existing data rows (kept header).")


def write_all_rows(ws: gspread.Worksheet, rows: list[list]):
    if not rows:
        log.info("No rows to write.")
        return
    ws.append_rows(rows, value_input_option="USER_ENTERED")
    log.info("Wrote %d fresh rows.", len(rows))


# ─── Core sync (shared by CLI + Cloud Function entry points) ─────────────────

def run_sync() -> dict:
    # API_KEY / FORM_ID / SPREADSHEET_NAME / WORKSHEET_NAME already raise a
    # clear KeyError at import time (via os.environ[...]) if unset, so no
    # placeholder-value checks are needed for them here.
    if not os.path.exists(GOOGLE_CREDS_FILE):
        raise RuntimeError(f"Creds file not found: {GOOGLE_CREDS_FILE}")

    log.info("Connecting to Google Sheets ...")
    creds = Credentials.from_service_account_file(GOOGLE_CREDS_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    ws = get_or_create_sheet(client, SPREADSHEET_NAME)

    is_new = ws.row_values(1) != HEADERS
    setup_headers(ws)
    if is_new:
        setup_conditional_formatting(ws)

    clear_sheet_body(ws)

    submissions = fetch_all_submissions(FORM_ID)
    log.info("Fetching threads for %d submissions with %d workers ...", len(submissions), MAX_WORKERS)
    all_rows = process_all_submissions(submissions)

    log.info("Writing %d rows ...", len(all_rows))
    write_all_rows(ws, all_rows)

    log.info("Done! Sheet: %s", SPREADSHEET_NAME)
    return {"status": "ok", "rows_written": len(all_rows)}


def sync_http(request):
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