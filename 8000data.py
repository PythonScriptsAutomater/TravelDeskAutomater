import os
import time
import requests
import gspread
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from oauth2client.service_account import ServiceAccountCredentials
from jotform import JotformAPIClient
from http.client import IncompleteRead
from requests.exceptions import ConnectionError as RequestsConnectionError

# ---------------- CONFIG ----------------
API_KEY          = os.environ['API_KEY']
FORM_ID          = os.environ['FORM_ID']
SPREADSHEET_NAME = os.environ['SPREADSHEET_NAME']
WORKSHEET_NAME   = os.environ['WORKSHEET_NAME_TDR']

PAGE_SIZE           = int(os.environ.get('PAGE_SIZE', 200))
SLEEP_BETWEEN_CALLS = int(os.environ.get('SLEEP_BETWEEN_CALLS', 2))
WRITE_BATCH_SIZE    = int(os.environ.get('WRITE_BATCH_SIZE', 500))
THREAD_WORKERS      = int(os.environ.get('THREAD_WORKERS', 10))
CREDS_FILE          = os.environ.get('CREDS_FILE', 'credentials.json')
BASE_URL            = 'https://pw.jotform.com/API'

TOTAL_LIMIT         = int(os.environ.get('TOTAL_LIMIT', 8000))
PAGE_SIZE           = int(os.environ.get('PAGE_SIZE', 200))
SLEEP_BETWEEN_CALLS = int(os.environ.get('SLEEP_BETWEEN_CALLS', 2))
WRITE_BATCH_SIZE    = int(os.environ.get('WRITE_BATCH_SIZE', 500))
THREAD_WORKERS      = int(os.environ.get('THREAD_WORKERS', 10))
CREDS_FILE          = os.environ.get('CREDS_FILE', 'admin-analytics-423707-08e7889d4394.json')
BASE_URL            = 'https://pw.jotform.com/API'

# ---------------- HTTP SESSION ----------------
retry_strategy = Retry(
    total=4,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)
adapter = HTTPAdapter(
    max_retries=retry_strategy,
    pool_connections=THREAD_WORKERS,
    pool_maxsize=THREAD_WORKERS * 2,
)
session = requests.Session()
session.mount("https://", adapter)
session.mount("http://", adapter)


# ---------------- HELPERS ----------------
def col_letter(n):
    result = ''
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def append_with_retry(sheet, batch, retries=3):
    for attempt in range(retries):
        try:
            sheet.append_rows(batch, value_input_option='USER_ENTERED')
            return
        except (RequestsConnectionError, Exception) as e:
            if attempt < retries - 1:
                wait = 5 * (attempt + 1)
                print(f"⚠️  Write failed (attempt {attempt + 1}/{retries}), retrying in {wait}s... [{e}]")
                time.sleep(wait)
            else:
                raise


def get_last_approval_date(thread_data):
    """
    Keeps BOTH detection mechanisms (old + new), since either event shape
    can occur depending on form/workflow config:

      OLD: actionType in (COMPLETE/APPROVE/APPROVED/APPROVE_REJECT) with
           title in ('Approval','Approval '); or MAIL with reason==COMPLETE
           and the same title check.
      NEW: actionType == APPROVE_REJECT with title == 'Approval' (the
           actual approve/reject click); or actionType == COMPLETE with
           title == 'Approval' (the approval step finishing).

    Returns the LAST (latest) matching timestamp across both mechanisms,
    deduplicated — correctly handles multi-level approvals.
    """
    approval_events = []
    for event in thread_data.get('content', []):
        action_type = event.get('actionType', '')
        details     = event.get('actionDetails', {})
        raw_title   = details.get('title', '')
        title       = raw_title.strip() if isinstance(raw_title, str) else ''

        is_approval_complete_old = (
            action_type in ('COMPLETE', 'APPROVE', 'APPROVED', 'APPROVE_REJECT')
            and details.get('title') in ('Approval', 'Approval ')
        )
        is_approval_mail_complete_old = (
            action_type == 'MAIL'
            and details.get('reason') == 'COMPLETE'
            and details.get('title') in ('Approval', 'Approval ')
        )
        is_approve_click_new     = action_type == 'APPROVE_REJECT' and title == 'Approval'
        is_approval_complete_new = action_type == 'COMPLETE' and title == 'Approval'

        if (is_approval_complete_old or is_approval_mail_complete_old
                or is_approve_click_new or is_approval_complete_new):
            ts = event.get('timestamp', '')
            if ts and ts not in approval_events:
                approval_events.append(ts)

    if not approval_events:
        return ''
    approval_events.sort()
    return approval_events[-1]


def fetch_thread_safe(sub_id, retries=4):
    url = f"{BASE_URL}/submission/{sub_id}/thread"
    for attempt in range(retries):
        try:
            resp = session.get(url, params={'apiKey': API_KEY}, timeout=(10, 20))
            resp.raise_for_status()
            return sub_id, get_last_approval_date(resp.json())
        except requests.exceptions.Timeout:
            time.sleep(3 * (attempt + 1))
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                time.sleep(15 * (attempt + 1))
            else:
                break
        except Exception:
            time.sleep(3 * (attempt + 1))
    return sub_id, ''


def fetch_threads_batch(sub_ids):
    results = {}
    with ThreadPoolExecutor(max_workers=THREAD_WORKERS) as executor:
        futures = {executor.submit(fetch_thread_safe, sid): sid for sid in sub_ids}
        for future in as_completed(futures):
            sid, approval_date = future.result()
            results[sid] = approval_date
    return results


def get_submissions_page(form_id, limit, offset, retries=5):
    """Fetch one page newest-first (DESC) — we want latest N, then reverse."""
    url = f"{BASE_URL}/form/{form_id}/submissions"
    params = {
        'apiKey':            API_KEY,
        'limit':              limit,
        'offset':             offset,
        'orderby':            'created_at',
        'direction':          'DESC',
        'addworkflowstatus':  1,
    }
    for attempt in range(retries):
        try:
            resp = session.get(url, params=params, timeout=(10, 30))
            resp.raise_for_status()
            data    = resp.json()
            content = data.get('content', data)
            return content if isinstance(content, list) else []
        except (IncompleteRead, ConnectionAbortedError, ConnectionResetError, OSError) as e:
            wait = 5 * (attempt + 1)
            print(f"⚠️  Fetch failed (attempt {attempt + 1}/{retries}), retrying in {wait}s... [{type(e).__name__}]")
            time.sleep(wait)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait = 15 * (attempt + 1)
                print(f"⚠️  Rate limited, retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise
        except Exception as e:
            print(f"⚠️  Unexpected error: {e}")
            raise
    raise Exception(f"❌ Failed to fetch submissions at offset {offset} after {retries} attempts")


def get_questions_with_retry(form_id, retries=5):
    """Fetch /form/{id}/questions with the same retry/backoff treatment as
    every other API call here — the original script let a single failed
    request crash the whole script before the main retry-protected loop
    even started."""
    url = f"{BASE_URL}/form/{form_id}/questions"
    for attempt in range(retries):
        try:
            resp = session.get(url, params={'apiKey': API_KEY}, timeout=(10, 20))
            resp.raise_for_status()
            return resp.json()
        except (IncompleteRead, ConnectionAbortedError, ConnectionResetError, OSError) as e:
            wait = 5 * (attempt + 1)
            print(f"⚠️  Questions fetch failed (attempt {attempt + 1}/{retries}), retrying in {wait}s... [{type(e).__name__}]")
            time.sleep(wait)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait = 15 * (attempt + 1)
                print(f"⚠️  Rate limited, retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise
        except Exception as e:
            print(f"⚠️  Unexpected error fetching questions: {e}")
            raise
    raise Exception(f"❌ Failed to fetch form questions after {retries} attempts")


# ── Spent columns to sum for Total Spent ──
SPENT_COLS = [
    'Flight Spent',
    'Stay Spent',
    'Train Spent',
    'Other Spent(Bus,Cab,Tempo etc)',
]
TOTAL_COL = 'Total Spent'


def build_option_lookup(q_content):
    """
    Some fields (e.g. control_radio with a Kanban-style options_array, like
    'Overall Status') return answers as a raw option KEY wrapped in braces,
    e.g. answer == "{hsgb3xk1njm}", instead of the human-readable value
    ("Done"). options_array is a JSON-encoded string of
    {key: {"key":..., "value": "Done", ...}, ...}.

    This builds {qid: {key: value}} so answers can be resolved to their
    display text before being written to the sheet.
    """
    lookup = {}
    for qid, meta in q_content.items():
        raw_options = meta.get('options_array')
        if not raw_options:
            continue
        try:
            options = json.loads(raw_options)
        except (TypeError, ValueError):
            continue
        key_to_value = {}
        for opt_key, opt_meta in options.items():
            if isinstance(opt_meta, dict) and 'value' in opt_meta:
                key_to_value[opt_key] = opt_meta['value']
        if key_to_value:
            lookup[qid] = key_to_value
    return lookup


def resolve_answer(qid, ans, option_lookup):
    """
    Resolve a single answer value through option_lookup if it looks like a
    keyed reference, e.g. "{hsgb3xk1njm}" -> "Done". Falls back to the raw
    value unchanged if there's no match (so normal text/number answers are
    untouched).
    """
    key_map = option_lookup.get(qid)
    if not key_map:
        return ans
    if isinstance(ans, str):
        stripped = ans.strip()
        if stripped.startswith('{') and stripped.endswith('}'):
            raw_key = stripped[1:-1]
            return key_map.get(raw_key, ans)
        # Some forms may return the bare key without braces
        if stripped in key_map:
            return key_map[stripped]
    return ans


def sub_to_row(sub, approval_date, headers, header_to_qid, option_lookup):
    sub_id = sub.get('id', '')
    row_data = {
        'Submission ID':      sub_id,
        'Submission Date':    sub.get('created_at', ''),
        'Last Update Date':   sub.get('updated_at', ''),
        'Approval Status':    (
            sub.get('workflowStatus')
            or sub.get('workflow_status')
            or sub.get('status')
            or ''
        ),
        'Last Approval Date': approval_date,
        UNIQUE_ID_COL:        sub_id,  # fallback; real value comes from answers below if present
    }
    answers = sub.get('answers', {})
    for header, qid in header_to_qid.items():
        if qid in answers and 'answer' in answers[qid]:
            ans = answers[qid]['answer']
            if isinstance(ans, list):
                resolved = [resolve_answer(qid, a, option_lookup) for a in ans]
                row_data[header] = '\n'.join(map(str, resolved))
            else:
                resolved = resolve_answer(qid, ans, option_lookup)
                row_data[header] = str(resolved)
        else:
            row_data[header] = ''

    # ── Auto-calculate Total Spent ──
    total = 0
    for col in SPENT_COLS:
        try:
            total += float(row_data.get(col) or 0)
        except (ValueError, TypeError):
            pass
    row_data[TOTAL_COL] = total if total > 0 else ''

    return [row_data.get(h, '') for h in headers]


# ---------------- JOTFORM CLIENT ----------------
jotform = JotformAPIClient(API_KEY)
jotform.set_baseurl('https://pw.jotform.com/API/')

# ---------------- GOOGLE SHEETS ----------------
scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]
creds  = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
client = gspread.authorize(creds)

try:
    sheet = client.open(SPREADSHEET_NAME).worksheet(WORKSHEET_NAME)
    print(f"✅ Opened: '{SPREADSHEET_NAME}' → '{WORKSHEET_NAME}'")
except gspread.exceptions.SpreadsheetNotFound:
    raise Exception(f"❌ SpreadsheetNotFound: '{SPREADSHEET_NAME}'")
except gspread.exceptions.WorksheetNotFound:
    raise Exception(f"❌ WorksheetNotFound: '{WORKSHEET_NAME}'")

# ---------------- SAFETY CHECK — abort if sheet already has data ----------------
existing_headers = sheet.row_values(1)
if not existing_headers:
    raise Exception("❌ Header row missing in destination sheet")

all_rows = sheet.get_all_values()
if len(all_rows) > 1:
    raise Exception(
        f"❌ Sheet already has {len(all_rows) - 1} data rows. "
        f"This script is for INITIAL LOAD only. "
        f"Use jotform_incremental_append.py for subsequent runs."
    )

print(f"✅ Sheet is empty — proceeding with initial load of latest {TOTAL_LIMIT} submissions.")

UNIQUE_ID_COL     = 'Unique ID'
APPROVAL_DATE_COL = 'Last Approval Date'

# ---------------- DISCOVER JOTFORM FIELDS via /questions ----------------
print("🔍 Discovering form fields via /questions endpoint...")

SKIP_TYPES = {
    'control_head', 'control_button', 'control_reference',
    'control_pagebreak',
}

q_data = get_questions_with_retry(FORM_ID)
option_lookup = build_option_lookup(q_data['content'])

header_to_qid = {}
new_headers   = []

for qid, meta in q_data['content'].items():
    if meta.get('type') in SKIP_TYPES:
        continue
    col_name = meta.get('text', f'Q_{qid}')
    if not col_name:
        continue
    if col_name in existing_headers:
        header_to_qid[col_name] = qid
    else:
        new_headers.append(col_name)
        header_to_qid[col_name] = qid

# ── FALLBACK DISCOVERY: some fields (e.g. 'Overall Status', a Kanban-style
#    control_radio) are submission-only metadata and never appear in
#    /form/{id}/questions at all — only inside an actual submission's
#    'answers' block. Without this, header_to_qid never gets an entry for
#    them, so they're silently skipped in every row (always written as '').
#    Fix: pull one sample submission and merge in any answer fields whose
#    'text' wasn't already discovered above. Also merge their
#    options_array into option_lookup so keyed answers (e.g. "{abc123}")
#    still resolve correctly. ──
sample_submissions = get_submissions_page(FORM_ID, limit=1, offset=0)
if sample_submissions:
    sample_answers = sample_submissions[0].get('answers', {})
    extra_meta = {
        qid: meta for qid, meta in sample_answers.items()
        if meta.get('type') not in SKIP_TYPES
    }
    extra_option_lookup = build_option_lookup(extra_meta)
    for qid, meta in extra_meta.items():
        col_name = meta.get('text', f'Q_{qid}')
        if not col_name or col_name in header_to_qid:
            continue  # already discovered via /questions
        if col_name in existing_headers:
            header_to_qid[col_name] = qid
        else:
            new_headers.append(col_name)
            header_to_qid[col_name] = qid
        if qid in extra_option_lookup and qid not in option_lookup:
            option_lookup[qid] = extra_option_lookup[qid]
    discovered_only_in_answers = sorted(set(header_to_qid) - {
        m.get('text', f'Q_{q}') for q, m in q_data['content'].items()
    })
    if discovered_only_in_answers:
        print(f"➕ Discovered submission-only fields (not in /questions): {discovered_only_in_answers}")

# Add Unique ID if missing (was previously declared but never wired up)
if UNIQUE_ID_COL not in existing_headers and UNIQUE_ID_COL not in new_headers:
    new_headers.append(UNIQUE_ID_COL)

# Add Last Approval Date if missing
if APPROVAL_DATE_COL not in existing_headers and APPROVAL_DATE_COL not in new_headers:
    new_headers.append(APPROVAL_DATE_COL)

# Add Total Spent if missing
if TOTAL_COL not in existing_headers and TOTAL_COL not in new_headers:
    new_headers.append(TOTAL_COL)

if new_headers:
    updated_headers = existing_headers + new_headers
    sheet.update([updated_headers], 'A1')
    existing_headers = updated_headers
    print(f"➕ Added new columns: {new_headers}")
else:
    print("✔ All columns already present")

# ---------------- FETCH LATEST (DESC) INTO MEMORY ----------------
print(f"🚀 Fetching latest {TOTAL_LIMIT} submissions (newest-first, will reverse before writing)...")

collected = []
offset    = 0

while len(collected) < TOTAL_LIMIT:
    remaining  = TOTAL_LIMIT - len(collected)
    page_limit = min(PAGE_SIZE, remaining)

    submissions = get_submissions_page(FORM_ID, limit=page_limit, offset=offset)
    if not submissions:
        print("✔ No more submissions from JotForm.")
        break

    collected.extend(submissions)
    offset += page_limit
    print(f"✔ Fetched {len(collected)} / {TOTAL_LIMIT}...")
    time.sleep(SLEEP_BETWEEN_CALLS)

print(f"\n📦 {len(collected)} submissions fetched. Reversing to oldest-first order...")
collected.reverse()

# ---------------- FETCH APPROVAL THREADS ----------------
print("🔗 Fetching approval threads...")
all_sub_ids    = [sub.get('id', '') for sub in collected]
approval_dates = fetch_threads_batch(all_sub_ids)
print("✔ Approval threads fetched.")

# ---------------- BUILD ORDERED ROWS (oldest first) ----------------
ordered_rows = [
    sub_to_row(sub, approval_dates.get(sub.get('id', ''), ''), existing_headers, header_to_qid, option_lookup)
    for sub in collected
]

# ---------------- WRITE IN ORDERED CHUNKS ----------------
total_written = 0
for i in range(0, len(ordered_rows), WRITE_BATCH_SIZE):
    chunk = ordered_rows[i : i + WRITE_BATCH_SIZE]
    append_with_retry(sheet, chunk)
    total_written += len(chunk)
    print(f"📝 Written {total_written} / {len(ordered_rows)} rows...")

print(f"\n✅ INITIAL LOAD DONE — {total_written} rows written oldest-first.")
print(f"   Last row is the most recent submission. Run jotform_incremental_append.py for future syncs.")