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

TOTAL_LIMIT         = int(os.environ.get('TOTAL_LIMIT', 8000))
PAGE_SIZE           = int(os.environ.get('PAGE_SIZE', 200))
SLEEP_BETWEEN_CALLS = int(os.environ.get('SLEEP_BETWEEN_CALLS', 2))
WRITE_BATCH_SIZE    = int(os.environ.get('WRITE_BATCH_SIZE', 500))
THREAD_WORKERS      = int(os.environ.get('THREAD_WORKERS', 10))
CREDS_FILE          = os.environ.get('CREDS_FILE', 'admin-analytics-423707-08e7889d4394.json')
DEBUG_THREAD        = os.environ.get('DEBUG_THREAD', 'false').lower() == 'true'  # set true to debug missing approval dates
BASE_URL            = 'https://pw.jotform.com/API'

# ---------------- HTTP SESSION (shared across all threads) ----------------
# Reuses TCP connections + auto-retries on transient network errors
retry_strategy = Retry(
    total=4,
    backoff_factor=1,          # waits 1s, 2s, 4s, 8s between retries
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)
adapter = HTTPAdapter(
    max_retries=retry_strategy,
    pool_connections=THREAD_WORKERS,   # one connection pool per worker
    pool_maxsize=THREAD_WORKERS * 2,   # headroom for bursts
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


def get_last_approval_date(thread_data, debug_sub_id=None):
    approval_events = []
    content = thread_data.get('content', [])
    for event in content:
        action_type = event.get('actionType', '')
        details     = event.get('actionDetails', {})
        is_approval_complete = (
            action_type in ('COMPLETE', 'APPROVE', 'APPROVED', 'APPROVE_REJECT')
            and details.get('title') in ('Approval', 'Approval ')
        )
        is_approval_mail_complete = (
            action_type == 'MAIL'
            and details.get('reason') == 'COMPLETE'
            and details.get('title') in ('Approval', 'Approval ')
        )
        if is_approval_complete or is_approval_mail_complete:
            approval_events.append(event.get('timestamp', ''))

    # If nothing found but thread has events, dump them so we can see what fields exist
    if not approval_events and content and debug_sub_id:
        print(f"  🔍 DEBUG sub {debug_sub_id} — {len(content)} events, no approval match:")
        for e in content:
            print(f"      actionType={e.get('actionType')} | title={e.get('actionDetails',{}).get('title')} | reason={e.get('actionDetails',{}).get('reason')} | ts={e.get('timestamp')}")

    return approval_events[-1] if approval_events else ''


def fetch_thread_safe(sub_id, retries=4):
    """Fetch thread for one submission using the shared session. Returns (sub_id, approval_date)."""
    url = f"{BASE_URL}/submission/{sub_id}/thread"
    for attempt in range(retries):
        try:
            resp = session.get(url, params={'apiKey': API_KEY}, timeout=(10, 20))
            resp.raise_for_status()
            data = resp.json()
            return sub_id, get_last_approval_date(data, debug_sub_id=sub_id if DEBUG_THREAD else None)
        except requests.exceptions.Timeout:
            wait = 3 * (attempt + 1)
            print(f"⚠️  Timeout for {sub_id} (attempt {attempt+1}/{retries}), retrying in {wait}s...")
            time.sleep(wait)
        except requests.exceptions.HTTPError as e:
            # 429 = rate limited — back off hard
            if e.response is not None and e.response.status_code == 429:
                wait = 15 * (attempt + 1)
                print(f"⚠️  Rate limited (429), backing off {wait}s...")
                time.sleep(wait)
            else:
                print(f"⚠️  HTTP error for {sub_id}: {e}")
                break
        except Exception as e:
            wait = 3 * (attempt + 1)
            print(f"⚠️  Thread error for {sub_id} ({type(e).__name__}), retrying in {wait}s...")
            time.sleep(wait)
    return sub_id, '__NO_WORKFLOW__'


def fetch_threads_batch(sub_ids):
    results = {}
    with ThreadPoolExecutor(max_workers=THREAD_WORKERS) as executor:
        futures = {executor.submit(fetch_thread_safe, sid): sid for sid in sub_ids}
        for future in as_completed(futures):
            sid, approval_date = future.result()
            results[sid] = approval_date

    # Log how many threads actually returned an approval date vs empty
    hits   = sum(1 for v in results.values() if v and v != '__NO_WORKFLOW__')
    no_wf  = sum(1 for v in results.values() if v == '__NO_WORKFLOW__')
    pending = sum(1 for v in results.values() if not v)
    print(f"    ✔ Approved: {hits} | Pending/no date: {pending} | No workflow (old subs): {no_wf} | Total: {len(results)}")

    # Strip sentinel before returning
    results = {k: ('' if v == '__NO_WORKFLOW__' else v) for k, v in results.items()}

    # Only pause if real failures, not just pre-workflow submissions
    if hits == 0 and no_wf == 0 and pending == len(sub_ids) and len(sub_ids) > 5:
        print("    🛑 All thread fetches returned empty — possible rate limit, pausing 30s...")
        time.sleep(30)

    return results


# ---------------- JOTFORM ----------------
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

# ---------------- PRESERVE HEADERS ----------------
existing_headers = sheet.row_values(1)
if not existing_headers:
    raise Exception("❌ Header row missing in destination sheet")

# ---------------- SAFE CLEAR ----------------
row_count = sheet.row_count
col_count = sheet.col_count
if row_count > 1:
    sheet.batch_clear([f"A2:{col_letter(col_count)}{row_count}"])
print("🧹 Old data cleared, header preserved")

# ---------------- DISCOVER JOTFORM FIELDS ----------------
print("🔍 Discovering form fields...")
first_batch = jotform.get_form_submissions(FORM_ID, limit=1, offset=0)
if not first_batch:
    raise Exception("❌ No submissions found for this form")

answers_meta  = first_batch[0].get('answers', {})
header_to_qid = {}
new_headers   = []

for qid, meta in answers_meta.items():
    col_name = meta.get('text', f'Q_{qid}')
    if col_name in existing_headers:
        header_to_qid[col_name] = qid
    else:
        new_headers.append(col_name)
        header_to_qid[col_name] = qid

APPROVAL_DATE_COL = 'Last Approval Date'
if APPROVAL_DATE_COL not in existing_headers:
    new_headers.append(APPROVAL_DATE_COL)

if new_headers:
    updated_headers = existing_headers + new_headers
    sheet.update([updated_headers], 'A1')
    existing_headers = updated_headers
    print(f"➕ Added new columns: {new_headers}")
else:
    print("✔ All columns already present")

# ---------------- FETCH & WRITE ----------------
offset        = 0
fetched       = 0
rows_buffer   = []
total_written = 0

print(f"🚀 Fetching submissions (workers={THREAD_WORKERS})...")

def get_submissions_with_retry(form_id, limit, offset, retries=5):
    """Fetch a page of submissions, retrying on any connection error."""
    for attempt in range(retries):
        try:
            return jotform.get_form_submissions(form_id, limit=limit, offset=offset)
        except (IncompleteRead, ConnectionAbortedError, ConnectionResetError, OSError) as e:
            wait = 5 * (attempt + 1)
            print(f"⚠️  Submission fetch failed (attempt {attempt + 1}/{retries}), retrying in {wait}s... [{type(e).__name__}]")
            time.sleep(wait)
        except Exception as e:
            print(f"⚠️  Unexpected error fetching submissions: {e}")
            raise
    raise Exception(f"❌ Failed to fetch submissions at offset {offset} after {retries} attempts")


while fetched < TOTAL_LIMIT:
    try:
        submissions = get_submissions_with_retry(FORM_ID, limit=PAGE_SIZE, offset=offset)

        if not submissions:
            print("✔ No more submissions.")
            break

        sub_ids = [sub.get('id', '') for sub in submissions]
        print(f"  ⚡ Fetching {len(sub_ids)} threads concurrently...")
        approval_dates = fetch_threads_batch(sub_ids)

        for sub in submissions:
            sub_id = sub.get('id', '')
            row_data = {
                'Submission ID':    sub_id,
                'Submission Date':  sub.get('created_at', ''),
                'Last Update Date': sub.get('updated_at', ''),
                'Approval Status':  (
                    sub.get('workflowStatus')
                    or sub.get('workflow_status')
                    or sub.get('status')
                    or ''
                ),
                APPROVAL_DATE_COL:  approval_dates.get(sub_id, ''),
            }

            answers = sub.get('answers', {})
            for header, qid in header_to_qid.items():
                if qid in answers and 'answer' in answers[qid]:
                    ans = answers[qid]['answer']
                    row_data[header] = (
                        '\n'.join(map(str, ans)) if isinstance(ans, list) else str(ans)
                    )
                else:
                    row_data[header] = ''

            rows_buffer.append([row_data.get(h, '') for h in existing_headers])
            fetched += 1
            if fetched >= TOTAL_LIMIT:
                break

        if len(rows_buffer) >= WRITE_BATCH_SIZE:
            append_with_retry(sheet, rows_buffer)
            total_written += len(rows_buffer)
            print(f"📝 Written {total_written} rows so far...")
            rows_buffer = []

        offset += PAGE_SIZE
        print(f"✔ Pulled {fetched} submissions so far...")
        time.sleep(SLEEP_BETWEEN_CALLS)

    except (IncompleteRead, ConnectionAbortedError, ConnectionResetError, OSError) as e:
        print(f"⚠️  Connection error in main loop: {type(e).__name__}, retrying after 5s...")
        time.sleep(5)
        continue

# ---------------- FLUSH REMAINING ----------------
if rows_buffer:
    append_with_retry(sheet, rows_buffer)
    total_written += len(rows_buffer)

print(f"✅ DONE — {total_written} rows written successfully")