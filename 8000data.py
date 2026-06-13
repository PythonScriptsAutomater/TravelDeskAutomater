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
    approval_events = []
    for event in thread_data.get('content', []):
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
    return approval_events[-1] if approval_events else ''


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
    """Fetch one page newest-first (DESC) — we want latest 8000, then reverse."""
    url = f"{BASE_URL}/form/{form_id}/submissions"
    params = {
        'apiKey':    API_KEY,
        'limit':     limit,
        'offset':    offset,
        'orderby':   'created_at',
        'direction': 'DESC',
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


def sub_to_row(sub, approval_date, headers, header_to_qid):
    row_data = {
        'Submission Date':    sub.get('created_at', ''),
        'Last Update Date':   sub.get('updated_at', ''),
        'Approval Status':    (
            sub.get('workflowStatus')
            or sub.get('workflow_status')
            or sub.get('status')
            or ''
        ),
        'Last Approval Date': approval_date,
    }
    answers = sub.get('answers', {})
    for header, qid in header_to_qid.items():
        if qid in answers and 'answer' in answers[qid]:
            ans = answers[qid]['answer']
            row_data[header] = '\n'.join(map(str, ans)) if isinstance(ans, list) else str(ans)
        else:
            row_data[header] = ''
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

# ---------------- DISCOVER JOTFORM FIELDS ----------------
print("🔍 Discovering form fields...")
first_batch = get_submissions_page(FORM_ID, limit=1, offset=0)
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

if APPROVAL_DATE_COL not in existing_headers:
    new_headers.append(APPROVAL_DATE_COL)

if new_headers:
    updated_headers = existing_headers + new_headers
    sheet.update([updated_headers], 'A1')
    existing_headers = updated_headers
    print(f"➕ Added new columns: {new_headers}")
else:
    print("✔ All columns already present")

# ---------------- FETCH LATEST 8000 (DESC) INTO MEMORY ----------------
# We fetch newest-first (DESC) to get the latest 8000, then reverse the
# entire collected list before writing so oldest lands on top in the sheet.

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

# Reverse so index 0 = oldest, index -1 = newest
collected.reverse()

# ---------------- FETCH APPROVAL THREADS ----------------
print("🔗 Fetching approval threads...")
all_sub_ids    = [sub.get('id', '') for sub in collected]
approval_dates = fetch_threads_batch(all_sub_ids)
print("✔ Approval threads fetched.")

# ---------------- BUILD ORDERED ROWS (oldest first) ----------------
ordered_rows = [
    sub_to_row(sub, approval_dates.get(sub.get('id', ''), ''), existing_headers, header_to_qid)
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