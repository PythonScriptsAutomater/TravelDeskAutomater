import os
import time
import requests
import gspread
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from oauth2client.service_account import ServiceAccountCredentials
from jotform import JotformAPIClient
from http.client import IncompleteRead
from requests.exceptions import ConnectionError as RequestsConnectionError

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

UNIQUE_ID_COL       = 'Unique ID'
SUBMISSION_ID_COL   = 'Submission ID'
SUBMISSION_DATE_COL = 'Submission Date'
LAST_UPDATE_COL     = 'Last Update Date'
APPROVAL_STATUS_COL = 'Approval Status'
APPROVAL_DATE_COL   = 'Last Approval Date'
TOTAL_COL           = 'Total Spent'

SKIP_TYPES = {
    'control_head',
    'control_button',
    'control_reference',
    'control_pagebreak',
}

SPENT_COLS = [
    'Flight Spent',
    'Stay Spent',
    'Train Spent',
    'Other Spent(Bus,Cab,Tempo etc)',
]

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
def append_with_retry(sheet, batch, retries=3):
    for attempt in range(retries):
        try:
            sheet.append_rows(batch, value_input_option='USER_ENTERED')
            return
        except (RequestsConnectionError, Exception) as e:
            if attempt < retries - 1:
                wait = 5 * (attempt + 1)
                print(f"⚠️ Write failed (attempt {attempt + 1}/{retries}), retrying in {wait}s... [{e}]")
                time.sleep(wait)
            else:
                raise


def get_last_approval_date(thread_data):
    approval_events = []
    for event in thread_data.get('content', []):
        action_type = event.get('actionType', '')
        details = event.get('actionDetails', {})
        raw_title = details.get('title', '')
        title = raw_title.strip() if isinstance(raw_title, str) else ''

        is_approval_complete_old = (
            action_type in ('COMPLETE', 'APPROVE', 'APPROVED', 'APPROVE_REJECT')
            and details.get('title') in ('Approval', 'Approval ')
        )
        is_approval_mail_complete_old = (
            action_type == 'MAIL'
            and details.get('reason') == 'COMPLETE'
            and details.get('title') in ('Approval', 'Approval ')
        )
        is_approve_click_new = action_type == 'APPROVE_REJECT' and title == 'Approval'
        is_approval_complete_new = action_type == 'COMPLETE' and title == 'Approval'

        if (
            is_approval_complete_old
            or is_approval_mail_complete_old
            or is_approve_click_new
            or is_approval_complete_new
        ):
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
    if not sub_ids:
        return results

    with ThreadPoolExecutor(max_workers=THREAD_WORKERS) as executor:
        futures = {executor.submit(fetch_thread_safe, sid): sid for sid in sub_ids}
        for future in as_completed(futures):
            sid, approval_date = future.result()
            results[sid] = approval_date
    return results


def get_submissions_with_retry(form_id, limit, offset, retries=5):
    url = f"{BASE_URL}/form/{form_id}/submissions"
    params = {
        'apiKey': API_KEY,
        'limit': limit,
        'offset': offset,
        'orderby': 'created_at',
        'direction': 'DESC',   # newest first
        'addworkflowstatus': 1,
    }

    for attempt in range(retries):
        try:
            resp = session.get(url, params=params, timeout=(10, 30))
            resp.raise_for_status()
            data = resp.json()
            content = data.get('content', data)
            return content if isinstance(content, list) else []
        except (IncompleteRead, ConnectionAbortedError, ConnectionResetError, OSError) as e:
            wait = 5 * (attempt + 1)
            print(f"⚠️ Submission fetch failed (attempt {attempt + 1}/{retries}), retrying in {wait}s... [{type(e).__name__}]")
            time.sleep(wait)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait = 15 * (attempt + 1)
                print(f"⚠️ Rate limited, retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise
        except Exception as e:
            print(f"⚠️ Unexpected error fetching submissions: {e}")
            raise

    raise Exception(f"❌ Failed to fetch submissions at offset {offset} after {retries} attempts")


def get_questions_with_retry(form_id, retries=5):
    url = f"{BASE_URL}/form/{form_id}/questions"
    for attempt in range(retries):
        try:
            resp = session.get(url, params={'apiKey': API_KEY}, timeout=(10, 20))
            resp.raise_for_status()
            return resp.json()
        except (IncompleteRead, ConnectionAbortedError, ConnectionResetError, OSError) as e:
            wait = 5 * (attempt + 1)
            print(f"⚠️ Questions fetch failed (attempt {attempt + 1}/{retries}), retrying in {wait}s... [{type(e).__name__}]")
            time.sleep(wait)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait = 15 * (attempt + 1)
                print(f"⚠️ Rate limited, retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise
        except Exception as e:
            print(f"⚠️ Unexpected error fetching questions: {e}")
            raise

    raise Exception(f"❌ Failed to fetch form questions after {retries} attempts")


def build_option_lookup(q_content):
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
    key_map = option_lookup.get(qid)
    if not key_map:
        return ans

    if isinstance(ans, str):
        stripped = ans.strip()
        if stripped.startswith('{') and stripped.endswith('}'):
            raw_key = stripped[1:-1]
            return key_map.get(raw_key, ans)
        if stripped in key_map:
            return key_map.get(stripped, ans)

    return ans


def safe_str(v):
    if v is None:
        return ''
    return str(v)


def sub_to_row(sub, approval_date, headers, header_to_qid, option_lookup):
    sub_id = safe_str(sub.get('id', ''))

    row_data = {
        SUBMISSION_ID_COL: sub_id,
        SUBMISSION_DATE_COL: safe_str(sub.get('created_at', '')),
        LAST_UPDATE_COL: safe_str(sub.get('updated_at', '')),
        APPROVAL_STATUS_COL: safe_str(
            sub.get('workflowStatus')
            or sub.get('workflow_status')
            or sub.get('status')
            or ''
        ),
        APPROVAL_DATE_COL: safe_str(approval_date),
        UNIQUE_ID_COL: sub_id,  # fallback
    }

    answers = sub.get('answers', {})
    for header, qid in header_to_qid.items():
        if qid in answers and 'answer' in answers[qid]:
            ans = answers[qid]['answer']
            if isinstance(ans, list):
                resolved = [resolve_answer(qid, a, option_lookup) for a in ans]
                row_data[header] = '\n'.join(map(safe_str, resolved))
            else:
                row_data[header] = safe_str(resolve_answer(qid, ans, option_lookup))
        else:
            if header not in row_data:
                row_data[header] = ''

    if not safe_str(row_data.get(UNIQUE_ID_COL, '')).strip():
        row_data[UNIQUE_ID_COL] = sub_id

    total = 0
    has_any_spent = False
    for col in SPENT_COLS:
        raw_val = row_data.get(col, '')
        if safe_str(raw_val).strip() != '':
            has_any_spent = True
        try:
            total += float(raw_val or 0)
        except (ValueError, TypeError):
            pass

    row_data[TOTAL_COL] = total if has_any_spent else ''

    return [row_data.get(h, '') for h in headers]


def get_existing_submission_ids(sheet, headers):
    existing_ids = set()

    if SUBMISSION_ID_COL not in headers:
        return existing_ids

    sid_idx = headers.index(SUBMISSION_ID_COL)
    all_rows = sheet.get_all_values()

    for row in all_rows[1:]:
        if len(row) > sid_idx and row[sid_idx].strip():
            existing_ids.add(row[sid_idx].strip())

    return existing_ids


# ---------------- JOTFORM CLIENT ----------------
jotform = JotformAPIClient(API_KEY)
jotform.set_baseurl('https://pw.jotform.com/API/')

# ---------------- GOOGLE SHEETS ----------------
scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]
creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
client = gspread.authorize(creds)

try:
    sheet = client.open(SPREADSHEET_NAME).worksheet(WORKSHEET_NAME)
    print(f"✅ Opened: '{SPREADSHEET_NAME}' → '{WORKSHEET_NAME}'")
except gspread.exceptions.SpreadsheetNotFound:
    raise Exception(f"❌ SpreadsheetNotFound: '{SPREADSHEET_NAME}'")
except gspread.exceptions.WorksheetNotFound:
    raise Exception(f"❌ WorksheetNotFound: '{WORKSHEET_NAME}'")

# ---------------- READ EXISTING HEADERS ----------------
existing_headers = sheet.row_values(1)
if not existing_headers:
    raise Exception("❌ Header row missing in destination sheet")

# ---------------- DISCOVER JOTFORM FIELDS ----------------
print("🔍 Discovering form fields via /questions endpoint...")
q_data = get_questions_with_retry(FORM_ID)
q_content = q_data.get('content', {})
option_lookup = build_option_lookup(q_content)

header_to_qid = {}
new_headers = []

for qid, meta in q_content.items():
    if meta.get('type') in SKIP_TYPES:
        continue

    col_name = meta.get('text', f'Q_{qid}')
    if not col_name:
        continue

    header_to_qid[col_name] = qid
    if col_name not in existing_headers and col_name not in new_headers:
        new_headers.append(col_name)

# Fallback: some fields appear only in answers, not /questions
sample_submissions = get_submissions_with_retry(FORM_ID, limit=1, offset=0)
if sample_submissions:
    sample_answers = sample_submissions[0].get('answers', {})
    extra_meta = {
        qid: meta for qid, meta in sample_answers.items()
        if meta.get('type') not in SKIP_TYPES
    }
    extra_option_lookup = build_option_lookup(extra_meta)

    for qid, meta in extra_meta.items():
        col_name = meta.get('text', f'Q_{qid}')
        if not col_name:
            continue

        if col_name not in header_to_qid:
            header_to_qid[col_name] = qid

        if col_name not in existing_headers and col_name not in new_headers:
            new_headers.append(col_name)

        if qid in extra_option_lookup and qid not in option_lookup:
            option_lookup[qid] = extra_option_lookup[qid]

# Ensure system columns exist
required_headers = [
    SUBMISSION_ID_COL,
    SUBMISSION_DATE_COL,
    LAST_UPDATE_COL,
    APPROVAL_STATUS_COL,
    UNIQUE_ID_COL,
    APPROVAL_DATE_COL,
    TOTAL_COL,
]

for col in required_headers:
    if col not in existing_headers and col not in new_headers:
        new_headers.append(col)

if new_headers:
    updated_headers = existing_headers + new_headers
    sheet.update([updated_headers], 'A1')
    existing_headers = updated_headers
    print(f"➕ Added new columns: {new_headers}")
else:
    print("✔ All columns already present")

# ---------------- FIND CUTOFF FROM LAST DATA ROW ----------------
all_rows = sheet.get_all_values()
data_rows = all_rows[1:]

if SUBMISSION_DATE_COL not in existing_headers:
    raise Exception(f"❌ Column '{SUBMISSION_DATE_COL}' not found in sheet headers")

date_col_index = existing_headers.index(SUBMISSION_DATE_COL)
uid_index = existing_headers.index(UNIQUE_ID_COL) if UNIQUE_ID_COL in existing_headers else None
sid_index = existing_headers.index(SUBMISSION_ID_COL) if SUBMISSION_ID_COL in existing_headers else None

last_cutoff_dt = None
last_row_id = None

if data_rows:
    for row in reversed(data_rows):
        date_val = row[date_col_index].strip() if len(row) > date_col_index else ''
        if date_val:
            last_cutoff_dt = date_val

            if uid_index is not None and len(row) > uid_index and row[uid_index].strip():
                last_row_id = row[uid_index].strip()
            elif sid_index is not None and len(row) > sid_index and row[sid_index].strip():
                last_row_id = row[sid_index].strip()
            else:
                last_row_id = '(unknown)'
            break

if last_cutoff_dt:
    print(f"📌 Last existing row: ID={last_row_id}, Submission Date={last_cutoff_dt}")
    print(f"🔎 Will only append submissions with created_at > '{last_cutoff_dt}'")
else:
    print("⚠️ No existing data rows found — will fetch all submissions from scratch")

# Optional dedupe guard
existing_submission_ids = get_existing_submission_ids(sheet, existing_headers)
print(f"🧾 Loaded {len(existing_submission_ids)} existing Submission IDs for dedupe check.")

# ---------------- FETCH NEW SUBMISSIONS ----------------
offset = 0
fetched_total = 0
collected_new_submissions = []

print(f"🚀 Fetching new submissions since '{last_cutoff_dt}' (workers={THREAD_WORKERS})...")

while True:
    try:
        submissions = get_submissions_with_retry(FORM_ID, limit=PAGE_SIZE, offset=offset)

        if not submissions:
            print("✔ No more submissions from JotForm.")
            break

        fetched_total += len(submissions)
        page_new = []
        hit_old_row = False

        for sub in submissions:
            sub_id = safe_str(sub.get('id', ''))
            created_at = safe_str(sub.get('created_at', ''))

            is_new_by_date = (last_cutoff_dt is None) or (created_at > last_cutoff_dt)
            is_not_duplicate = sub_id not in existing_submission_ids

            if is_new_by_date and is_not_duplicate:
                page_new.append(sub)
            else:
                if last_cutoff_dt is not None and created_at <= last_cutoff_dt:
                    hit_old_row = True
                    break

        if not page_new and offset == 0:
            print("✔ Already up to date — first page has no submissions newer than cutoff.")
            break

        if page_new:
            collected_new_submissions.extend(page_new)
            print(f"✔ Scanned {fetched_total} submissions total, found {len(collected_new_submissions)} new rows so far...")

        if hit_old_row:
            print(f"🛑 Reached cutoff boundary on page at offset {offset} — stopping.")
            break

        offset += PAGE_SIZE
        time.sleep(SLEEP_BETWEEN_CALLS)

    except (IncompleteRead, ConnectionAbortedError, ConnectionResetError, OSError) as e:
        print(f"⚠️ Connection error: {type(e).__name__}, retrying after 5s...")
        time.sleep(5)
        continue

# JotForm returned newest-first; reverse before writing to preserve sheet order
collected_new_submissions.reverse()

# ---------------- FETCH APPROVAL THREADS ----------------
approval_dates = fetch_threads_batch([safe_str(sub.get('id', '')) for sub in collected_new_submissions])

# ---------------- BUILD & WRITE ROWS ----------------
rows_buffer = []
total_written = 0

for sub in collected_new_submissions:
    sub_id = safe_str(sub.get('id', ''))
    row = sub_to_row(
        sub,
        approval_dates.get(sub_id, ''),
        existing_headers,
        header_to_qid,
        option_lookup,
    )
    rows_buffer.append(row)

    if len(rows_buffer) >= WRITE_BATCH_SIZE:
        append_with_retry(sheet, rows_buffer)
        total_written += len(rows_buffer)
        print(f"📝 Written {total_written} new rows so far...")
        rows_buffer = []

if rows_buffer:
    append_with_retry(sheet, rows_buffer)
    total_written += len(rows_buffer)

print(f"✅ DONE — {total_written} new rows appended (scanned {fetched_total} total submissions)")