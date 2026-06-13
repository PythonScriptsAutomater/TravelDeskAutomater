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

# ── CONFIG ───────────────────────────────────────────────────────────────────

# ── CONFIG ───────────────────────────────────────────────────────────────────
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

# ── HTTP SESSION ──────────────────────────────────────────────────────────────
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


# ── HELPERS ───────────────────────────────────────────────────────────────────
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


def get_answer_value(answers, qid):
    """Extract a clean string value from a JotForm answers dict entry."""
    if qid not in answers or 'answer' not in answers[qid]:
        return ''
    ans = answers[qid]['answer']
    return '\n'.join(map(str, ans)) if isinstance(ans, list) else str(ans)


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
        row_data[header] = get_answer_value(answers, qid)
    return [row_data.get(h, '') for h in headers]


# ── JOTFORM CLIENT ────────────────────────────────────────────────────────────
jotform = JotformAPIClient(API_KEY)
jotform.set_baseurl('https://pw.jotform.com/API/')

# ── GOOGLE SHEETS ─────────────────────────────────────────────────────────────
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

# ── READ EXISTING HEADERS & DATA ──────────────────────────────────────────────
existing_headers = sheet.row_values(1)
if not existing_headers:
    raise Exception("❌ Header row missing in destination sheet")

UNIQUE_ID_COL       = 'Unique ID'
SUBMISSION_DATE_COL = 'Submission Date'
APPROVAL_DATE_COL   = 'Last Approval Date'

date_index = existing_headers.index(SUBMISSION_DATE_COL) if SUBMISSION_DATE_COL in existing_headers else None
uid_index  = existing_headers.index(UNIQUE_ID_COL)       if UNIQUE_ID_COL in existing_headers       else None

if date_index is None:
    raise Exception(f"❌ Column '{SUBMISSION_DATE_COL}' not found in sheet headers")

all_rows  = sheet.get_all_values()
data_rows = all_rows[1:]
if not data_rows:
    raise Exception("❌ Sheet has no data rows. Run jotform_initial_load.py first.")

# ── DETERMINE CUTOFF ──────────────────────────────────────────────────────────
# Collect every Unique ID already in the sheet — this is our authoritative
# "already seen" set.  We also find the LATEST submission date among all rows
# (not just the last row) to guard against out-of-order writes or blank cells
# at the bottom of the sheet.

def normalise_date(raw: str) -> str:
    """
    Convert any date string the sheet might contain into
    'YYYY-MM-DD HH:MM:SS' so it compares correctly with JotForm's created_at.
    Handles formats like:
      '2026-06-12 05:24:34'   → unchanged
      '2026-06-12T05:24:34'   → normalised
      '12/06/2026 05:24:34'   → normalised  (DD/MM/YYYY)
      '06/12/2026 05:24:34'   → normalised  (MM/DD/YYYY — treated as MM/DD)
    Returns '' if the string cannot be parsed.
    """
    from datetime import datetime
    raw = raw.strip()
    if not raw:
        return ''
    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d',
        '%d/%m/%Y %H:%M:%S',
        '%d/%m/%Y',
        '%m/%d/%Y %H:%M:%S',
        '%m/%d/%Y',
        '%d-%m-%Y %H:%M:%S',
        '%d-%m-%Y',
    ]
    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt).strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            continue
    print(f"⚠️  Could not parse date: {repr(raw)}")
    return ''


known_unique_ids: set[str] = set()
last_known_date: str = ''

for row in data_rows:
    # Collect Unique IDs
    if uid_index is not None and len(row) > uid_index:
        uid = row[uid_index].strip()
        if uid:
            known_unique_ids.add(uid)

    # Normalise and track the maximum submission date seen
    raw_dv = row[date_index].strip() if len(row) > date_index else ''
    dv = normalise_date(raw_dv)
    if dv and dv > last_known_date:
        last_known_date = dv

if not last_known_date:
    raise Exception("❌ Could not determine last known date from sheet.")

# Diagnostic: show a sample of raw date values from the sheet so mismatches are obvious
sample_dates = [
    row[date_index].strip()
    for row in data_rows[-5:]
    if len(row) > date_index and row[date_index].strip()
]
print(f"🔍 Sample raw dates from sheet (last 5): {sample_dates}")

print(f"📌 Latest Submission Date in sheet : {last_known_date}  (normalised)")
print(f"🔍 Sample raw dates from sheet (last 5): {sample_dates}")
print(f"📌 Known Unique IDs in sheet       : {len(known_unique_ids)}")
print(f"🔎 Fetching newest-first; will stop once an entire page predates the cutoff")

# ── DISCOVER JOTFORM FIELDS ───────────────────────────────────────────────────
print("🔍 Discovering form fields...")
first_batch = get_submissions_page(FORM_ID, limit=1, offset=0)
if not first_batch:
    raise Exception("❌ No submissions found for this form")

answers_meta  = first_batch[0].get('answers', {})
header_to_qid: dict[str, str] = {}
new_headers: list[str] = []

for qid, meta in answers_meta.items():
    col_name = meta.get('text', f'Q_{qid}')
    header_to_qid[col_name] = qid
    if col_name not in existing_headers:
        new_headers.append(col_name)

if APPROVAL_DATE_COL not in existing_headers:
    new_headers.append(APPROVAL_DATE_COL)

if new_headers:
    updated_headers = existing_headers + new_headers
    sheet.update([updated_headers], 'A1')
    existing_headers = updated_headers
    print(f"➕ Added new columns: {new_headers}")
else:
    print("✔ All columns already present")

# ── FETCH ONLY NEW SUBMISSIONS (DESC, stop on first fully-old page) ───────────
#
# Pages arrive newest-first (DESC).  A submission is NEW if EITHER:
#   (a) its created_at  > last_known_date, OR
#   (b) its created_at == last_known_date AND its Unique ID is NOT in known_unique_ids
#
# Stop conditions:
#   • Entire page is already known → stop immediately.
#   • Oldest row on page is already known → boundary page, collect new rows then stop.
#   • Entire page is new → collect all, advance offset, continue.

def is_new_submission(sub: dict) -> bool:
    """Return True if this submission is not yet in the sheet."""
    created = sub.get('created_at', '')
    if created > last_known_date:
        return True
    if created == last_known_date:
        uid = get_answer_value(sub.get('answers', {}),
                               next((qid for qid, meta in answers_meta.items()
                                     if meta.get('name') == 'uniqueId'), ''))
        return uid not in known_unique_ids
    return False


offset        = 0
fetched_total = 0
all_new_subs: list[dict] = []

print(f"\n🚀 Fetching new submissions (newest-first)...")

while True:
    try:
        submissions = get_submissions_page(FORM_ID, limit=PAGE_SIZE, offset=offset)

        if not submissions:
            print("✔ No more submissions from JotForm.")
            break

        fetched_total += len(submissions)

        # newest = first item (DESC); oldest = last item
        newest_created = submissions[0].get('created_at', '')
        oldest_created = submissions[-1].get('created_at', '')

        if offset == 0:
            print(f"🔍 First page — newest: {newest_created!r}, oldest: {oldest_created!r}, cutoff: {last_known_date!r}")

        # Entire page is strictly older than our cutoff — done
        if newest_created < last_known_date:
            print(f"🛑 Entire page predates cutoff ({newest_created} < {last_known_date}) — stopping.")
            break

        # Newest row equals cutoff: check by ID whether it's actually new
        if newest_created == last_known_date:
            new_on_page = [s for s in submissions if is_new_submission(s)]
            if new_on_page:
                all_new_subs.extend(new_on_page)
                print(f"🎯 Same-timestamp page: collected {len(new_on_page)} new rows — stopping.")
            else:
                print(f"🛑 Same-timestamp page, all IDs already known — stopping.")
            break

        # Oldest row on page is at-or-before cutoff → boundary page
        if oldest_created <= last_known_date:
            new_on_page = [s for s in submissions if is_new_submission(s)]
            all_new_subs.extend(new_on_page)
            print(f"🎯 Boundary page: collected {len(new_on_page)} new rows — stopping.")
            break

        # Entire page is new — collect all and fetch next page
        all_new_subs.extend(submissions)
        offset += PAGE_SIZE
        print(f"✔ Full page collected ({len(submissions)} rows) | {len(all_new_subs)} new total | scanned {fetched_total}")
        time.sleep(SLEEP_BETWEEN_CALLS)

    except (IncompleteRead, ConnectionAbortedError, ConnectionResetError, OSError) as e:
        print(f"⚠️  Connection error: {type(e).__name__}, retrying after 5s...")
        time.sleep(5)
        continue

# ── WRITE NEW ROWS ────────────────────────────────────────────────────────────
if not all_new_subs:
    print("\n✅ Sheet is already up to date — no new submissions found.")
else:
    # De-duplicate: a submission could theoretically appear on two pages near
    # a boundary; keep only the first occurrence (set of seen IDs).
    seen_ids: set[str] = set()
    deduped: list[dict] = []
    for sub in all_new_subs:
        sid = sub.get('id', '')
        if sid not in seen_ids:
            seen_ids.add(sid)
            deduped.append(sub)
    all_new_subs = deduped

    print(f"\n📦 {len(all_new_subs)} new submissions. Fetching approval threads...")

    all_sub_ids    = [sub.get('id', '') for sub in all_new_subs]
    approval_dates = fetch_threads_batch(all_sub_ids)
    print("✔ Approval threads fetched.")

    # Sort oldest-first before writing so the sheet stays chronological
    all_new_subs.sort(key=lambda s: s.get('created_at', ''))

    ordered_rows = [
        sub_to_row(sub, approval_dates.get(sub.get('id', ''), ''), existing_headers, header_to_qid)
        for sub in all_new_subs
    ]

    total_written = 0
    for i in range(0, len(ordered_rows), WRITE_BATCH_SIZE):
        chunk = ordered_rows[i : i + WRITE_BATCH_SIZE]
        append_with_retry(sheet, chunk)
        total_written += len(chunk)
        print(f"📝 Written {total_written} / {len(ordered_rows)} rows...")

    print(f"\n✅ DONE — {total_written} new rows appended (scanned {fetched_total} total submissions)")