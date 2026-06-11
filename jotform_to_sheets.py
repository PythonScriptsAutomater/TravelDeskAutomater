import os
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from jotform import JotformAPIClient
from http.client import IncompleteRead
from requests.exceptions import ConnectionError as RequestsConnectionError


def col_letter(n):
    """Convert column number to Excel-style letter (1 -> A, 27 -> AA)"""
    result = ''
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def append_with_retry(sheet, batch, retries=3):
    """Write a batch of rows to Google Sheets with retry on connection errors."""
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


# ---------------- CONFIG ----------------
# Matching exact secret names from GitHub:
# API_KEY, FORM_ID, SPREADSHEET_NAME, WORKSHEET_NAME_TDR, GOOGLE_CREDENTIALS_JSON

API_KEY          = os.environ['API_KEY']
FORM_ID          = os.environ['FORM_ID']
SPREADSHEET_NAME = os.environ['SPREADSHEET_NAME']
WORKSHEET_NAME   = os.environ['WORKSHEET_NAME_TDR']

TOTAL_LIMIT         = int(os.environ.get('TOTAL_LIMIT', 8000))
PAGE_SIZE           = int(os.environ.get('PAGE_SIZE', 200))
SLEEP_BETWEEN_CALLS = int(os.environ.get('SLEEP_BETWEEN_CALLS', 2))
WRITE_BATCH_SIZE    = int(os.environ.get('WRITE_BATCH_SIZE', 500))

# ---------------- JOTFORM ----------------
jotform = JotformAPIClient(API_KEY)
jotform.set_baseurl('https://pw.jotform.com/API/')

# ---------------- GOOGLE SHEETS ----------------
scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]
creds  = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
client = gspread.authorize(creds)

try:
    sheet = client.open(SPREADSHEET_NAME).worksheet(WORKSHEET_NAME)
    print(f"✅ Opened: '{SPREADSHEET_NAME}' → '{WORKSHEET_NAME}'")
except gspread.exceptions.SpreadsheetNotFound:
    raise Exception(f"❌ SpreadsheetNotFound: '{SPREADSHEET_NAME}' — check secret value and sharing")
except gspread.exceptions.WorksheetNotFound:
    raise Exception(f"❌ WorksheetNotFound: '{WORKSHEET_NAME}' — check WORKSHEET_NAME_TDR secret")

# ---------------- PRESERVE HEADERS ----------------
existing_headers = sheet.row_values(1)
if not existing_headers:
    raise Exception("❌ Header row missing in destination sheet")

# ---------------- SAFE CLEAR ----------------
row_count = sheet.row_count
col_count = sheet.col_count

if row_count > 1:
    last_col = col_letter(col_count)
    sheet.batch_clear([f"A2:{last_col}{row_count}"])

print("🧹 Old data cleared, header preserved")

# ---------------- DISCOVER JOTFORM FIELDS ----------------
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

if new_headers:
    updated_headers = existing_headers + new_headers
    sheet.update([updated_headers], 'A1')
    existing_headers = updated_headers
    print(f"➕ Added new columns: {new_headers}")

# ---------------- FETCH & WRITE ----------------
offset        = 0
fetched       = 0
rows_buffer   = []
total_written = 0

print("🚀 Fetching submissions from JotForm...")

while fetched < TOTAL_LIMIT:
    try:
        submissions = jotform.get_form_submissions(
            FORM_ID,
            limit=PAGE_SIZE,
            offset=offset
        )

        if not submissions:
            print("✔ No more submissions.")
            break

        for sub in submissions:
            row_data = {
                'Submission ID':    sub.get('id', ''),
                'Submission Date':  sub.get('created_at', ''),
                'Last Update Date': sub.get('updated_at', ''),
                'Approval Status':  (
                    sub.get('workflowStatus')
                    or sub.get('workflow_status')
                    or sub.get('status')
                    or ''
                )
            }

            answers = sub.get('answers', {})
            for header, qid in header_to_qid.items():
                if qid in answers and 'answer' in answers[qid]:
                    ans = answers[qid]['answer']
                    row_data[header] = (
                        '\n'.join(map(str, ans))
                        if isinstance(ans, list)
                        else str(ans)
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
            time.sleep(2)

        offset += PAGE_SIZE
        print(f"✔ Pulled {fetched} submissions so far...")
        time.sleep(SLEEP_BETWEEN_CALLS)

    except IncompleteRead:
        print("⚠️  IncompleteRead, retrying after 5s...")
        time.sleep(5)
        continue

# ---------------- FLUSH REMAINING ----------------
if rows_buffer:
    append_with_retry(sheet, rows_buffer)
    total_written += len(rows_buffer)

print(f"✅ DONE — {total_written} rows written successfully")