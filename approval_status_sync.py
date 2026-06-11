import os
import json
import time
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import WorksheetNotFound
from requests.exceptions import ConnectionError as RequestsConnectionError

# ---------------- CONFIG ----------------
# Matches secrets: API_KEY, FORM_ID, SPREADSHEET_NAME, WORKSHEET_NAME_APPROVAL
API_KEY          = os.environ['API_KEY']
FORM_ID          = os.environ['FORM_ID']
SPREADSHEET_NAME = os.environ['SPREADSHEET_NAME']
WORKSHEET_NAME   = os.environ['WORKSHEET_NAME_APPROVAL']
START_DATE       = os.environ.get('START_DATE', '2023-08-01 00:00:00')
BASE_URL         = 'https://pw.jotform.com/API'

PAGE_SIZE           = int(os.environ.get('PAGE_SIZE', 300))
SLEEP_BETWEEN_CALLS = int(os.environ.get('SLEEP_BETWEEN_CALLS', 1))
MAX_PAGES           = int(os.environ.get('MAX_PAGES', 500))
WRITE_BATCH_SIZE    = int(os.environ.get('WRITE_BATCH_SIZE', 500))

# ---------------- GOOGLE SHEETS ----------------
scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]
creds  = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
client = gspread.authorize(creds)

try:
    spreadsheet = client.open(SPREADSHEET_NAME)
    print(f"✅ Opened spreadsheet: '{SPREADSHEET_NAME}'")
except gspread.exceptions.SpreadsheetNotFound:
    raise Exception(f"❌ SpreadsheetNotFound: '{SPREADSHEET_NAME}' — check secret and sharing")

try:
    sheet = spreadsheet.worksheet(WORKSHEET_NAME)
    print(f"✅ Opened worksheet: '{WORKSHEET_NAME}'")
except WorksheetNotFound:
    sheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=10)
    print(f"➕ Created worksheet: '{WORKSHEET_NAME}'")

headers = ['Unique ID', 'Submission Date', 'Updated at', 'Approval Status']
sheet.clear()
sheet.update([headers], 'A1')
print("🧹 Sheet cleared, headers written")

# ---------------- HELPERS ----------------
def fetch_submissions(offset=0, limit=300):
    url = f"{BASE_URL}/form/{FORM_ID}/submissions"
    params = {
        'apiKey': API_KEY,
        'limit': limit,
        'offset': offset,
        'orderby[created_at]': 'asc',
        'addWorkflowStatus': 1,
        'filter': json.dumps({
            'created_at:gt': START_DATE
        })
    }
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    if data.get('responseCode') != 200:
        raise Exception(f"❌ Jotform API error: {data}")

    return data.get('content', [])


def extract_unique_id(answers):
    for _, meta in answers.items():
        if meta.get('name') == 'uniqueId' or meta.get('text') == 'Unique ID':
            return meta.get('answer', '')
    return ''


def append_with_retry(sheet, batch, retries=3):
    for attempt in range(retries):
        try:
            sheet.append_rows(batch, value_input_option='RAW')
            return
        except (RequestsConnectionError, Exception) as e:
            if attempt < retries - 1:
                wait = 5 * (attempt + 1)
                print(f"⚠️  Write failed (attempt {attempt + 1}/{retries}), retrying in {wait}s... [{e}]")
                time.sleep(wait)
            else:
                raise


# ---------------- FETCH & WRITE ----------------
rows_buffer   = []
total_written = 0
offset        = 0
page          = 0

print("🚀 Fetching submissions...")

while page < MAX_PAGES:
    submissions = fetch_submissions(offset=offset, limit=PAGE_SIZE)

    if not submissions:
        print("✔ No more submissions.")
        break

    for sub in submissions:
        answers         = sub.get('answers', {})
        unique_id       = extract_unique_id(answers)
        created_at      = sub.get('created_at', '')
        last_update     = sub.get('updated_at', '')
        approval_status = sub.get('workflowStatus', '')

        rows_buffer.append([
            unique_id,
            created_at,
            last_update,
            approval_status,
        ])

    if len(rows_buffer) >= WRITE_BATCH_SIZE:
        append_with_retry(sheet, rows_buffer)
        total_written += len(rows_buffer)
        print(f"📝 Written {total_written} rows so far...")
        rows_buffer = []
        time.sleep(2)

    offset += PAGE_SIZE
    page   += 1
    print(f"✔ Page {page} done, {total_written + len(rows_buffer)} rows so far...")
    time.sleep(SLEEP_BETWEEN_CALLS)

# ---------------- FLUSH REMAINING ----------------
if rows_buffer:
    append_with_retry(sheet, rows_buffer)
    total_written += len(rows_buffer)

print(f"✅ DONE — {total_written} rows written to '{SPREADSHEET_NAME}' → '{WORKSHEET_NAME}'")