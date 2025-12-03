import requests
import csv
import gspread
import smtplib
import json
import os
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ---------------- CONFIGURATION ----------------
SHEET_NAME = 'Transportation Data V2'
SHEET_URL = "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID_HERE" # Optional: For the email link
BASE_URL = "https://data.transportation.gov/api/views/9mw4-x3tu/rows.csv"
FILTER_DATE_KEY = 'DISP_DECIDED_DATE'
COLUMNS_TO_KEEP = ['DOCKET_NUMBER', 'DOT_NUMBER', 'OP_AUTH_TYPE', 'DISP_DECIDED_DATE', 'DISP_ACTION_DESC', 'ORIGINAL_ACTION_DESC']

# Email Config (Loaded from GitHub Secrets)
SENDER_EMAIL = os.environ.get('EMAIL_USER')
EMAIL_PASSWORD = os.environ.get('EMAIL_PASS')
RECEIVER_EMAIL = "your_email@gmail.com"  # <--- REPLACE THIS WITH YOUR EMAIL

# ---------------- AUTHENTICATION ----------------
# Load the JSON key string from GitHub Secrets
json_creds = json.loads(os.environ['GCP_KEYS'])
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
client = gspread.authorize(creds)

# ---------------- DATA PROCESSING ----------------
print("Scanning database...")
today = datetime.now()
cutoff_date = today - timedelta(days=30) # Monthly run captures last 30 days

filtered_rows = []
try:
    with requests.get(BASE_URL, stream=True) as r:
        r.raise_for_status()
        lines = (line.decode('utf-8') for line in r.iter_lines())
        reader = csv.DictReader(lines)
        
        for row in reader:
            date_str = row.get(FILTER_DATE_KEY, '').strip()
            if not date_str: continue
            
            try:
                row_date = datetime.strptime(date_str, '%m/%d/%Y')
                if row_date > today: continue # Skip future typos
                if row_date >= cutoff_date:
                    clean_row = [row.get(key, '') for key in COLUMNS_TO_KEEP]
                    filtered_rows.append((row_date, clean_row))
            except ValueError:
                continue
except Exception as e:
    print(f"Error downloading data: {e}")
    exit(1)

# Sort and Prep
filtered_rows.sort(key=lambda x: x[0], reverse=True)
final_data = [x[1] for x in filtered_rows]

# ---------------- UPDATE SHEETS ----------------
print(f"Updating Google Sheet with {len(final_data)} rows...")
try:
    try:
        sh = client.open(SHEET_NAME)
    except gspread.exceptions.SpreadsheetNotFound:
        sh = client.create(SHEET_NAME)
        # Share the new sheet with your real email so you can see it
        sh.share(RECEIVER_EMAIL, perm_type='user', role='writer')
        
    sheet = sh.sheet1
    sheet.clear()
    sheet.append_row(COLUMNS_TO_KEEP)
    if final_data:
        sheet.append_rows(final_data)
    sheet_link = f"https://docs.google.com/spreadsheets/d/{sh.id}"
except Exception as e:
    print(f"Sheet Error: {e}")
    exit(1)

# ---------------- SEND EMAIL ----------------
print("Sending Email...")
msg = MIMEMultipart()
msg['From'] = SENDER_EMAIL
msg['To'] = RECEIVER_EMAIL
msg['Subject'] = f"Monthly Transport Report: {len(final_data)} New Records"

body = f"""
<h3>Transportation Data Update</h3>
<p>The monthly scan is complete.</p>
<ul>
    <li><strong>Date Range:</strong> {cutoff_date.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')}</li>
    <li><strong>Records Found:</strong> {len(final_data)}</li>
</ul>
<p><a href="{sheet_link}">Click here to view the updated Google Sheet</a></p>
"""
msg.attach(MIMEText(body, 'html'))

try:
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(SENDER_EMAIL, EMAIL_PASSWORD)
    text = msg.as_string()
    server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, text)
    server.quit()
    print("Email Sent Successfully!")
except Exception as e:
    print(f"Email Failed: {e}")
