import json
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---------------- CONFIGURATION ----------------
# Load the JSON key string from GitHub Secrets
json_creds = json.loads(os.environ['GCP_KEYS'])
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
client = gspread.authorize(creds)

# ---------------- CLEANUP ----------------
print("--- STARTING CLEANUP ---")
print("Scanning the Bot's Google Drive for old files...")

# List all spreadsheets visible to the bot
files = client.list_spreadsheet_files()
print(f"Found {len(files)} files taking up space.")

if len(files) == 0:
    print("Drive is already empty!")
else:
    print("Deleting files now...")
    for f in files:
        try:
            print(f"Deleting: {f['name']} (ID: {f['id']})")
            client.del_spreadsheet(f['id'])
        except Exception as e:
            print(f"Could not delete {f['name']}: {e}")

print("--- CLEANUP COMPLETE ---")
print("The Bot's drive is now empty. You can restore your original script.")
