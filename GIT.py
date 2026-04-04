import requests
import json
import base64
import logging
import sys
import os
import re

# Force UTF-8 output on Windows to handle emojis in print statements
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
if sys.stderr.encoding and sys.stderr.encoding.lower() != 'utf-8':
    sys.stderr.reconfigure(encoding='utf-8')

from datetime import date
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account
import pandas as pd
import time
from dotenv import load_dotenv
from requests.exceptions import RequestException

load_dotenv()
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
log = logging.getLogger()

# ========= CONFIG ==========
ODOO_URL = os.getenv("ODOO_URL")
DB = os.getenv("ODOO_DB")
USERNAME = os.getenv("ODOO_USERNAME")
PASSWORD = os.getenv("ODOO_PASSWORD")

SHEET_KEY = "16KUtHe-6R6Sd1HarFgS6Y2rEZGmUcM3ohAzPLy7Kxkg"
WORKSHEET_NAME = "Git_Raw"

today = date.today()

session = requests.Session()
USER_ID = None

# ========= GOOGLE SHEETS CLIENT ==========
def get_gspread_client():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    for sa_path in [
        os.path.join(script_dir, "service_account.json"),
        "service_account.json",
    ]:
        if os.path.exists(sa_path):
            return gspread.service_account(filename=sa_path)

    creds_raw = os.getenv("GOOGLE_CREDS_BASE64")
    if creds_raw:
        creds_dict = None
        try:
            creds_dict = json.loads(creds_raw.strip())
        except json.JSONDecodeError:
            pass
        if creds_dict is None:
            try:
                padded = creds_raw.strip() + '=' * (-len(creds_raw.strip()) % 4)
                creds_dict = json.loads(base64.b64decode(padded).decode("utf-8"))
            except Exception:
                pass
        if creds_dict is None:
            raise Exception("GOOGLE_CREDS_BASE64 is neither valid JSON nor valid base64-encoded JSON")
        scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
        return gspread.authorize(creds)

    raise Exception("No Google credentials found. Place service_account.json in the script folder or set GOOGLE_CREDS_BASE64.")

# ========= RETRY LOGIC ==========
def retry_request(method, url, max_retries=3, backoff=3, **kwargs):
    for attempt in range(1, max_retries + 1):
        try:
            r = method(url, **kwargs)
            r.raise_for_status()
            return r
        except RequestException as e:
            print(f"⚠️ Attempt {attempt} failed: {e}")
            if attempt < max_retries:
                print(f"⏳ Retrying in {backoff} seconds...")
                time.sleep(backoff)
            else:
                print("❌ All retry attempts failed.")
                raise

# ========= LOGIN ==========
def login():
    global USER_ID
    payload = {"jsonrpc": "2.0", "params": {"db": DB, "login": USERNAME, "password": PASSWORD}}
    r = retry_request(session.post, f"{ODOO_URL}/web/session/authenticate", json=payload)
    result = r.json().get("result")
    if result and "uid" in result:
        USER_ID = result["uid"]
        print(f"✅ Logged in (uid={USER_ID})")
        return result
    else:
        raise Exception("❌ Login failed")

# ========= GET CSRF TOKEN ==========
def get_csrf_token():
    r = session.get(f"{ODOO_URL}/web")
    r.raise_for_status()
    match = re.search(r'"csrf_token"\s*:\s*"([^"]+)"', r.text)
    if match:
        return match.group(1)
    match = re.search(r"csrf_token['\"]?\s*:\s*['\"]([^'\"]+)['\"]", r.text)
    if match:
        return match.group(1)
    raise Exception("❌ Could not extract CSRF token from /web page")

# ========= SWITCH COMPANY ==========
def switch_company(company_id):
    if USER_ID is None:
        raise Exception("User not logged in yet")
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "res.users",
            "method": "write",
            "args": [[USER_ID], {"company_id": company_id}],
            "kwargs": {"context": {"allowed_company_ids": [company_id], "company_id": company_id}},
        },
    }
    r = retry_request(session.post, f"{ODOO_URL}/web/dataset/call_kw", json=payload)
    if "error" in r.json():
        print(f"❌ Failed to switch to company {company_id}: {r.json()['error']}")
        return False
    print(f"🔄 Session switched to company {company_id}")
    return True

# ========= FETCH ALL TRANSIT IDs ==========
def fetch_transit_ids(company_id):
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "transit.model",
            "method": "search",
            "args": [[]],
            "kwargs": {
                "limit": 20000,
                "context": {
                    "lang": "en_US",
                    "tz": "Asia/Dhaka",
                    "uid": USER_ID,
                    "allowed_company_ids": [company_id],
                },
            },
        },
    }
    r = retry_request(session.post, f"{ODOO_URL}/web/dataset/call_kw", json=payload)
    ids = r.json().get("result", [])
    print(f"📋 Found {len(ids)} transit records")
    return ids

# ========= DOWNLOAD TRANSIT EXCEL REPORT ==========
def download_report(transit_ids, company_id, csrf_token, output_path):
    ids_str = ",".join(str(i) for i in transit_ids)
    context = {
        "lang": "en_US",
        "tz": "Asia/Dhaka",
        "uid": USER_ID,
        "allowed_company_ids": [company_id],
    }

    import urllib.parse
    context_str = urllib.parse.quote(json.dumps(context, separators=(",", ":")))
    report_url = f"/report/xlsx/taps_purchase.transit_excel_report/{ids_str}?context={context_str}"

    form_data = {
        "data": json.dumps([report_url, "xlsx"]),
        "context": json.dumps(context),
        "token": "dummy-because-api-expects-one",
        "csrf_token": csrf_token,
    }

    r = retry_request(session.post, f"{ODOO_URL}/report/download", data=form_data)

    content_type = r.headers.get("Content-Type", "")
    if "spreadsheet" not in content_type and "octet-stream" not in content_type and "xlsx" not in content_type:
        print(f"⚠️ Unexpected content type: {content_type}")
        print("Response snippet:", r.text[:300])
        raise Exception("❌ Response does not appear to be an xlsx file")

    try:
        with open(output_path, "wb") as f:
            f.write(r.content)
    except PermissionError:
        base, ext = os.path.splitext(output_path)
        output_path = f"{base}_new{ext}"
        with open(output_path, "wb") as f:
            f.write(r.content)
        print(f"⚠️ Original file was locked — saved as: {output_path}")
    print(f"📂 Report saved: {output_path}")
    return output_path

# ========= MAIN ==========
if __name__ == "__main__":
    userinfo = login()
    print("User info (allowed companies):", userinfo.get("user_companies", {}))

    company_id = 3
    cname = "Metal Trims"

    if not switch_company(company_id):
        sys.exit(1)

    csrf_token = get_csrf_token()
    print(f"🔑 CSRF token obtained")

    transit_ids = fetch_transit_ids(company_id)
    if not transit_ids:
        print(f"❌ No transit records found for {cname}")
        sys.exit(1)

    output_file = f"git_{today.isoformat()}.xlsx"
    saved_path = download_report(transit_ids, company_id, csrf_token, output_file)

    # ========= GOOGLE SHEETS ==========
    try:
        df = pd.read_excel(saved_path)
        client = get_gspread_client()
        sheet = client.open_by_key(SHEET_KEY)
        worksheet = sheet.worksheet(WORKSHEET_NAME)
        worksheet.batch_clear(["A:ZZ"])
        set_with_dataframe(worksheet, df)
        print(f"✅ Data pasted to Google Sheets → '{WORKSHEET_NAME}'")
    except Exception as e:
        import traceback
        print(f"❌ Error while pasting to Google Sheets: {e}")
        traceback.print_exc()

    print(f"✅ Done — {output_file}")
