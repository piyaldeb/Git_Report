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
import time
from dotenv import load_dotenv
from requests.exceptions import RequestException
from google.oauth2 import service_account
import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd

load_dotenv()
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
log = logging.getLogger()

# ========= CONFIG ==========
ODOO_URL = os.getenv("ODOO_URL")
DB = os.getenv("ODOO_DB")
USERNAME = os.getenv("ODOO_USERNAME")
PASSWORD = os.getenv("ODOO_PASSWORD")

SHEET_KEY = "1ho7ihCKKCzg7de9hvuesledI7tdCpWjOiz9-EFGgIuI"
WORKSHEET_NAME = "GIT_REPORT_RAW"

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
    # Odoo embeds csrf_token in the HTML as a JS variable or meta tag
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

# ========= CREATE REPORT WIZARD ==========
def create_wizard(company_id, upto_date, report_type="monthly_transit_report"):
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "transit.report.wizard",
            "method": "web_save",
            "args": [[], {"report_type": report_type, "upto_date": upto_date, "company_id": company_id}],
            "kwargs": {
                "context": {
                    "lang": "en_US",
                    "tz": "Asia/Dhaka",
                    "uid": USER_ID,
                    "allowed_company_ids": [company_id],
                },
                "specification": {
                    "report_type": {},
                    "upto_date": {},
                    "company_id": {"fields": {"display_name": {}}},
                },
            },
        },
    }
    r = retry_request(session.post, f"{ODOO_URL}/web/dataset/call_kw", json=payload)
    result = r.json().get("result", [])
    if not result:
        raise Exception("❌ Failed to create report wizard")
    wizard_id = result[0]["id"]
    print(f"📋 Wizard created (id={wizard_id})")
    return wizard_id

# ========= TRIGGER REPORT ACTION ==========
def trigger_report_action(wizard_id, company_id):
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "args": [[wizard_id]],
            "kwargs": {
                "context": {
                    "lang": "en_US",
                    "tz": "Asia/Dhaka",
                    "uid": USER_ID,
                    "allowed_company_ids": [company_id],
                }
            },
            "method": "action_generate_xlsx_report",
            "model": "transit.report.wizard",
        },
    }
    r = retry_request(session.post, f"{ODOO_URL}/web/dataset/call_button", json=payload)
    result = r.json().get("result")
    if not result:
        raise Exception("❌ Failed to trigger report action")
    print(f"📄 Report action triggered: {result.get('name', 'Report')}")
    return result

# ========= DOWNLOAD REPORT ==========
def download_report(action, wizard_id, company_id, csrf_token, output_path):
    report_name = action["report_name"]
    options = action.get("data", {})
    context = {
        "lang": "en_US",
        "tz": "Asia/Dhaka",
        "uid": USER_ID,
        "allowed_company_ids": [company_id],
        "active_model": "transit.report.wizard",
        "active_id": wizard_id,
        "active_ids": [wizard_id],
    }

    import urllib.parse
    options_str = urllib.parse.quote(json.dumps(options, separators=(",", ":")))
    context_str = urllib.parse.quote(json.dumps(context, separators=(",", ":")))
    report_url = (
        f"/report/xlsx/{report_name}"
        f"?options={options_str}"
        f"&context={context_str}"
    )

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
    login()

    company_id = 3
    cname = "Metal Trims"

    if not switch_company(company_id):
        sys.exit(1)

    csrf_token = get_csrf_token()
    print(f"🔑 CSRF token obtained")

    upto_date = today.isoformat()
    wizard_id = create_wizard(company_id, upto_date)
    action = trigger_report_action(wizard_id, company_id)

    output_file = f"git_report_{today.isoformat()}.xlsx"
    saved_path = download_report(action, wizard_id, company_id, csrf_token, output_file)

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
        print(f"❌ Error while pasting to Google Sheets: {e}")

    print(f"✅ Done — {output_file}")
