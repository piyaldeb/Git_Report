import requests
import json
import base64
import logging
import sys
import os
from datetime import date, datetime
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account
import pandas as pd
import pytz
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

# ========= LABEL MAPPING ==========
LABELS = {
    "name": "Transit Name",
    "parent_id": "Parent",
    "invoice_number": "Invoice Number",
    "invoice_date": "Invoice Date",
    "po_numbers": "PO Numbers",
    "vendor": "Vendor",
    "company_id": "Company",
    "shipment_mode": "Shipment Mode",
    "shipment_type": "Shipment Type",
    "lc_number": "LC Number",
    "bl_number": "BL Number",
    "eta": "ETA",
    "grn_date": "GRN Date",
    "state": "State",
    "subtotal": "Subtotal",
    "create_uid": "Created By",
}

# ========= GOOGLE SHEETS CLIENT ==========
def get_gspread_client():
    creds_b64 = os.getenv("GOOGLE_CREDS_BASE64")
    if creds_b64:
        creds_json = base64.b64decode(creds_b64).decode("utf-8")
        creds_dict = json.loads(creds_json)
        scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
        return gspread.authorize(creds)
    else:
        return gspread.service_account(filename="service_account.json")

# ========= RETRY LOGIC ==========
def retry_request(method, url, max_retries=3, backoff=3, **kwargs):
    """
    Wrapper for requests with retry logic.
    Retries up to `max_retries` times with `backoff` seconds delay.
    """
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
    r.raise_for_status()
    if "error" in r.json():
        print(f"❌ Failed to switch to company {company_id}: {r.json()['error']}")
        return False
    else:
        print(f"🔄 Session switched to company {company_id}")
        return True

# ========= FETCH GOODS IN TRANSIT ==========
def fetch_git(company_id, cname):
    specification = {
        "name": {},
        "parent_id": {"fields": {"display_name": {}}},
        "invoice_number": {},
        "invoice_date": {},
        "po_numbers": {"fields": {}},
        "vendor": {"fields": {"display_name": {}}},
        "company_id": {"fields": {"display_name": {}}},
        "shipment_mode": {"fields": {"display_name": {}}},
        "shipment_type": {},
        "lc_number": {},
        "bl_number": {},
        "eta": {},
        "grn_date": {},
        "state": {},
        "subtotal": {},
        "create_uid": {"fields": {"display_name": {}}},
    }
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "transit.model",
            "method": "web_search_read",
            "args": [],
            "kwargs": {
                "specification": specification,
                "offset": 0,
                "order": "invoice_date DESC",
                "limit": 5000,
                "context": {
                    "lang": "en_US",
                    "tz": "Asia/Dhaka",
                    "uid": USER_ID,
                    "allowed_company_ids": [company_id],
                    "bin_size": True,
                    "current_company_id": company_id,
                },
                "count_limit": 10001,
                "domain": [],
            },
        },
    }
    r = retry_request(
        session.post,
        f"{ODOO_URL}/web/dataset/call_kw/transit.model/web_search_read",
        json=payload,
    )
    r.raise_for_status()
    try:
        data = r.json()["result"]["records"]

        def flatten(record):
            flat = {}
            for k, v in record.items():
                label = LABELS.get(k, k)
                if isinstance(v, dict) and "display_name" in v:
                    flat[label] = v["display_name"]
                elif isinstance(v, list):
                    flat[label] = ", ".join(str(item.get("id", "")) for item in v) if v else ""
                else:
                    flat[label] = v
            return flat

        flattened = [flatten(rec) for rec in data]
        print(f"📊 {cname}: {len(flattened)} GIT rows fetched")
        return flattened
    except Exception as e:
        print(f"❌ {cname}: Failed to parse GIT report: {e}")
        print(r.text[:200])
        return []

# ========= MAIN ==========
if __name__ == "__main__":
    userinfo = login()
    print("User info (allowed companies):", userinfo.get("user_companies", {}))

    # Only Metal Trims (company 3)
    company_id = 3
    cname = "Metal Trims"

    if switch_company(company_id):
        records = fetch_git(company_id, cname)

        if records:
            df = pd.DataFrame(records)
            output_file = f"git_{today.isoformat()}.xlsx"
            df.to_excel(output_file, index=False)
            print(f"📂 Saved: {output_file}")

            # ========= GOOGLE SHEETS ==========
            try:
                client = get_gspread_client()
                sheet = client.open_by_key(SHEET_KEY)
                worksheet = sheet.worksheet(WORKSHEET_NAME)
                worksheet.batch_clear(["A:T"])
                set_with_dataframe(worksheet, df)
                print(f"✅ Data pasted to Google Sheets → '{WORKSHEET_NAME}'")
            except Exception as e:
                print(f"❌ Error while pasting to Google Sheets: {e}")

        else:
            print(f"❌ No GIT data fetched for {cname}")
