import requests
import json
import base64
import logging
import sys
import os

# Force UTF-8 output on Windows to handle emojis in print statements
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
if sys.stderr.encoding and sys.stderr.encoding.lower() != 'utf-8':
    sys.stderr.reconfigure(encoding='utf-8')
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

# ========= COLUMN ORDER ==========
COLUMNS = [
    "Company", "PO No", "PO Apprvd Stat", "P Cat", "P Type",
    "Inv Month", "Vendor", "Item Details", "Inv No", "Inv Date",
    "Inv Quantity", "Inv Value", "Adjust", "Pmt Term", "Ship Mode",
    "Inco", "Booked Ship ETD", "Booked Ship ETA", "ETD", "ETA",
    "BL Number", "BL Date", "LC Number", "LC Date",
    "I/H Plan Month", "Inhoused Date", "I/H Status",
]

# ========= GOOGLE SHEETS CLIENT ==========
def get_gspread_client():
    # Check service_account.json in script dir or current dir (local dev)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    for sa_path in [
        os.path.join(script_dir, "service_account.json"),
        "service_account.json",
    ]:
        if os.path.exists(sa_path):
            return gspread.service_account(filename=sa_path)

    # Fall back to GOOGLE_CREDS_BASE64 env var (GitHub Actions)
    creds_raw = os.getenv("GOOGLE_CREDS_BASE64")
    if creds_raw:
        creds_dict = None
        # Try 1: raw JSON string directly
        try:
            creds_dict = json.loads(creds_raw.strip())
        except json.JSONDecodeError:
            pass
        # Try 2: base64-encoded JSON
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
        "company_id": {"fields": {"display_name": {}}},
        "po_numbers": {"fields": {
            "name": {},
            "state": {},
            "itemtypes": {"fields": {"display_name": {}}},
            "po_type": {},
        }},
        "vendor": {"fields": {"display_name": {}}},
        "item_details": {},
        "invoice_number": {},
        "invoice_date": {},
        "item_qty": {},
        "subtotal": {},
        "adjusted_state": {},
        "payment_term": {"fields": {"display_name": {}}},
        "shipment_mode": {"fields": {"display_name": {}}},
        "inco_terms": {"fields": {"display_name": {}}},
        "booked_etd": {},
        "booked_eta": {},
        "etd": {},
        "eta": {},
        "bl_number": {},
        "bl_date": {},
        "lc_number": {},
        "lc_date": {},
        "ih_plan": {},
        "grn_date": {},
        "state": {},
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
            pos = record.get("po_numbers", [])
            inv_date = record.get("invoice_date") or ""
            try:
                inv_month = pd.to_datetime(inv_date).strftime("%b-%y") if inv_date else ""
            except Exception:
                inv_month = ""

            flat = {
                "Company":          (record.get("company_id") or {}).get("display_name", ""),
                "PO No":            ", ".join(p.get("name", "") for p in pos),
                "PO Apprvd Stat":   ", ".join(p.get("state", "") for p in pos),
                "P Cat":            ", ".join((p.get("itemtypes") or {}).get("display_name", "") for p in pos),
                "P Type":           ", ".join(p.get("po_type", "") or "" for p in pos),
                "Inv Month":        inv_month,
                "Vendor":           (record.get("vendor") or {}).get("display_name", ""),
                "Item Details":     record.get("item_details") or "",
                "Inv No":           record.get("invoice_number") or "",
                "Inv Date":         inv_date,
                "Inv Quantity":     record.get("item_qty") or "",
                "Inv Value":        record.get("subtotal") or "",
                "Adjust":           record.get("adjusted_state") or "",
                "Pmt Term":         (record.get("payment_term") or {}).get("display_name", ""),
                "Ship Mode":        (record.get("shipment_mode") or {}).get("display_name", ""),
                "Inco":             (record.get("inco_terms") or {}).get("display_name", ""),
                "Booked Ship ETD":  record.get("booked_etd") or "",
                "Booked Ship ETA":  record.get("booked_eta") or "",
                "ETD":              record.get("etd") or "",
                "ETA":              record.get("eta") or "",
                "BL Number":        record.get("bl_number") or "",
                "BL Date":          record.get("bl_date") or "",
                "LC Number":        record.get("lc_number") or "",
                "LC Date":          record.get("lc_date") or "",
                "I/H Plan Month":   record.get("ih_plan") or "",
                "Inhoused Date":    record.get("grn_date") or "",
                "I/H Status":       record.get("state") or "",
            }
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
            df = pd.DataFrame(records, columns=COLUMNS)
            output_file = f"git_{today.isoformat()}.xlsx"
            df.to_excel(output_file, index=False)
            print(f"📂 Saved: {output_file}")

            # ========= GOOGLE SHEETS ==========
            try:
                client = get_gspread_client()
                sheet = client.open_by_key(SHEET_KEY)
                worksheet = sheet.worksheet(WORKSHEET_NAME)
                worksheet.batch_clear(["A:AA"])
                set_with_dataframe(worksheet, df)
                print(f"✅ Data pasted to Google Sheets → '{WORKSHEET_NAME}'")
            except Exception as e:
                print(f"❌ Error while pasting to Google Sheets: {e}")

        else:
            print(f"❌ No GIT data fetched for {cname}")
