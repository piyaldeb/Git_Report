import requests
import json
import base64
import logging
import sys
import os

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
WORKSHEET_NAME = "PO_Details"

today = date.today()

session = requests.Session()
USER_ID = None

# ========= COLUMN ORDER ==========
COLUMNS = [
    "Invoice Date", "Invoice Number", "Item Category", "LC Number", "Po",
    "PO Qty Total", "Product", "Qty In Transit", "Qty Remaining",
    "Shipment Mode", "Shipment Type", "Subtotal", "Transit Status",
    "Transit Vendor", "Unit Price",
]

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

    raise Exception("No Google credentials found.")

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

# ========= FETCH TRANSIT LINES ==========
def fetch_po_details(company_id, cname):
    specification = {
        "invoice_date":   {},
        "invoice_number": {},
        "item_category":  {"fields": {"display_name": {}}},
        "lc_number":      {},
        "po_id":          {"fields": {"display_name": {}}},
        "qty_total":      {},
        "product_id":     {"fields": {"display_name": {}}},
        "qty_in_transit": {},
        "qty_remaining":  {},
        "shipment_mode":  {"fields": {"display_name": {}}},
        "shipment_type":  {},
        "subtotal":       {},
        "state":          {},
        "vendor_main":    {"fields": {"display_name": {}}},
        "price_unit":     {},
    }
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "transit.line",
            "method": "web_search_read",
            "args": [],
            "kwargs": {
                "specification": specification,
                "offset": 0,
                "order": "invoice_date DESC",
                "limit": 10000,
                "context": {
                    "lang": "en_US",
                    "tz": "Asia/Dhaka",
                    "uid": USER_ID,
                    "allowed_company_ids": [company_id],
                    "bin_size": True,
                    "current_company_id": company_id,
                },
                "count_limit": 100001,
                "domain": [],
            },
        },
    }
    r = retry_request(
        session.post,
        f"{ODOO_URL}/web/dataset/call_kw/transit.line/web_search_read",
        json=payload,
    )
    r.raise_for_status()

    try:
        data = r.json()["result"]["records"]
    except Exception as e:
        print(f"❌ {cname}: Failed to parse response: {e}")
        print(r.text[:2000])
        return []

    def map_record(rec):
        return {
            "Invoice Date":   rec.get("invoice_date") or "",
            "Invoice Number": rec.get("invoice_number") or "",
            "Item Category":  (rec.get("item_category") or {}).get("display_name", ""),
            "LC Number":      rec.get("lc_number") or "",
            "Po":             (rec.get("po_id") or {}).get("display_name", ""),
            "PO Qty Total":   rec.get("qty_total") if rec.get("qty_total") is not None else "",
            "Product":        (rec.get("product_id") or {}).get("display_name", ""),
            "Qty In Transit": rec.get("qty_in_transit") if rec.get("qty_in_transit") is not None else "",
            "Qty Remaining":  rec.get("qty_remaining") if rec.get("qty_remaining") is not None else "",
            "Shipment Mode":  (rec.get("shipment_mode") or {}).get("display_name", ""),
            "Shipment Type":  rec.get("shipment_type") or "",
            "Subtotal":       rec.get("subtotal") if rec.get("subtotal") is not None else "",
            "Transit Status": rec.get("state") or "",
            "Transit Vendor": (rec.get("vendor_main") or {}).get("display_name", ""),
            "Unit Price":     rec.get("price_unit") if rec.get("price_unit") is not None else "",
        }

    all_rows = [map_record(rec) for rec in data]
    print(f"📊 {cname}: {len(all_rows)} transit lines fetched")

    # ---- coverage check ----
    df_check = pd.DataFrame(all_rows, columns=COLUMNS)
    empty = {c: int(df_check[c].replace("", pd.NA).isna().sum())
             for c in COLUMNS if df_check[c].replace("", pd.NA).isna().any()}
    if empty:
        print("⚠️  Columns with blank values:")
        for col, n in empty.items():
            print(f"   {col}: {n} blank row(s)")
    else:
        print("✅ No blank values in any column")

    return all_rows

# ========= MAIN ==========
if __name__ == "__main__":
    userinfo = login()
    print("User info (allowed companies):", userinfo.get("user_companies", {}))

    company_id = 1
    cname = "Zipper"

    if switch_company(company_id):
        records = fetch_po_details(company_id, cname)

        if records:
            df = pd.DataFrame(records, columns=COLUMNS)
            output_file = f"po_details_{today.isoformat()}.xlsx"
            df.to_excel(output_file, index=False)
            print(f"📂 Saved: {output_file}")

            # ========= GOOGLE SHEETS ==========
            try:
                client = get_gspread_client()
                sheet = client.open_by_key(SHEET_KEY)
                worksheet = sheet.worksheet(WORKSHEET_NAME)
                worksheet.batch_clear(["A:O"])
                set_with_dataframe(worksheet, df)
                print(f"✅ Data pasted to Google Sheets → '{WORKSHEET_NAME}'")
            except Exception as e:
                import traceback
                print(f"❌ Error while pasting to Google Sheets: {e}")
                traceback.print_exc()
        else:
            print(f"❌ No data fetched for {cname}")
