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

SHEET_KEY = "1n3I1G48V2r9JgJcPyG22KAV2I47FqS5MmBfRH3DKJrw"
WORKSHEET_NAME = "Sheet1"

today = date.today()

session = requests.Session()
USER_ID = None

# ========= MAPPINGS ==========
STATE_MAP = {
    "draft":      "Draft",
    "sent":       "RFQ Sent",
    "to approve": "To Approve",
    "purchase":   "Approved",
    "done":       "Locked",
    "cancel":     "Cancelled",
}

# ========= COLUMN ORDER ==========
COLUMNS = [
    "Currency",
    "Order Reference",
    "Vendor",
    "Product Name",
    "Product Reference",
    "Quantity",
    "Received Qty",
    "Unit Price",
    "Total",
    "Status",
    "Shipment Mode",
    "Payment Terms",
    "Created on",
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

# ========= FETCH PO ORDER LINES ==========
def fetch_po_local(company_id, cname):
    ctx = {
        "lang": "en_US", "tz": "Asia/Dhaka", "uid": USER_ID,
        "allowed_company_ids": [company_id], "bin_size": True,
        "current_company_id": company_id,
    }

    line_spec = {
        "currency_id": {"fields": {"display_name": {}}},
        "order_id": {
            "fields": {
                "name": {},
                "partner_id": {"fields": {"display_name": {}}},
                "state": {},
                "shipment_mode": {"fields": {"display_name": {}}},
                "payment_term_id": {"fields": {"display_name": {}}},
            }
        },
        "product_id": {
            "fields": {
                "name": {},
                "default_code": {},
            }
        },
        "product_qty": {},
        "qty_received": {},
        "price_unit": {},
        "price_total": {},
        "create_date": {},
    }

    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "purchase.order.line",
            "method": "web_search_read",
            "args": [],
            "kwargs": {
                "specification": line_spec,
                "offset": 0,
                "order": "create_date DESC",
                "limit": 10000,
                "context": ctx,
                "count_limit": 100001,
                "domain": [["order_id.company_id", "=", company_id]],
            },
        },
    }

    r = retry_request(
        session.post,
        f"{ODOO_URL}/web/dataset/call_kw/purchase.order.line/web_search_read",
        json=payload,
    )
    r.raise_for_status()

    try:
        data = r.json()["result"]["records"]
    except Exception as e:
        print(f"❌ {cname}: Failed to parse response: {e}")
        print(r.text[:2000])
        return []

    print(f"📋 {cname}: {len(data)} order lines fetched")

    def map_record(rec):
        order = rec.get("order_id") or {}
        product = rec.get("product_id") or {}
        state_raw = order.get("state", "")

        return {
            "Currency":          (rec.get("currency_id") or {}).get("display_name", ""),
            "Order Reference":   order.get("name", ""),
            "Vendor":            (order.get("partner_id") or {}).get("display_name", ""),
            "Product Name":      product.get("name", ""),
            "Product Reference": product.get("default_code", "") or "",
            "Quantity":          rec.get("product_qty", ""),
            "Received Qty":      rec.get("qty_received", ""),
            "Unit Price":        rec.get("price_unit", ""),
            "Total":             rec.get("price_total", ""),
            "Status":            STATE_MAP.get(state_raw, state_raw),
            "Shipment Mode":     (order.get("shipment_mode") or {}).get("display_name", ""),
            "Payment Terms":     (order.get("payment_term_id") or {}).get("display_name", ""),
            "Created on":        rec.get("create_date", "") or "",
        }

    all_rows = [map_record(rec) for rec in data]
    print(f"📊 {cname}: {len(all_rows)} rows mapped")

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

    company_id = 3
    cname = "Metal Trims"

    if switch_company(company_id):
        records = fetch_po_local(company_id, cname)

        if records:
            df = pd.DataFrame(records, columns=COLUMNS)
            output_file = f"po_local_{today.isoformat()}.xlsx"
            df.to_excel(output_file, index=False)
            print(f"📂 Saved: {output_file}")

            # ========= GOOGLE SHEETS ==========
            try:
                client = get_gspread_client()
                sheet = client.open_by_key(SHEET_KEY)
                worksheet = sheet.worksheet(WORKSHEET_NAME)
                worksheet.batch_clear(["A:M"])
                set_with_dataframe(worksheet, df)
                print(f"✅ Data pasted to Google Sheets → '{WORKSHEET_NAME}'")
            except Exception as e:
                import traceback
                print(f"❌ Error while pasting to Google Sheets: {e}")
                traceback.print_exc()
        else:
            print(f"❌ No data fetched for {cname}")
