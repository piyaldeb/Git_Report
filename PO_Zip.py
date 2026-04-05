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
WORKSHEET_NAME = "PO_Zip"

today = date.today()

session = requests.Session()
USER_ID = None

# ========= COLUMN ORDER ==========
COLUMNS = [
    "Company", "Created by", "Created on", "Currency.", "Gate Entry",
    "Last Approver", "Next Approver", "Order Reference", "Order Status",
    "PI No.", "Priority", "Status", "Total", "Vendor", "Vendor Reference",
]

PRIORITY_MAP = {"0": "Normal", "1": "Urgent"}
STATE_MAP = {
    "draft":      "RFQ",
    "sent":       "RFQ Sent",
    "to approve": "To Approve",
    "purchase":   "Purchase Order",
    "done":       "Locked",
    "cancel":     "Cancelled",
}

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

# ========= FETCH PURCHASE ORDERS ==========
def fetch_po(company_id, cname):
    specification = {
        "company_id":            {"fields": {"display_name": {}}},
        "create_uid":            {"fields": {"display_name": {}}},
        "create_date":           {},
        "x_studio_currency":     {"fields": {"display_name": {}}},
        "x_studio_gate_entry":   {},
        "last_approver":         {},
        "next_approver":         {},
        "name":                  {},
        "x_studio_order_status": {},
        "x_studio_pi_no":        {},
        "priority":              {},
        "state":                 {},
        "amount_total":          {},
        "partner_id":            {"fields": {"display_name": {}}},
        "partner_ref":           {},
    }
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "purchase.order",
            "method": "web_search_read",
            "args": [],
            "kwargs": {
                "specification": specification,
                "offset": 0,
                "order": "create_date DESC",
                "limit": 15000,
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
        f"{ODOO_URL}/web/dataset/call_kw/purchase.order/web_search_read",
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
        gate = rec.get("x_studio_gate_entry")
        total = rec.get("amount_total")
        return {
            "Company":          (rec.get("company_id") or {}).get("display_name", ""),
            "Created by":       (rec.get("create_uid") or {}).get("display_name", ""),
            "Created on":       rec.get("create_date") or "",
            "Currency.":        (rec.get("x_studio_currency") or {}).get("display_name", ""),
            "Gate Entry":       gate if gate not in (None, False) else "",
            "Last Approver":    rec.get("last_approver") or "",
            "Next Approver":    rec.get("next_approver") or "",
            "Order Reference":  rec.get("name") or "",
            "Order Status":     rec.get("x_studio_order_status") or "",
            "PI No.":           rec.get("x_studio_pi_no") or "",
            "Priority":         PRIORITY_MAP.get(str(rec.get("priority") or ""), rec.get("priority") or ""),
            "Status":           STATE_MAP.get(rec.get("state") or "", rec.get("state") or ""),
            "Total":            total if total is not None else "",
            "Vendor":           (rec.get("partner_id") or {}).get("display_name", ""),
            "Vendor Reference": rec.get("partner_ref") or "",
        }

    all_rows = [map_record(rec) for rec in data]
    print(f"📊 {cname}: {len(all_rows)} purchase orders fetched")

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
        records = fetch_po(company_id, cname)

        if records:
            df = pd.DataFrame(records, columns=COLUMNS)
            output_file = f"po_zip_{today.isoformat()}.xlsx"
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
            print(f"❌ No PO data fetched for {cname}")
