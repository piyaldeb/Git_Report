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
from collections import defaultdict
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
WORKSHEET_NAME = "Git_Zip_Raw"

today = date.today()

session = requests.Session()
USER_ID = None

# ========= COLUMN ORDER ==========
COLUMNS = [
    "BU", "PO No", "PO Apprvd Stat", "P Cat", "P Type",
    "Inv Month", "Vendor", "Item Details", "Odoo Code", "Inv No", "Inv Date",
    "Inv Quantity", "Inv Value", "Adjust", "Pmt Term", "Ship Mode",
    "Inco", "BL/AWB No", "BL/AWB Date", "Container No.", "Booked Ship ETD", "Booked Ship ETA",
    "ETD", "ETA", "I/H Plan Month", "I/H Date", "I/H Status",
    "Docs Rcvd Stat", "Delayed Days Shipment I/H", "Delayed Days on ETA",
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

# ========= FETCH GOODS IN TRANSIT ==========
def fetch_git(company_id, cname):
    specification = {
        "company_id":      {"fields": {"display_name": {}}},
        "po_numbers":      {"fields": {
            "name":        {},
            "state":       {},
            "itemtypes":   {"fields": {"display_name": {}}},
            "po_type":     {},
        }},
        "vendor":          {"fields": {"display_name": {}}},
        "invoice_number":  {},
        "invoice_date":    {},
        "subtotal":        {},
        "adjusted_state":  {},
        "payment_term":    {"fields": {"display_name": {}}},
        "shipment_mode":   {"fields": {"display_name": {}}},
        "inco_terms":      {"fields": {"display_name": {}}},
        "bl_number":       {},
        "bl_date":         {},
        "container_no":    {},
        "booked_etd":      {},
        "booked_eta":      {},
        "etd":             {},
        "eta":             {},
        "ih_plan":         {},
        "grn_date":        {},
        "state":           {},
        "line_ids":        {"fields": {
            "po_id":       {"fields": {}},
            "product_id":  {"fields": {"display_name": {}, "default_code": {}}},
            "qty_in_transit": {},
        }},
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
    except Exception as e:
        print(f"❌ {cname}: Failed to parse response: {e}")
        print(r.text[:2000])
        return []

    def _product_name(line):
        dn = (line.get("product_id") or {}).get("display_name", "") or ""
        if dn.startswith("[") and "]" in dn:
            return dn[dn.index("]") + 2:]
        return dn

    def _product_code(line):
        return (line.get("product_id") or {}).get("default_code", "") or ""

    def expand(record):
        pos   = record.get("po_numbers", []) or []
        lines = record.get("line_ids", []) or []

        inv_date = record.get("invoice_date") or ""
        try:
            inv_month = pd.to_datetime(inv_date).strftime("%b-%y") if inv_date else ""
        except Exception:
            inv_month = ""

        # Group lines by po_id
        lines_by_po = defaultdict(list)
        lines_no_po = []
        for line in lines:
            po_id = (line.get("po_id") or {}).get("id")
            if po_id:
                lines_by_po[po_id].append(line)
            else:
                lines_no_po.append(line)

        base = {
            "BU":                        (record.get("company_id") or {}).get("display_name", ""),
            "Inv Month":                 inv_month,
            "Vendor":                    (record.get("vendor") or {}).get("display_name", ""),
            "Inv No":                    record.get("invoice_number") or "",
            "Inv Date":                  inv_date,
            "Inv Value":                 record.get("subtotal") if record.get("subtotal") not in (None, False) else "",
            "Adjust":                    record.get("adjusted_state") or "",
            "Pmt Term":                  (record.get("payment_term") or {}).get("display_name", ""),
            "Ship Mode":                 (record.get("shipment_mode") or {}).get("display_name", ""),
            "Inco":                      (record.get("inco_terms") or {}).get("display_name", ""),
            "BL/AWB No":                 record.get("bl_number") or "",
            "BL/AWB Date":               record.get("bl_date") or "",
            "Container No.":             record.get("container_no") or "",
            "Booked Ship ETD":           record.get("booked_etd") or "",
            "Booked Ship ETA":           record.get("booked_eta") or "",
            "ETD":                       record.get("etd") or "",
            "ETA":                       record.get("eta") or "",
            "I/H Plan Month":            record.get("ih_plan") or "",
            "I/H Date":                  record.get("grn_date") or "",
            "I/H Status":                record.get("state") or "",
            "Docs Rcvd Stat":            "",
            "Delayed Days Shipment I/H": "",
            "Delayed Days on ETA":       "",
        }

        def rows_for_po(p, po_lines):
            po_fields = {
                "PO No":          p.get("name", "") if p else "",
                "PO Apprvd Stat": p.get("state", "") if p else "",
                "P Cat":          (p.get("itemtypes") or {}).get("display_name", "") if p else "",
                "P Type":         (p.get("po_type") or "") if p else "",
            }
            # Only real product lines (have a default_code)
            real = [l for l in po_lines if _product_code(l)]
            if not real:
                return [{**base, **po_fields,
                         "Item Details": "", "Odoo Code": "", "Inv Quantity": ""}]
            return [{
                **base, **po_fields,
                "Item Details": _product_name(l),
                "Odoo Code":    _product_code(l),
                "Inv Quantity": l.get("qty_in_transit") if l.get("qty_in_transit") not in (None, False) else "",
            } for l in real]

        if not pos:
            return rows_for_po(None, lines)

        rows = []
        for p in pos:
            rows.extend(rows_for_po(p, lines_by_po.get(p["id"], [])))
        return rows

    all_rows = [row for rec in data for row in expand(rec)]
    print(f"📊 {cname}: {len(all_rows)} rows expanded from {len(data)} transit records")

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
        records = fetch_git(company_id, cname)

        if records:
            df = pd.DataFrame(records, columns=COLUMNS)
            output_file = f"git_zip_{today.isoformat()}.xlsx"
            df.to_excel(output_file, index=False)
            print(f"📂 Saved: {output_file}")

            # ========= GOOGLE SHEETS ==========
            try:
                client = get_gspread_client()
                sheet = client.open_by_key(SHEET_KEY)
                worksheet = sheet.get_worksheet_by_id(440183221)
                worksheet.batch_clear(["A:AD"])
                set_with_dataframe(worksheet, df)
                print(f"✅ Data pasted to Google Sheets → '{WORKSHEET_NAME}'")
            except Exception as e:
                import traceback
                print(f"❌ Error while pasting to Google Sheets: {e}")
                traceback.print_exc()
        else:
            print(f"❌ No GIT data fetched for {cname}")
