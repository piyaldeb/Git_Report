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

SHEET_KEY = "1ho7ihCKKCzg7de9hvuesledI7tdCpWjOiz9-EFGgIuI"
WORKSHEET_NAME = "GIT_REPORT_RAW"

today = date.today()

session = requests.Session()
USER_ID = None

# ========= COLUMN ORDER ==========
COLUMNS = [
    "Company", "PO No", "PO Apprvd Stat", "P Cat", "P Type",
    "Inv Month", "Vendor", "Item Details", "Odoo Code", "Inv No", "Inv Date",
    "Inv Quantity", "Inv Value", "Adjust", "Pmt Term", "Ship Mode",
    "Inco", "Booked Ship ETD", "Booked Ship ETA", "ETD", "ETA",
    "BL Number", "BL Date", "LC Number", "LC Date",
    "I/H Plan Month", "Inhoused Date", "I/H Status",
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

# ========= FETCH PRODUCT CLASSIFICATION ==========
def fetch_product_map(odoo_codes, company_id):
    """Batch-fetch categ_type (P Cat) and classification_id (P Type) from product.template by default_code."""
    if not odoo_codes:
        return {}

    codes = [c for c in odoo_codes if c]
    if not codes:
        return {}

    # Fetch in chunks to avoid too-large domains
    chunk_size = 500
    product_map = {}

    for i in range(0, len(codes), chunk_size):
        chunk = codes[i:i + chunk_size]
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "product.template",
                "method": "web_search_read",
                "args": [],
                "kwargs": {
                    "specification": {
                        "default_code": {},
                        "categ_type":        {"fields": {"display_name": {}}},
                        "classification_id": {"fields": {"display_name": {}}},
                    },
                    "offset": 0,
                    "order": "id ASC",
                    "limit": len(chunk) + 10,
                    "context": {
                        "lang": "en_US",
                        "tz": "Asia/Dhaka",
                        "uid": USER_ID,
                        "allowed_company_ids": [company_id],
                    },
                    "count_limit": 100001,
                    "domain": [["default_code", "in", chunk]],
                },
            },
        }
        r = retry_request(
            session.post,
            f"{ODOO_URL}/web/dataset/call_kw/product.template/web_search_read",
            json=payload,
        )
        for rec in r.json().get("result", {}).get("records", []):
            code = rec.get("default_code") or ""
            if code:
                product_map[code] = {
                    "P Cat":  (rec.get("categ_type") or {}).get("display_name", ""),
                    "P Type": (rec.get("classification_id") or {}).get("display_name", ""),
                }

    print(f"📦 Product map built: {len(product_map)} products matched")
    return product_map

# ========= FETCH GOODS IN TRANSIT ==========
def fetch_git(company_id, cname):
    specification = {
        "company_id":    {"fields": {"display_name": {}}},
        "po_numbers":    {"fields": {
            "name":      {},
            "state":     {},
            "next_approver": {"fields": {"display_name": {}}},
        }},
        "vendor":        {"fields": {"display_name": {}}},
        "invoice_number": {},
        "invoice_date":  {},
        "subtotal":      {},
        "adjusted_state":{},
        "payment_term":  {"fields": {"display_name": {}}},
        "shipment_mode": {"fields": {"display_name": {}}},
        "inco_terms":    {"fields": {"display_name": {}}},
        "booked_etd":    {},
        "booked_eta":    {},
        "etd":           {},
        "eta":           {},
        "bl_number":     {},
        "bl_date":       {},
        "lc_number":     {},
        "lc_date":       {},
        "ih_plan":       {},
        "grn_date":      {},
        "state":         {},
        "line_ids":      {"fields": {
            "po_id":     {"fields": {}},
            "product_id":{"fields": {"display_name": {}, "default_code": {}}},
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
        print(r.text[:200])
        return []

    print(f"📊 {cname}: {len(data)} transit records fetched")

    # ---- Collect all Odoo codes for product lookup ----
    STATE_MAP = {
        "draft":      "Draft",
        "sent":       "RFQ Sent",
        "to approve": "To Approve",
        "purchase":   "Approved",
        "done":       "Locked",
        "cancel":     "Cancelled",
    }

    def _product_name(line):
        dn = (line.get("product_id") or {}).get("display_name", "") or ""
        if dn.startswith("[") and "]" in dn:
            return dn[dn.index("]") + 2:]
        return dn

    def _product_code(line):
        return (line.get("product_id") or {}).get("default_code", "") or ""

    all_codes = set()
    for record in data:
        for line in (record.get("line_ids") or []):
            code = _product_code(line)
            if code:
                all_codes.add(code)

    product_map = fetch_product_map(all_codes, company_id)

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
            "Company":         (record.get("company_id") or {}).get("display_name", ""),
            "Inv Month":       inv_month,
            "Vendor":          (record.get("vendor") or {}).get("display_name", ""),
            "Inv No":          record.get("invoice_number") or "",
            "Inv Date":        inv_date,
            "Inv Value":       record.get("subtotal") if record.get("subtotal") not in (None, False) else "",
            "Adjust":          record.get("adjusted_state") or "",
            "Pmt Term":        (record.get("payment_term") or {}).get("display_name", ""),
            "Ship Mode":       (record.get("shipment_mode") or {}).get("display_name", ""),
            "Inco":            (record.get("inco_terms") or {}).get("display_name", ""),
            "Booked Ship ETD": record.get("booked_etd") or "",
            "Booked Ship ETA": record.get("booked_eta") or "",
            "ETD":             record.get("etd") or "",
            "ETA":             record.get("eta") or "",
            "BL Number":       record.get("bl_number") or "",
            "BL Date":         record.get("bl_date") or "",
            "LC Number":       record.get("lc_number") or "",
            "LC Date":         record.get("lc_date") or "",
            "I/H Plan Month":  record.get("ih_plan") or "",
            "Inhoused Date":   record.get("grn_date") or "",
            "I/H Status":      record.get("state") or "",
        }

        def _po_apprvd_stat(p):
            if not p:
                return ""
            state = p.get("state", "")
            if state in ("purchase", "cancel", "done", ""):
                return STATE_MAP.get(state, state)
            return (
                (p.get("next_approver") or {}).get("display_name", "")
                or STATE_MAP.get(state, state)
            )

        def rows_for_po(p, po_lines):
            po_fields = {
                "PO No":          p.get("name", "") if p else "",
                "PO Apprvd Stat": _po_apprvd_stat(p),
                "P Cat":          "",   # filled below per product line
                "P Type":         "",   # filled below per product line
            }
            real = [l for l in po_lines if _product_code(l)]
            if not real:
                return [{**base, **po_fields,
                         "Item Details": "", "Odoo Code": "", "Inv Quantity": ""}]
            rows = []
            for l in real:
                code = _product_code(l)
                prod_info = product_map.get(code, {})
                rows.append({
                    **base,
                    **po_fields,
                    "P Cat":        prod_info.get("P Cat", ""),
                    "P Type":       prod_info.get("P Type", ""),
                    "Item Details": _product_name(l),
                    "Odoo Code":    code,
                    "Inv Quantity": l.get("qty_in_transit") if l.get("qty_in_transit") not in (None, False) else "",
                })
            return rows

        if not pos:
            return rows_for_po(None, lines)

        rows = []
        for p in pos:
            rows.extend(rows_for_po(p, lines_by_po.get(p["id"], [])))
        return rows

    all_rows = [row for rec in data for row in expand(rec)]
    print(f"📊 {cname}: {len(all_rows)} rows expanded")

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
    login()

    company_id = 3
    cname = "Metal Trims"

    if not switch_company(company_id):
        sys.exit(1)

    records = fetch_git(company_id, cname)

    if records:
        df = pd.DataFrame(records, columns=COLUMNS)
        output_file = f"git_report_{today.isoformat()}.xlsx"
        df.to_excel(output_file, index=False)
        print(f"📂 Saved: {output_file}")

        # ========= GOOGLE SHEETS ==========
        try:
            client = get_gspread_client()
            sheet = client.open_by_key(SHEET_KEY)
            worksheet = sheet.worksheet(WORKSHEET_NAME)
            worksheet.batch_clear(["A:AB"])
            set_with_dataframe(worksheet, df)
            print(f"✅ Data pasted to Google Sheets → '{WORKSHEET_NAME}'")
        except Exception as e:
            import traceback
            print(f"❌ Error while pasting to Google Sheets: {e}")
            traceback.print_exc()
    else:
        print(f"❌ No GIT data fetched for {cname}")

    print(f"✅ Done")
