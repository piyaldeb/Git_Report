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

import calendar
from datetime import date, timedelta
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

SHEET_KEY = "1_UctuxmWdo_zkFyU1q2v5uIB3k_ISweBR7lWZGuynZM"

COMPANY_ID = 3
COMPANY_NAME = "Metal Trims"

PAGE_SIZE = 500

today = date.today()
session = requests.Session()
USER_ID = None

# ========= DATE LOGIC ==========
# Report covers the previous full month.
# Today is in May -> from = Apr 1, to = Apr 30 (last day of April).
def previous_month_range(ref_date):
    """Return (first-of-prev-month, last-of-current-month) — matches HAR window."""
    first_of_this_month = ref_date.replace(day=1)
    last_of_prev_month = first_of_this_month - timedelta(days=1)
    first_of_prev_month = last_of_prev_month.replace(day=1)
    last_of_this_month = first_of_this_month.replace(
        day=calendar.monthrange(ref_date.year, ref_date.month)[1]
    )
    return first_of_prev_month, last_of_this_month, last_of_prev_month

FROM_DATE_OBJ, TO_DATE_OBJ, REPORT_MONTH_LAST = previous_month_range(today)

ENV_FROM = os.getenv("FROM_DATE")
if ENV_FROM:
    FROM_DATE_OBJ = date.fromisoformat(ENV_FROM)
    REPORT_MONTH_LAST = FROM_DATE_OBJ.replace(
        day=calendar.monthrange(FROM_DATE_OBJ.year, FROM_DATE_OBJ.month)[1]
    )
FROM_DATE = FROM_DATE_OBJ.isoformat()
TO_DATE = os.getenv("TO_DATE") or TO_DATE_OBJ.isoformat()

# Rows are kept only when Receive Date falls inside the report month
# (FROM_DATE_OBJ .. REPORT_MONTH_LAST); the wizard window is wider on purpose.

# Worksheet per Po Type, like "Jul_import" / "Jul_local"
MONTH_ABBR = calendar.month_abbr[FROM_DATE_OBJ.month]  # Jan..Dec
PO_TYPE_TABS = {
    "import": os.getenv("STOCK_WORKSHEET") or f"{MONTH_ABBR}_import",
    "local":  f"{MONTH_ABBR}_local",
}

# ========= TARGET COLUMN ORDER (matches live sheet — 29 cols A..AC, blank at U) ==========
# "_blank1" is a placeholder so pandas keeps it unique;
# it's rewritten to "" right before pasting to Sheets.
COLUMNS = [
    "Product Type", "Category", "item name", "PO", "Invoice", "Receive Date",
    "Incoterm", "Receive Quantity", "Receive Value", "Shipment Mode",
    "Vendor", "Closing Quantity",
    "invoice date", "Closing Value", "Issue Quantity", "Issue Value",
    "Invoice/Purchase Orders/Created on", "Item Code", "Landed Cost", "Opening Value",
    "_blank1",  # U
    "Po Type", "Price", "Pur Price", "Rejected", "Unit", "Item Type",
    "ETD", "ETA",
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

# ========= RETRY ==========
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
    raise Exception("❌ Login failed")

# ========= SWITCH COMPANY ==========
def switch_company(company_id):
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

# ========= CREATE WIZARD ==========
def create_stock_wizard(company_id, from_date, to_date):
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "stock.forecast.report",
            "method": "web_save",
            "args": [[], {
                "report_type": "rmstock",
                "report_for": "rm",
                "all_iteam_list": [],
                "from_date": from_date,
                "to_date": to_date,
            }],
            "kwargs": {
                "context": {
                    "lang": "en_US", "tz": "Asia/Dhaka", "uid": USER_ID,
                    "allowed_company_ids": [company_id],
                },
                "specification": {
                    "report_type": {},
                    "report_for": {},
                    "all_iteam_list": {"fields": {"display_name": {}}},
                    "from_date": {},
                    "to_date": {},
                },
            },
        },
    }
    r = retry_request(
        session.post,
        f"{ODOO_URL}/web/dataset/call_kw/stock.forecast.report/web_save",
        json=payload,
    )
    result = r.json().get("result", [])
    if isinstance(result, list) and result:
        wiz_id = result[0]["id"]
        print(f"🪄 Stock wizard {wiz_id} created (company {company_id}, {from_date} → {to_date})")
        return wiz_id
    raise Exception(f"❌ Failed to create stock wizard: {r.text[:300]}")

def compute_stock(company_id, wizard_id, max_retries=4, backoff=30):
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "stock.forecast.report",
            "method": "print_date_wise_stock_register",
            "args": [[wizard_id]],
            "kwargs": {"context": {
                "lang": "en_US", "tz": "Asia/Dhaka", "uid": USER_ID,
                "allowed_company_ids": [company_id],
            }},
        },
    }
    # The wizard SQL can hit transient SerializationFailure when the
    # every-30-min report jobs touch the same tables — retry, don't give up.
    for attempt in range(1, max_retries + 1):
        r = retry_request(session.post, f"{ODOO_URL}/web/dataset/call_button", json=payload)
        err = r.json().get("error")
        if not err:
            print(f"⚡ Stock register computed for wizard {wizard_id}")
            return True
        print(f"⚠️ Compute attempt {attempt}/{max_retries} failed: {str(err)[:200]}")
        if attempt < max_retries:
            print(f"⏳ Retrying compute in {backoff} seconds...")
            time.sleep(backoff)
    print("❌ Compute failed after all retries")
    return False

# ========= FETCH STOCK ROWS (paginated) ==========
def fetch_stock_rows(company_id, wizard_id):
    spec = {
        "parent_category":   {"fields": {"display_name": {}}},
        "product_category":  {"fields": {"display_name": {}}},
        "classification_id": {"fields": {"display_name": {}}},
        "product_type":      {"fields": {"display_name": {}}},
        "item_category":     {"fields": {"display_name": {}}},
        "product_id":        {"fields": {"display_name": {}}},
        "pr_code":           {},
        "product_uom":       {"fields": {"display_name": {}}},
        "lot_id":            {"fields": {"display_name": {}}},
        "location":          {"fields": {"display_name": {}}},
        "rejected":          {},
        "lot_price":         {},
        "pur_price":         {},
        "landed_cost":       {},
        "opening_qty":       {},
        "opening_value":     {},
        "receive_date":      {},
        "receive_qty":       {},
        "receive_value":     {},
        "issue_qty":         {},
        "issue_value":       {},
        "cloing_qty":        {},
        "cloing_value":      {},
        "shipment_mode":     {},
        "po_type":           {},
        "partner_id":        {"fields": {"display_name": {}}},
        "po_number":         {},
    }
    ctx = {
        "lang": "en_US", "tz": "Asia/Dhaka", "uid": USER_ID,
        "allowed_company_ids": [company_id],
        "bin_size": True,
        "active_model": "stock.forecast.report",
        "active_id": wizard_id,
        "active_ids": [wizard_id],
        "current_company_id": company_id,
    }

    all_records = []
    offset = 0
    while True:
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "stock.opening.closing",
                "method": "web_search_read",
                "args": [],
                "kwargs": {
                    "specification": spec,
                    "offset": offset,
                    "order": "",
                    "limit": PAGE_SIZE,
                    "context": ctx,
                    "count_limit": 100001,
                    "domain": [["product_id.categ_id.complete_name", "ilike", "All / RM"]],
                },
            },
        }
        r = retry_request(
            session.post,
            f"{ODOO_URL}/web/dataset/call_kw/stock.opening.closing/web_search_read",
            json=payload,
        )
        result = r.json().get("result", {})
        recs = result.get("records", []) or []
        total = result.get("length", len(recs) + offset)
        all_records.extend(recs)
        print(f"📊 Fetched {len(all_records)}/{total} stock rows")
        if len(recs) < PAGE_SIZE or len(all_records) >= total:
            break
        offset += PAGE_SIZE

    return all_records

# ========= FETCH LOT INVOICE DATES ==========
def fetch_lot_dates(company_id, lot_ids):
    """lot_id.invoice_date for the 'invoice date' column."""
    if not lot_ids:
        return {}
    ids = list({i for i in lot_ids if i})
    out = {}
    chunk = 500
    ctx = {"lang": "en_US", "tz": "Asia/Dhaka", "uid": USER_ID, "allowed_company_ids": [company_id]}
    for i in range(0, len(ids), chunk):
        sub = ids[i:i + chunk]
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "stock.lot",
                "method": "web_search_read",
                "args": [],
                "kwargs": {
                    "specification": {
                        "name": {},
                        "create_date": {},
                    },
                    "offset": 0,
                    "order": "id ASC",
                    "limit": len(sub) + 10,
                    "context": ctx,
                    "count_limit": 100001,
                    "domain": [["id", "in", sub]],
                },
            },
        }
        try:
            r = retry_request(session.post, f"{ODOO_URL}/web/dataset/call_kw/stock.lot/web_search_read", json=payload)
            for rec in r.json().get("result", {}).get("records", []) or []:
                out[rec["id"]] = {
                    "invoice_date": (rec.get("create_date") or "")[:10],
                    "create_date":  (rec.get("create_date") or "")[:10],
                }
        except Exception as e:
            print(f"⚠️ stock.lot lookup failed: {e}")
            break
    print(f"📦 Lot dates fetched: {len(out)}")
    return out

# ========= FETCH PO CREATED DATES ==========
def fetch_po_created(company_id, po_names):
    if not po_names:
        return {}
    names = list({n for n in po_names if n})
    out = {}
    chunk = 500
    ctx = {"lang": "en_US", "tz": "Asia/Dhaka", "uid": USER_ID, "allowed_company_ids": [company_id]}
    for i in range(0, len(names), chunk):
        sub = names[i:i + chunk]
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "purchase.order",
                "method": "web_search_read",
                "args": [],
                "kwargs": {
                    "specification": {
                        "name": {},
                        "create_date": {},
                        "date_order": {},
                        "incoterm_id": {"fields": {"display_name": {}}},
                    },
                    "offset": 0,
                    "order": "id ASC",
                    "limit": len(sub) + 10,
                    "context": ctx,
                    "count_limit": 100001,
                    "domain": [["name", "in", sub]],
                },
            },
        }
        try:
            r = retry_request(session.post, f"{ODOO_URL}/web/dataset/call_kw/purchase.order/web_search_read", json=payload)
            for rec in r.json().get("result", {}).get("records", []) or []:
                nm = rec.get("name") or ""
                if nm:
                    out[nm] = {
                        "created_on": (rec.get("create_date") or rec.get("date_order") or "")[:10],
                        "incoterm":   (rec.get("incoterm_id") or {}).get("display_name", "") or "",
                    }
        except Exception as e:
            print(f"⚠️ purchase.order lookup failed: {e}")
            break
    print(f"📦 PO created dates fetched: {len(out)}")
    return out

# ========= FETCH TRANSIT INFO (invoice date / ETD / ETA) ==========
def fetch_transit_info(company_id, invoice_numbers):
    """transit.model rows keyed by invoice_number → invoice_date, etd, eta."""
    if not invoice_numbers:
        return {}
    names = list({n.strip() for n in invoice_numbers if n and n.strip()})
    out = {}
    chunk = 500
    ctx = {"lang": "en_US", "tz": "Asia/Dhaka", "uid": USER_ID,
           "allowed_company_ids": [company_id], "bin_size": True,
           "current_company_id": company_id}
    for i in range(0, len(names), chunk):
        sub = names[i:i + chunk]
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "transit.model",
                "method": "web_search_read",
                "args": [],
                "kwargs": {
                    "specification": {
                        "invoice_number": {},
                        "invoice_date": {},
                        "etd": {},
                        "eta": {},
                        "state": {},
                    },
                    "offset": 0,
                    "order": "id ASC",
                    "limit": len(sub) + 10,
                    "context": ctx,
                    "count_limit": 100001,
                    "domain": [["invoice_number", "in", sub]],
                },
            },
        }
        try:
            r = retry_request(session.post, f"{ODOO_URL}/web/dataset/call_kw/transit.model/web_search_read", json=payload)
            for rec in r.json().get("result", {}).get("records", []) or []:
                nm = (rec.get("invoice_number") or "").strip()
                if not nm:
                    continue
                # Prefer non-cancelled shipments when an invoice number repeats
                if nm in out and rec.get("state") == "cancel":
                    continue
                out[nm] = {
                    "invoice_date": (rec.get("invoice_date") or "")[:10],
                    "etd":          (rec.get("etd") or "")[:10],
                    "eta":          (rec.get("eta") or "")[:10],
                }
        except Exception as e:
            print(f"⚠️ transit.model lookup failed: {e}")
            break
    print(f"🚢 Transit info fetched: {len(out)} invoices matched")
    return out

# ========= MAP TO TARGET COLUMNS ==========
def map_records(records, lot_date_map, po_created_map, transit_map):
    rows = []
    for rec in records:
        product = rec.get("product_id") or {}
        lot = rec.get("lot_id") or {}
        product_dn = product.get("display_name", "") or ""

        # item name = product display_name without "[code] " prefix
        if product_dn.startswith("[") and "]" in product_dn:
            item_name = product_dn[product_dn.index("]") + 1:].lstrip()
            if item_name.startswith(" "):
                item_name = item_name[1:]
        else:
            item_name = product_dn

        lot_id = lot.get("id")
        lot_dates = lot_date_map.get(lot_id, {}) if lot_id else {}

        po_no = rec.get("po_number") or ""
        po_info = po_created_map.get(po_no, {}) if po_no else {}
        po_created = po_info.get("created_on", "")
        po_incoterm = po_info.get("incoterm", "")

        invoice_no = (lot.get("display_name") or "").strip()
        transit = transit_map.get(invoice_no, {}) if invoice_no else {}
        # Real invoice date lives on transit.model; lot create_date is only a fallback
        invoice_date = transit.get("invoice_date") or (lot_dates.get("invoice_date") or "")[:10]

        product_type_name = (rec.get("product_type") or {}).get("display_name", "")
        rows.append([
            product_type_name,                                              # A: Product Type
            (rec.get("product_category") or {}).get("display_name", ""),    # B: Category
            item_name,                                                       # C: item name
            po_no,                                                           # D: PO
            (rec.get("lot_id") or {}).get("display_name", ""),               # E: Invoice
            rec.get("receive_date") or "",                                   # F: Receive Date
            po_incoterm,                                                     # G: Incoterm (from purchase.order)
            rec.get("receive_qty") if rec.get("receive_qty") is not None else "",   # H
            rec.get("receive_value") if rec.get("receive_value") is not None else "",# I
            rec.get("shipment_mode") or "",                                  # J: Shipment Mode
            (rec.get("partner_id") or {}).get("display_name", ""),           # K: Vendor
            rec.get("cloing_qty") if rec.get("cloing_qty") is not None else "",     # L
            invoice_date,                                                    # M: invoice date (transit.model)
            rec.get("cloing_value") if rec.get("cloing_value") is not None else "", # N
            rec.get("issue_qty") if rec.get("issue_qty") is not None else "",       # O
            rec.get("issue_value") if rec.get("issue_value") is not None else "",   # P
            po_created,                                                       # Q: Created on
            rec.get("pr_code") or "",                                        # R: Item Code (Odoo internal ref)
            rec.get("landed_cost") if rec.get("landed_cost") is not None else "",   # S
            rec.get("opening_value") if rec.get("opening_value") is not None else "",# T
            "",                                                              # U: blank
            rec.get("po_type") or "",                                        # V: Po Type
            rec.get("lot_price") if rec.get("lot_price") is not None else "",       # W: Price
            rec.get("pur_price") if rec.get("pur_price") is not None else "",       # X: Pur Price
            rec.get("rejected") or "",                                       # Y: Rejected
            (rec.get("product_uom") or {}).get("display_name", ""),          # Z: Unit
            (rec.get("item_category") or {}).get("display_name", ""),        # AA: Item Type
            transit.get("etd", ""),                                          # AB: ETD
            transit.get("eta", ""),                                          # AC: ETA
        ])
    return rows

# ========= MAIN ==========
if __name__ == "__main__":
    print(f"📅 Reporting window: {FROM_DATE} → {TO_DATE}  (tabs: {', '.join(PO_TYPE_TABS.values())})")

    login()
    if not switch_company(COMPANY_ID):
        sys.exit(1)

    wiz_id = create_stock_wizard(COMPANY_ID, FROM_DATE, TO_DATE)
    if not compute_stock(COMPANY_ID, wiz_id):
        sys.exit(1)
    records = fetch_stock_rows(COMPANY_ID, wiz_id)

    if not records:
        print(f"❌ No stock rows fetched for {COMPANY_NAME}")
        sys.exit(1)

    lot_ids = [(r.get("lot_id") or {}).get("id") for r in records]
    po_names = [r.get("po_number") for r in records]
    invoice_nos = [(r.get("lot_id") or {}).get("display_name") for r in records]
    lot_date_map = fetch_lot_dates(COMPANY_ID, lot_ids)
    po_created_map = fetch_po_created(COMPANY_ID, po_names)
    transit_map = fetch_transit_info(COMPANY_ID, invoice_nos)

    rows = map_records(records, lot_date_map, po_created_map, transit_map)
    df = pd.DataFrame(rows, columns=COLUMNS)

    # ========= SHARED FILTERS =========
    recv_qty_num = pd.to_numeric(df["Receive Quantity"], errors="coerce").fillna(0)
    before = len(df)
    df = df[recv_qty_num != 0].reset_index(drop=True)
    print(f"🔎 Receive Quantity != 0 filter: {before} → {len(df)} rows")

    # Keep only receipts of the report month — the wizard window extends into
    # the current month, which otherwise leaks current-month rows into the sheet.
    recv_dt = pd.to_datetime(df["Receive Date"], errors="coerce")
    before = len(df)
    df = df[
        (recv_dt >= pd.Timestamp(FROM_DATE_OBJ)) & (recv_dt <= pd.Timestamp(REPORT_MONTH_LAST))
    ].reset_index(drop=True)
    print(f"🔎 Receive Date in {FROM_DATE_OBJ} → {REPORT_MONTH_LAST} filter: {before} → {len(df)} rows")

    po_type_norm = df["Po Type"].astype(str).str.strip().str.lower()
    print(f"📋 Po Type values: {po_type_norm.value_counts().to_dict()}")

    client = None
    try:
        client = get_gspread_client()
        sheet = client.open_by_key(SHEET_KEY)
    except Exception as e:
        print(f"❌ Google Sheets unavailable: {e}")

    # ========= ONE TAB PER PO TYPE =========
    for po_type, tab_name in PO_TYPE_TABS.items():
        sub = df[po_type_norm == po_type].reset_index(drop=True)
        print(f"🔎 Po Type={po_type}: {len(sub)} rows → tab '{tab_name}'")

        sub_out = sub.rename(columns={"_blank1": ""})
        output_file = f"stock_report_{COMPANY_NAME.lower().replace(' ', '_')}_{po_type}_{FROM_DATE}_{TO_DATE}.xlsx"
        sub_out.to_excel(output_file, index=False)
        print(f"📂 Saved: {output_file}  ({len(sub_out)} rows)")

        if client is None:
            continue
        try:
            try:
                worksheet = sheet.worksheet(tab_name)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = sheet.add_worksheet(title=tab_name, rows=max(len(sub_out) + 50, 100), cols=len(COLUMNS) + 2)
                print(f"➕ Created new worksheet '{tab_name}'")
            # Clear wide (beyond Z) to wipe any leftover/shifted columns from old pastes
            worksheet.batch_clear(["A:AH"])
            set_with_dataframe(worksheet, sub_out)
            # Force numeric format on quantity/value columns so Sheets doesn't
            # auto-render integers as 1900-era dates.
            num_fmt = {"numberFormat": {"type": "NUMBER", "pattern": "0.########"}}
            for col in ("H:I", "L:L", "N:P", "S:T", "W:X"):
                worksheet.format(col, num_fmt)
            print(f"✅ Data pasted to Google Sheets → '{tab_name}'")
        except Exception as e:
            import traceback
            print(f"❌ Error while pasting '{tab_name}': {e}")
            traceback.print_exc()

    print("✅ Done")
