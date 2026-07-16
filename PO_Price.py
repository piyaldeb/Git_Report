import requests
import json
import base64
import logging
import re
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

SHEET_KEY = "1kD4iCUqEAQsE_CLuv3dFSFNSjD2Hj2dTrE40deGZaK0"
WORKSHEET_NAME = "Po_Price_MT"

COMPANY_ID = 3
COMPANY_NAME = "Metal Trims"

PAGE_SIZE = 2000

today = date.today()

session = requests.Session()
USER_ID = None

# ========= COLUMN ORDER ==========
COLUMNS = [
    "Odoo Code",
    "Item Name",
    "Unit Price",
    "Currency",
    "PO No",
    "PO Date",
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
        print(f"🔑 Service account: {creds_dict.get('client_email', '?')} (share the sheet with this email as Editor)")
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

# ========= FETCH PO ORDER LINES (paginated) ==========
def fetch_po_lines(company_id, cname):
    ctx = {
        "lang": "en_US", "tz": "Asia/Dhaka", "uid": USER_ID,
        "allowed_company_ids": [company_id], "bin_size": True,
        "current_company_id": company_id,
    }

    # product_id is fetched id-only: nested product fields raise AccessError on
    # old lines whose product belongs to another company. Codes/names are
    # resolved afterwards in fetch_product_map(); "name" (line description)
    # is the fallback for products that stay unreadable.
    line_spec = {
        "currency_id": {"fields": {"display_name": {}}},
        "order_id": {
            "fields": {
                "name": {},
                "date_order": {},
            }
        },
        "product_id": {"fields": {}},
        "name": {},
        "price_unit": {},
        "create_date": {},
    }

    all_records = []
    offset = 0
    while True:
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "purchase.order.line",
                "method": "web_search_read",
                "args": [],
                "kwargs": {
                    "specification": line_spec,
                    "offset": offset,
                    "order": "create_date DESC",
                    "limit": PAGE_SIZE,
                    "context": ctx,
                    "count_limit": 1000001,
                    "domain": [
                        ["order_id.company_id", "=", company_id],
                        ["order_id.state", "!=", "cancel"],
                        ["display_type", "=", False],
                        ["product_id", "!=", False],
                    ],
                },
            },
        }
        r = retry_request(
            session.post,
            f"{ODOO_URL}/web/dataset/call_kw/purchase.order.line/web_search_read",
            json=payload,
        )
        resp = r.json()
        if "error" in resp:
            raise Exception(f"❌ web_search_read failed at offset {offset}: {json.dumps(resp['error'])[:500]}")
        result = resp.get("result", {})
        recs = result.get("records", []) or []
        total = result.get("length", len(recs) + offset)
        all_records.extend(recs)
        print(f"📋 {cname}: fetched {len(all_records)}/{total} order lines")
        if len(recs) < PAGE_SIZE or len(all_records) >= total:
            break
        offset += PAGE_SIZE

    return all_records

# ========= RESOLVE PRODUCT CODES/NAMES ==========
def fetch_product_map(company_id, product_ids):
    """id -> {code, name}; search silently drops products the user can't read."""
    ids = list({i for i in product_ids if i})
    out = {}
    chunk = 1000
    ctx = {
        "lang": "en_US", "tz": "Asia/Dhaka", "uid": USER_ID,
        "allowed_company_ids": [company_id],
        "active_test": False,  # include archived products
    }
    for i in range(0, len(ids), chunk):
        sub = ids[i:i + chunk]
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "product.product",
                "method": "web_search_read",
                "args": [],
                "kwargs": {
                    "specification": {"default_code": {}, "name": {}},
                    "offset": 0,
                    "order": "id ASC",
                    "limit": len(sub) + 10,
                    "context": ctx,
                    "count_limit": 1000001,
                    "domain": [["id", "in", sub]],
                },
            },
        }
        r = retry_request(
            session.post,
            f"{ODOO_URL}/web/dataset/call_kw/product.product/web_search_read",
            json=payload,
        )
        resp = r.json()
        if "error" in resp:
            raise Exception(f"❌ product.product lookup failed: {json.dumps(resp['error'])[:500]}")
        for rec in resp.get("result", {}).get("records", []) or []:
            out[rec["id"]] = {
                "code": rec.get("default_code") or "",
                "name": rec.get("name") or "",
            }
    print(f"📦 Products resolved: {len(out)}/{len(ids)}")
    return out

# ========= MAP + DEDUPE (latest PO per Odoo code) ==========
def build_price_list(records, product_map):
    rows = []
    for rec in records:
        order = rec.get("order_id") or {}
        prod_id = (rec.get("product_id") or {}).get("id")
        prod = product_map.get(prod_id)
        if prod:
            code, item_name = prod["code"], prod["name"]
        else:
            # Product unreadable — fall back to the line description,
            # parsing an "[CODE] Name" prefix when present.
            desc = (rec.get("name") or "").strip()
            if desc.startswith("[") and "]" in desc:
                code = desc[1:desc.index("]")]
                item_name = desc[desc.index("]") + 1:].strip()
            else:
                code, item_name = "", desc
        rows.append({
            "Odoo Code":  code,
            "Item Name":  item_name,
            "Unit Price": rec.get("price_unit", ""),
            "Currency":   (rec.get("currency_id") or {}).get("display_name", ""),
            "PO No":      order.get("name", ""),
            "PO Date":    (order.get("date_order") or "")[:10],
            "_sort_date": order.get("date_order") or rec.get("create_date") or "",
            "_line_id":   rec.get("id") or 0,
            "_prod_id":   prod_id or 0,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    total_lines = len(df)

    # Latest PO first; tie-break on line id so the newest line of the same PO wins.
    df = df.sort_values(by=["_sort_date", "_line_id"], ascending=[False, False], kind="mergesort")

    # One row per product: dedupe by Odoo code (fall back to product id when code is blank).
    df["_dedupe_key"] = df["Odoo Code"].astype(str).str.strip()
    blank = df["_dedupe_key"] == ""
    df.loc[blank, "_dedupe_key"] = "prod_" + df.loc[blank, "_prod_id"].astype(str)
    df = df.drop_duplicates(subset="_dedupe_key", keep="first")

    print(f"🔎 Deduped: {total_lines} order lines → {len(df)} products (latest PO price each)")

    # Coded items first (alphabetical), products without a code at the bottom.
    df["_no_code"] = df["Odoo Code"].astype(str).str.strip() == ""
    df = df.sort_values(by=["_no_code", "Odoo Code"], kind="mergesort").reset_index(drop=True)
    df = df[COLUMNS]

    # Strip control characters that openpyxl/Sheets reject.
    illegal = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
    for col in df.columns:
        df[col] = df[col].map(lambda v: illegal.sub("", v) if isinstance(v, str) else v)
    return df

# ========= MAIN ==========
if __name__ == "__main__":
    login()
    if not switch_company(COMPANY_ID):
        sys.exit(1)

    records = fetch_po_lines(COMPANY_ID, COMPANY_NAME)
    if not records:
        print(f"❌ No order lines fetched for {COMPANY_NAME}")
        sys.exit(0)

    product_ids = [(r.get("product_id") or {}).get("id") for r in records]
    product_map = fetch_product_map(COMPANY_ID, product_ids)

    df = build_price_list(records, product_map)
    if df.empty:
        print("❌ No rows after mapping")
        sys.exit(0)

    output_file = f"po_price_{COMPANY_NAME.lower().replace(' ', '_')}_{today.isoformat()}.xlsx"
    df.to_excel(output_file, index=False)
    print(f"📂 Saved: {output_file}  ({len(df)} rows)")

    # ========= GOOGLE SHEETS ==========
    try:
        client = get_gspread_client()
        sheet = client.open_by_key(SHEET_KEY)
        try:
            worksheet = sheet.worksheet(WORKSHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=WORKSHEET_NAME, rows=max(len(df) + 50, 100), cols=len(COLUMNS) + 2)
            print(f"➕ Created new worksheet '{WORKSHEET_NAME}'")
        worksheet.batch_clear(["A:F"])
        set_with_dataframe(worksheet, df)
        num_fmt = {"numberFormat": {"type": "NUMBER", "pattern": "0.########"}}
        worksheet.format("C:C", num_fmt)
        print(f"✅ Data pasted to Google Sheets → '{WORKSHEET_NAME}'")
    except Exception as e:
        import traceback
        print(f"❌ Error while pasting to Google Sheets: {e}")
        traceback.print_exc()

    print("✅ Done")
