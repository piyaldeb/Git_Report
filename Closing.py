import requests
import json
import re
import logging
import sys
import os
from datetime import date, datetime
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account
import pandas as pd
import pytz
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
log = logging.getLogger()

# ========= CONFIG ==========
ODOO_URL = os.getenv("ODOO_URL")
DB = os.getenv("ODOO_DB")
USERNAME = os.getenv("ODOO_USERNAME")
PASSWORD = os.getenv("ODOO_PASSWORD")

COMPANIES = {
    1: "Zipper",
    3: "Metal Trims",
}

today = date.today()

# ========= GITHUB ENV ==========
from datetime import date, datetime, timedelta
import calendar

# ========= GITHUB ENV ==========
FROM_DATE = os.getenv("FROM_DATE")  # from GitHub Actions
TO_DATE = os.getenv("TO_DATE")      # from GitHub Actions

today = date.today()

if not TO_DATE:
    # last day of the previous month
    first_day_current_month = today.replace(day=1)
    last_day_prev_month = first_day_current_month - timedelta(days=1)
    TO_DATE = last_day_prev_month.isoformat()

if not FROM_DATE:
    FROM_DATE = False  # keep False if wizard supports it


session = requests.Session()
USER_ID = None

# ========= LABEL MAPPING ==========
LABELS = {
    "parent_category": "Product",
    "product_category": "Category",
    "product_id": "Item",
    "lot_id": "Invoice",
    "receive_date": "Receive Date",
    "shipment_mode": "Shipment Mode",
    "slot_1": "0-30",
    "slot_2": "31-60",
    "slot_3": "61-90",
    "slot_4": "91-180",
    "slot_5": "181-365",
    "slot_6": "365+",
    "duration": "Duration",
    "cloing_qty": "Quantity",
    "cloing_value": "Value",
    "landed_cost": "Landed Cost",
    "lot_price": "Price",
    "pur_price": "Pur Price",
    "rejected": "Rejected",
    "company_id": "Company",
}

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

# ========= CREATE AGEING WIZARD ==========
def create_ageing_wizard(company_id, from_date, to_date):
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "stock.forecast.report",
            "method": "web_save",
            "args": [[], {
                "report_type": "ageing",
                "report_for": "rm",
                "all_iteam_list": [],
                "from_date": from_date,
                "to_date": to_date
            }],
            "kwargs": {
                "context": {"lang": "en_US", "tz": "Asia/Dhaka", "uid": USER_ID,
                            "allowed_company_ids": [company_id], "company_id": company_id},
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
    r = retry_request(session.post, f"{ODOO_URL}/web/dataset/call_kw/stock.forecast.report/web_save", json=payload)
    r.raise_for_status()
    result = r.json().get("result", [])
    if isinstance(result, list) and result:
        wiz_id = result[0]["id"]
        print(f"🪄 Ageing wizard {wiz_id} created for company {company_id}")
        return wiz_id
    else:
        raise Exception(f"❌ Failed to create ageing wizard: {r.text}")

# ========= COMPUTE AGEING ==========
def compute_ageing(company_id, wizard_id):
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "stock.forecast.report",
            "method": "print_date_wise_stock_register",
            "args": [[wizard_id]],
            "kwargs": {"context": {"lang": "en_US", "tz": "Asia/Dhaka",
                                   "uid": USER_ID,
                                   "allowed_company_ids": [company_id],
                                   "company_id": company_id}},
        },
    }
    r = retry_request(session.post, f"{ODOO_URL}/web/dataset/call_button", json=payload)

    r.raise_for_status()
    result = r.json()
    if "error" in result:
        print(f"❌ Error computing ageing for {company_id}: {result['error']}")
    else:
        print(f"⚡ Ageing computed for wizard {wizard_id} (company {company_id})")
    return result

# ========= FETCH AGEING REPORT ==========
def fetch_ageing(company_id, cname, wizard_id):
    context = {"allowed_company_ids": [company_id], "company_id": company_id,
               "active_model": "stock.forecast.report", "active_id": wizard_id, "active_ids": [wizard_id]}
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "stock.ageing",
            "method": "web_search_read",
            "args": [],
            "kwargs": {
                "specification": {k: ({"fields": {"display_name": {}}} if k.endswith("_id") or k.endswith("_category") else {}) for k in LABELS.keys()},
                "offset": 0,
                "limit": 5000,
                "context": context,
                "count_limit": 10000,
                "domain": [["product_id.categ_id.complete_name", "ilike", "All / RM"]],
            },
        },
    }
    r = retry_request(session.post, f"{ODOO_URL}/web/dataset/call_kw", json=payload)

    r.raise_for_status()
    try:
        data = r.json()["result"]["records"]
        def flatten(record):
            flat = {}
            for k, v in record.items():
                if isinstance(v, dict) and "display_name" in v:
                    flat[LABELS.get(k, k)] = v["display_name"]
                else:
                    flat[LABELS.get(k, k)] = v
            return flat
        flattened = [flatten(rec) for rec in data]
        print(f"📊 {cname}: {len(flattened)} ageing rows fetched")
        return flattened
    except Exception:
        print(f"❌ {cname}: Failed to parse ageing report:", r.text[:200])
        return []
import time
from requests.exceptions import RequestException

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

# ========= MAIN ==========
if __name__ == "__main__":
    userinfo = login()
    print("User info (allowed companies):", userinfo.get("user_companies", {}))

    for cid, cname in COMPANIES.items():
        if switch_company(cid):
            wiz_id = create_ageing_wizard(cid, FROM_DATE, TO_DATE)
            compute_ageing(cid, wiz_id)
            records = fetch_ageing(cid, cname, wiz_id)

            if records:
                df = pd.DataFrame(records)
                # Drop first column
                df = df.iloc[:, 1:]
                output_file = f"{cname.lower().replace(' ', '_')}_stock_ageing_{today.isoformat()}.xlsx"
                df.to_excel(output_file, index=False)
                print(f"📂 Saved: {output_file}")

                # ========= GOOGLE SHEETS ==========
                try:
                    if cid == 1:  # Zipper
                        client = gspread.service_account(filename="service_account.json")
                        sheet = client.open_by_key("1j37Y6g3pnMWtwe2fjTe1JTT32aRLS0Z1YPjl3v657Cc")
                        worksheet = sheet.worksheet("Closing Stock")
                    elif cid == 3:  # Metal Trims
                        client = gspread.service_account(filename="service_account.json")
                        sheet = client.open_by_key("1j37Y6g3pnMWtwe2fjTe1JTT32aRLS0Z1YPjl3v657Cc")
                        worksheet = sheet.worksheet("Closing Stock - MT")
                    else:
                        worksheet = None

                    if worksheet is not None and not df.empty:
                        worksheet.batch_clear(["A:T"])
                        set_with_dataframe(worksheet, df)
                        # local_tz = pytz.timezone("Asia/Dhaka")
                        # local_time = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
                        # worksheet.update("W2", [[f"{local_time}"]])
                        # print(f"✅ Data pasted & timestamp updated: {local_time}")

                except Exception as e:
                    print(f"❌ Error while pasting to Google Sheets: {e}")

            else:
                print(f"❌ No ageing data fetched for {cname}")