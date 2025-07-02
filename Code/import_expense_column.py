import pandas as pd
import pyodbc
import os
import numpy as np
from datetime import datetime

EXCEL_FILE = r'D:\Budget\Budget_BP\Database\April.xlsx'
SHEET_NAME = 0
HEADER_ROW = 0

DB_SERVER = '10.73.148.27'
DB_NAME = 'Budget_BP'
DB_USER = 'TN00244'
DB_PASS = 'Wasupakkanut@TN00244'

TABLE_NAME = 'expense'

# key mapping
KEY_MAPPING = {
    'Month': 'month',
    'Department Code': 'cost_center_code',
    'Account Code': 'account_code'
}

# data mapping (excel_col: db_col)
DATA_MAPPING = {
    "Target reduction (Start from Jul'25)": "targe_reduction",
    "ratio_plan_MC": "ratio_plan_MC",
    "ratio_result_MC": "ratio_result_MC",
    "ratio_plan_ASSY": "ratio_plan_ASSY",
    "ratio_result_ASSY": "ratio_result_ASSY",
}

# ------------- CLEANING FUNCTIONS -------------
def clean_str(val, max_len=100):
    if pd.isna(val) or val is None:
        return None
    sval = str(val).strip()
    if sval.lower() == 'nan' or sval == '':
        return None
    return sval[:max_len]

def clean_decimal(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return None
    try:
        return float(val)
    except Exception:
        return None

def clean_month(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return None
    str_val = str(val).strip()
    months = {
        'january': 1, 'jan': 1,
        'february': 2, 'feb': 2,
        'march': 3, 'mar': 3,
        'april': 4, 'apr': 4,
        'may': 5,
        'june': 6, 'jun': 6,
        'july': 7, 'jul': 7,
        'august': 8, 'aug': 8,
        'september': 9, 'sep': 9,
        'october': 10, 'oct': 10,
        'november': 11, 'nov': 11,
        'december': 12, 'dec': 12
    }
    try:
        n = int(str_val)
        if 1 <= n <= 12:
            return datetime(2025, n, 1).date()
    except:
        pass
    s = str_val.lower()
    for k, v in months.items():
        if k in s:
            return datetime(2025, v, 1).date()
    return None

def get_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={DB_SERVER};DATABASE={DB_NAME};"
        f"UID={DB_USER};PWD={DB_PASS};TrustServerCertificate=yes;Connection Timeout=300;"
    )
    return pyodbc.connect(conn_str)

def main():
    print(f"=== IMPORTING: {EXCEL_FILE}")
    if not os.path.exists(EXCEL_FILE):
        print(f"❌ File not found: {EXCEL_FILE}")
        return

    df = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME, header=HEADER_ROW)
    # check required columns
    required_cols = list(KEY_MAPPING.keys()) + list(DATA_MAPPING.keys())
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        print(f"❌ Missing columns in Excel: {missing_cols}")
        return

    # Keep only mapped columns
    df = df[required_cols]
    # Rename for DB
    rename_map = {}
    rename_map.update(KEY_MAPPING)
    rename_map.update(DATA_MAPPING)
    df = df.rename(columns=rename_map)
    
    # Clean key columns
    df["month"] = df["month"].apply(clean_month)
    df["cost_center_code"] = df["cost_center_code"].apply(lambda x: clean_str(x, 20))
    df["account_code"] = df["account_code"].apply(lambda x: clean_str(x, 20))
    # Clean value columns
    for col in DATA_MAPPING.values():
        df[col] = df[col].apply(clean_decimal)
    # Drop any row missing key
    df = df.dropna(subset=["month", "cost_center_code", "account_code"])

    print(f"Rows to import: {len(df)}")
    if len(df) == 0:
        print("No data to import, exiting.")
        return

    conn = get_connection()
    cursor = conn.cursor()

    # Prepare upsert (update if exists, else insert)
    key_db = list(KEY_MAPPING.values())
    data_db = list(DATA_MAPPING.values())
    rowcount = 0
    update_count = 0
    insert_count = 0
    for idx, row in df.iterrows():
        # 1. Try update first
        update_set = ', '.join([f"{col} = ?" for col in data_db])
        where_clause = ' AND '.join([f"{col} = ?" for col in key_db])
        update_sql = f"UPDATE [{TABLE_NAME}] SET {update_set} WHERE {where_clause}"
        update_vals = [row[col] for col in data_db] + [row[col] for col in key_db]
        cursor.execute(update_sql, update_vals)
        if cursor.rowcount > 0:
            update_count += 1
        else:
            # 2. If not updated, do insert
            insert_cols = key_db + data_db
            insert_placeholders = ','.join(['?' for _ in insert_cols])
            insert_sql = f"INSERT INTO [{TABLE_NAME}] ({','.join(insert_cols)}) VALUES ({insert_placeholders})"
            insert_vals = [row[col] for col in insert_cols]
            try:
                cursor.execute(insert_sql, insert_vals)
                insert_count += 1
            except Exception as e:
                print(f"❌ Row {idx+1} insert error: {e}")
        rowcount += 1
        if (rowcount % 500) == 0:
            conn.commit()
            print(f"  ...processed {rowcount} rows")

    conn.commit()
    print(f"✅ Update: {update_count} rows, Insert: {insert_count} rows")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()