import pandas as pd
import pyodbc
import os
import numpy as np
from datetime import datetime

# =====================================
# กำหนดค่าคงที่ ไม่ต้องกรอก
# =====================================
DEFAULT_DIR = r'D:\Budget\Budget_BP'
DEFAULT_FILE_NAME = 'Expense_Final_Long_Format.xlsx'
DEFAULT_FILE_PATH = os.path.join(DEFAULT_DIR, DEFAULT_FILE_NAME)
DEFAULT_SHEET_NAME = 0
DEFAULT_HEADER_ROW = 0
DEFAULT_BATCH_SIZE = 1000
DEFAULT_AUTH_METHOD = "4"  # ใช้ username/password ในโค้ด

def get_connection_string():
    server = '10.73.148.27'
    database = 'Budget_BP'
    print("✅ Using direct authentication (method 4)")
    username = 'TN00244'
    password = 'Wasupakkanut@TN00244'
    
    connection_string = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password};'
        f'TrustServerCertificate=yes;'
        f'Connection Timeout=300;'
    )
    print(f"✅ Using direct authentication for user: {username}")
    return connection_string

def clean_str(val, max_len=100):
    if pd.isna(val) or val is None:
        return None
    sval = str(val).strip()
    if sval.lower() == 'nan' or sval == '':
        return None
    return sval[:max_len]

def clean_int(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return None
    try:
        if isinstance(val, (np.integer, np.int64, np.int32, np.int16, np.int8)):
            return int(val)
        str_val = str(val).strip()
        if str_val.replace('.', '').replace('-', '').isdigit():
            return int(float(val))
        else:
            return None
    except Exception:
        return None

def clean_decimal(val):
    if pd.isna(val) or val is None or str(val).strip() == '':
        return None
    try:
        if isinstance(val, (np.floating, np.float64, np.float32, np.float16)):
            return float(val)
        return float(val)
    except Exception:
        return None

def clean_account_code(val):
    if pd.isna(val) or val is None:
        return None
    str_val = str(val).strip()
    if str_val.lower() == 'nan' or str_val == '':
        return None
    return str_val

def clean_month_to_date(val):
    if pd.isna(val) or val is None:
        return None
    str_val = str(val).strip()
    if str_val.lower() == 'nan' or str_val == '':
        return None
    month_map = {
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
        month_num = int(str_val)
        if 1 <= month_num <= 12:
            return datetime(2025, month_num, 1).date()
    except:
        pass
    month_lower = str_val.lower()
    for month_name, month_num in month_map.items():
        if month_name in month_lower:
            return datetime(2025, month_num, 1).date()
    print(f"⚠️  Cannot parse month: '{str_val}', using NULL")
    return None

def get_database_columns(cursor):
    try:
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'expense'
            ORDER BY ORDINAL_POSITION
        """)
        db_columns_info = cursor.fetchall()
        db_columns = [col[0] for col in db_columns_info]
        print("Database columns found:")
        for i, (col_name, data_type, nullable, max_len) in enumerate(db_columns_info):
            print(f"  {i+1}. {col_name} ({data_type}, nullable: {nullable}, max_len: {max_len})")
        return db_columns, db_columns_info
    except Exception as e:
        print(f"⚠️  Could not read table structure: {e}")
        return [], []

def convert_to_native_types(df):
    print("\nConverting data types to native Python types...")
    for col in df.columns:
        print(f"  Converting column: {col}")
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: convert_single_value(x))
        elif pd.api.types.is_integer_dtype(df[col]):
            df[col] = df[col].apply(lambda x: int(x) if not pd.isna(x) else None)
        elif pd.api.types.is_float_dtype(df[col]):
            df[col] = df[col].apply(lambda x: float(x) if not pd.isna(x) else None)
        elif pd.api.types.is_bool_dtype(df[col]):
            df[col] = df[col].apply(lambda x: bool(x) if not pd.isna(x) else None)
    return df

def convert_single_value(val):
    if pd.isna(val) or val is None:
        return None
    elif isinstance(val, (np.integer, np.int64, np.int32, np.int16, np.int8)):
        return int(val)
    elif isinstance(val, (np.floating, np.float64, np.float32, np.float16)):
        return float(val)
    elif isinstance(val, np.bool_):
        return bool(val)
    elif isinstance(val, (np.str_, np.bytes_)):
        return str(val)
    else:
        return val

def prepare_insert_values(row, columns):
    values = []
    for col in columns:
        val = row[col]
        if pd.isna(val) or val is None:
            values.append(None)
        elif isinstance(val, (np.integer, np.int64, np.int32, np.int16, np.int8)):
            values.append(int(val))
        elif isinstance(val, (np.floating, np.float64, np.float32, np.float16)):
            values.append(float(val))
        elif isinstance(val, np.bool_):
            values.append(bool(val))
        elif isinstance(val, (np.str_, np.bytes_)):
            values.append(str(val))
        else:
            values.append(val)
    return values

def import_expense_excel_to_db(
    file_path=DEFAULT_FILE_PATH,
    sheet_name=DEFAULT_SHEET_NAME,
    batch_size=DEFAULT_BATCH_SIZE,
    header_row=DEFAULT_HEADER_ROW,
    connection_string=None
):
    if connection_string is None:
        connection_string = get_connection_string()
        if connection_string is None:
            print("❌ Cannot proceed without valid connection string")
            return False

    column_mapping = {
        'Department Code': 'cost_center_code',
        'Department Name': 'cost_center_name',
        'Account Code': 'account_code',
        'Account Name': 'account_name',
        'Running Code': 'running_code',
        'Activity Name': 'activity_name',
        'Project No': 'project_no',
        'Item No.': 'item_no',
        'Unique': 'unique_field',
        'Month': 'month',
        'Plan': 'plan',
        "Target reduction (Start from Jul'25)": 'target_reduction_jul25',
        'ratio_plan_MC': 'ratio_plan_MC',
        'ratio_result_MC': 'ratio_result_MC',
        'ratio_plan_ASSY': 'ratio_plan_ASSY',
        'ratio_result_ASSY': 'ratio_result_ASSY'
    }

    print("=" * 60)
    print("EXCEL TO SQL SERVER: expense IMPORT TOOL (COMPLETE VERSION)")
    print("=" * 60)
    print(f"File: {file_path}")
    print(f"Sheet: {sheet_name}")
    print(f"Header row: {header_row+1}")
    print(f"Batch size: {batch_size:,} records")

    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return False

    print("\nConnecting to SQL Server...")
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    print("\nChecking database table structure...")
    db_columns, db_columns_info = get_database_columns(cursor)
    if not db_columns:
        print("❌ Could not retrieve database columns")
        cursor.close()
        conn.close()
        return False

    print("\nClearing table [expense] ...")
    try:
        cursor.execute("DELETE FROM [dbo].[expense]")
        conn.commit()
        print("✅ Table cleared.")
    except Exception as e:
        print(f"❌ Error clearing table: {e}")
        cursor.close()
        conn.close()
        return False

    print("\nReading Excel columns preview...")
    try:
        df_head = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row, nrows=0)
        excel_cols = df_head.columns.tolist()
        print("Excel Columns Found:")
        for i, col in enumerate(excel_cols): 
            print(f"  {i+1}. '{col}'")
    except Exception as e:
        print(f"❌ Error reading excel header: {e}")
        return False

    valid_mappings = {}
    for excel_col, db_col in column_mapping.items():
        if excel_col in excel_cols and db_col in db_columns:
            valid_mappings[excel_col] = db_col
            print(f"✅ Mapped: '{excel_col}' -> '{db_col}'")

    if not valid_mappings:
        print("❌ No valid column mappings found!")
        cursor.close()
        conn.close()
        return False

    print("\nReading Excel data...")
    df = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        header=header_row,
        usecols=list(valid_mappings.keys())
    )

    total_rows = len(df)
    print(f"Total data rows found: {total_rows:,}")
    if total_rows == 0:
        print("No data found in Excel file!")
        return False

    df = df.rename(columns=valid_mappings)

    print("\nProcessing and cleaning data...")
    if 'cost_center_code' in df.columns:
        print("  Processing cost_center_code...")
        df['cost_center_code'] = df['cost_center_code'].apply(lambda x: clean_str(x, 20))
    if 'cost_center_name' in df.columns:
        print("  Processing cost_center_name...")
        df['cost_center_name'] = df['cost_center_name'].apply(lambda x: clean_str(x, 100))
    if 'account_code' in df.columns:
        print("  Processing account_code...")
        df['account_code'] = df['account_code'].apply(clean_account_code)
    if 'account_name' in df.columns:
        print("  Processing account_name...")
        df['account_name'] = df['account_name'].apply(lambda x: clean_str(x, 100))
    if 'running_code' in df.columns:
        print("  Processing running_code...")
        df['running_code'] = df['running_code'].apply(lambda x: clean_str(x, 30))
    if 'activity_name' in df.columns:
        print("  Processing activity_name...")
        df['activity_name'] = df['activity_name'].apply(lambda x: clean_str(x, 255))
    if 'project_no' in df.columns:
        print("  Processing project_no...")
        df['project_no'] = df['project_no'].apply(lambda x: clean_str(x, 50))
    if 'item_no' in df.columns:
        print("  Processing item_no...")
        df['item_no'] = df['item_no'].apply(clean_int)
    if 'month' in df.columns:
        print("  Processing month...")
        df['month'] = df['month'].apply(clean_month_to_date)
    decimal_columns = ['plan', 'ratio_plan_MC', 'ratio_result_MC', 'ratio_plan_ASSY', 'ratio_result_ASSY']
    for col in decimal_columns:
        if col in df.columns:
            print(f"  Processing {col} as decimal...")
            df[col] = df[col].apply(clean_decimal)
    if 'unique_field' in df.columns:
        print("  Processing unique_field...")
        df['unique_field'] = df['unique_field'].apply(lambda x: clean_str(x, 100))

    df = convert_to_native_types(df)

    print(f"\nSample processed data (first row):")
    if len(df) > 0:
        sample_row = df.iloc[0]
        for col, val in sample_row.items():
            print(f"  {col}: {val} ({type(val).__name__})")

    print("\nTesting single row insert...")
    try:
        insert_columns = list(df.columns)
        insert_sql = f"""
        INSERT INTO [dbo].[expense] (
            {', '.join([f'[{col}]' for col in insert_columns])}
        ) VALUES ({', '.join(['?' for _ in insert_columns])})
        """
        test_values = prepare_insert_values(df.iloc[0], insert_columns)
        print(f"Test values: {test_values}")
        print(f"Test value types: {[type(v).__name__ for v in test_values]}")
        cursor.execute(insert_sql, tuple(test_values))
        conn.rollback()
        print("✅ Single insert test successful")
    except Exception as e:
        print(f"❌ Single insert test failed: {e}")
        cursor.close()
        conn.close()
        return False

    total_batches = (total_rows + batch_size - 1) // batch_size
    print(f"\nWill process in {total_batches:,} batches")

    total_processed = 0
    total_success = 0
    total_errors = 0

    print(f"\n{'='*60}")
    print("STARTING BATCH PROCESSING")
    print(f"{'='*60}")

    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, total_rows)
        batch_df = df.iloc[start_idx:end_idx]
        print(f"\nBatch {batch_num + 1}/{total_batches}: Processing rows {start_idx + 1:,} to {end_idx:,} ({len(batch_df):,} records)")

        batch_success = 0
        batch_errors = 0

        for idx, row in batch_df.iterrows():
            try:
                values = prepare_insert_values(row, insert_columns)
                cursor.execute(insert_sql, tuple(values))
                batch_success += 1
            except Exception as e:
                print(f"    Error in row {start_idx + (idx - batch_df.index[0]) + 1}: {str(e)}")
                batch_errors += 1
                if batch_errors > 10:
                    print(f"    Too many errors in this batch, stopping...")
                    break
        
        try:
            conn.commit()
        except Exception as e:
            print(f"    Error committing batch: {e}")
            conn.rollback()
            
        total_processed += len(batch_df)
        total_success += batch_success
        total_errors += batch_errors
        print(f"  ✅ Batch {batch_num + 1}: Success={batch_success:,}, Errors={batch_errors:,}")
        if total_errors > total_processed * 0.5:
            print(f"❌ Too many errors ({total_errors:,}), stopping import...")
            break

    print(f"\n{'='*60}")
    print("IMPORT SUMMARY")
    print(f"{'='*60}")
    print(f"📊 Total processed: {total_processed:,} records")
    print(f"✅ Successfully inserted: {total_success:,} records")
    print(f"❌ Errors: {total_errors:,} records")
    print(f"📈 Success rate: {(total_success/total_processed*100 if total_processed > 0 else 0):.1f}%")

    try:
        cursor.execute("SELECT COUNT(*) FROM [dbo].[expense]")
        db_count = cursor.fetchone()[0]
        print(f"\n🗄️  Records in database: {db_count:,}")
    except Exception as e:
        print(f"⚠️  Could not retrieve final statistics: {e}")

    cursor.close()
    conn.close()

    if total_success > 0:
        print(f"\n🎉 Import completed with {total_success:,} successful records!")
        return True
    else:
        print(f"\n❌ Import failed - no records were successfully inserted!")
        return False

if __name__ == "__main__":
    print("Expense Excel to SQL Server Import Tool (COMPLETE VERSION)")
    print("All settings are pre-configured - no input required!")
    print("=" * 80)
    print(f"📄 File: {DEFAULT_FILE_PATH}")
    print(f"📊 Sheet: {DEFAULT_SHEET_NAME}")
    print(f"📋 Header row: {DEFAULT_HEADER_ROW + 1}")
    print(f"⚡ Batch size: {DEFAULT_BATCH_SIZE:,}")
    print(f"🔐 Authentication: Direct (TN00244)")
    print("=" * 80)
    
    print("\n🚀 Starting import process...")
    success = import_expense_excel_to_db()
    if success:
        print("\n🎉 Process completed successfully!")
    else:
        print("\n❌ Process failed!")
        exit(1) 