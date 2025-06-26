import pandas as pd
import pyodbc
import os
import getpass

# =====================================
# กำหนดค่าคงที่ ไม่ต้องกรอก
# =====================================
DEFAULT_FILE_PATH = 'Expense_Final_Long_Format.xlsx'
DEFAULT_SHEET_NAME = 0
DEFAULT_HEADER_ROW = 0
DEFAULT_BATCH_SIZE = 1000
DEFAULT_AUTH_METHOD = "4"  # ใช้ username/password ในโค้ด

def get_connection_string():
    server = '10.73.148.27'
    database = 'Budget_BP'
    
    # ใช้ method 4 (Direct Username/Password) โดยอัตโนมัติ
    print("✅ Using direct authentication (method 4)")
    username = 'TN00244'
    password = 'Wasupakkanut@TN00244'
    
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;Connection Timeout=300;'
    print(f"✅ Using direct authentication for user: {username}")
    
    return connection_string

def clean_str(val, max_len=100):
    """ทำความสะอาดข้อมูล string"""
    if pd.isna(val) or val is None:
        return None
    sval = str(val).strip()
    if sval.lower() == 'nan' or sval == '':
        return None
    return sval[:max_len]

def clean_int(val):
    """ทำความสะอาดข้อมูล integer เท่านั้น"""
    if pd.isna(val) or val is None or str(val).strip() == '':
        return None
    try:
        # ถ้าเป็นตัวเลขล้วนๆ ถึงจะแปลง
        str_val = str(val).strip()
        if str_val.replace('.', '').replace('-', '').isdigit():
            return int(float(val))
        else:
            return None
    except Exception:
        return None

def clean_account_code(val):
    """ทำความสะอาดข้อมูล account code - รักษารูปแบบเดิม"""
    if pd.isna(val) or val is None:
        return None
    
    str_val = str(val).strip()
    if str_val.lower() == 'nan' or str_val == '':
        return None
    
    # รักษารูปแบบเดิม เช่น 95135-95135001
    return str_val

def clean_month(val):
    """ทำความสะอาดข้อมูล month - รักษารูปแบบเดิม"""
    if pd.isna(val) or val is None:
        return None
    
    str_val = str(val).strip()
    if str_val.lower() == 'nan' or str_val == '':
        return None
    
    # รักษารูปแบบเดิม เช่น April, January, etc.
    return str_val

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

    # Mapping Excel -> DB
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
        'Plan': 'plan'
    }

    required_excel_columns = list(column_mapping.keys())

    print("=" * 60)
    print("EXCEL TO SQL SERVER: expense IMPORT TOOL (AUTO CONFIG)")
    print("=" * 60)
    print(f"File: {file_path}")
    print(f"Sheet: {sheet_name}")
    print(f"Header row: {header_row+1}")
    print(f"Batch size: {batch_size:,} records")

    print("\nConnecting to SQL Server...")
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

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
        df_head = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row, nrows=0, dtype=str)
        excel_cols = df_head.columns.tolist()
        print("Excel Columns Found:")
        for i, col in enumerate(excel_cols): 
            print(f"  {i+1}. '{col}'")
    except Exception as e:
        print(f"❌ Error reading excel header: {e}")
        return False

    missing = [col for col in required_excel_columns if col not in excel_cols]
    if missing:
        print(f"\n❌ Columns missing in Excel: {missing}")
        print("กรุณาเปิดไฟล์ excel แล้วดูว่า header ตรงกับนี้ไหม (เว้นวรรค/พิมพ์ใหญ่-เล็กต้องตรง)")
        return False

    # Read data from Excel with dtype=str to preserve original format
    print("\nReading Excel data...")
    df = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        header=header_row,
        usecols=required_excel_columns,
        dtype=str  # อ่านทุกคอลัมน์เป็น string ก่อน
    )

    total_rows = len(df)
    print(f"Total data rows found: {total_rows:,}")
    if total_rows == 0:
        print("No data found in Excel file!")
        return False

    df = df.rename(columns=column_mapping)

    print("\nProcessing and cleaning data...")
    
    # Data Cleaning - แยกประเภทข้อมูล
    # Integer columns (เฉพาะที่เป็นตัวเลขล้วนๆ)
    for col in ['cost_center_code', 'project_no', 'item_no']:
        print(f"  Processing {col} as integer...")
        df[col] = df[col].apply(clean_int)

    # Account Code - รักษารูปแบบเดิม (mixed format)
    print("  Processing account_code as text...")
    df['account_code'] = df['account_code'].apply(clean_account_code)
    
    # Month - รักษารูปแบบเดิม (text)
    print("  Processing month as text...")
    df['month'] = df['month'].apply(clean_month)

    # String columns
    for col in ['cost_center_name', 'account_name', 'running_code', 'activity_name', 'unique_field', 'plan']:
        print(f"  Processing {col} as string...")
        df[col] = df[col].apply(lambda x: clean_str(x, 100))

    # Debug: แสดงตัวอย่างข้อมูลที่ประมวลผลแล้ว
    print(f"\nSample processed data:")
    print(f"account_code samples: {df['account_code'].dropna().head(3).tolist()}")
    print(f"month samples: {df['month'].dropna().head(3).tolist()}")
    print(f"cost_center_code samples: {df['cost_center_code'].dropna().head(3).tolist()}")

    insert_columns = [
        'cost_center_code',
        'cost_center_name',
        'account_code',
        'account_name',
        'running_code',
        'activity_name',
        'project_no',
        'item_no',
        'unique_field',
        'month',
        'plan'
    ]
    
    insert_sql = f"""
    INSERT INTO [dbo].[expense] (
        {', '.join([f'[{col}]' for col in insert_columns])}
    ) VALUES ({', '.join(['?' for _ in insert_columns])})
    """

    total_batches = (total_rows + batch_size - 1) // batch_size
    print(f"Will process in {total_batches:,} batches")

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
                values = []
                for col in insert_columns:
                    val = row[col]
                    # Debug สำหรับ account_code และ month
                    if col in ['account_code', 'month'] and not pd.isna(val) and val is not None:
                        if batch_num == 0 and len(values) < 3:  # แสดงตัวอย่างแค่ batch แรก
                            print(f"    Debug {col}: '{val}' (type: {type(val)})")
                    values.append(val)
                
                cursor.execute(insert_sql, tuple(values))
                batch_success += 1
            except Exception as e:
                print(f"    Error in row {start_idx + idx + 1}: {str(e)[:100]}...")
                print(f"    Problematic data: account_code='{row['account_code']}', month='{row['month']}'")
                batch_errors += 1
        
        conn.commit()
        total_processed += len(batch_df)
        total_success += batch_success
        total_errors += batch_errors
        print(f"  ✅ Batch {batch_num + 1}: Success={batch_success:,}, Errors={batch_errors:,}")

    print(f"\n{'='*60}")
    print("IMPORT SUMMARY")
    print(f"{'='*60}")
    print(f"📊 Total processed: {total_processed:,} records")
    print(f"✅ Successfully inserted: {total_success:,} records")
    print(f"❌ Errors: {total_errors:,} records")
    print(f"📈 Success rate: {(total_success/total_processed*100 if total_processed > 0 else 0):.1f}%")

    # ตรวจสอบข้อมูลในฐานข้อมูล
    cursor.execute("SELECT COUNT(*) FROM [dbo].[expense]")
    db_count = cursor.fetchone()[0]
    print(f"\n🗄️  Records in database: {db_count:,}")
    
    # ตรวจสอบข้อมูลตัวอย่าง
    print("\nSample data in database:")
    cursor.execute("""
        SELECT TOP 3 account_code, month, cost_center_code 
        FROM [dbo].[expense] 
        WHERE account_code IS NOT NULL AND month IS NOT NULL
    """)
    samples = cursor.fetchall()
    for i, sample in enumerate(samples):
        print(f"  {i+1}. account_code: '{sample[0]}', month: '{sample[1]}', cost_center_code: {sample[2]}")

    cursor.close()
    conn.close()

    print(f"\n🎉 Import completed successfully!")
    print(f"{'='*60}")
    return True

if __name__ == "__main__":
    print("Expense Excel to SQL Server Import Tool (AUTO CONFIG)")
    print("All settings are pre-configured - no input required!")
    print("=" * 80)
    print(f"📄 File: {DEFAULT_FILE_PATH}")
    print(f"📊 Sheet: {DEFAULT_SHEET_NAME}")
    print(f"📋 Header row: {DEFAULT_HEADER_ROW + 1}")
    print(f"⚡ Batch size: {DEFAULT_BATCH_SIZE:,}")
    print(f"🔐 Authentication: Direct (TN00244)")
    print("=" * 80)
    
    # ไม่ต้องถาม input ใดๆ เลย เริ่มทำงานเลย
    print("\n🚀 Starting import process...")
    
    success = import_expense_excel_to_db()
    
    if success:
        print("\n🎉 Process completed successfully!")
    else:
        print("\n❌ Process failed!")
        exit(1)