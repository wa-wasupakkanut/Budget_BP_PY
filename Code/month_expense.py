import pandas as pd
import pyodbc
import math
import os
import getpass

def get_connection_string():
    server = '10.73.148.27'
    database = 'Budget_BP'
    print("Authentication Options:")
    print("1. Environment Variables (SQL_USERNAME, SQL_PASSWORD)")
    print("2. Interactive Input")
    print("3. Windows Authentication")
    print("4. Direct Username/Password (in code)")
    try:
        choice = input("Select authentication method (1-4, default: 2): ").strip()
        if not choice:
            choice = "2"
    except:
        choice = "2"
    if choice == "1":
        username = os.getenv('SQL_USERNAME')
        password = os.getenv('SQL_PASSWORD')
        if not username or not password:
            print("❌ Environment variables SQL_USERNAME and SQL_PASSWORD not found!")
            return None
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;Connection Timeout=300;'
        print(f"✅ Using environment variables for user: {username}")
    elif choice == "2":
        username = input("Username: ").strip()
        password = getpass.getpass("Password: ")
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;Connection Timeout=300;'
        print(f"✅ Using interactive authentication for user: {username}")
    elif choice == "3":
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;Connection Timeout=300;'
        print("✅ Using Windows Authentication")
    elif choice == "4":
        print("⚠️  WARNING: This method stores credentials in code!")
        username = 'TN00244'
        password = 'Wasupakkanut@TN00244'
        if username == 'your_username' or password == 'your_password':
            print("❌ Please edit the username and password in the code first!")
            return None
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;Connection Timeout=300;'
        print(f"✅ Using direct authentication for user: {username}")
    else:
        print("❌ Invalid choice!")
        return None
    return connection_string

def test_connection(connection_string):
    try:
        print("Testing database connection...")
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION, DB_NAME()")
        result = cursor.fetchone()
        print(f"✅ Connection successful!")
        print(f"📊 Database: {result[1]}")
        print(f"🔧 SQL Server: {result[0][:80]}...")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def preview_excel_headers(file_path, sheet_name, max_rows=5):
    print("\n===== Excel HEADER PREVIEW =====")
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=max_rows)
        for i in range(max_rows):
            row = df.iloc[i].tolist()
            print(f"Row {i}: {row}")
    except Exception as e:
        print(f"❌ Error previewing excel: {e}")

def import_expense_excel_to_db(
    file_path='expense.xlsx',
    sheet_name='expense',
    batch_size=1000,
    header_row=1,
    connection_string=None
):
    if connection_string is None:
        connection_string = get_connection_string()
        if connection_string is None:
            print("❌ Cannot proceed without valid connection string")
            return False
    if not test_connection(connection_string):
        print("❌ Cannot proceed with failed connection")
        return False

    column_mapping = {
        'Running Code': 'running_code',
        'April': 'april',
        'May': 'may',
        'June': 'june',
        'July': 'july',
        'August': 'august',
        'September': 'september',
        'October': 'october',
        'November': 'november',
        'December': 'december',
        'January': 'january',
        'February': 'february',
        'March': 'march',
        "Target reduction (Start from Jul'25)": 'target_reduction'
    }

    print("=" * 60)
    print("EXCEL TO SQL SERVER: month_expense IMPORT TOOL")
    print("=" * 60)
    print(f"Batch size: {batch_size:,} records")
    print(f"Header row: {header_row+1}")

    print("\nConnecting to SQL Server...")
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    # ===== รีเซ็ตตาราง month_expense และ reseed identity =====
    print("\nClearing table [month_expense] and reseeding IDENTITY (month_id)...")
    try:
        cursor.execute("DELETE FROM [dbo].[month_expense]")
        cursor.execute("DBCC CHECKIDENT ('[dbo].[month_expense]', RESEED, 0)")
        conn.commit()
        print("✅ Table cleared and identity reseeded to 1.")
    except Exception as e:
        print(f"❌ Error clearing or reseeding table: {e}")
        cursor.close()
        conn.close()
        return False

    print("\nReading Excel columns preview...")
    try:
        df_head = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row, nrows=0)
        excel_cols = df_head.columns.tolist()
        print("Excel Columns Found:")
        for i, col in enumerate(excel_cols): print(f"  {i+1}. '{col}'")
    except Exception as e:
        print(f"❌ Error reading excel header: {e}")
        preview_excel_headers(file_path, sheet_name)
        return False

    missing = [col for col in column_mapping if col not in excel_cols]
    if missing:
        print(f"\n❌ Columns missing in Excel: {missing}")
        print("กรุณาเปิดไฟล์ excel แล้วดูว่า header ตรงกับนี้ไหม (เว้นวรรค/พิมพ์ใหญ่-เล็กต้องตรง)")
        print("\n==== Preview first 5 rows in Excel (for debug) ====")
        preview_excel_headers(file_path, sheet_name)
        print("\nTIP: ถ้า header ไม่ตรงหรืออยู่แถวอื่น ให้เปลี่ยน header_row=เลขแถว (เริ่ม 0 คือแถวแรกใน excel)")
        print("     หรือปรับชื่อ column_mapping ให้ตรงกับในไฟล์")
        return False

    # อ่านข้อมูล (skip header ตาม row ที่กำหนด)
    df = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        header=header_row,
        usecols=list(column_mapping.keys())
    )
    # drop เฉพาะ row ที่ Running Code ว่าง
    df = df.dropna(subset=['Running Code'], how='all')
    total_rows = len(df)
    print(f"\nTotal data rows found: {total_rows:,}")
    if total_rows == 0:
        print("No data found in Excel file!")
        return

    df = df.rename(columns=column_mapping)

    # ==== Data Cleaning ====
    # กำหนดชื่อคอลัมน์ตัวเลข
    numeric_cols = [
        'april', 'may', 'june', 'july', 'august', 'september', 'october',
        'november', 'december', 'january', 'february', 'march', 'target_reduction'
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # ถ้าคุณรู้ว่าคอลัมน์ใน SQL Server เป็น int, float, decimal เท่าไร ควรตัดค่าที่เกิน
    # ตัวอย่างเช่น ถ้าเป็น INT
    for col in numeric_cols:
        df.loc[df[col] > 2147483647, col] = None  # ถ้าเกิน int32

    # ตัด string ให้อยู่ในขนาดที่ SQL Server รับได้ (ตัวอย่าง 50 ตัวอักษร สำหรับ running_code)
    df['running_code'] = df['running_code'].astype(str).str[:50]

    insert_columns = list(column_mapping.values())
    insert_sql = f"""
    INSERT INTO [dbo].[month_expense] (
        {', '.join([f'[{col}]' for col in insert_columns])}
    ) VALUES ({', '.join(['?' for _ in insert_columns])})
    """

    total_batches = math.ceil(total_rows / batch_size)
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
                values = [row[col] if not pd.isna(row[col]) else None for col in insert_columns]
                cursor.execute(insert_sql, tuple(values))
                batch_success += 1
            except Exception as e:
                print(f"    Error in row {total_processed + idx + 1}: {str(e)[:120]}... Data: {row.to_dict()}")
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

    cursor.execute("SELECT COUNT(*) FROM [dbo].[month_expense]")
    db_count = cursor.fetchone()[0]
    print(f"\n🗄️  Records in database: {db_count:,}")

    cursor.close()
    conn.close()

    print(f"\n🎉 Import completed successfully!")
    print(f"{'='*60}")
    return True

if __name__ == "__main__":
    print("Expense Excel to SQL Server Import Tool")
    print("Support batch processing and flexible authentication")
    print("-" * 80)
    print(f"\n{'='*60}")
    print("DATABASE AUTHENTICATION")
    print(f"{'='*60}")
    connection_string = get_connection_string()
    if connection_string is None:
        print("❌ Cannot proceed without authentication")
        exit(1)
    print(f"\n{'='*60}")
    print("BATCH SIZE CONFIGURATION")
    print(f"{'='*60}")
    print("Batch Size Options:")
    print("1. Small files (< 1,000 rows): 500")
    print("2. Medium files (1,000 - 10,000 rows): 1,000")
    print("3. Large files (10,000 - 30,000 rows): 2,000")
    print("4. Very large files (> 30,000 rows): 5,000")
    print("5. Custom size")
    try:
        batch_choice = input("\nSelect batch size option (1-5, default: 2): ").strip()
        if not batch_choice:
            batch_choice = "2"
        if batch_choice == "1":
            batch_size = 500
        elif batch_choice == "2":
            batch_size = 1000
        elif batch_choice == "3":
            batch_size = 2000
        elif batch_choice == "4":
            batch_size = 5000
        elif batch_choice == "5":
            custom_size = input("Enter custom batch size: ").strip()
            batch_size = int(custom_size) if custom_size.isdigit() else 1000
        else:
            batch_size = 1000
    except:
        batch_size = 1000
    print(f"\n🚀 Starting import with batch size: {batch_size:,}")
    print(f"Connection: SQL Server Authentication")
    confirm = input(f"\nProceed with import? (y/N): ").strip().lower()
    if confirm not in ['y', 'yes']:
        print("Import cancelled by user")
        exit(0)
    # ====== IMPORTANT: header_row=1 (Excel row 2)
    success = import_expense_excel_to_db(
        batch_size=batch_size,
        header_row=1,
        connection_string=connection_string
    )
    if success:
        print("\n🎉 Process completed successfully!")
    else:
        print("\n❌ Process failed!")
        exit(1)