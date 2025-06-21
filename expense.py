import pandas as pd
import pyodbc
import math
import os
import getpass
import numpy as np

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
            print("Please set them first:")
            print("Windows: set SQL_USERNAME=your_username && set SQL_PASSWORD=your_password")
            print("Linux:   export SQL_USERNAME=your_username && export SQL_PASSWORD=your_password")
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

def table_has_column(cursor, table, column):
    cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?", (table,))
    cols = [row[0].lower() for row in cursor.fetchall()]
    return column.lower() in cols

def clean_str(val):
    """Return None if value is nan, empty string, or 'nan' string, else str(val)"""
    if pd.isna(val) or val is None:
        return None
    sval = str(val)
    if sval.strip().lower() == 'nan' or sval.strip() == '':
        return None
    return sval.strip()

def clean_int_str(val):
    """Return str(int(val)) if val is a numeric and is a whole number, else str(val) or None if blank."""
    if pd.isna(val) or val is None or str(val).strip() == '':
        return None
    try:
        f = float(val)
        if f.is_integer():
            return str(int(f))
        else:
            return str(f)
    except Exception:
        sval = str(val)
        if sval.strip() == '':
            return None
        if '.' in sval:
            v = sval.split('.')[0]
            if v.isdigit():
                return v
        return sval

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

    # Mapping Excel -> DB
    column_mapping = {
        'Department Code': 'cost_center_code',
        'Department Name': 'cost_center_name',
        'Account Code': 'account_code',
        'Account Name': 'account_name',
        'Running Code': 'running_code',
        'Activity Name': 'activity_name',
        'Item No.': 'item_no',
        'Unique': 'unique',
        'Category of Expense': 'category_of_expense',
        'First Half Total': 'first_half_total',
        'Second Half Total': 'second_half_total',
        'Total 2025': 'total',
        "Target reduction (Start from Jul'25)": 'target_reduction',
        'AP': 'ap'
    }

    required_excel_columns = list(column_mapping.keys())

    print("=" * 60)
    print("EXCEL TO SQL SERVER: expense IMPORT TOOL")
    print("=" * 60)
    print(f"Batch size: {batch_size:,} records")
    print(f"Header row: {header_row+1}")

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
        df_head = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row, nrows=0)
        excel_cols = df_head.columns.tolist()
        print("Excel Columns Found:")
        for i, col in enumerate(excel_cols): print(f"  {i+1}. '{col}'")
    except Exception as e:
        print(f"❌ Error reading excel header: {e}")
        preview_excel_headers(file_path, sheet_name)
        return False

    missing = [col for col in required_excel_columns if col not in excel_cols]
    if missing:
        print(f"\n❌ Columns missing in Excel: {missing}")
        print("กรุณาเปิดไฟล์ excel แล้วดูว่า header ตรงกับนี้ไหม (เว้นวรรค/พิมพ์ใหญ่-เล็กต้องตรง)")
        print("\n==== Preview first 5 rows in Excel (for debug) ====")
        preview_excel_headers(file_path, sheet_name)
        print("\nTIP: ถ้า header ไม่ตรงหรืออยู่แถวอื่น ให้เปลี่ยน header_row=เลขแถว (เริ่ม 0 คือแถวแรกใน excel)")
        print("     หรือปรับชื่อ column_mapping ให้ตรงกับในไฟล์")
        return False

    # Read data from Excel
    df = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        header=header_row,
        usecols=required_excel_columns
    )
    df = df.dropna(subset=['Running Code'], how='all')
    total_rows = len(df)
    print(f"\nTotal data rows found: {total_rows:,}")
    if total_rows == 0:
        print("No data found in Excel file!")
        return

    df = df.rename(columns=column_mapping)

    # Data Cleaning
    numeric_cols = [
        'first_half_total', 'second_half_total', 'total', 'target_reduction'
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df.loc[df[col] > 2147483647, col] = None

    # Columns that must not have decimal points
    int_str_cols = [
        'cost_center_code', 'account_code', 'item_no'
    ]
    for col in int_str_cols:
        if col in df.columns:
            df[col] = df[col].apply(clean_int_str)

    string_cols = [
        'cost_center_name', 'account_name',
        'running_code', 'activity_name', 'unique', 'category_of_expense', 'ap'
    ]
    for col in string_cols:
        df[col] = df[col].apply(clean_str).astype(object)
        df[col] = df[col].str[:100] # limit string to 100 chars

    # item_no: ensure int (as str for the DB column, but not with .0)
    if 'item_no' in df.columns:
        df['item_no'] = df['item_no'].apply(clean_int_str)

    # Fetch month_id from month_expense and map
    print("\nFetching all month_id from month_expense for mapping ...")
    try:
        df_monthid = pd.read_sql("SELECT month_id, running_code FROM month_expense", conn)
        map_dict = dict(zip(df_monthid['running_code'], df_monthid['month_id']))
        df['month_id'] = df['running_code'].map(map_dict)
        print("Sample month_id mapping:")
        print(df[['running_code', 'month_id']].head())
    except Exception as e:
        print(f"❌ Error fetching or mapping month_id: {e}")
        cursor.close()
        conn.close()
        return False

    if not table_has_column(cursor, 'expense', 'month_id'):
        print("\n❌ Column 'month_id' does not exist in [expense] table! Please add this column to your SQL Server table:\n  ALTER TABLE expense ADD month_id INT NULL;")
        cursor.close()
        conn.close()
        return False

    insert_columns = [
        'month_id',
        'cost_center_code', 'cost_center_name', 'account_code', 'account_name',
        'running_code', 'activity_name', 'item_no', 'unique', 'category_of_expense',
        'first_half_total', 'second_half_total', 'total', 'target_reduction', 'ap'
    ]
    insert_sql = f"""
    INSERT INTO [dbo].[expense] (
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
                # Prepare values with correct types for SQL Server
                values = []
                for col in insert_columns:
                    val = row[col]
                    if col in numeric_cols:
                        if pd.isna(val):
                            values.append(None)
                        else:
                            try:
                                ival = float(val)
                                if ival.is_integer():
                                    values.append(int(ival))
                                else:
                                    values.append(float(ival))
                            except:
                                values.append(None)
                    elif col in int_str_cols:
                        values.append(str(val) if val not in [None, '', np.nan, 'nan'] else None)
                    else:
                        values.append(val if not (pd.isna(val) or val is None or str(val).lower() == 'nan' or str(val).strip() == '') else None)
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

    cursor.execute("SELECT COUNT(*) FROM [dbo].[expense]")
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