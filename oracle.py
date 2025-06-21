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
        print("\nCommon solutions:")
        print("1. Check server name and database name")
        print("2. Verify username and password")
        print("3. Check if SQL Server allows remote connections")
        print("4. Install ODBC Driver 17 for SQL Server")
        return False

def import_excel_to_oracle_table(batch_size=1000, start_row=12, connection_string=None):
    if connection_string is None:
        connection_string = get_connection_string()
        if connection_string is None:
            print("❌ Cannot proceed without valid connection string")
            return False
    if not test_connection(connection_string):
        print("❌ Cannot proceed with failed connection")
        return False

    try:
        print("=" * 60)
        print("EXCEL TO SQL SERVER IMPORT TOOL")
        print("=" * 60)
        print(f"Batch size: {batch_size:,} records")
        print(f"Starting from row: {start_row + 1}")

        print("\nConnecting to SQL Server...")
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        print("\nAnalyzing Excel file...")
        df_header = pd.read_excel(
            'oracle.xlsx',
            sheet_name='oracle',
            usecols=[1, 2, 7, 8, 9, 10, 16, 17, 23, 24, 26, 27, 28, 29, 34, 35, 36],
            skiprows=start_row - 1,
            header=0,
            nrows=1
        )
        df_full = pd.read_excel(
            'oracle.xlsx',
            sheet_name='oracle',
            usecols=[1],
            skiprows=start_row,
            header=None
        )
        df_full = df_full.dropna()
        total_rows = len(df_full)
        print(f"Total data rows found: {total_rows:,}")
        if total_rows == 0:
            print("No data found in Excel file!")
            return
        total_batches = math.ceil(total_rows / batch_size)
        print(f"Will process in {total_batches:,} batches")

        column_mapping = {
            'Period': 'period',
            'Date': 'date',
            'Invoice No.': 'invoice_no',
            'Account Code': 'account_code',
            'Account Name': 'account_name',
            'Sub Account Code': 'sub_account_code',
            'Department': 'cost_center_code',
            'Department Name': 'cost_center_name',
            'Debit Accounted Amount': 'debit_accounted_amount',
            'Credit Accounted Amount': 'credit_accounted_amount',
            'Description': 'description',
            'Line Description': 'line_description',
            'Issuer': 'issuer',
            'Issuance Dept.': 'issuanee_dept',
            'Supplier Code': 'supplier_code',
            'Supplier Name': 'supplier_name',
            'Supplier Site Code': 'supplier_site_code'
        }

        def process_period(period_str):
            if pd.isna(period_str):
                return None, None, None
            try:
                month_abbr, year_short = str(period_str).split('-')
                year = f"20{year_short}"
                month_mapping = {
                    'Jan': ('01', 'January'), 'Feb': ('02', 'February'), 'Mar': ('03', 'March'),
                    'Apr': ('04', 'April'), 'May': ('05', 'May'), 'Jun': ('06', 'June'),
                    'Jul': ('07', 'July'), 'Aug': ('08', 'August'), 'Sep': ('09', 'September'),
                    'Oct': ('10', 'October'), 'Nov': ('11', 'November'), 'Dec': ('12', 'December')
                }
                month_num, month_name = month_mapping.get(month_abbr, ('', ''))
                period_sort = f"{year}{month_num}"
                return year, month_name, period_sort
            except:
                return None, None, None

        total_processed = 0
        total_success = 0
        total_errors = 0
        total_debit = 0
        total_credit = 0

        print(f"\n{'='*60}")
        print("STARTING BATCH PROCESSING")
        print(f"{'='*60}")

        # Remove combined_account_code if not in DB
        # You can get real column list from DB with the below if needed:
        # cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'oracle'")
        # print([row[0] for row in cursor.fetchall()])
        # For now, we remove combined_account_code from insert and df

        insert_columns = [
            'period', 'period_year', 'period_month', 'period_sort', 'date',
            'invoice_no', 'account_code', 'account_name', 'sub_account_code',
            'cost_center_code', 'cost_center_name', 'description', 'line_description',
            'issuer', 'issuanee_dept', 'supplier_code', 'supplier_name',
            'supplier_site_code', 'debit_accounted_amount', 'credit_accounted_amount'
        ]

        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, total_rows)
            current_batch_size = end_idx - start_idx
            print(f"\nBatch {batch_num + 1}/{total_batches}: Processing rows {start_idx + 1:,} to {end_idx:,} ({current_batch_size:,} records)")

            try:
                df = pd.read_excel(
                    'oracle.xlsx',
                    sheet_name='oracle',
                    usecols=[1, 2, 7, 8, 9, 10, 16, 17, 23, 24, 26, 27, 28, 29, 34, 35, 36],
                    skiprows=start_row + start_idx,
                    header=0 if batch_num == 0 else None,
                    nrows=current_batch_size
                )
                if batch_num > 0:
                    df.columns = list(column_mapping.keys())
                df = df.dropna(how='all')
                if len(df) == 0:
                    print(f"  Batch {batch_num + 1}: No data, skipping...")
                    continue
                df = df.rename(columns=column_mapping)
                df[['period_year', 'period_month', 'period_sort']] = df['period'].apply(
                    lambda x: pd.Series(process_period(x))
                )
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df['debit_accounted_amount'] = pd.to_numeric(df['debit_accounted_amount'], errors='coerce').fillna(0)
                df['credit_accounted_amount'] = pd.to_numeric(df['credit_accounted_amount'], errors='coerce').fillna(0)
                # df['combined_account_code'] = df['account_code'].astype(str) + '-' + df['sub_account_code'].astype(str) # REMOVE

                # ไม่ต้อง map cost_center_id, account_id, ไม่ต้องสร้าง combined_account_code

                df_insert = df[insert_columns]

                insert_sql = f"""
                INSERT INTO [dbo].[oracle] (
                    {', '.join([f'[{col}]' for col in insert_columns])}
                ) VALUES ({', '.join(['?' for _ in insert_columns])})
                """

                batch_success = 0
                batch_errors = 0

                for index, row in df_insert.iterrows():
                    try:
                        values = []
                        for col in insert_columns:
                            value = row[col]
                            if pd.isna(value):
                                values.append(None)
                            elif col in ['debit_accounted_amount', 'credit_accounted_amount']:
                                values.append(float(value) if value != 0 else None)
                            else:
                                values.append(value)
                        cursor.execute(insert_sql, tuple(values))
                        batch_success += 1
                    except Exception as e:
                        print(f"    Error in row {total_processed + index + 1}: {str(e)[:100]}...")
                        batch_errors += 1
                conn.commit()
                total_processed += len(df_insert)
                total_success += batch_success
                total_errors += batch_errors
                total_debit += df['debit_accounted_amount'].sum()
                total_credit += df['credit_accounted_amount'].sum()
                print(f"  ✅ Batch {batch_num + 1}: Success={batch_success:,}, Errors={batch_errors:,}")
                print(f"  💰 Batch Amount: Debit={df['debit_accounted_amount'].sum():,.2f}, Credit={df['credit_accounted_amount'].sum():,.2f}")
            except Exception as e:
                print(f"  ❌ Batch {batch_num + 1} failed: {e}")
                total_errors += current_batch_size
                continue

        print(f"\n{'='*60}")
        print("IMPORT SUMMARY")
        print(f"{'='*60}")
        print(f"📊 Total processed: {total_processed:,} records")
        print(f"✅ Successfully inserted: {total_success:,} records")
        print(f"❌ Errors: {total_errors:,} records")
        print(f"📈 Success rate: {(total_success/total_processed*100 if total_processed > 0 else 0):.1f}%")
        print(f"💰 Total Debit Amount: {total_debit:,.2f}")
        print(f"💰 Total Credit Amount: {total_credit:,.2f}")
        print(f"⚖️  Balance: {(total_debit - total_credit):,.2f}")

        cursor.execute("SELECT COUNT(*) FROM [dbo].[oracle] WHERE period LIKE '%25'")
        db_count = cursor.fetchone()[0]
        print(f"\n🗄️  Records in database: {db_count:,}")

        cursor.close()
        conn.close()

        print(f"\n🎉 Import completed successfully!")
        print(f"{'='*60}")
        return True

    except Exception as e:
        print(f"\n❌ Critical Error: {e}")
        return False

def analyze_excel_file(file_path='oracle.xlsx', start_row=12):
    print("EXCEL FILE ANALYSIS")
    print("="*50)
    try:
        df_sample = pd.read_excel(
            file_path,
            sheet_name='oracle',
            usecols=[1],
            skiprows=start_row,
            header=None,
            nrows=10000
        )
        df_sample = df_sample.dropna()
        sample_rows = len(df_sample)
        import openpyxl
        workbook = openpyxl.load_workbook(file_path, read_only=True)
        worksheet = workbook.active
        max_row = worksheet.max_row
        estimated_data_rows = max_row - start_row - 1
        oracle_data_rows = min(sample_rows, estimated_data_rows)
        print(f"📁 File: {file_path}")
        print(f"📊 Estimated data rows: {estimated_data_rows:,}")
        print(f"📊 Sample data rows: {sample_rows:,}")
        print(f"⏱️  Estimated processing time: {estimated_data_rows/1000*2:.1f} seconds")
        print(f"💾 Recommended batch size: {min(1000, max(100, estimated_data_rows//10))}")
        workbook.close()
    except Exception as e:
        print(f"Analysis failed: {e}")

if __name__ == "__main__":
    print("Excel to SQL Server Import Tool")
    print("Support unlimited rows with batch processing and flexible authentication")
    print("-" * 80)
    try:
        analyze_excel_file()
    except Exception as e:
        print(f"Warning: Could not analyze file - {e}")
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
    success = import_excel_to_oracle_table(
        batch_size=batch_size,
        start_row=12,
        connection_string=connection_string
    )
    if success:
        print("\n🎉 Process completed successfully!")
    else:
        print("\n❌ Process failed!")
        exit(1)