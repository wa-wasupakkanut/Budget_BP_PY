import os
import pandas as pd
import datetime

def convert_to_long_format(df):
    print("Columns in DataFrame:", df.columns.tolist())

    # 1. เลือกคอลัมน์ B-K (index 0 ถึง 10)
    main_cols = df.columns[1:10].tolist()  # B-K รวม 11 คอลัมน์แรก

    # 2. เลือกเดือน L-Q (index 11 ถึง 16) + S-X (index 18 ถึง 23)
    month_cols = df.columns[11:17].tolist() + df.columns[18:24].tolist()  # L-Q และ S-X

    print("Main columns (B-K):", main_cols)
    print("Month columns (L-Q,S-X):", month_cols)

    # 3. melt เฉพาะเดือน (L-Q,S-X)
    df_long = pd.melt(df, id_vars=main_cols, value_vars=month_cols, var_name='Month', value_name='Plan')

    # 4. บันทึกไฟล์
    timestamp = datetime.datetime.now()
    download_path = os.path.join(os.path.expanduser("~"), "Downloads", f"Expense_Final_Long_Format.xlsx")

    try:
        df_long.to_excel(download_path, index=False)
        print(f"ไฟล์ถูกบันทึกที่: {download_path}")
    except PermissionError:
        print("เกิดข้อผิดพลาด: ไม่สามารถบันทึกไฟล์ได้ อาจเปิดไฟล์นี้ค้างอยู่ใน Excel หรือไม่มีสิทธิ์เขียนไฟล์ที่ Downloads")
        local_path = f"Expense_Final_Long_Format_{timestamp}.xlsx"
        df_long.to_excel(local_path, index=False)
        print(f"จึงบันทึกไฟล์ไว้ที่โฟลเดอร์ปัจจุบันแทน: {local_path}")

# อ่านไฟล์ Excel โดยไม่กำหนด usecols เพื่อให้ดึงทุกคอลัมน์
df_main = pd.read_excel(
    'Expense.xlsx',
    sheet_name='Living cost merge',
    header=0,
    skiprows=2
)

convert_to_long_format(df_main)