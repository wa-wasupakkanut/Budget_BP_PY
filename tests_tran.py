import os
import pandas as pd
import datetime

def convert_to_long_format(df):
    print("Columns in DataFrame:", df.columns.tolist())

    # 1. เลือกคอลัมน์ B-K (index 1 ถึง 10)
    main_cols = df.columns[1:10].tolist()

    # 2. เลือกเดือน L-Q (index 11 ถึง 16) + S-X (index 18 ถึง 23)
    month_cols = df.columns[11:17].tolist() + df.columns[18:24].tolist()

    # 3. คอลัมน์เพิ่มเติมที่ต้องแนบไปกับ long format
    desired_extra_cols = [
        "Target reduction (Start from Jul'25)",
        'ratio_plan_MC', 'ratio_result_MC',
        'ratio_plan_ASSY', 'ratio_result_ASSY'
    ]

    # เพิ่มคอลัมน์เปล่าหากยังไม่มีใน DataFrame
    for col in desired_extra_cols:
        if col not in df.columns:
            df[col] = None

    print("Main columns (B-K):", main_cols)
    print("Month columns (L-Q,S-X):", month_cols)
    print("Extra columns used:", desired_extra_cols)

    # 4. melt เฉพาะเดือน พร้อมแนบคอลัมน์เพิ่มเติม
    df_long = pd.melt(
        df,
        id_vars=main_cols + desired_extra_cols,
        value_vars=month_cols,
        var_name='Month',
        value_name='Plan'
    )

    # 5. จัดเรียงคอลัมน์ตามลำดับที่ต้องการ
    desired_order = [
        'Department Code', 'Department Name', 'Account Code', 'Account Name', 'Running Code',
        'Activity Name', 'Project No', 'Item No.', 'Unique', 'Month', 'Plan',
        "Target reduction (Start from Jul'25)",
        'ratio_plan_MC', 'ratio_result_MC',
        'ratio_plan_ASSY', 'ratio_result_ASSY'
    ]

    # ตรวจสอบว่าคอลัมน์มีอยู่จริงก่อนจัดเรียง
    existing_cols = [col for col in desired_order if col in df_long.columns]
    df_long = df_long[existing_cols]

    # 6. สร้างชื่อไฟล์และพาธในโฟลเดอร์ปัจจุบัน
    local_path = os.path.join(os.getcwd(), f"Expense_Final_Long_Format.xlsx")

    # 7. บันทึกไฟล์
    try:
        df_long.to_excel(local_path, index=False)
        print(f"ไฟล์ถูกบันทึกไว้ที่โฟลเดอร์ปัจจุบัน: {local_path}")
    except Exception as e:
        print(f"เกิดข้อผิดพลาดในการบันทึกไฟล์: {e}")

# อ่านไฟล์ Excel โดยไม่กำหนด usecols เพื่อให้ดึงทุกคอลัมน์
df_main = pd.read_excel(
    'Expense.xlsx',
    sheet_name='Living cost merge',
    header=0,
    skiprows=2,
    engine='openpyxl'
)

# เรียกใช้ฟังก์ชัน
convert_to_long_format(df_main)
