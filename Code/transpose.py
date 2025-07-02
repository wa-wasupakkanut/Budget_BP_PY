import os
import pandas as pd

def convert_to_long_format(df):
    """
    แปลงข้อมูลจาก wide เป็น long format โดยไม่คำนวณสูตร
    """
    print("Columns in DataFrame:", df.columns.tolist())

    # คอลัมน์หลัก
    main_cols = df.columns[1:10].tolist()

    # คอลัมน์เดือน
    month_cols = df.columns[11:17].tolist() + df.columns[18:24].tolist()

    # คอลัมน์เพิ่มเติม
    desired_extra_cols = [
        "Target reduction (Start from Jul'25)",
        'ratio_plan_MC', 'ratio_result_MC',
        'ratio_plan_ASSY', 'ratio_result_ASSY'
    ]

    # เพิ่มคอลัมน์เปล่าหากยังไม่มี
    for col in desired_extra_cols:
        if col not in df.columns:
            df[col] = None

    print("Main columns (B-K):", main_cols)
    print("Month columns (L-Q,S-X):", month_cols)
    print("Extra columns used:", desired_extra_cols)

    # melt เฉพาะเดือน
    df_long = pd.melt(
        df,
        id_vars=main_cols + desired_extra_cols,
        value_vars=month_cols,
        var_name='Month',
        value_name='Plan'
    )

    # จัดเรียงคอลัมน์
    desired_order = main_cols + ['Month', 'Plan'] + desired_extra_cols
    existing_cols = [col for col in desired_order if col in df_long.columns]
    df_long = df_long[existing_cols]

    # แปลง Plan เป็นตัวเลข
    df_long['Plan'] = pd.to_numeric(df_long['Plan'], errors='coerce').fillna(0)

    # ไม่ใส่ค่าอะไรในคอลัมน์เพิ่มเติมสำหรับไฟล์หลัก (เก็บเป็นค่าว่าง)
    for col in desired_extra_cols:
        df_long[col] = None

    return df_long

def save_master_file(df_long, output_path):
    """
    บันทึกไฟล์หลักโดยใช้ชื่อไฟล์ Expense_Final_Long_Format2.xlsx
    """
    master_file_path = os.path.join(output_path, "Expense_Final_Long_Format2.xlsx")
    try:
        df_long.to_excel(master_file_path, index=False)
        print(f"✅ ไฟล์หลักถูกบันทึกที่: {master_file_path}")
        return True
    except Exception as e:
        print(f"❌ เกิดข้อผิดพลาดในการบันทึกไฟล์หลัก: {e}")
        return False

def save_monthly_files(df_long, database_path):
    """
    แยกข้อมูลตามเดือนและคำนวณสูตรก่อนบันทึก
    """
    print("กำลังแยกไฟล์ตามเดือน...")

    if not os.path.exists(database_path):
        print(f"❌ โฟลเดอร์ {database_path} ไม่พบ")
        return False

    saved_files = []

    for month in df_long['Month'].unique():
        if pd.isna(month):
            continue

        df_month = df_long[df_long['Month'] == month].copy()

        # คำนวณสูตรใหม่สำหรับไฟล์รายเดือน
        df_month["Target reduction (Start from Jul'25)"] = df_month['Plan'] * 0.1  # 10% ของค่า Plan
        df_month['ratio_plan_MC'] = df_month['Plan'] * 0.05 / 100  # 0.05%
        df_month['ratio_result_MC'] = df_month['Plan'] * 0.03 / 100  # 0.03%
        df_month['ratio_plan_ASSY'] = df_month['Plan'] * 0.02 / 100  # 0.02%
        df_month['ratio_result_ASSY'] = df_month['Plan'] * 0.01 / 100  # 0.01%

        # สร้างชื่อไฟล์
        safe_month_name = str(month).replace('/', '_').replace('\\', '_').replace(':', '_')
        month_file_path = os.path.join(database_path, f"{safe_month_name}.xlsx")

        try:
            df_month.to_excel(month_file_path, index=False)
            saved_files.append(month_file_path)
            print(f"✅ บันทึกไฟล์เดือน: {month_file_path}")
        except Exception as e:
            print(f"❌ เกิดข้อผิดพลาดในการบันทึกไฟล์เดือน {month}: {e}")

    print(f"✅ บันทึกไฟล์รายเดือนทั้งหมด {len(saved_files)} ไฟล์")
    return len(saved_files) > 0

def main():
    """
    ฟังก์ชันหลัก
    """
    input_file = r"D:\Budget\Budget_BP\Data\expense.xlsx"
    output_get_path = r"D:\Budget\Budget_BP\Get"
    output_database_path = r"D:\Budget\Budget_BP\Database"

    print("🚀 เริ่มต้นการประมวลผลข้อมูล Budget...")
    print(f"📁 อ่านไฟล์จาก: {input_file}")
    print(f"📁 บันทึกไฟล์หลักไปที่: {output_get_path}")
    print(f"📁 บันทึกไฟล์รายเดือนไปที่: {output_database_path}")

    if not os.path.exists(input_file):
        print(f"❌ ไม่พบไฟล์: {input_file}")
        return False

    for path in [output_get_path, output_database_path]:
        if not os.path.exists(path):
            print(f"❌ ไม่พบโฟลเดอร์: {path}")
            return False

    try:
        df_main = pd.read_excel(
            input_file,
            sheet_name='Living cost merge',
            header=0,
            skiprows=2,
            engine='openpyxl'
        )

        print(f"📊 อ่านข้อมูลได้ {len(df_main)} แถว, {len(df_main.columns)} คอลัมน์")
        print(f"คอลัมน์ทั้งหมด: {df_main.columns.tolist()[:10]}...")

        df_long = convert_to_long_format(df_main)
        print(f"✅ แปลงข้อมูลเสร็จสิ้น: {len(df_long)} แถว")

        if not save_master_file(df_long, output_get_path):
            return False

        if not save_monthly_files(df_long, output_database_path):
            return False

        print("🎉 ประมวลผลข้อมูลเสร็จสมบูรณ์!")
        return True

    except Exception as e:
        print(f"❌ เกิดข้อผิดพลาดในการประมวลผล: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("\n✅ โปรแกรมทำงานเสร็จสิ้นเรียบร้อย")
    else:
        print("\n❌ โปรแกรมทำงานไม่สำเร็จ โปรดตรวจสอบข้อผิดพลาด")