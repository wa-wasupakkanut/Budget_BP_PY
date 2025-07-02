import pandas as pd
from sqlalchemy import create_engine

# ตั้งค่าชื่อไฟล์และชีท
excel_file = "expense.xlsx"
sheet_name = "month_expense"

# กำหนดข้อมูลเชื่อมต่อ SQL Server
server = '10.73.148.27'
database = 'Budget_BP'
username = 'TN00244'
password = 'Wasupakkanut@TN00244'
table_name = 'month_expense'
port = 1433 # port ไหน

# กำหนดคอลัมน์ใน Excel ที่ต้องการนำเข้า และ mapping เป็นชื่อคอลัมน์ใน DB
excel_columns = ['Running Code', 'April', 'June', 'July', 'August', 'September', 'October', 
                 'November', 'December', 'January', 'February', 'March', "Target reduction (Start from Jul'25)"]
db_columns = ['running_code', 'april', 'june', 'july', 'august', 'september', 'october', 
              'november', 'december', 'january', 'february', 'march', 'target_reduction']  # ชื่อคอลัมน์ใน DB

# อ่านเฉพาะคอลัมน์ที่ต้องการ
df = pd.read_excel(excel_file, sheet_name=sheet_name, usecols=excel_columns)

# เปลี่ยนชื่อคอลัมน์ใน DataFrame ให้ตรงกับ DB
df.columns = db_columns

# สร้าง connection string
engine = create_engine(
    f"mssql+pyodbc://{username}:{password}@{server},{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
)
# นำข้อมูลเข้า SQL Server (เฉพาะคอลัมน์ที่เลือกและแม็ปชื่อแล้ว)
df.to_sql(table_name, engine, if_exists='append', index=False)  # ใช้ append ถ้าไม่ต้องการลบทิ้งทุกครั้ง