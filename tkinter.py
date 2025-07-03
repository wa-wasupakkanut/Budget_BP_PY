import tkinter as tk

def run_action():
    print("กำลังทำงาน...")  # หรือใส่ฟังก์ชันที่คุณต้องการให้ทำงานตรงนี้

# สร้างหน้าต่างหลัก
root = tk.Tk()
root.title("โปรแกรมคลิกเดียวเพื่อ Run")
root.geometry("300x150")

# สร้างปุ่ม Run
run_button = tk.Button(root, text="Run", command=run_action, font=("Arial", 14), bg="green", fg="white")
run_button.pack(pady=40)

# เริ่มต้น loop ของ tkinter
root.mainloop()
