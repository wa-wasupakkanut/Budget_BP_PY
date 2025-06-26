UPDATE a
SET 
    a.cost_center_id = c.cost_center_id,
    a.account_id = acc.account_id
FROM actual a
LEFT JOIN cost_center c ON a.cost_center_code = c.cost_center_code
LEFT JOIN account acc ON a.account_code = acc.account_code;

--------------------------------------------
-- 1. ดึงข้อมูลจาก Oracle
INSERT INTO actual (period, period_year, period_month, period_sort, date, invoice_no, account_code, account_name, sub_account_code, cost_center_code, cost_center_name, description, line_description, issuer, issuanee_dept, supplier_code, supplier_name, supplier_site_code, debit_accounted_amount, credit_accounted_amount)
SELECT period, period_year, period_month, period_sort, date, invoice_no, account_code, account_name, sub_account_code, cost_center_code, cost_center_name, description, line_description, issuer, issuanee_dept, supplier_code, supplier_name, supplier_site_code, debit_accounted_amount, credit_accounted_amount
FROM OPENQUERY(ORACLESRV, 'SELECT period, period_year, period_month, period_sort, date, invoice_no, account_code, account_name, sub_account_code, cost_center_code, cost_center_name, description, line_description, issuer, issuanee_dept, supplier_code, supplier_name, supplier_site_code, debit_accounted_amount, credit_accounted_amount FROM ACTUAL_ORACLE');

-- 2.
UPDATE actual
SET combined_account_code = account_code + '-' + sub_account_code;

-- 3. Update cost_center_id และ account_id
UPDATE a
SET 
    a.cost_center_id = c.cost_center_id,
    a.account_id = acc.account_id
FROM actual a
LEFT JOIN cost_center c ON a.cost_center_code = c.cost_center_code
LEFT JOIN account acc ON a.account_code = acc.account_code;


--------------------------------------
---TABLE group_section_ratio
CREATE TABLE group_section_ratio (
    id INT IDENTITY(1,1) PRIMARY KEY,
    group_section_name VARCHAR(100) NOT NULL
);

INSERT INTO group_section_ratio (group_section_name) VALUES
('HP3 (Production Quantity)'),
('HP3 (Overhual Kit) (Production Quantity)'),
('SCV (Production Quantity)'),
('G2&G3 (Production Quantity)'),
('G2+G3+PRV (Production Quantity)'),
('RC (Production Quantity)'),
('PRV (Production Quantity)'),
('UC (Production Quantity)'),
('GDP3S (Production Quantity)'),
('HP5 (Production Quantity)'),
('HP5E (Production Quantity)'),
('G4 (Production Quantity)'),
('HP5 & G4 & HP5E (Production Quantity)'),
('SDM (Production Quantity)'),
('SKD (Production Quantity)'),
('SDM & SKD'),
('SDM (Sale Export)'),
('SKD (Sale Export)'),
('SDM & SKD (Sale Export)'),
('SDM (Sale Quantity)'),
('SKD (Sale Quantity)'),
('SDM & SKD (Sale Quantity)'),
('QA Diesel (Production Quantity)'),
('QA Gasoline (Production Quantity)'),
('QA Gas & SKD (Production Quantity)'),
('PC Diesel (Sale Export)'),
('PC UC (Sale Export)'),
('PC GDP (Sale Export)'),
('Washing INJ (Production Quantity)'),
('Washing Pump (Production Quantity)'),
('G2&G3 (Assy Production Quantity)'),
('G4 (Assy Production Quantity)');
-----------------------------------------------
CREATE TABLE product_production_ratio (
    product_production_ratio_id INT IDENTITY(1,1) PRIMARY KEY,
    month_list_id INT NOT NULL,
    year_list_id INT NOT NULL,
    group_section_ratio_id INT NOT NULL,
    Plan DECIMAL(18,3) NOT NULL,
    Result DECIMAL(18,3) NOT NULL,
    CONSTRAINT fk_month_list_id FOREIGN KEY (month_list_id) REFERENCES month_list(month_id),
    CONSTRAINT fk_year_list_id FOREIGN KEY (year_list_id) REFERENCES year_list(year_id),
    CONSTRAINT fk_group_section_ratio_id FOREIGN KEY (group_section_ratio_id) REFERENCES group_section_ratio(id)
);

-- ถ้าต้องการ insert สำหรับทุก group_section_ratio_id (1-32) วนเดือนและปี สามารถใช้ Cursor หรือ Loop เพื่อสร้างชุดข้อมูลได้
-------------------------------------------------------
CREATE TABLE expense (
    cost_center_code VARCHAR(20),
    cost_center_name VARCHAR(100),
    account_code VARCHAR(20),
    account_name VARCHAR(100),
    running_code VARCHAR(30),
    activity_name NVARCHAR(255),
    project_no VARCHAR(50),
    item_no INT,
    unique_field VARCHAR(100),  -- 'unique' is a reserved word in SQL, so renamed to 'unique_field'
    month date,
    plan DECIMAL(18,2)
);
----------------------------------------------------------------------------
CREATE TABLE actual (
    actual_id INT,
    cost_center_id INT,
    account_id INT,
    oracle_id INT,
    actual DECIMAL(18,2)
);
----------------------------------------------------
CREATE TABLE actual (
    account_code              VARCHAR(20),
    debit_accounted_amount    DECIMAL(18,2),
    credit_accounted_amount   DECIMAL(18,2),
    actual                    DECIMAL(18,2)
);
-----------------------------------------------------
CREATE TABLE plan (
    account_code              VARCHAR(20),
    month                     VARCHAR(20)
    plan                      DECIMAL(18,2)
);
------------------------------------------------------------
CREATE TABLE master (
    account_code              VARCHAR(20),
    month                     VARCHAR(20)
    plan                      DECIMAL(18,2)
);
------------------------------------------------------------------
DELETE FROM oracle;

ALTER TABLE oracle
DROP COLUMN actual;

DELETE FROM oracle;
DELETE FROM expense;

DROP TABLE actual;
