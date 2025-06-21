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
DELETE FROM oracle;