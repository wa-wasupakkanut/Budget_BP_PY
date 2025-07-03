CREATE TABLE "oracle"(
    "period" VARCHAR(255) NULL,
    "date" VARCHAR(255) NULL,
    "invoice_no" VARCHAR(255) NULL,
    "account_code" VARCHAR(255) NULL,
    "account_name" VARCHAR(255) NULL,
    "sub_account_code" VARCHAR(255) NULL,
    "cost_center_code" VARCHAR(255) NULL,
    "cost_center_name" VARCHAR(255) NULL,
    "description" VARCHAR(255) NULL,
    "line_description" VARCHAR(255) NULL,
    "issuer" VARCHAR(255) NULL,
    "issuanee_dept" VARCHAR(255) NULL,
    "supplier_code" VARCHAR(255) NULL,
    "supplier_name" VARCHAR(255) NULL,
    "supplier_site_code" VARCHAR(255) NULL
);
--------------------------------------------------
CREATE TABLE actual (
    [date] DATE,
    [invoice_no] NVARCHAR(50),
    [account_name] NVARCHAR(100),
    [cost_center_code] NVARCHAR(50),
    [cost_center_name] NVARCHAR(100),
    [description] NVARCHAR(MAX),
    [line_description] NVARCHAR(MAX),
    [issuer] NVARCHAR(100),
    [issuanee_dept] NVARCHAR(100),
    [supplier_code] NVARCHAR(50),
    [supplier_name] NVARCHAR(100),
    [supplier_site_code] NVARCHAR(50),
    [debit_accounted_amount] DECIMAL(18,2),
    [credit_accounted_amount] DECIMAL(18,2),
    [combined_acc_code] NVARCHAR(100)
);
