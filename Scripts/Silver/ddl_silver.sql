/*
Purpose of the Silver Layer DDL

The Silver schema contains cleaned, standardized, and lightly transformed data derived from the raw Bronze tables.
While the Bronze layer preserves the raw source format, the Silver layer focuses on:

🧼 Data cleaning (handling nulls, fixing data types, removing duplicates)

🏗️ Standardizing structures (aligning column naming, formats, and references)

🔗 Applying basic joins to bring related datasets together

✔️ Ensuring data quality rules (valid dates, valid IDs, correct domain values)

📚 Preparing data for business modeling in the Gold layer

The purpose of the Silver DDL is to define all the tables and structures needed to hold this refined data.
These tables serve as the backbone for downstream analytical models, facts, and dimensions.

🪙 What the Silver Layer Is (Good Explanation)

The Silver layer is the refined, cleaned, and validated version of your data warehouse pipeline.
It sits between raw ingestion (Bronze) and business-ready presentation (Gold).

🔎 Key Characteristics of the Silver Layer

Cleaned & standardized: Source inconsistencies are fixed

Type-casted: Values are converted to proper SQL data types

Validated: Data quality checks ensure reliable downstream use

Integrated: Related objects (e.g., customer + location) are joined

Slowly changing structure: Prepares entities for dimensional modeling

Query-friendly: Data is shaped to be easy to analyze and aggregate
*/

-----1
GO
IF OBJECT_ID ('silver.crm_cust_info','U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info(
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_martial_status NVARCHAR(50),
    cst_gender NVARCHAR(50),
    cst_create_date DATE,
    dwh_create_date Datetime DEFAULT GETDATE()
);
----------2
GO

IF OBJECT_ID ('silver.crm_prd_info','U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info(
    prd_id INT,
    cat_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm  NVARCHAR(100),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date Datetime DEFAULT GETDATE()
);
GO

--------------3
IF OBJECT_ID ('silver.crm_sales_details','U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date Datetime DEFAULT GETDATE()
);
GO

-----------------4
IF OBJECT_ID ('silver.erp_cust_az12','U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12 
(
    CID NVARCHAR(50),
    BDATE DATE,
    GEN NVARCHAR(50),
    dwh_create_date Datetime DEFAULT GETDATE()
);
GO

------------------------5
IF OBJECT_ID ('silver.erp_loc_a101','U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101(
    CID NVARCHAR(50),
    CNTRY NVARCHAR(50),
    dwh_create_date Datetime DEFAULT GETDATE()
);
GO

-----------------------------------6
IF OBJECT_ID ('silver.erp_px_cat_g1v2','U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2(
    ID NVARCHAR(50),
    CAT NVARCHAR(50),
    SUBCAT NVARCHAR(50),
    MAINTENANCE NVARCHAR(100),
    dwh_create_date Datetime DEFAULT GETDATE()
);
GO


