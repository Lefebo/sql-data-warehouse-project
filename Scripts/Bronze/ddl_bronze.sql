/*
Bronze Table Overview
============================================
The bronze tables are the first layer in the data pipeline. Their purpose is to capture and store raw, unprocessed data exactly 
as it comes from the source systems such as ERP and CRM. The scripts provided here create the bronze-layer tables under the bronze 
schema and serve as the landing zone for all incoming source data.This layer acts as the foundational storage stage in the data-
engineering workflow. By keeping the raw data intact, the bronze layer ensures that we always have an auditable, reproducible 
version of the source data before any cleaning, transformation, or business logic is applied. Future layers (such as silver and 
gold) will read from these bronze tables to perform validation, enrichment, and modeling.
/*


-----1
GO
IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL
DROP TABLE bronze.crm_cust_info;
create table bronze.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_martial_status NVARCHAR(50),
cst_gender NVARCHAR(50),
cst_create_date DATE
);
----------2
GO

IF OBJECT_ID ('bronze.crm_prd_info','U') IS NOT NULL
 DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm  NVARCHAR(100),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE);
GO
--------------3
IF OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL
 DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT);
GO

-----------------4
IF OBJECT_ID ('bronze.erp_cust_az12','U') IS NOT NULL
 DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 
(
CID NVARCHAR(50),
BDATE DATE,
GEN NVARCHAR(50)

);
GO

------------------------5
IF OBJECT_ID ('bronze.erp_loc_a101','U') IS NOT NULL
 DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
CID NVARCHAR(50),
CNTRY NVARCHAR(50), 
);
GO

-----------------------------------6
IF OBJECT_ID ('bronze.erp_px_cat_g1v2','U') IS NOT NULL
 DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
ID NVARCHAR(50),
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(100)
);

GO
