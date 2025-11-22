
CREATE OR ALTER PROCEDURE Silver.load_silver AS
BEGIN
DECLARE @start_time DATETIME,@end_time DATETIME, @batch_start_time DATETIME,@batch_end_time DATETIME;

set @start_time=GETDATE()
PRINT 'Load the whole CRM tables'
PRINT '>> Truncating Table : Silver.crm_cus_info'
TRUNCATE TABLE [silver].[crm_cust_info];   
PRINT '>> Inserting Data into Table : silver.crm_cus_info'
INSERT INTO [silver].[crm_cust_info] (
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_martial_status,
cst_gender,
cst_create_date
)
select 
   cst_id,
   cst_key,
   TRIM(cst_firstname) as cst_firstname,
   TRIM(cst_lastname) as cst_lastname,
   Case when Upper(TRIM(cst_martial_status)) = 'S' Then 'Single'
            when Upper(TRIM(cst_martial_status)) = 'M' Then 'Married'
            else 'n/a'
            end as cst_martial_status,
   Case when Upper(TRIM(cst_gender)) = 'F' Then 'Female'
            when Upper(TRIM(cst_gender)) = 'M' Then 'Male'
            else 'n/a'
            end as cst_gender,
   cst_create_date


from ( 

select 
   *
from 
(select 

        *,
        ROW_NUMBER() over (partition by cst_id order by  cst_create_date desc) as rn
       
from [bronze].[crm_cust_info]
 
where [bronze].[crm_cust_info].[cst_id] IS NOT NULL

) t
where rn = 1 
) d;


PRINT '>> Truncating Table : Silver.crm_prd_info'
TRUNCATE TABLE [silver].[crm_prd_info];   
PRINT '>> Inserting Data into Table : silver.crm_prd_info' 
Insert into silver.crm_prd_info(
    prd_id, 
    cat_id ,
    prd_key ,
    prd_nm  ,
    prd_cost,
    prd_line ,
    prd_start_dt ,
    prd_end_dt 
    
)
select 
     prd_id,
     prd_nm,
     REPLACE(SUBSTRING(TRIM(prd_key),1,5),'-','_') as cat_id,
     SUBSTRING(TRIM(prd_key),7,len(prd_key)) as prd_key,
     ISNULL(PRD_COST,0) AS PRD_COST,
      CASE UPPER(TRIM(PRD_LINE))
         WHEN 'M' THEN 'MANUFACTURING'
         WHEN 'T' THEN 'TESTING'
         WHEN 'R' THEN 'REVIEW'
         WHEN 'S' THEN 'STAGING'
         ELSE 'n/a' 
         end as prd_line,

     CAST (prd_start_dt As DATE) as prd_start_dt,
      CAST(
         DATEADD(DAY, -1,
             LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
         ) 
         AS DATE
     ) AS prd_end_date 
from bronze.crm_prd_info;


PRINT '>> Truncating Table : Silver.crm_sales_details'
TRUNCATE TABLE [silver].[CRM_SALES_DETAILS];   
PRINT '>> Inserting Data into Table : silver.crm_sales_details' 
 
INSERT INTO SILVER.CRM_SALES_DETAILS(
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  sls_order_dt,
  sls_ship_dt,
  sls_due_dt,
  sls_quantity,
  sls_sales,
  sls_price
)
select 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0 or len(sls_order_dt)!=8 then Null
    else CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE WHEN  sls_ship_dt = 0 or len( sls_ship_dt)!=8 then Null
    else CAST(CAST( sls_ship_dt AS VARCHAR) AS DATE)
    END AS  sls_ship_dt,
   CASE WHEN   sls_due_dt = 0 or len( sls_due_dt)!=8 then Null
    else CAST(CAST(  sls_due_dt AS VARCHAR) AS DATE)
    END AS   sls_due_dt,
    sls_quantity,
    CASE when sls_sales is null or sls_sales <0 OR sls_sales ! = abs(sls_price) * sls_quantity then abs(sls_price) * sls_quantity
else sls_sales end as sls_sales,
CASE when sls_price is null or sls_price <0 then sls_sales / sls_quantity
else sls_price end as sls_price
from bronze.crm_sales_details;
set @end_time=GETDATE()
print '>> load duration for CRM silver table :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' ' +'seconds';
PRINT '===========';
PRINT '>> load the silver ERP table'
set @start_time=GETDATE()
PRINT '>> Truncating Table : Silver.erp_px_cat_g1v2'
TRUNCATE TABLE [silver].[erp_px_cat_g1v2];   
PRINT '>> Inserting Data into Table :  Silver.erp_px_cat_g1v2'  

INSERT INTO silver.erp_px_cat_g1v2(
     ID,
     CAT,
     SUBCAT,
     MAINTENANCE
)
select 
     ID,
     CAT,
     SUBCAT,
     MAINTENANCE
from 
    bronze.erp_px_cat_g1v2;


PRINT '>> Truncating Table : Silver.erp_cust_az12'
TRUNCATE TABLE [silver].[erp_cust_az12];   
PRINT '>> Inserting Data into Table :  Silver.erp_cust_az12'  

INSERT INTO Silver.erp_cust_az12(
cid,
bdate,
gen
)
select 
    CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,len(cid))
         else cid
         end as cid,
   CASE when bdate > GETDATE() or bdate < CAST('1930-01-01' as DATE) then NULL
        else bdate
        end as bdate,
    Case when UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
         when UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
         else 'n/a'
         end as gen
 from bronze.erp_cust_az12;

PRINT '>> Truncating Table : silver.erp_loc_a101'
TRUNCATE TABLE [silver].[erp_loc_a101];   
PRINT '>> Inserting Data into Table :  silver.erp_loc_a101' 


INSERT INTO silver.erp_loc_a101(
CID,
CNTRY
)

select 
   REPLACE(CID,'-','') as cid,
   CASE when UPPER(TRIM(cntry)) in ('US','USA') THEN 'United States'
        when UPPER(TRIM(cntry)) in ('DE') THEN 'Germany'
        when TRIM(CNTRY) = '' or cntry is null then 'n/a'
        ELSE cntry
        end as cntry
 from bronze.erp_loc_a101;
set @end_time = GETDATE()

PRINT 'The duration of loading ERP tables are :' + CAST(DATEDIFF(Second,@start_time,@end_time) AS NVARCHAR) +' ' +'seconds'
END

