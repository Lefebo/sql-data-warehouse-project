/*
Procedure: bronze.load_bronze
Purpose:
        Performs a full raw data ingestion into the Bronze layer.
        Loads CRM and ERP datasets from source CSV files into the Bronze schema.
        Ensures clean landing via table truncation and BULK INSERT operations.
        Measures and logs load durations for monitoring.
        Includes error handling for safe execution.

    The Bronze layer represents the raw zone of the data warehouse,
    where source data is ingested exactly as received—without 
    transformations or business rules. This layer serves as the 
    foundation for downstream Silver (cleaned) and Gold (curated) layers.
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
   DECLARE @start_time DATETIME,@end_time DATETIME, @batch_start_time DATETIME,@batch_end_time DATETIME;
   BEGIN TRY
   PRINT '-------------------------------';
   PRINT 'Loading data to Bronze layers';
   PRINT '-------------------------------' ;
   SET @batch_start_time=GETDATE();
   PRINT '-------------------------------';
   PRINT 'Loading data from CRM Tables';
   PRINT '-------------------------------' 
   SET @start_time=GETDATE();
    TRUNCATE TABLE [bronze].[crm_cust_info];
    BULK INSERT [bronze].[crm_cust_info]
    FROM "C:\Users\ritaj\OneDrive\Desktop\Data Warehouse Project\Source_crm\cust_info.csv"
    WITH 
        (
        FIRSTROW=2,
        FIELDTERMINATOR = ',',
        TABLOCK
        );
   SET @end_time=GETDATE();
   print '>> load duration :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
   PRINT '===========';
   
   SET @start_time=GETDATE();
    TRUNCATE TABLE [bronze].[crm_prd_info];
    BULK INSERT [bronze].[crm_prd_info]
    FROM "C:\Users\ritaj\OneDrive\Desktop\Data Warehouse Project\Source_crm\prd_info.csv"
    WITH 
        (
        FIRSTROW=2,
        FIELDTERMINATOR = ',',
        TABLOCK
        );
   SET @end_time=GETDATE();
   print '>> load duration :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
   PRINT '===========';
   
   SET @start_time=GETDATE();
    TRUNCATE TABLE [bronze].[crm_sales_details];
    BULK INSERT [bronze].[crm_sales_details]
    FROM "C:\Users\ritaj\OneDrive\Desktop\Data Warehouse Project\Source_crm\sales_details.csv"
    WITH 
        (
        FIRSTROW=2,
        FIELDTERMINATOR = ',',
        TABLOCK
        );
   SET @end_time=GETDATE();
   print '>> load duration :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
   PRINT '===========';
   PRINT '------------------------------';
   PRINT 'Loading data from ERP Tables';
   PRINT '---------------------------------' ;
   
   SET @start_time=GETDATE();
    TRUNCATE TABLE [bronze].[erp_cust_az12];

    BULK INSERT [bronze].[erp_cust_az12]
    FROM "C:\Users\ritaj\OneDrive\Desktop\Data Warehouse Project\Source_erp\CUST_AZ12.csv"
    WITH 
        (
        FIRSTROW=2,
        FIELDTERMINATOR = ',',
        TABLOCK
        );
   SET @end_time=GETDATE();
   print '>> load duration :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
   PRINT '===========';

   SET @start_time=GETDATE();
    TRUNCATE TABLE [bronze].[erp_loc_a101];
    BULK INSERT [bronze].[erp_loc_a101]
    FROM "C:\Users\ritaj\OneDrive\Desktop\Data Warehouse Project\Source_erp\LOC_A101.csv"
    WITH 
        (
        FIRSTROW=2,
        FIELDTERMINATOR = ',',
        TABLOCK
        );
   SET @end_time=GETDATE();
   print '>> load duration :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
   PRINT '===========';
   
   SET @start_time=GETDATE();
    TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];
    BULK INSERT [bronze].[erp_px_cat_g1v2]
    FROM "C:\Users\ritaj\OneDrive\Desktop\Data Warehouse Project\Source_erp\PX_CAT_G1V2.csv"
    WITH 
        (
        FIRSTROW=2,
        FIELDTERMINATOR = ',',
        TABLOCK
        );
   SET @end_time=GETDATE();
   print '>> load duration :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
   PRINT '===========';

   SET @batch_end_time =GETDATE();
   PRINT 'The whole duration of our bronze load ';
   PRINT 'data load duration:' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds';
   END TRY
        BEGIN CATCH
        PRINT 'Error occured loading file to bronze layer';
        print 'Error message' + ERROR_MESSAGE();
        print 'Error message ' + CAST (ERROR_NUMBER() AS NVARCHAR);
        print 'Error message ' + CAST (ERROR_STATE() AS NVARCHAR);
        END CATCH
  
END
