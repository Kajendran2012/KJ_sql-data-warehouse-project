/*
=======================================================
Stored procedure: Load Bronze Layer( Source -> Bronze )
=======================================================
Script's Purpose:
  This stored procedure loads data into the 'bronze' schema from external csv files.
  It performs the following actions:
- Truncate the bronze tables before loading data.
- uses the 'BULK INSERT' command to load data from CSV files to bronze tables.

PARAMETERS
  None.
This stored procedure does not accept any parameters or return any values.
Usage Example:
  EXEC bronze.load_bronze;
=======================================================
*/


-- upload the file as Bulk ---
exec bronze.load_bronze 		

CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME,@start_batch_time DATETIME,
	@end_batch_time DATETIME;
	
	set @start_batch_time = getdate()
	BEGIN TRY
		PRINT'======================================';
		PRINT'LOADING BRONZE LAYER';
		PRINT'======================================';

		PRINT('-------------------------------------');
		PRINT('LOADING CRM SOURCE TABLES');
		PRINT('-------------------------------------');

		SET @start_time = GETDATE();
		PRINT(' >> TRUNCATING TABLE:bronze.crm_cust_info');
		truncate table bronze.crm_cust_info;
		PRINT(' >> INSERTING TABLE:bronze.crm_cust_info');
		BULK INSERT bronze.crm_cust_info
		from 'C:\Users\USER\Desktop\SQL with Baraa\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH 
		(
			FIRSTROW = 2,
			fieldterminator = ',',
			tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + cast(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar(50)) + 'seconds';
		PRINT ' ----------------------------- ';

		SET @start_time = GETDATE();
		PRINT(' >> TRUNCATING TABLE:bronze.crm_prd_info');
		truncate table [bronze].[crm_prd_info];
		PRINT(' >> INSERTING TABLE:bronze.crm_prd_info');
		BULK INSERT [bronze].[crm_prd_info]
		from 'C:\Users\USER\Desktop\SQL with Baraa\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH 
		(
			FIRSTROW = 2,
			fieldterminator = ',',
			tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + cast(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar(50)) + 'seconds';
		PRINT ' ----------------------------- ';

		SET @start_time = GETDATE();
		PRINT(' >> TRUNCATING TABLE:[bronze].[crm_sales_details]');
		truncate table [bronze].[crm_sales_details];
		PRINT(' >> INSERTING TABLE:[crm_sales_details]');
		BULK INSERT [bronze].[crm_sales_details]
		from 'C:\Users\USER\Desktop\SQL with Baraa\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH
		(
			FIRSTROW = 2,
			fieldterminator = ',',
			tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + cast(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar(50)) + 'seconds';
		PRINT ' ----------------------------- ';

		PRINT('-------------------------------------');
		PRINT('LOADING ERP SOURCE TABLES');
		PRINT('-------------------------------------');

		SET @end_time = GETDATE();
		PRINT(' >> TRUNCATING TABLE:[bronze].[bronze].[erp_cust_info]');
		truncate table [bronze].[erp_cust_info];
		PRINT(' >> INSERTING TABLE:[bronze].[bronze].[erp_cust_info]');
		BULK INSERT [bronze].[erp_cust_info]
		from 'C:\Users\USER\Desktop\SQL with Baraa\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH
		(
			FIRSTROW = 2,
			fieldterminator = ',',
			tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + cast(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar(50)) + 'seconds';
		PRINT ' ----------------------------- ';

		SET @end_time = GETDATE();
		PRINT(' >> TRUNCATING TABLE:[bronze].[erp_loc_info]');
		truncate table [bronze].[erp_loc_info];
		PRINT(' >> INSERTING TABLE:[bronze].[erp_loc_info]');
		BULK INSERT [bronze].[erp_loc_info]
		from 'C:\Users\USER\Desktop\SQL with Baraa\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH
		(
			FIRSTROW = 2,
			fieldterminator = ',',
			tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + cast(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar(50)) + 'seconds';
		PRINT ' ----------------------------- ';

		SET @end_time = GETDATE();
		PRINT(' >> TRUNCATING TABLE:[bronze].[erp_px_category]');
		truncate table [bronze].[erp_px_category];
		PRINT(' >> INSERTING TABLE:[bronze].[erp_px_category]');
		BULK INSERT [bronze].[erp_px_category]
		from 'C:\Users\USER\Desktop\SQL with Baraa\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH
		(
			FIRSTROW = 2,
			fieldterminator = ',',
			tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + cast(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar(50)) + 'seconds';
		PRINT ' ----------------------------- ';


	END TRY 
	BEGIN CATCH 
		PRINT'========================================================';
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT'ERROR MESSAGE:' + ERROR_MESSAGE();
		PRINT'ERROR MESSAGE:' + CAST(ERROR_NUMBER() AS NVARCHAR(50));
		PRINT'ERROR MESSAGE:' + CAST(ERROR_STATE() AS NVARCHAR(50));
		PRINT'========================================================';
	END CATCH

	set @end_batch_time = getdate()
	PRINT '----------------------------------------------------'
	PRINT ' LOADING BRONZE LAYER IS COMPLETED:'
	PRINT ' TOTAL DURATION:' + cast(DATEDIFF(SECOND, @start_batch_time, @end_batch_time) as nvarchar(50)) + 'seconds';
	PRINT '-----------------------------------------------------'

END


