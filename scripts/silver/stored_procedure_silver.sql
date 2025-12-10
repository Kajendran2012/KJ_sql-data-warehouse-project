/*
==============================================================================================================================================================================================
This stored procedure, silver.load_silver, is an ETL script that moves and cleans data from the bronze (raw) layer to the silver (cleansed) layer.
It performs two main tasks:
silver.erp_cust_info Load (Customer Data):
Cleansing: Standardizes CID (removes 'NAS' prefix), validates BDATE (sets future dates to NULL), and normalizes GEN (maps 'F'/'M' to 'Female'/'Male', sets blanks to 'n/a').
silver.erp_px_category Load (Product Data):
Refresh: Truncates the table for a full refresh.
Cleansing: Trims whitespace from all columns.
The script includes time tracking (logging durations) and robust error handling (TRY...CATCH).
===================================================================================================================================================================================================
*/


CREATE or ALTER PROCEDURE silver.load_silver
AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME,@start_batch_time DATETIME,
		@end_batch_time DATETIME;
	
		set @start_batch_time = getdate()
		BEGIN TRY
			PRINT'======================================';
			PRINT'LOADING SILVER LAYER';
			PRINT'======================================';

			PRINT('-------------------------------------');
			PRINT('LOADING CRM SOURCE TABLES');
			PRINT('-------------------------------------');

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';

		insert into silver.crm_cust_info 
		(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_martial_status,
			cst_gndr,
			cst_create_date
		)
		select 
			cst_id,
			cst_key,
			trim(cst_firstname) as cst_firstname,
			trim(cst_lastname) as cst_lasttname,
			CASE 
			WHEN upper(trim(cst_martial_status)) = 'S' then 'Single'
			when upper(trim(cst_martial_status)) = 'M' then 'Married'
			else 'n/a'
			end cst_martial_status,
			CASE 
			when upper(trim(cst_gndr)) = 'F' then 'Female'
			when upper(trim(cst_gndr)) = 'M' then 'Male'
			else 'n/a'
			end cst_gndr,
			cst_create_date 
		from 
		(
		select 
		*, 
		row_number() over(partition by cst_id order by cst_create_date desc) as flag_last  
		from bronze.crm_cust_info 
		)t 
		where flag_last =1 
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + cast(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar(50)) + 'seconds';
			PRINT ' ----------------------------- ';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';

		insert into silver.crm_sales_details
		(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)
		select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
		case
			when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
			else cast(cast(sls_order_dt as nvarchar) as date)                     --- changing the data type to date type
		end sls_order_dt,
			cast(cast( sls_ship_dt as nvarchar) as date) as sls_ship_dt,          --- changing the data type to date type
			cast(cast( sls_due_dt as nvarchar) as date) as sls_due_dt,            --- changing the data type to date type
		case 
			when sls_sales != sls_quantity * abs(sls_price) or sls_sales is null or sls_sales <= 0   
			then sls_quantity * abs(sls_price) --- Recalculating the original value is missing or incorrect.
			else sls_sales
			end sls_sales,
			sls_quantity,
		case 
			when sls_price is null or sls_price <= 0 
			then sls_sales / nullif(sls_quantity,0) -- Deriving price if original value is invalid.
			else sls_sales
			end as sls_price 
		from bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + cast(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar(50)) + 'seconds';
			PRINT ' ----------------------------- ';

		PRINT('-------------------------------------');
		PRINT('LOADING ERP SOURCE TABLES');
		PRINT('-------------------------------------');	

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_info';
		TRUNCATE TABLE silver.erp_loc_info;
		PRINT '>> Inserting Data Into: silver.erp_loc_info';

		insert into silver.erp_loc_info 
		(
		loc_cid,
		loc_country
		)
		SELECT 
		replace(loc_CID, '-', '') as loc_idnw,
		case
		when trim(loc_country) is null or trim(loc_country) = '' then 'n/a'
		when trim(loc_country) = 'DE' then 'Germany'
		when trim(loc_country) = 'US' then 'United States'
		when trim(loc_country) = 'USA' then 'United States'
		else trim(loc_country)
		end loc_country
		FROM bronze.erp_loc_info
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + cast(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar(50)) + 'seconds';
			PRINT ' ----------------------------- ';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_info';
		TRUNCATE TABLE silver.erp_cust_info;
		PRINT '>> Inserting Data Into: silver.erp_cust_info';

		insert into silver.erp_cust_info
		(
		CID,
		BDATE,
		GEN
		)
		Select  
			CASE WHEN CID LIKE 'NAS%' THEN trim(substring(CID,4,len(cid))) 
			ELSE CID
			END CID,
			case when BDATE > getdate() then NULL
			else BDATE
			end BDATE,
			case when trim(GEN)   is null or trim(GEN) = ' ' then 'n/a'
			when trim(GEN)   = 'F' then 'Female'
			when trim(GEN)   = 'M' then 'Male'
			else trim(GEN)  
			end GEN
		from bronze.erp_cust_info
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + cast(DATEDIFF(SECOND, @start_time, @end_time) as nvarchar(50)) + 'seconds';
			PRINT ' ----------------------------- ';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_category';
		TRUNCATE TABLE silver.erp_px_category;
		PRINT '>> Inserting Data Into: silver.erp_px_category';

		insert into silver.erp_px_category
		(
		id,
		cat,
		subcat,
		maintenance
		)
		select
		trim(id) as id,
		trim(cat) as cat,
		trim(subcat) as subcat,
		trim(maintenance) as maintenance
		from bronze.erp_px_category
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
	PRINT ' LOADING SILVER LAYER IS COMPLETED:'
	PRINT ' TOTAL DURATION:' + cast(DATEDIFF(SECOND, @start_batch_time, @end_batch_time) as nvarchar(50)) + 'seconds';
	PRINT '-----------------------------------------------------'


END
