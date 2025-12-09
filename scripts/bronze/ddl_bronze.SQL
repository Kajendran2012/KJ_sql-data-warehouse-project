/*
=======================================================
Bronze Layer Data Ingestion and ETL Flow Control
=======================================================
Script's Purpose:
  This script creates a table in the 'bronze' schema, dropping existing tables if they already exist
Run this scripts to re-define the DDL structure of 'bronze' Tables
=======================================================
*/


create database DataWarehouse;
use DataWarehouse
create schema bronze
go
create schema silver
go
create schema gold

if object_id ('bronze.crm_cust_info', 'U') is not null
	drop table bronze.crm_cust_info;
create table bronze.crm_cust_info
(
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_martial_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date
);

if object_id ('bronze.crm_prd_info', 'U') is not null
	drop table bronze.crm_prd_info;
create table bronze.crm_prd_info
(
	prd_id	int,
	prd_key	nvarchar(50),
	prd_nm	nvarchar(50),
	prd_cost int,	
	prd_line nvarchar(50),	
	prd_start_dt datetime,
	prd_end_dt datetime
);
if object_id ('bronze.crm_sales_details', 'U') is not null
	drop table bronze.crm_sales_details
create table bronze.crm_sales_details
(
	sls_ord_num	nvarchar(50),
	sls_prd_key	nvarchar(50),
	sls_cust_id	int,
	sls_order_dt int, 
	sls_ship_dt	int,
	sls_due_dt	int,
	sls_sales	int,
	sls_quantity int,
	sls_price int
);
if object_id ('bronze.erp_cust_info', 'U') is not null
	drop table bronze.erp_cust_info
create table bronze.erp_cust_info
(
	CID	nvarchar(50),
	BDATE date,
	GEN nvarchar(50)
);
if object_id ('bronze.erp_loc_info', 'U') is not null
	drop table bronze.erp_loc_info
create table bronze.erp_loc_info
(
	loc_CID	nvarchar(50),
	loc_country nvarchar(50)
);

if object_id ('bronze.erp_px_category', 'U') is not null
	drop table bronze.erp_px_category
create table bronze.erp_px_category
(
	ID	nvarchar(50),
	CAT	nvarchar(50),
	SUBCAT	nvarchar(50),
	MAINTENANCE nvarchar(50)
);
