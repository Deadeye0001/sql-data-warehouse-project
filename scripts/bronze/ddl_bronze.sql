/* 
DDL Script : Create Bronze Layer

Script Purpsose : 
    This script creates tables in the 'bronze' schema, dropping existing tables
    if they already exist.
    Run this script to re-define the ddl structure of 'bronze' tables.
*/

--Tables for crm source data

If Object_ID ('bronze.crm_cust_info', 'U') is not null
	Drop Table bronze.crm_cust_info;
Go
  
Create Table bronze.crm_cust_info (
cst_id Int,
cst_key Nvarchar(50),
cst_firstname Nvarchar(50),
cst_lastname Nvarchar(50),
cst_marital_status Nvarchar(50),
cst_gndr Nvarchar(50),
cst_create_date Date 
);
Go

If Object_ID ('bronze.crm_prd_info', 'U') is not null
	Drop Table bronze.crm_prd_info;
Go
  
Create Table bronze.crm_prd_info (
prd_id Int,
prd_key Nvarchar(50),
prd_nm Nvarchar(50),
prd_cost Int,
prd_line Nvarchar(50),
prd_start_dt Datetime,	
prd_end_dt Datetime
);
Go

If Object_ID ('bronze.crm_sales_details', 'U') is not null
	Drop Table bronze.crm_sales_details;
Go
  
Create Table bronze.crm_sales_details (
sls_ord_num Nvarchar(50),	
sls_prd_key	Nvarchar(50),
sls_cust_id	Int,
sls_order_dt Int,
sls_ship_dt	Int,
sls_due_dt	Int,
sls_sales	Int,
sls_quantity Int,
sls_price Int
);
Go

-- Tables for erp source data
If
  Object_ID ('bronze.erp_cust_az12', 'U') is not null
	Drop Table bronze.erp_cust_az12;
Go

Create Table bronze.erp_cust_az12 (
cid Nvarchar(50),
bdate Date,
gen Nvarchar(50)
);
Go
  
If Object_ID ('bronze.erp_loc_a101', 'U') is not null
	Drop Table bronze.erp_loc_a101;
Go

Create Table bronze.erp_loc_a101 (
cid Nvarchar(50),
cntry Nvarchar(50)
);
Go
  
If Object_ID ('bronze.erp_px_cat_g1v2', 'U') is not null
	Drop Table bronze.erp_px_cat_g1v2;
Go
  
Create Table bronze.erp_px_cat_g1v2 (
id Nvarchar(50),
cat Nvarchar(50),
subcat Nvarchar(50),
maintenance Nvarchar(50)
);
