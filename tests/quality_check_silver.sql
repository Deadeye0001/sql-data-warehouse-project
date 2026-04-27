*/
Quality Check

Scrip purpose:
  This script perform various qulaity check for data consistency, accuracy
  and standardization acroos the 'silver' shcema. It include checks for:

  - Null or duplicate primary key
  - Unwanted spaces in col.
  - Data standardization and consistancy
  - Invalid data ranges and order
  - Data consistancy between related field

Usage Notes:
    - Run it in separate query to check the data while transforming 
*/

-- extract col name from a table
SELECT COLUMN_NAME + ',' AS column_list
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'crm_sales_details'
AND TABLE_SCHEMA = 'bronze'

-- Check for null or duplicate in primary key

Select prd_id, Count(*)
From silver.crm_prd_info
Group by prd_id
Having count(*) >1 Or prd_id is null

-- Check for unwanted Spaces

Select prd_nm
From silver.crm_prd_info
Where prd_nm != Trim (prd_nm)

-- check for nulls or negative numbers

Select prd_cost 
From silver.crm_prd_info
Where prd_cost < 0 Or prd_cost is Null

-- Data standardization & consistency
Select Distinct prd_line
From silver.crm_prd_info

-- check for Invalid date orders

Select 
NullIf(sls_order_dt, 0) sls_order_dt
From bronze.crm_sales_details
Where sls_order_dt <= 0
Or len(sls_order_dt) != 8 
Or sls_order_dt > 20500101
Or sls_order_dt < 19000101

-- Check for invalid date orders

Select 
*
From bronze.crm_sales_details
Where sls_order_dt > sls_ship_dt Or sls_order_dt > sls_due_dt

-- Check Data consistencey

Select 
sls_sales oldsales,
sls_quantity,
sls_price oldprice,
Case When sls_sales is null or sls_sales <= 0 or sls_sales != abs(sls_quantity) * abs(sls_price)
	Then  abs(sls_quantity) * abs(sls_price)
	Else sls_sales
End sls_sales,
Case When sls_price is null or sls_price <= 0 
	Then  sls_sales/ Coalesce (sls_quantity, 0)
	Else sls_price
End sls_price
From bronze.crm_sales_details
Where sls_sales != sls_price * sls_quantity
or sls_sales is null or sls_quantity is null or sls_price is null
Or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
Order by sls_sales, sls_quantity, sls_price
