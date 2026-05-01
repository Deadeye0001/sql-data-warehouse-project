/* 
DDL Script: Create Gold Views

Script Purpose:
  This script creates views for the Gold layer in the data warehouse.
  The gold layer represents the final dimension and fact tables (Star Schema)


  Each view performs transformation and combination data from the silver layer 
  to produce a clean, enriched, and business ready datasets.

Usage:
  - These views can be queried directly for analystics and reporting.
*/

-- Create dimention : gold.dim_customers

If OBJECT_ID ('gold.dim_customers') is not null
	Drop view gold.dim_customers
Go

Create view gold.dim_customers As 
Select
	ROW_NUMBER() Over (Order by cst_id) As customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country,
	ci.cst_marital_status as marital_status,
	Case When ci.cst_gndr != 'n/a' Then ci.cst_gndr  -- Crm is the master for infor
		 Else 	Coalesce(ca.gen, 'n/a')
	End as gender,
	ca.bdate birth_date,
	ci.cst_create_date as create_date
From silver.crm_cust_info ci
Left Join silver.erp_cust_az12 ca
on		  ci.cst_key = ca.cid
Left Join silver.erp_loc_a101 la
on		  ci.cst_key = la.cid

  
-- Create dimention : gold.dim_products

If OBJECT_ID ('gold.dim_products') is not null
	Drop view gold.dim_products
Go

Create View gold.dim_products as 
Select 
ROW_NUMBER() Over(Order by pn.prd_start_dt, pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
From silver.crm_prd_info pn
Left Join	silver.erp_px_cat_g1v2 pc
On		pc.id = pn.cat_id
Where prd_end_dt is null  -- filter out all historical data


-- Create fact : gold.fact_sales
  
If OBJECT_ID ('gold.fact_sales') is not null
	Drop view gold.fact_sales
Go

Create View gold.fact_sales as 
Select 
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt shipping_date,
sd.sls_due_dt as  due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
From silver.crm_sales_details sd
Left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
Left join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id
