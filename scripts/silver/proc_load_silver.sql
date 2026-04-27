/*
Stored Procedure : Load Silver Layer (Source > Bronze)

Script Purpose :
     This stored procedure loads data into the 'silver' schema from Bronze layer.
    It perfroms the following action:
    -Truncate the silver tables before loading data.
    -Insert the cleane and transformed data from bronze to silver.

Usage Example:
    Exec bronze.load_bronze;
*/

Use DataWarehouse;
Go

Create or Alter Procedure silver.load_silver as 
Begin
	Declare @start_time Datetime, @end_time Datetime, @batch_start_time Datetime, @batch_end_time Datetime;
	Begin try
		Set @batch_start_time = Getdate();
		Print 'Loading silver layer';
		Print '===========================';
		Print 'Loading crm Tables';
		Print '---------------------------';
		
		set @start_time = getdate();
		Print '>>Truncating and inserting the data silver.crm_cust_info';
		Truncate Table silver.crm_cust_info;
		Insert Into silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)
		Select
			cst_id,
			cst_key,
			Trim(cst_firstname) cst_firstname,
			Trim(cst_lastname) cst_lastname,
			Case When Upper(Trim(cst_marital_status)) = 'S' Then 'Single'
				 When Upper(Trim(cst_marital_status)) = 'M' Then 'Married'
				 Else 'n/a'
			End cst_marital_status,
			Case When Upper(Trim(cst_gndr)) = 'F' Then 'Female'
				 When Upper(Trim(cst_gndr)) = 'M' Then 'Male'
				 Else 'n/a'
			End cst_gndr,
			cst_create_date
		From (
			Select *,
				ROW_NUMBER() Over (partition by cst_id Order by cst_create_date DESC) as flag
			From bronze.crm_cust_info
			Where cst_id is not null
		)t
		Where flag = 1
		set @end_time = getdate();
		Print '>>Load Duration: ' + Cast (Datediff(second, @start_time, @end_time) as Nvarchar) + ' seconds'
		Print '-----------------'


		set @start_time = getdate();
		Print '>>Truncating and inserting the data in table silver.crm_prd_info';
		Truncate Table silver.crm_prd_info;
		Insert Into silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,	
		prd_end_dt
		)
		SELECT prd_id,
			  Replace(SUBSTRING(prd_key, 1, 5), '-', '_') cat_id, -- Extracted Category ID
			  SUBSTRING(prd_key, 7, Len(prd_key)) prd_key, -- Extracted product key
			  prd_nm,
			  coalesce(prd_cost, 0) prd_cost,
			  Case When Upper(Trim(prd_line)) = 'M' Then 'Mountain'
				   When Upper(Trim(prd_line)) = 'R' Then 'Road'
				   When Upper(Trim(prd_line)) = 'S' Then 'Other Sales'
				   When Upper(Trim(prd_line)) = 'T' Then 'Touring'
				   Else 'n/a'
			  End prd_line, -- Map product line codes to descriptive values
			  Cast(prd_start_dt as Date) prd_start_dt,
			  Cast(
				  Lead(prd_start_dt) Over(Partition by prd_key Order by prd_start_dt)-1 
				  as Date
			  ) prd_end_dt -- calculated end date as one day before the next start date
		FROM bronze.crm_prd_info
		set @end_time = GETDATE();
		Print '>>Load Duration: ' + Cast (Datediff(second, @start_time, @end_time) as Nvarchar) + ' seconds'
		Print '-----------------'


		Set @start_time = GETDATE();
		Print '>>Truncating and inserting the data in table silver.crm_sales_details';
		Truncate Table silver.crm_sales_details;
		Insert into silver.crm_sales_details (
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
		Select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			Case When sls_order_dt = 0 Or Len(sls_order_dt) != 8 Then null
				Else Cast(Cast(sls_order_dt as varchar) as Date)	
			End sls_order_dt,
			Case When sls_ship_dt = 0 Or Len(sls_ship_dt) != 8 Then null
				Else Cast(Cast(sls_ship_dt as varchar) as Date)	
			End sls_ship_dt,
			Case When sls_due_dt = 0 Or Len(sls_due_dt) != 8 Then null
				Else Cast(Cast(sls_due_dt as varchar) as Date)	
			End sls_due_dt,
			Case When sls_sales is null or sls_sales <= 0 or sls_sales != abs(sls_quantity) * abs(sls_price)
				Then  abs(sls_quantity) * abs(sls_price)
				Else sls_sales
			End sls_sales, -- Recalculating sales if original value is incorrect or missing
			abs(sls_quantity) sls_quantity,
			Case When sls_price is null or sls_price <= 0 
				Then  sls_sales/ Coalesce (sls_quantity, 0)
				Else sls_price
			End sls_price -- Derive price if original value is incorrect
		From bronze.crm_sales_details
		Set @end_time = GETDATE();
		Print '>>Load Duration: ' + Cast (Datediff(second, @start_time, @end_time) as Nvarchar) + ' seconds'
		Print '-----------------'

		Print 'Loading erp Tables';
		Print '---------------------------';


		Set @start_time = getdate();
		Print '>>Truncating and inserting the data in table silver.erp_cust_az12';
		Truncate Table bronze.erp_cust_az12;
		Insert into silver.erp_cust_az12 (
		cid,
		bdate,
		gen
		)
		Select
			Case When cid like 'NAS%' Then Substring(cid, 4, len(cid))
				Else cid
			End cid,  -- Removed Nas prefix if present
			Case When bdate > Getdate() Then null
				Else bdate
			End bdate,  -- Set future birthdate to null
			Case When Upper(Trim(gen)) in ( 'F', 'FEMALE') Then 'Female'
			When Upper(Trim(gen)) in ( 'M', 'MALE') Then 'Male'
			Else 'n/a'
		End gen -- Normalise the gender values and unknown
		From bronze.erp_cust_az12
		Set @end_time = GETDATE();
		Print '>>Load Duration: ' + Cast (Datediff(second, @start_time, @end_time) as Nvarchar) + ' seconds'
		Print '-----------------'


		Set @start_time = GETDATE();
		Print '>>Truncating and inserting the data in table silver.erp_loc_a101';
		Truncate Table silver.erp_loc_a101;
		Insert Into silver.erp_loc_a101 (
		cid,
		cntry
		)  
		Select 
		Replace(cid, '-', '') cid,
		Case When Trim(cntry) = 'DE' Then 'Germany'
			 When Trim(cntry) in ('US', 'USA') Then 'United States'
			 When Trim(cntry) = '' Or cntry is null Then 'n/a'
			 Else Trim(cntry) 
		End cntry -- Normalize and Handled missing or blank country codes
		From bronze.erp_loc_a101
		Set @end_time = getdate();
		Print '>>Load Duration: ' + Cast (Datediff(second, @start_time, @end_time) as Nvarchar) + ' seconds'
		Print '-----------------'


		Set @start_time = GETDATE();
		Print '>>Truncating and inserting the data in table silver.erp_px_cat_g1v2';
		Truncate Table silver.erp_px_cat_g1v2;
		Insert Into silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
		)
		Select
		id,
		cat,
		subcat,
		maintenance
		From bronze.erp_px_cat_g1v2
		Set @end_time = getdate();
		Print '>>Load Duration: ' + Cast (Datediff(second, @start_time, @end_time) as Nvarchar) + ' seconds'
		Print '-----------------'

		Set @batch_end_time = getdate();
		Print 'Loading Silver Layer is completed';
		Print '>>Total Load Duration: ' + Cast (Datediff(second, @batch_start_time, @batch_end_time) as Nvarchar) + ' seconds'
		Print '-----------------'

	End Try
	Begin Catch
	Print '================================================';
	Print 'Erro Occured during loading Silver layer';
	Print 'Error Message' + Error_message();
	Print 'Error Message' + Cast (Error_number() as Nvarchar);
	Print 'Error Message' + Cast (Error_state() as Nvarchar);
	Print '================================================';
	End Catch
End 

Exec Silver.load_silver
