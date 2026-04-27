/*
Stored Procedure : Load Bronze Layer (Source > Bronze)

Script Purpose :
     This stored procedure loads data into the 'bronze' schema from external CSV files.
    It perfroms the following action:
    -Truncate the bronze tables before loading data.
    -Uses the 'Bulk Insert' command to load data from csv files to bronze tables.

Usage Example:
    Exec bronze.load_bronze;
*/

Use DataWarehouse;
Go

Create or Alter Procedure bronze.load_bronze as 
Begin
	Declare @start_time Datetime, @end_time Datetime, @batch_start_time Datetime, @batch_end_time Datetime;
	Begin try
		Set @batch_start_time = Getdate();
		Print 'Loading Bronze layer';
		Print '===========================';
		Print 'Loading crm Tables';
		Print '---------------------------';
		
		set @start_time = getdate();
		Print '>>Truncating and inserting the data in table bronze.crm_cust_info';
		Truncate Table bronze.crm_cust_info;
		Bulk Insert bronze.crm_cust_info
		From 'C:\Users\Amit\Desktop\Data Analytics\SQL\cousrse data\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		With (
			FirstRow = 2,
			FieldTerminator = ',',
			Tablock
		);
		set @end_time = getdate();
		Print '>>Load Duration: ' + Cast (Datediff(second, @start_time, @end_time) as Nvarchar) + ' seconds'
		Print '-----------------'

		set @start_time = getdate();
		Print '>>Truncating and inserting the data in table bronze.crm_prd_info';
		Truncate Table bronze.crm_prd_info;
		Bulk Insert bronze.crm_prd_info
		From 'C:\Users\Amit\Desktop\Data Analytics\SQL\cousrse data\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		With (
			FirstRow = 2,
			FieldTerminator = ',',
			Tablock
		);
		set @end_time = GETDATE();
		Print '>>Load Duration: ' + Cast (Datediff(second, @start_time, @end_time) as Nvarchar) + ' seconds'
		Print '-----------------'

		Set @start_time = GETDATE();
		Print '>>Truncating and inserting the data in table bronze.crm_sales_details';
		Truncate Table bronze.crm_sales_details;
		Bulk Insert bronze.crm_sales_details
		From 'C:\Users\Amit\Desktop\Data Analytics\SQL\cousrse data\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		With (
			FirstRow = 2,
			FieldTerminator = ',',
			Tablock
		);
		Set @end_time = GETDATE();
		Print '>>Load Duration: ' + Cast (Datediff(second, @start_time, @end_time) as Nvarchar) + ' seconds'
		Print '-----------------'

		Print 'Loading erp Tables';
		Print '---------------------------';

		Set @start_time = getdate();
		Print '>>Truncating and inserting the data in table bronze.erp_cust_az12';
		Truncate Table bronze.erp_cust_az12;
		Bulk Insert bronze.erp_cust_az12
		From 'C:\Users\Amit\Desktop\Data Analytics\SQL\cousrse data\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		With (
			FirstRow = 2,
			FieldTerminator = ',',
			Tablock
		);
		Set @end_time = GETDATE();
		Print '>>Load Duration: ' + Cast (Datediff(second, @start_time, @end_time) as Nvarchar) + ' seconds'
		Print '-----------------'

		Set @start_time = GETDATE();
		Print '>>Truncating and inserting the data in table bronze.erp_loc_a101';
		Truncate Table bronze.erp_loc_a101;
		Bulk Insert bronze.erp_loc_a101
		From 'C:\Users\Amit\Desktop\Data Analytics\SQL\cousrse data\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		With (
			FirstRow = 2,
			FieldTerminator = ',',
			Tablock
		);
		Set @end_time = getdate();
		Print '>>Load Duration: ' + Cast (Datediff(second, @start_time, @end_time) as Nvarchar) + ' seconds'
		Print '-----------------'

		Set @start_time = GETDATE();
		Print '>>Truncating and inserting the data in table bronze.erp_px_cat_g1v2';
		Truncate Table bronze.erp_px_cat_g1v2;
		Bulk Insert bronze.erp_px_cat_g1v2
		From 'C:\Users\Amit\Desktop\Data Analytics\SQL\cousrse data\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		With (
			FirstRow = 2,
			FieldTerminator = ',',
			Tablock
		);
		Set @end_time = getdate();
		Print '>>Load Duration: ' + Cast (Datediff(second, @start_time, @end_time) as Nvarchar) + ' seconds'
		Print '-----------------'

		Set @batch_end_time = getdate();
		Print 'Loading Bronze Layer is completed';
		Print '>>Total Load Duration: ' + Cast (Datediff(second, @batch_start_time, @batch_end_time) as Nvarchar) + ' seconds'
		Print '-----------------'

	End Try
	Begin Catch
	Print '================================================';
	Print 'Erro Occured during loading bronze layer';
	Print 'Error Message' + Error_message();
	Print 'Error Message' + Cast (Error_number() as Nvarchar);
	Print 'Error Message' + Cast (Error_state() as Nvarchar);
	Print '================================================';
	End Catch
End 

Exec bronze.load_bronze
