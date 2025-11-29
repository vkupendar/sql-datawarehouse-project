/*
-------------------------------------
Stored Procedure: Load Bronze Layer (Source -> Bronze)
----------------------------------------------
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It prevents the foloowing actions:
  -Truncates the bronze table before laoding the data.
  -uses the 'BULK INSERT' command to load data from csv files to bronze tables.

Parameters:
None
This stored procedure does not accept any parameters or return any avlues.
Usage example:
EXEC bronze.load_bronze
*/
  


EXEC bronze.load_bronze;

create or alter procedure bronze.load_bronze as
begin
	Declare @start_time DATETIME, @end_time DATETIME, @batch_start_time datetime,@batch_end_time datetime;
	BEGIN TRY

		set @batch_start_time=GETDATE();
	
		print '====================';
	
		print 'Loading Bronze Layer';

		print '====================';

		print '-----------------------------------';
		print 'Loading CRM Tables'
		print '-----------------------------------';

		set @start_time=GETDATE();

		print '>> Truncating Table:bronze.crm_cust_info';
		truncate table  bronze.crm_cust_info;

		print '>> Inserting Data Into:bronze.crm_cust_info';


		BULk INSERT bronze.crm_cust_info

		from 'C:\Users\kupen\Desktop\datawarehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'

		with (
				FirstROW=2,
				FIELDTERMINATOR =',',
				TABLOCK--IT WILL LOCK THE WHOLE TABLE
		);
		set @end_time=GETDATE();
		print '>> load duaration:'+ cast(datediff(second,@start_time,@end_time) as nvarchar) + 'second';

		set @start_time=GETDATE();

		print '>> Truncating Table:bronze.crm_prd_info';

		truncate table bronze.crm_prd_info;

		print '>> Inserting Data Into:bronze.crm_prd_info';

		BULK INSERT bronze.crm_prd_info

		from 'C:\Users\kupen\Desktop\datawarehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);
			set @end_time=GETDATE();
		print '>> load duaration:'+ cast(datediff(second,@start_time,@end_time) as nvarchar) + 'second';

		set @start_time=GETDATE();
		print '>> Truncating Table:bronze.crm_sales_details';
		truncate table  bronze.crm_sales_details;
		print '>> Inserting Data Into:bronze.crm_sales_details';

		BULK INSERT bronze.crm_sales_details

		from 'C:\Users\kupen\Desktop\datawarehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);
			set @end_time=GETDATE();
		print '>> load duaration:'+ cast(datediff(second,@start_time,@end_time) as nvarchar) + 'second';

		print '-----------------------------------'

		print 'Loading ERP Tables';

		print '-----------------------------------'

		set @start_time=GETDATE();
		print '>> Truncating Table:bronze.erp_cust_az12';
		truncate table  bronze.erp_cust_az12;
		print '>> Inserting Data Into:bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12

			from 'C:\Users\kupen\Desktop\datawarehouse\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
			with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);
			set @end_time=GETDATE();
		print '>> load duaration:'+ cast(datediff(second,@start_time,@end_time) as nvarchar) + 'second';

		set @start_time=GETDATE();
		print '>> Truncating Table:bronze.erp_loc_a101';
		truncate table  bronze.erp_loc_a101;
		print '>> Inserting Data Into:bronze.erp_loc_a101';

		BULK INSERT bronze.erp_loc_a101

		from 'C:\Users\kupen\Desktop\datawarehouse\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);
			set @end_time=GETDATE();
		print '>> load duaration:'+ cast(datediff(second,@start_time,@end_time) as nvarchar) + 'second';

		set @start_time=GETDATE();
		print '>> Truncating Table:bronze.erp_px_cat_g1v2';
		truncate table  bronze.erp_px_cat_g1v2;
		print '>> Inserting Data Into:bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
	
		from 'C:\Users\kupen\Desktop\datawarehouse\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
		);
			set @end_time=GETDATE();
		print '>> load duaration:'+ cast(datediff(second,@start_time,@end_time) as nvarchar) + 'second';

		set @batch_end_time =GETDATE();
		print '===========================================';
		print 'Loading bronze layer is Completed';
		print 'Total duration :' + Cast(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' SECONDS';

	END TRY
	BEGIN CATCH
	PRINT '=================================';
	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
	PRINT 'ERROR MESSAGE'+ERROR_MESSAGE();
	PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR MESSAGE' +CAST (ERROR_STATE() AS NVARCHAR); 

	END CATCH
END
