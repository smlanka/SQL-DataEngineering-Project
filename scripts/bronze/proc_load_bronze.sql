/*
Stored Procedure: Load Bronze layer (src -> bronze)
This script loads the data into the created tables from the csv files
it performs:
  -Truncates the bronze tables before loading
  -uses the 'Bulk Insert' command to load data into the tables at a time

*/

  
--Storing the procedure for repeatability

create or alter procedure bronze.load_bronze as
begin
	declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	begin try
	set @batch_start_time=GETDATE();
	print '==========================================================================================================================='
	print 'Loading the bronze layer'
	--For different tables use their respective names below and also change the path of the source
	--Bulk load to insert all the data from the csv to the table
	print '-----------------------------------------------Loading crm tables-------------------------------------------------'

	set @start_time=GETDATE();
	print 'TRUNCATE TABLE bronze.crm_cust_info'                            
	print 'BULK INSERT bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info                             --making sure that the table is empty so that the rows are not duplicated
		BULK INSERT bronze.crm_cust_info
		from 'C:\Users\lshiv\OneDrive\Desktop\Data Engineering Proj\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock                     --SQL locks the table while the load
		);
		set @end_time=GETDATE();
		print'---- Load Duration: ' + CAST(datediff(second, @start_time,@end_time) as nvarchar)+ 'seconds------'
		print'----------------------------------------------------------------------------------'

		set @start_time=GETDATE();
		print 'TRUNCATE TABLE bronze.crm_prod_info'                             --making sure that the table is empty so that the rows are not duplicated
		print 'BULK INSERT bronze.crm_prod_info'
		TRUNCATE TABLE bronze.crm_prod_info                             --making sure that the table is empty so that the rows are not duplicated
		BULK INSERT bronze.crm_prod_info
		from 'C:\Users\lshiv\OneDrive\Desktop\Data Engineering Proj\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock                     --SQL locks the table while the load
		);
		set @end_time=GETDATE();
		print'---- Load Duration: ' + CAST(datediff(second, @start_time,@end_time) as nvarchar)+ 'seconds------'
		print'----------------------------------------------------------------------------------'

		set @start_time=GETDATE();
		print'TRUNCATE TABLE bronze.crm_sales_details'                             --making sure that the table is empty so that the rows are not duplicated
		print'BULK INSERT bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details                             --making sure that the table is empty so that the rows are not duplicated
		BULK INSERT bronze.crm_sales_details
		from 'C:\Users\lshiv\OneDrive\Desktop\Data Engineering Proj\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock                     --SQL locks the table while the load
		);
		set @end_time=GETDATE();
		print'---- Load Duration: ' + CAST(datediff(second, @start_time,@end_time) as nvarchar)+ 'seconds------'
		print'----------------------------------------------------------------------------------'

		print '-------------------------------------------------------Loading ERP tables----------------------------------------------------------------------'
		set @start_time=GETDATE();
		print'TRUNCATE TABLE bronze.erp_cust_az12'                             --making sure that the table is empty so that the rows are not duplicated
		print'BULK INSERT bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12                             --making sure that the table is empty so that the rows are not duplicated
		BULK INSERT bronze.erp_cust_az12
		from 'C:\Users\lshiv\OneDrive\Desktop\Data Engineering Proj\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock                     --SQL locks the table while the load
		);
		set @end_time=GETDATE();
		print'---- Load Duration: ' + CAST(datediff(second, @start_time,@end_time) as nvarchar)+ 'seconds------'
		print'----------------------------------------------------------------------------------'

		set @start_time=GETDATE();
		print 'TRUNCATE TABLE bronze.erp_loc_a101'                             --making sure that the table is empty so that the rows are not duplicated
		print'BULK INSERT bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101                             --making sure that the table is empty so that the rows are not duplicated
		BULK INSERT bronze.erp_loc_a101
		from 'C:\Users\lshiv\OneDrive\Desktop\Data Engineering Proj\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock                     --SQL locks the table while the load
		);
		set @end_time=GETDATE();
		print'---- Load Duration: ' + CAST(datediff(second, @start_time,@end_time) as nvarchar)+ 'seconds------'
		print'----------------------------------------------------------------------------------'

		set @start_time=GETDATE();
		print'TRUNCATE TABLE bronze.erp_px_cat_g1v2'                             --making sure that the table is empty so that the rows are not duplicated
		print'BULK INSERT bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2                             --making sure that the table is empty so that the rows are not duplicated
		BULK INSERT bronze.erp_px_cat_g1v2
		from 'C:\Users\lshiv\OneDrive\Desktop\Data Engineering Proj\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock                     --SQL locks the table while the load
		);
		set @end_time=GETDATE();
		print'---- Load Duration: ' + CAST(datediff(second, @start_time,@end_time) as nvarchar)+ 'seconds------'
		print'----------------------------------------------------------------------------------'
		set @batch_end_time=GETDATE();
		print'=================================Bronze Layer loading done===================================='
		print'---- Total Load Duration: ' + CAST(datediff(second, @batch_start_time,@batch_end_time) as nvarchar)+ 'seconds------'

		end try
	begin catch
	print '=============================================================='
	print 'Error occured during the bronze layer'
	print 'Error Message'+ERROR_MESSAGE();
	print 'Error Message+ CAST(Error_NUMBER() AS NVARCHAR)'
	print 'Error Message+ CAST(Error_STATE() AS NVARCHAR)'
	print '=============================================================='
	end catch

end

----Use the following code to execute the stored procedure above
exec bronze.load_bronze


