create or alter procedure bronze.load_bronze as
begin

	declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; --to get runtime duration
	begin try
		set @batch_start_time = GETDATE();
		/* use DataWarehouse; */

		--Inserting Data Into Tables
		
		set @start_time = GETDATE();

		truncate table bronze.crm_cust_info;

		bulk insert bronze.crm_cust_info from 'D:\Learn\Advance SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		firstrow = 2,             -- Makes data start loading from 2nd row since first row is header
		fieldterminator = ','     --delimiter between columns seperator
		); 
		
		set @end_time= GETDATE();
		print '>> Load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'

		/* select * from bronze.crm_cust_info; */

		-------------------------------------------------------------------------------------------------------------------------------------------------

		set @start_time = GETDATE();

		truncate table bronze.crm_prd_info;

		bulk insert bronze.crm_prd_info from 'D:\Learn\Advance SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
		firstrow = 2,             -- Makes data start loading from 2nd row since first row is header
		fieldterminator = ','     --delimiter between columns seperator
		); 

		set @end_time= GETDATE();
		print '>> Load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'

		/* select * from bronze.crm_prd_info; */


		-------------------------------------------------------------------------------------------------------------------------------------------------

		set @start_time = GETDATE();

		truncate table bronze.crm_sales_details;

		bulk insert bronze.crm_sales_details from 'D:\Learn\Advance SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		firstrow = 2,             -- Makes data start loading from 2nd row since first row is header
		fieldterminator = ','     --delimiter between columns seperator
		); 

		set @end_time= GETDATE();
		print '>> Load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'

		/* select * from bronze.crm_sales_details; */

		-------------------------------------------------------------------------------------------------------------------------------------------------

		set @start_time = GETDATE();

		truncate table bronze.erp_CUST_AZ12;

		bulk insert bronze.erp_CUST_AZ12 from 'D:\Learn\Advance SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
		firstrow = 2,             -- Makes data start loading from 2nd row since first row is header
		fieldterminator = ','     --delimiter between columns seperator
		); 

		set @end_time= GETDATE();
		print '>> Load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'

		/* select * from bronze.erp_CUST_AZ12; */

		-------------------------------------------------------------------------------------------------------------------------------------------------

		set @start_time = GETDATE();

		truncate table bronze.erp_LOC_A101;

		bulk insert bronze.erp_LOC_A101 from 'D:\Learn\Advance SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
		firstrow = 2,             -- Makes data start loading from 2nd row since first row is header
		fieldterminator = ','     --delimiter between columns seperator
		); 

		set @end_time= GETDATE();
		print '>> Load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'

		/* select * from bronze.erp_LOC_A101; */


		-------------------------------------------------------------------------------------------------------------------------------------------------

		set @start_time = GETDATE();

		truncate table bronze.erp_PX_CAT_G1V2;

		bulk insert bronze.erp_PX_CAT_G1V2 from 'D:\Learn\Advance SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
		firstrow = 2,             -- Makes data start loading from 2nd row since first row is header
		fieldterminator = ','     --delimiter between columns seperator
		); 

		set @end_time= GETDATE();
		print '>> Load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'

		/* select * from bronze.erp_PX_CAT_G1V2; */

		set @batch_end_time = GETDATE();
		print '>> Total duration:' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) as NVARCHAR) + ' seconds'

	end try
	begin catch

	print 'ERROR DURING LOADING BRONZE LAYER' + ERROR_MESSAGE();

	end catch
end

select * from [bronze].[crm_cust_info]

