CREATE OR ALTER PROCEDURE load_bronze AS
BEGIN
DECLARE @table_start_time, @table_end_time;

  BEGINT TRY
      PRINT '================================';
  		PRINT 'Loading Brozne Layer';
  		PRINT '================================';
  	
  		PRINT '---------------------------------';
  		PRINT 'Loading CRM tables';
  		PRINT '---------------------------------';
  		
  		SET @table_start_time = GETDATE();
  		PRINT '>>Truncating Table bronze.crm_cust_info';
  		TRUNCATE TABLE bronze.crm_cust_info;
  
  		PRINT 'Inserting Data into: bronze.crm_cust_info';
  		BULK INSERT bronze.crm_cust_info
  		FROM 'D:\Projects\SQL\SQL With Baraa\sql-ultimate-course\Data Warehouse Projects\datasets\source_crm\cust_info.csv'
  		WITH(
  		FIRSTROW = 2,
  		FIELDTERMINATOR = ',',
  		TABLOCK
  		);
  		SET @table_end_time = GETDATE();
  		PRINT 'Loading Time is: ' + CAST(DATEDIFF(second, @table_start_time, @table_end_time)AS NVARCHAR) + 'seconds';
  
  		-- -- Bulk Insert table 2
  
  		SET @table_start_time = GETDATE();
  		PRINT '>>Truncating Table bronze.crm_prd_info';
  		TRUNCATE TABLE bronze.crm_prd_info;
  
  		PRINT 'Inserting Data into: bronze.crm_prd_info';
  		BULK INSERT bronze.crm_prd_info
  		FROM 'D:\Projects\SQL\SQL With Baraa\sql-ultimate-course\Data Warehouse Projects\datasets\source_crm\prd_info.csv'
  		WITH(
  		FIRSTROW = 2,
  		FIELDTERMINATOR = ',',
  		TABLOCK
  		);
  		SET @table_end_time = GETDATE();
  		PRINT 'Loading Time is: ' + CAST(DATEDIFF(second, @table_start_time, @table_end_time)AS NVARCHAR) + 'seconds';
  
  		-- Bulk Insert table 3
  
  		SET @table_start_time = GETDATE();
  		PRINT '>>Truncating Table bronze.crm_sales_details';
  
  		TRUNCATE TABLE bronze.crm_sales_details;
  
  		PRINT 'Inserting Data into: bronze.crm_sales_details';
  		BULK INSERT bronze.crm_sales_details
  		FROM 'D:\Projects\SQL\SQL With Baraa\sql-ultimate-course\Data Warehouse Projects\datasets\source_crm\sales_details.csv'
  		WITH(
  		FIRSTROW = 2,
  		FIELDTERMINATOR = ',',
  		TABLOCK
  		);
  		SET @table_end_time = GETDATE();
  		PRINT 'Loading Time is: ' + CAST(DATEDIFF(second, @table_start_time, @table_end_time)AS NVARCHAR) + 'seconds';
  
  		PRINT '---------------------------------';
  		PRINT 'Loading ERP tables';
  		PRINT '---------------------------------';
  		-- Bulk Insert table 4
  		
  		PRINT 'Loading ERP tables';
  		SET @table_start_time = GETDATE();
  		PRINT '>>Truncating Table bronze.erp_cust_az12';
  		TRUNCATE TABLE bronze.erp_cust_az12;
  
  		PRINT 'Inserting Data into: bronze.erp_cust_az12';
  		BULK INSERT bronze.erp_cust_az12
  		FROM 'D:\Projects\SQL\SQL With Baraa\sql-ultimate-course\Data Warehouse Projects\datasets\source_erp\CUST_AZ12.csv'
  		WITH(
  		FIRSTROW = 2,
  		FIELDTERMINATOR = ',',
  		TABLOCK
  		);
  		SET @table_end_time = GETDATE();
  		PRINT 'Loading Time is: ' + CAST(DATEDIFF(second, @table_start_time, @table_end_time)AS NVARCHAR) + 'seconds';
  
  		-- Bulk Insert table 5
  
  		SET @table_start_time = GETDATE();
  		PRINT '>>Truncating Table bronze.erp_loc_a101';
  		TRUNCATE TABLE bronze.erp_loc_a101;
  
  		PRINT 'Inserting Data into: bronze.erp_loc_a101';
  		BULK INSERT bronze.erp_loc_a101
  		FROM 'D:\Projects\SQL\SQL With Baraa\sql-ultimate-course\Data Warehouse Projects\datasets\source_erp\LOC_A101.csv'
  		WITH(
  		FIRSTROW = 2,
  		FIELDTERMINATOR = ',',
  		TABLOCK
  		);
  		SET @table_end_time = GETDATE();
  		PRINT 'Loading Time is: ' + CAST(DATEDIFF(second, @table_start_time, @table_end_time)AS NVARCHAR) + 'seconds';
  
  		--Bulk Insert table 6
  
  		SET @table_start_time = GETDATE();
  		PRINT '>>Truncating Table bronze.erp_px_cat_g1v2';
  		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
  
  		PRINT 'Inserting Data into: bronze.erp_px_cat_g1v2';
  		BULK INSERT bronze.erp_px_cat_g1v2
  		FROM 'D:\Projects\SQL\SQL With Baraa\sql-ultimate-course\Data Warehouse Projects\datasets\source_erp\PX_CAT_G1V2.csv'
  		WITH(
  		FIRSTROW = 2,
  		FIELDTERMINATOR = ',',
  		TABLOCK
  		);
  		SET @table_end_time = GETDATE();
  		PRINT 'Loading Time is: ' + CAST(DATEDIFF(second, @table_start_time, @table_end_time)AS NVARCHAR) + 'seconds';
  
  		PRINT 'Loaded All CRM tables';
  
  SET @batch_finish_time = GETDATE()
  
  PRINT'======================================='
  PRINT'complete time for Bronze Layer is: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_finish_time) AS NVARCHAR) + 'seconds';
  PRINT'======================================='
  
  END TRY
  BEGIN CATCH
    PRINT '============================================='
  	PRINT 'Error Occurred for loading Bronze Layer'
  	PRINT 'Error Number' + CAST(Error_number() AS NVARCHAR)
  	PRINT 'Error Line' + CAST(Error_line() AS NVARCHAR)
  	PRINT '============================================='
  END CATCH
    END 
