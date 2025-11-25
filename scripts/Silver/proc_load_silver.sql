/*============================================================================================
All 6 tables silver queries needs to be executed again. To avoid duplicated being inserted
============================================================================================
Here we have Stored Procedure for Complete Silver Layer process to CREATE Structure for tables to Inserting Clean Data into Silver Layer Tables.

*/


-- Table 1 silver.crm_cust_info

CREATE OR ALTER PROCEDURE silver.complete_silver AS
BEGIN
DECLARE @table_start_time DATETIME, @table_end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
BEGIN TRY
-- creating table 1 silver.crm_cust_info
IF OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE,
	dwh_creation_date DATETIME2 DEFAULT GETDATE()
);

-- Creating table 2 silver.crm_prd_info
IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm  NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_creation_date DATETIME2 DEFAULT GETDATE()
);
 

--Creating table 3 silver.crm_sales_details
IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price FLOAT,
	dwh_creation_date DATETIME2 DEFAULT GETDATE()
);
SELECT * FROM [silver].[crm_sales_details]

--Creating table 4 from silver.erp_CUST_AZ12
IF OBJECT_ID ('silver.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
	dwh_creation_date DATETIME2 DEFAULT GETDATE()
);

--Creating table 5 from silver.erp_LOC_A101
IF OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_creation_date DATETIME2 DEFAULT GETDATE()
);


--Creating table 6 from silver.erp_PX_CAT_G1V2
IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_creation_date DATETIME2 DEFAULT GETDATE()
);
/*=========================================================================================================================

Clean data insertion into Silver Layer Table Begins Here
*/
	SET @batch_start_time = GETDATE()
	PRINT '>>Truncation, Insertion started for Table silver.crm_cust_info'
	SET @table_start_time = GETDATE()
	TRUNCATE TABLE silver.crm_cust_info;
	INSERT INTO silver.crm_cust_info (
				cst_id, 
				cst_key, 
				cst_firstname, 
				cst_lastname, 
				cst_marital_status, 
				cst_gndr,
				cst_create_date
			)
			SELECT
				cst_id,
				cst_key,
				TRIM(cst_firstname) AS cst_firstname,
				TRIM(cst_lastname) AS cst_lastname,
				CASE 
					WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
					WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
					ELSE 'n/a'
				END AS cst_marital_status, -- Normalize marital status values to readable format
				CASE 
					WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
					WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
					ELSE 'n/a'
				END AS cst_gndr, -- Normalize gender values to readable format
				cst_create_date
			FROM (
				SELECT
					*,
					ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
				FROM bronze.crm_cust_info
				WHERE cst_id IS NOT NULL
			) t
			WHERE flag_last = 1; -- Select the most recent record per customer
	SET @table_end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @table_start_time, @table_end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';


	-- Table 2 silver.crm_prd_info

	PRINT '>>Truncation, Insertion started for Table silver.crm_prd_info'
	SET @table_start_time = GETDATE()
	TRUNCATE TABLE silver.crm_prd_info
	INSERT INTO [silver].[crm_prd_info]
	(
	[prd_id],
	[cat_id],
	[prd_key],
	[prd_nm],
	[prd_cost],
	[prd_line],
	[prd_start_dt],
	[prd_end_dt]
	)
	SELECT 
		prd_id,
		REPLACE(LEFT(prd_key, 5),'-','_') AS cat_id,	
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,		-- Making 2 keys out of prd_key column i.e. cat_id,prd_key
		prd_nm,
		COALESCE(prd_cost,0) AS prd_cost,					-- Replacing NULLs with 0
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS prd_end_dt
	FROM bronze.crm_prd_info
	SET @table_end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @table_start_time, @table_end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';


	-- Table 3 silver.crm_prd_info



	PRINT '>>Truncation, Insertion started for Table silver.crm_sales_details'
	SET @table_start_time = GETDATE()
	TRUNCATE TABLE silver.crm_sales_details
	INSERT INTO [silver].[crm_sales_details](
	[sls_ord_num],
	[sls_prd_key],
	[sls_cust_id],
	[sls_order_dt],
	[sls_ship_dt],
	[sls_due_dt],
	[sls_sales],
	[sls_quantity],
	[sls_price]
	)
	SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE 
			WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END AS sls_order_dt,
		CASE 
			WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END AS sls_ship_dt,
		CASE 
			WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END AS sls_due_dt,
		CASE 
			WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
			THEN sls_quantity * ABS(sls_price) 
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE 
			WHEN sls_price IS NULL or sls_price <= 0 
			THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price
		END AS sls_price
	FROM bronze.[crm_sales_details]
	WHERE sls_sales != sls_price * sls_quantity OR
	sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
	ORDER BY sls_sales,sls_quantity,sls_price
	SET @table_end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @table_start_time, @table_end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';

	-- Table 4 silver.erp_cust_az12


	PRINT '>>Truncation, Insertion started for Table silver.erp_cust_az12'
	SET @table_start_time = GETDATE()
	TRUNCATE TABLE silver.erp_cust_az12
	INSERT INTO [silver].[erp_cust_az12](
	[cid],
	[bdate],
	[gen]
	)
	SELECT 
		CASE 
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))		-- Case When for if there's any Cid that has NAS at start then grab it's substring
			ELSE cid
		END AS cid,
		CASE 
			WHEN bdate > GETDATE() THEN NULL							-- checking for any DOB greathan Today's date
			ELSE bdate
		END AS bdate,
		CASE
			WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female'		-- Fixed any other Genders being present in the Gen col
			WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
			ELSE 'n/a'
		END AS gen
	FROM [bronze].[erp_cust_az12]
	SET @table_end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @table_start_time, @table_end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';

	-- Table 5 silver.erp_loc_a101


	PRINT '>>Truncation, Insertion started for Table silver.erp_loc_a101'
	SET @table_start_time = GETDATE()
	TRUNCATE TABLE silver.erp_loc_a101
	INSERT INTO [silver].[erp_loc_a101](
	[cid],
	[cntry]	
	)
	SELECT REPLACE(cid,'-','') cid,
	CASE 
		WHEN TRIM(cntry) IN ('US','USA','United States') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		WHEN TRIM(cntry) IN ('DE') THEN 'Germany'
		ELSE TRIM(cntry)
	END AS cntry
	FROM [bronze].[erp_loc_a101]
	SET @table_end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @table_start_time, @table_end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';

	-- Table 6 silver.erp_px_cat_g1v2


	PRINT '>>Truncation, Insertion started for Table silver.erp_px_cat_g1v2'
	SET @table_start_time = GETDATE()
	TRUNCATE TABLE silver.erp_px_cat_g1v2
	INSERT INTO [silver].[erp_px_cat_g1v2](
	id,
	cat,
	subcat,
	maintenance
	)
	SELECT id,
			cat,
			subcat,
			maintenance 
	FROM bronze.erp_px_cat_g1v2
	SET @table_end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @table_start_time, @table_end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';
	SET @batch_end_time = GETDATE()
	PRINT '>> Silver Layer Load Duration ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time)AS NVARCHAR) + 'seconds';
END TRY
BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
END CATCH
END

EXEC silver.complete_silver
