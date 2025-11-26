/*

INSPECTION FOR ETL Process Begins here
Here we can use these commands to check for quality issues in the table in Bronze Layer after Bulk Insertion of data into Bronze layer tables. Just need to replace (Silver.) reference to (Bronze.)
So it can refer to Bronze layer schema and after executing below Insert command for clean data quality checks then simple change the referene back to (Silver.)
We can also use these commands to check for quality issues in the table in Silver Layer to check if those issues are Still there or not after doing ETL
=================================================*/

-- Checking duplicates

SELECT cst_id, COUNT(*) AS duplicate_counts 
FROM [silver].[crm_cust_info]
WHERE cst_id IS NOT NULL
GROUP BY cst_id
HAVING COUNT(*) > 1

-- Checking for Extra spaces in names
--Expectation after loading Clean table into Silver layer is to be blank

SELECT cst_firstname
FROM [silver].[crm_cust_info]
WHERE cst_firstname != TRIM(cst_firstname)

-- lastname spaces

SELECT cst_lastname
FROM [silver].[crm_cust_info]
WHERE cst_lastname != TRIM(cst_lastname)

-- Checking gender abbrevations

SELECT DISTINCT(cst_gndr) AS unique_gender
FROM [silver].[crm_cust_info]

SELECT DISTINCT(cst_marital_status) AS unique_marital_status
FROM [silver].[crm_cust_info]

-- Final Insert statement for Table 1
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


--====================================================================================
--Checking quality for [silver].[crm_prd_info] Table 2
--====================================================================================
SELECT * 
FROM bronze.[crm_prd_info]

SELECT prd_id, COUNT(*) AS duplicate_counts 
FROM bronze.crm_prd_info
WHERE prd_id IS NOT NULL
GROUP BY prd_id
HAVING COUNT(*) > 1


-- Checking of Extra Spaces in prd_nm col

SELECT prd_nm
FROM [bronze].[crm_prd_info]
WHERE prd_nm != TRIM(prd_nm)

-- Checking for NULLS in price columns

SELECT prd_cost 
FROM bronze.[crm_prd_info]
WHERE prd_cost IS NULL

-- Data standardizing & Normalizing

SELECT DISTINCT(prd_line)
FROM bronze.crm_prd_info


/*Handling Dates by CAST into proper dates and Handling dates where Prd_end_date is less than  Prd_stat_date
Also Filling the NULL Start dates wherever it is NULL
*/

SELECT *
FROM bronze.crm_prd_info


SELECT prd_key,CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info
/*
Filtering out some random sample rows to work with them for Solution work upon
WHERE prd_end_dt < prd_start_dt AND prd_key IN
('AC-HE-HL-U509-R','AC-HE-HL-U509-B')
*/


-- Fully Transformed Table for Insertion

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
	CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info

--====================================================================================
-- Checking quality of transfered data into [silver].[crm_prd_info] after Cleaning
--====================================================================================

SELECT * 
FROM silver.crm_prd_info

SELECT prd_id, COUNT(*) AS duplicate_counts 
FROM silver.crm_prd_info
WHERE prd_id IS NOT NULL
GROUP BY prd_id
HAVING COUNT(*) > 1


-- Checking of Extra Spaces in prd_nm col

SELECT prd_nm
FROM silver.[crm_prd_info]
WHERE prd_nm != TRIM(prd_nm)

-- Checking for NULLS in price columns

SELECT prd_cost 
FROM silver.[crm_prd_info]
WHERE prd_cost IS NULL

-- Data standardizing & Normalizing

SELECT DISTINCT(prd_line)
FROM silver.crm_prd_info


/*Handling Dates by CAST into proper dates and Handling dates where Prd_end_date is less than  Prd_stat_date
Also Filling the NULL Start dates wherever it is NULL
*/

SELECT *
FROM silver.crm_prd_info


SELECT prd_key,CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS new_prd_start_dt
FROM silver.crm_prd_info
/*
Filtering out some random sample rows to work with them for Solution work upon
WHERE prd_end_dt < prd_start_dt AND prd_key IN
('AC-HE-HL-U509-R','AC-HE-HL-U509-B')
*/



--====================================================================================
--Checking quality for [silver].[crm_prd_info] Table 3
--====================================================================================

-- Checking for any Missing Keys for the table it's going to connect with i.e. [sls_cust_id], [crm_prd_info]

SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.[crm_sales_details]
WHERE [sls_cust_id] NOT IN (SELECT cst_id FROM [silver].[crm_cust_info])


--Checking for Dates issues
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.[crm_sales_details]
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101

-- Checking if orderdate is greater than ship or due date
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.[crm_sales_details]
WHERE sls_order_dt > sls_due_dt OR sls_order_dt > sls_ship_dt


--checking if sales are in -ve,0 NULLS are not allowed

SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.[crm_sales_details]
WHERE sls_sales != sls_quantity * sls_price  OR
sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales,sls_quantity,sls_price



--Final table for Insertion into Silver layer table 3

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


SELECT * FROM [silver].[crm_sales_details]


/*========================================================
  Checking quality for [bronze].[erp_cust_az12] table 4
  ========================================================
*/

SELECT * 
FROM [silver].[erp_cust_az12]


SELECT bdate 
FROM [silver].[erp_cust_az12]
WHERE bdate > GETDATE()

-- Distinct Gender

SELECT DISTINCT gen 
FROM [silver].[erp_cust_az12]


-- Inserting statement into Table 4

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



/*========================================================
  Checking quality for [bronze].[erp_loc_a101] table 5
  ========================================================
*/


-- Replacing - with to convert it into a key

SELECT cid, cntry 
FROM silver.erp_loc_a101

-- Data standardisation for cntry column



SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry


-- Final Table

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


SELECT * 
FROM [silver].[erp_loc_a101]


/*==========================================================
  Checking quality for [bronze].[erp_px_cat_g1v2] table 6
  ==========================================================
*/

-- For strings we need to check always if theres any blank spaces

SELECT *
FROM [silver].[erp_px_cat_g1v2]
WHERE cat != TRIM(cat)
OR subcat != TRIM(subcat)
OR maintenance != TRIM(maintenance)

--Data Standardization

SELECT DISTINCT maintenance
FROM [silver].[erp_px_cat_g1v2]


-- Final table for insertion
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

SELECT * 
FROM [silver].[erp_px_cat_g1v2]
