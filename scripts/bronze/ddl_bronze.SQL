======================================
--Project 1: Data Warehousing Projects
======================================

/*Step 1: Requirement Analysis for Data warehousing Project

Objective: Build a Modern Data Warehouse using SQL Server to Consolidate Sale data, enabling analytical Reporting and informed decision making.

Specification: 

1.Data sources: Import data from 2 Source system (ERP and CRM) provided as CSV files.
2.Data quality: Cleanse and Resolve both data quality issues Prior to analysis.
3.Integration: Combine both Source into a Single, user friendly data model designed for analytical queries.
4.Scope: Focus on Latest dataset only, Historization of data is not needed.
5.Documentation: Provide clear Documentation of the data model to support both business stakeholders and analytical teams.


Step 2: We need to Decide what we are going to build 
1.Data Warehouse: Here we have Strucutred data and business wants to provide solid foundations for Analytics Teams
2.Data Lake: Here we have only Semi Structured data. As well we have many different data types like Images, Video, Database tables etc
3.Data Lakehouse: It is Mix of both we have Structured from warehouse and different type of data from lake
4.Data Mesh: Here we do not have centralized data we make it Decentralized data.


Data Warehouse has 4 types of architectures but we will understand only 1 for this Projects:

Medallian Architecture: Here we have 3 layers:
1.Bronze layer(here we have Raw data as it is to help us in Tracebility).
2.Silver layer(here we do Transformation and Cleansing on data but we do not apply Business logic yet)
3.Gold layer(Here we build Objects like Data Marts not only for Analytical team but also for Machine learning, AI)


Naming Convention: For this project we are going with Snake_case (snake_case) where everything is in lower case and word seperation is handled by underscore _
Language: English
Avoid Reserve Words: do not use SQL keywords for naming

Project Initialization

Creating Database and Schemas
*/



	/*Creating Tables after doing Data Profiling ourself and bringing tables from Source System to Datawarehouse as it is without any changes

	DROP TABLE bronze.erp_PX_CAT_G1V2
	DROP TABLE bronze.erp_LOC_A101
	DROP TABLE bronze.erp_CUST_AZ12
	DROP TABLE bronze.crm_sales_details
	DROP TABLE bronze.crm_prd_info
	DROP TABLE bronze.crm_cust_info*/



USE master

CREATE DATABASE DataWarehouse

USE DataWarehouse

-- Creating Schemas(It is like a Logical container to help us organize things)

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;


CREATE OR ALTER PROCEDURE bronze.complete_bronze AS
BEGIN
	DECLARE @batch_start_time DATETIME, @batch_finish_time DATETIME, @table_start_time DATETIME, @table_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time = GETDATE()
	
	-- creating table 1 bronze.crm_cust_info
	IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
		DROP TABLE bronze.crm_cust_info;
	CREATE TABLE bronze.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
	);

	-- Creating table 2 bronze.crm_prd_info
	IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
		DROP TABLE bronze.crm_prd_info;
	CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm  NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
	);
 
	--Creating table 3 bronze.crm_sales_details
	IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
		DROP TABLE bronze.crm_sales_details;
	CREATE TABLE bronze.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price FLOAT
	);

	--Creating table 4 from bronze.erp_CUST_AZ12
	IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
		DROP TABLE bronze.erp_cust_az12;
	CREATE TABLE bronze.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
	);

	--Creating table 5 from bronze.erp_LOC_A101
	IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
		DROP TABLE bronze.erp_loc_a101;
	CREATE TABLE bronze.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
	);


	--Creating table 6 from bronze.erp_PX_CAT_G1V2
	IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
		DROP TABLE bronze.erp_px_cat_g1v2;
	CREATE TABLE bronze.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
	);




	/*
	DROP TABLE bronze.erp_PX_CAT_G1V2
	DROP TABLE bronze.erp_LOC_A101
	DROP TABLE bronze.erp_CUST_AZ12
	DROP TABLE bronze.crm_sales_details
	DROP TABLE bronze.crm_prd_info
	DROP TABLE bronze.crm_cust_info
	Doing Bulk Insert into the table as we have already defined the Table structure
	Bulk insert is a method to insert Huge amount of data in one go. 
	Using TRUNCATE & INSERT method so if there's any change in file it'll also get loaded in Bronzle layer table

	Add Prints to Track execution, debug issues and understand it's flow of programm 

	Handle errors by using Try & Catch statement

	Track ETL Time duration to understand which table/column is taking too long to load
	*/



END TRY
BEGIN CATCH
	PRINT '============================================='
	PRINT 'Error Occurred for Complete Bronze Layer'
	PRINT 'Error Number' + CAST(Error_number() AS NVARCHAR)
	PRINT 'Error Line' + CAST(Error_line() AS NVARCHAR)
	PRINT '============================================='
END CATCH
-- End for complete Bronze Layer
END	

EXEC bronze.load_bronze;



/*Checking data by comparing it with Source file. Checking whether the data has not shifted from one to another columns

SELECT COUNT(*) FROM [bronze].[crm_cust_info]
SELECT COUNT(*) FROM [bronze].[crm_prd_info]
SELECT COUNT(*) FROM [bronze].[erp_loc_a101]
SELECT COUNT(*) FROM [bronze].[erp_px_cat_g1v2]
SELECT COUNT(*) FROM [bronze].[erp_cust_az12]
SELECT COUNT(*) FROM [bronze].[crm_sales_details]
*/
















