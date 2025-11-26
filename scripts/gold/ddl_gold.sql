/*Creating Views for Further Analytical process of doing queries

Table Joining strategy should be that we should Start with Master Table always and Avoid INNER Join so that we do not lose any uncommon data

After Joining Multiple Tables we need to Check if there's any Duplicate got introduced by the Join logic
*/

SELECT * FROM silver.erp_cust_az12
SELECT * FROM silver.erp_loc_a101


SELECT cst_id, COUNT(*) FROM (
SELECT 
	ci.cst_id,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_gndr,
	ci.cst_marital_status,
	ca.bdate,
	ca.gen,
	el.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 el
ON ci.cst_key = el.cid
) t
GROUP BY cst_id
HAVING COUNT(*) >1


-- Checking for Data Integration for Gender column that is there in both tables

IF OBJECT_ID('gold.dim_customers','V') IS NOT NULL
DROP VIEW gold.dim_customers

GO

CREATE VIEW gold.dim_customers AS							-- Converting this query into full fledged View so now analytical team now perform query in this view
SELECT 
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	ci.cst_marital_status AS marital_status,
	CASE
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr			-- we are using Gender information from cust_info as this column mast table is cust_info and for any accurate cust infor
		ELSE COALESCE(ca.gen,'n/a')								-- we can rely on its Mast table
	END AS gender,
	ca.bdate AS birthdate,
	el.cntry AS country,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 el
ON ci.cst_key = el.cid
--ORDER BY 1,2

-- verifying the quality of View we created


SELECT DISTINCT gender 
FROM gold.dim_customers

IF OBJECT_ID('gold.dim_products','V') IS NOT NULL
DROP VIEW gold.dim_products

GO

CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt  AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL		-- Filtering out only those data which are current data based on end_date if it's absent/null then it means it is current data

SELECT * 
FROM gold.dim_products

-- Creating of FACT Table Sales and Connecting it with Dimensions table Product,Customers and replacing Fact table key prod_number,cust_number 
--with surrogate Key that we made in their Gold Views

IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
DROP VIEW gold.fact_sales

GO

CREATE VIEW	gold.fact_sales AS
SELECT 
	sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sls_order_dt AS order_date,
	sls_ship_dt AS ship_date,
	sls_due_dt AS due_date,
	sls_sales AS sales,
	sls_quantity AS quantity,
	sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id 

-- Checking quality for gold.fact_sales tables
SELECT * 
FROM gold.fact_sales


SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE c.customer_key IS NULL
