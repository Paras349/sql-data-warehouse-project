/*
After Joining Multiple Tables we need to Check if there's any Duplicate got introduced by the Join logic
*/

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



-- verifying the quality of View we created


SELECT DISTINCT gender 
FROM gold.dim_customers
  
  
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
