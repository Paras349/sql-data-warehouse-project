/*
======================================================================================
CUSTOMER REPORT
======================================================================================
Purpose:
		This report consolidates key customer metrics and behaviours

Highlights:
		1. Gather essential fiels such as names, age, transaction details
		2. Segments customers into categories (VIP, Regular, New) and age groups
		3. Aggregate customer level metrics:
			- total orders
			- total sales
			- total quantity purchased
			- lifespan (in months)
		4. Calculate valuable KPIs:
			- recenecy (month since last order)
			- average order values
			- average monthly spend
=========================================================================================
*/
CREATE OR ALTER VIEW gold.report_customer AS
WITH base_query AS
(
--1. Gather essential fiels such as names, age, transaction details
	SELECT 
		c.customer_key,
		c.customer_number,
		f.product_key,
		f.order_date,
		f.order_number,
		f.sales,
		f.quantity,
		f.price,
		CONCAT(c.first_name, ' ', c.last_name) AS full_name,
		DATEDIFF(YEAR, c.birthdate, GETDATE()) AS customer_age
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_customers c
	ON f.customer_key = c.customer_key
	WHERE order_date IS NOT NULL
), customer_aggregat AS (
/*3. Aggregate customer level metrics:
			- total orders
			- total sales
			- total quantity purchased
			- lifespan (in months)*/
SELECT 
	customer_key,
	customer_number,
	full_name,
	customer_age,
	SUM(sales) AS total_sales,
	COUNT(DISTINCT(order_number)) AS total_no_orders,
	COUNT(DISTINCT(product_key)) AS total_no_products,
	SUM(quantity) AS total_quantity,
	MAX(order_date) AS last_order_date,
	DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
FROM base_query
GROUP BY customer_key, customer_number, full_name, customer_age
)

SELECT  
	customer_key,
	customer_number,
	full_name,
	customer_age,
	--2. Segments customers into categories (VIP, Regular, New) and age groups
	CASE 
		WHEN customer_age < 20 THEN 'Below 20'
		WHEN customer_age BETWEEN 21 AND 29 THEN '21-29'
		WHEN customer_age BETWEEN 31 AND 39 THEN '31-39'
		WHEN customer_age BETWEEN 41 AND 49 THEN '41-49'
		ELSE 'Above 50'
	END AS age_group,
	CASE 
		WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
		WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
		ELSE 'New'
	END AS categorical,
	/*4. Calculate valuable KPIs:
			-- recenecy (month since last order)
			-- average order values
			*/
	last_order_date,
	DATEDIFF(MONTH, last_order_date, GETDATE()) AS recency,			-- recenecy (month since last order)
	total_sales,
	total_no_orders,
	-- average order values also if any customer has 0 order values
	CASE
		WHEN total_sales = 0 THEN 0
		ELSE total_sales / total_no_orders 
	END AS Average_order,					
	-- average monthly spend (AMS = Total sales/ Nr of Months)
	CASE 
		WHEN lifespan =0 THEN total_sales
		ELSE total_sales / lifespan
	END AS avg_monthly_spend,
	total_no_products,
	total_quantity,
	lifespan
FROM customer_aggregat


/* At the very end we need to make a VIEW of the overall Customer report summary that we have created and then further
Analysts can bring it into BI tools to create Dashboards and Reports and we can also further make queries upon this data/view that we created
*/

SELECT 
age_group,
COUNT(categorical) AS category_count,
MAX(total_sales) AS max_sales,
MIN(total_sales) AS min_sales,
SUM(total_sales) AS total_sales_groups
FROM gold.report_customer
GROUP BY age_group




/*
======================================================================================
PRODUCT REPORT
======================================================================================
Purpose:
		This report consolidates key Product metrics and behaviours

Highlights:
		1. Gather essential fiels such as product names, category, sub-category and cost
		2. Segments Products by Revenue (High Performers, Mid-Range, Low Performers)
		3. Aggregate customer level metrics:
			- total orders
			- total sales
			- total quantity sold
			- total customers (unique)
			- lifespan (in months)
		4. Calculate valuable KPIs:
			- recenecy (month since last order)
			- average order revenue (AOR)
			- average monthly revenue
=========================================================================================
*/


CREATE OR ALTER VIEW gold.report_products AS
WITH base_query AS(
SELECT 
	p.product_key,
	p.product_name,
	c.customer_key,
	p.category,
	p.subcategory,
	f.order_date,
	f.order_number,
	f.sales,
	f.quantity,
	f.price,
	p.cost
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
WHERE f.order_date IS NOT NULL
), product_aggregate AS(
SELECT 
	product_name,
	category,
	subcategory,
	cost,
	/*3. Aggregate customer level metrics:
			- total orders
			- total sales
			- total quantity sold
			- total customers (unique)
			- lifespan (in months)*/
	DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan,
	DATEDIFF(MONTH, MAX(order_date), GETDATE()) AS recency,
	COUNT(DISTINCT order_number) AS total_orders,
	COUNT(DISTINCT customer_key) AS total_customer,
	SUM(sales) AS total_sales,
	SUM(quantity) AS total_quantity,
	SUM(sales) * SUM(quantity) AS total_revenue,
	AVG(CAST(sales AS FLOAT) / NULLIF(quantity,0)) AS avg_selling_price
FROM base_query
GROUP BY 
	product_name, 
	category, 
	cost,
	subcategory
)
SELECT 
	--2. Segments Products by Revenue (High Performers, Mid-Range, Low Performers)
	product_name,
	CASE 
		WHEN total_sales > 50000 THEN 'High Performer'
		WHEN total_sales >= 10000  THEN 'Mid-Range' 
		ELSE 'Low Performers'
	END AS products_segments,
	category,
	subcategory,
	lifespan,
	recency,
	cost,
	CASE
		WHEN total_orders = 0 THEN 0
		ELSE total_sales / total_orders 
	END AS average_order_revenue,
	CASE
		WHEN lifespan = 0 THEN total_sales
		ELSE total_sales / lifespan
	END AS average_monthly_revenue,
	total_orders,
	total_customer,
	total_sales,
	avg_selling_price,
	total_quantity,
	total_revenue
FROM product_aggregate

SELECT * 
FROM gold.report_products
