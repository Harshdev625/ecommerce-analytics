-- Lakeview dashboard queries (Task 10.5)
-- Create one visualization per query in Databricks Lakeview.

-- 1. Revenue trend — monthly revenue over time
SELECT
  date_trunc('month', d.full_date) AS order_month,
  ROUND(SUM(f.line_total_value), 2) AS monthly_revenue
FROM globalmart.gold.fact_sales f
INNER JOIN globalmart.gold.dim_date d ON f.date_key = d.date_key
GROUP BY date_trunc('month', d.full_date)
ORDER BY order_month;

-- 2. Geographic distribution — revenue by customer state (top 10)
SELECT
  dc.customer_state,
  ROUND(SUM(f.line_total_value), 2) AS revenue,
  COUNT(DISTINCT f.order_id) AS order_count
FROM globalmart.gold.fact_sales f
INNER JOIN globalmart.gold.dim_customer dc
  ON f.customer_sk = dc.customer_sk AND dc.is_current = true
GROUP BY dc.customer_state
ORDER BY revenue DESC
LIMIT 10;

-- 3. Delivery performance — late delivery rate by month
SELECT
  date_trunc('month', d.full_date) AS order_month,
  ROUND(AVG(CASE WHEN f.delivery_late THEN 1.0 ELSE 0.0 END), 4) AS late_delivery_rate
FROM globalmart.gold.fact_sales f
INNER JOIN globalmart.gold.dim_date d ON f.date_key = d.date_key
GROUP BY date_trunc('month', d.full_date)
ORDER BY order_month;

-- 4. Top categories — revenue by product category
SELECT
  dp.category_name_en AS category,
  ROUND(SUM(f.line_total_value), 2) AS revenue
FROM globalmart.gold.fact_sales f
INNER JOIN globalmart.gold.dim_product dp ON f.product_sk = dp.product_sk
GROUP BY dp.category_name_en
ORDER BY revenue DESC
LIMIT 10;

-- 5. Seller performance — top sellers by revenue
SELECT
  ds.seller_id,
  ds.seller_state,
  ROUND(SUM(f.line_total_value), 2) AS revenue,
  COUNT(DISTINCT f.order_id) AS order_count
FROM globalmart.gold.fact_sales f
INNER JOIN globalmart.gold.dim_seller ds ON f.seller_sk = ds.seller_sk
GROUP BY ds.seller_id, ds.seller_state
ORDER BY revenue DESC
LIMIT 10;
