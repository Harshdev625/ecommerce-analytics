"""SQL-backed datasets for the five Lakeview dashboard charts."""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_dashboard_datasets(spark: SparkSession) -> dict:
    """Run aggregate queries used by the business dashboard."""
    monthly_revenue = spark.sql(
        """
        SELECT
          date_trunc('month', d.full_date) AS order_month,
          ROUND(SUM(f.total_amount), 2) AS monthly_revenue
        FROM globalmart.gold.fact_sales f
        INNER JOIN globalmart.gold.dim_date d ON f.date_key = d.date_key
        GROUP BY date_trunc('month', d.full_date)
        ORDER BY order_month
        """
    ).collect()

    revenue_by_state = spark.sql(
        """
        SELECT
          dc.customer_state,
          ROUND(SUM(f.total_amount), 2) AS revenue,
          COUNT(DISTINCT f.order_id) AS order_count
        FROM globalmart.gold.fact_sales f
        INNER JOIN globalmart.gold.dim_customer dc
          ON f.customer_sk = dc.customer_sk AND dc.is_current = true
        GROUP BY dc.customer_state
        ORDER BY revenue DESC
        LIMIT 10
        """
    ).collect()

    delivery_performance = spark.sql(
        """
        SELECT
          date_trunc('month', d.full_date) AS order_month,
          ROUND(AVG(CASE WHEN f.delivery_late THEN 1.0 ELSE 0.0 END), 4) AS late_delivery_rate
        FROM globalmart.gold.fact_sales f
        INNER JOIN globalmart.gold.dim_date d ON f.date_key = d.date_key
        GROUP BY date_trunc('month', d.full_date)
        ORDER BY order_month
        """
    ).collect()

    top_categories = spark.sql(
        """
        SELECT
          dp.category_name_en AS category,
          ROUND(SUM(f.total_amount), 2) AS revenue
        FROM globalmart.gold.fact_sales f
        INNER JOIN globalmart.gold.dim_product dp ON f.product_sk = dp.product_sk
        GROUP BY dp.category_name_en
        ORDER BY revenue DESC
        LIMIT 10
        """
    ).collect()

    seller_concentration = spark.sql(
        """
        SELECT
          ds.seller_id,
          ds.seller_state,
          ROUND(SUM(f.total_amount), 2) AS revenue,
          COUNT(DISTINCT f.order_id) AS order_count
        FROM globalmart.gold.fact_sales f
        INNER JOIN globalmart.gold.dim_seller ds ON f.seller_sk = ds.seller_sk
        GROUP BY ds.seller_id, ds.seller_state
        ORDER BY revenue DESC
        LIMIT 10
        """
    ).collect()

    return {
        "revenue_trend_monthly": [row.asDict() for row in monthly_revenue],
        "geographic_distribution_top_states": [row.asDict() for row in revenue_by_state],
        "delivery_performance_monthly": [row.asDict() for row in delivery_performance],
        "top_categories_by_revenue": [row.asDict() for row in top_categories],
        "seller_performance_top10": [row.asDict() for row in seller_concentration],
    }
