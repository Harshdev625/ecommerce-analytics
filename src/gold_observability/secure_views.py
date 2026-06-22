"""Column masking and row-level security via secure views."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession


@dataclass
class SecureViewsConfig:
    customers: str = "globalmart.silver.customers"
    fact_sales: str = "globalmart.gold.fact_sales"
    access_control_table: str = "globalmart.metadata.user_state_access"
    masked_customers_view: str = "globalmart.gold.v_customers_masked"
    filtered_fact_view: str = "globalmart.gold.v_fact_sales_by_state"


def ensure_access_control_table(spark: SparkSession, config: SecureViewsConfig | None = None) -> None:
    config = config or SecureViewsConfig()
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {config.access_control_table} (
          user_email STRING,
          allowed_state STRING
        )
        USING DELTA
        """
    )
    spark.sql(
        f"""
        MERGE INTO {config.access_control_table} AS t
        USING (
          SELECT 'analyst@example.com' AS user_email, 'SP' AS allowed_state
          UNION ALL SELECT 'analyst@example.com', 'RJ'
          UNION ALL SELECT 'manager@example.com', 'MG'
        ) AS s
        ON t.user_email = s.user_email AND t.allowed_state = s.allowed_state
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def create_secure_views(spark: SparkSession, config: SecureViewsConfig | None = None) -> None:
    config = config or SecureViewsConfig()
    ensure_access_control_table(spark, config)

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {config.masked_customers_view} AS
        SELECT
          customer_id,
          customer_unique_id,
          CONCAT('***', SUBSTRING(customer_zip_code_prefix, -3)) AS customer_zip_code_prefix,
          customer_city,
          customer_state
        FROM {config.customers}
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {config.filtered_fact_view} AS
        SELECT f.*
        FROM {config.fact_sales} f
        INNER JOIN globalmart.gold.dim_customer dc
          ON f.customer_sk = dc.customer_sk AND dc.is_current = true
        INNER JOIN {config.access_control_table} acl
          ON dc.customer_state = acl.allowed_state
        WHERE acl.user_email = current_user()
        """
    )


def demo_secure_views(spark: SparkSession, config: SecureViewsConfig | None = None) -> dict:
    config = config or SecureViewsConfig()
    create_secure_views(spark, config)
    masked_sample = spark.table(config.masked_customers_view).limit(3).collect()
    return {
        "task": "dynamic_secure_views",
        "masked_customers_view": config.masked_customers_view,
        "filtered_fact_view": config.filtered_fact_view,
        "access_control_table": config.access_control_table,
        "masked_sample_rows": len(masked_sample),
    }
