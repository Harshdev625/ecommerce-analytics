"""Fact sales table — delivered order items with dimension foreign keys."""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.joins.business_questions import SilverJoinTables, load_all_orders

FACT_FK_COLUMNS = ("date_key", "product_sk", "seller_sk", "customer_sk")


@dataclass
class FactSalesConfig:
    source_items: str = "globalmart.silver.order_items"
    dim_date: str = "globalmart.gold.dim_date"
    dim_product: str = "globalmart.gold.dim_product"
    dim_seller: str = "globalmart.gold.dim_seller"
    dim_customer: str = "globalmart.gold.dim_customer"
    target_table: str = "globalmart.gold.fact_sales"
    source_tables: SilverJoinTables = field(default_factory=SilverJoinTables)


def build_fact_sales(
    spark: SparkSession,
    config: FactSalesConfig | None = None,
) -> DataFrame:
    config = config or FactSalesConfig()

    delivered_orders = (
        load_all_orders(spark, config.source_tables)
        .filter(F.col("order_status") == "delivered")
        .select(
            "order_id",
            "customer_id",
            "order_purchase_timestamp_ts",
            "delivery_duration_days",
            "delivery_late",
        )
    )

    items = spark.table(config.source_items)
    dim_date = spark.table(config.dim_date).select(
        F.col("date_key"),
        F.col("full_date").alias("order_date"),
    )
    dim_product = spark.table(config.dim_product).select("product_sk", "product_id")
    dim_seller = spark.table(config.dim_seller).select("seller_sk", "seller_id")
    dim_customer = (
        spark.table(config.dim_customer)
        .filter(F.col("is_current"))
        .select("customer_sk", "customer_id")
    )

    return (
        items.join(delivered_orders, "order_id", "inner")
        .withColumn("order_date", F.to_date(F.col("order_purchase_timestamp_ts")))
        .join(dim_date, "order_date", "inner")
        .join(dim_product, "product_id", "inner")
        .join(dim_seller, "seller_id", "inner")
        .join(dim_customer, "customer_id", "inner")
        .select(
            "date_key",
            "product_sk",
            "seller_sk",
            "customer_sk",
            F.col("order_id"),
            F.col("order_item_id"),
            F.col("price"),
            F.col("freight_value"),
            F.col("line_total_value").alias("total_amount"),
            F.col("delivery_duration_days"),
            F.col("delivery_late"),
        )
        .withColumn("processed_at", F.current_timestamp())
    )


def validate_fact_sales(
    spark: SparkSession,
    fact_df: DataFrame,
    config: FactSalesConfig | None = None,
) -> dict:
    config = config or FactSalesConfig()

    null_fks = {}
    for col in FACT_FK_COLUMNS:
        null_fks[col] = fact_df.filter(F.col(col).isNull()).count()

    delivered_items = (
        load_all_orders(spark, config.source_tables)
        .filter(F.col("order_status") == "delivered")
        .select("order_id")
        .join(spark.table(config.source_items), "order_id", "inner")
    )
    expected_count = delivered_items.count()
    fact_count = fact_df.count()

    expected_revenue = (
        delivered_items.agg(F.round(F.sum("line_total_value"), 2).alias("total"))
        .collect()[0]["total"]
    )
    fact_revenue = (
        fact_df.agg(F.round(F.sum("total_amount"), 2).alias("total")).collect()[0]["total"]
    )

    return {
        "null_foreign_keys": null_fks,
        "all_foreign_keys_non_null": all(v == 0 for v in null_fks.values()),
        "fact_row_count": fact_count,
        "expected_delivered_item_count": expected_count,
        "row_count_matches": fact_count == expected_count,
        "fact_total_revenue": float(fact_revenue or 0),
        "expected_total_revenue": float(expected_revenue or 0),
        "revenue_matches": fact_revenue == expected_revenue,
        "all_validations_passed": (
            all(v == 0 for v in null_fks.values())
            and fact_count == expected_count
            and fact_revenue == expected_revenue
        ),
    }


def run_fact_sales(
    spark: SparkSession,
    config: FactSalesConfig | None = None,
) -> dict:
    config = config or FactSalesConfig()
    fact = build_fact_sales(spark, config)
    fact.write.format("delta").mode("overwrite").saveAsTable(config.target_table)

    written = spark.table(config.target_table)
    validation = validate_fact_sales(spark, written, config)

    return {
        "task": "fact_sales",
        "target_table": config.target_table,
        "fact_row_count": written.count(),
        "validation": validation,
    }
