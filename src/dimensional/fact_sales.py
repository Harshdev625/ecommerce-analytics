"""Fact sales table — delivered order items with dimension foreign keys."""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.dimensional.customer_dim import (
    CustomerDimConfig,
    ensure_delivered_order_customers_in_dim,
    ensure_one_current_customer_version,
)
from src.dimensional.product_dim import ProductDimConfig, ensure_order_item_products_in_dim
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


def _delivered_order_items_base(
    spark: SparkSession,
    config: FactSalesConfig,
) -> DataFrame:
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
    return (
        items.join(delivered_orders, "order_id", "inner")
        .withColumn("order_date", F.to_date(F.col("order_purchase_timestamp_ts")))
        .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
    )


def prepare_dimensions_for_fact(
    spark: SparkSession,
    config: FactSalesConfig | None = None,
) -> dict:
    """Repair common dimension gaps that cause inner-join row loss in fact_sales."""
    config = config or FactSalesConfig()
    customer_cfg = CustomerDimConfig(
        target_table=config.dim_customer,
        source_tables=config.source_tables,
    )
    product_cfg = ProductDimConfig(
        target_table=config.dim_product,
        source_order_items=config.source_items,
    )

    customer_orphans = ensure_delivered_order_customers_in_dim(spark, customer_cfg)
    customer_repair = ensure_one_current_customer_version(spark, customer_cfg)
    product_repair = ensure_order_item_products_in_dim(spark, product_cfg)

    return {
        "customer_orphan_merge": customer_orphans,
        "customer_current_repair": customer_repair,
        "product_orphan_merge": product_repair,
    }


def diagnose_fact_join_gaps(
    spark: SparkSession,
    config: FactSalesConfig | None = None,
) -> dict:
    """Count delivered line items lost at each dimension inner join."""
    config = config or FactSalesConfig()
    base = _delivered_order_items_base(spark, config)
    total = base.count()

    dim_date_keys = spark.table(config.dim_date).select("date_key")
    dim_product = spark.table(config.dim_product).select("product_id")
    dim_seller = spark.table(config.dim_seller).select("seller_id")
    dim_customer_current = (
        spark.table(config.dim_customer)
        .filter(F.col("is_current"))
        .select("customer_id")
    )
    dim_customer_all = spark.table(config.dim_customer).select("customer_id").distinct()

    after_date = base.join(dim_date_keys, "date_key", "inner")
    after_product = after_date.join(dim_product, "product_id", "inner")
    after_seller = after_product.join(dim_seller, "seller_id", "inner")
    after_customer = after_seller.join(dim_customer_current, "customer_id", "inner")

    missing_customer_not_in_dim = base.join(dim_customer_all, "customer_id", "left_anti").count()
    missing_customer_not_current = (
        base.join(dim_customer_all, "customer_id", "inner")
        .join(dim_customer_current, "customer_id", "left_anti")
        .count()
    )

    return {
        "delivered_item_count": total,
        "missing_or_null_date_key": base.filter(F.col("date_key").isNull()).count()
        + base.filter(F.col("date_key").isNotNull())
        .join(dim_date_keys, "date_key", "left_anti")
        .count(),
        "missing_product_in_dim": base.join(dim_product, "product_id", "left_anti").count(),
        "missing_seller_in_dim": base.join(dim_seller, "seller_id", "left_anti").count(),
        "missing_customer_not_in_dim": missing_customer_not_in_dim,
        "missing_customer_not_current": missing_customer_not_current,
        "missing_current_customer_in_dim": missing_customer_not_in_dim + missing_customer_not_current,
        "rows_after_date_join": after_date.count(),
        "rows_after_product_join": after_product.count(),
        "rows_after_seller_join": after_seller.count(),
        "rows_after_all_joins": after_customer.count(),
        "rows_dropped_total": total - after_customer.count(),
    }


def build_fact_sales(
    spark: SparkSession,
    config: FactSalesConfig | None = None,
) -> DataFrame:
    config = config or FactSalesConfig()

    dim_date = spark.table(config.dim_date).select("date_key")
    dim_product = spark.table(config.dim_product).select("product_sk", "product_id")
    dim_seller = spark.table(config.dim_seller).select("seller_sk", "seller_id")
    dim_customer = (
        spark.table(config.dim_customer)
        .filter(F.col("is_current"))
        .select("customer_sk", "customer_id")
    )

    return (
        _delivered_order_items_base(spark, config)
        .join(dim_date, "date_key", "inner")
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

    delivered_items = _delivered_order_items_base(spark, config)
    expected_count = delivered_items.count()
    fact_count = fact_df.count()

    expected_revenue = (
        delivered_items.agg(F.round(F.sum("line_total_value"), 2).alias("total"))
        .collect()[0]["total"]
    )
    fact_revenue = (
        fact_df.agg(F.round(F.sum("total_amount"), 2).alias("total")).collect()[0]["total"]
    )

    row_count_matches = fact_count == expected_count
    revenue_matches = fact_revenue == expected_revenue
    all_fks_ok = all(v == 0 for v in null_fks.values())
    all_passed = all_fks_ok and row_count_matches and revenue_matches

    result = {
        "null_foreign_keys": null_fks,
        "all_foreign_keys_non_null": all_fks_ok,
        "fact_row_count": fact_count,
        "expected_delivered_item_count": expected_count,
        "row_count_matches": row_count_matches,
        "fact_total_revenue": float(fact_revenue or 0),
        "expected_total_revenue": float(expected_revenue or 0),
        "revenue_matches": revenue_matches,
        "all_validations_passed": all_passed,
    }
    if not all_passed:
        result["join_gaps"] = diagnose_fact_join_gaps(spark, config)

    return result


def run_fact_sales(
    spark: SparkSession,
    config: FactSalesConfig | None = None,
    *,
    repair_dimensions: bool = True,
) -> dict:
    config = config or FactSalesConfig()
    dim_prep = prepare_dimensions_for_fact(spark, config) if repair_dimensions else None

    fact = build_fact_sales(spark, config)
    fact.write.format("delta").mode("overwrite").saveAsTable(config.target_table)

    written = spark.table(config.target_table)
    validation = validate_fact_sales(spark, written, config)

    return {
        "task": "fact_sales",
        "target_table": config.target_table,
        "fact_row_count": written.count(),
        "dimension_prep": dim_prep,
        "validation": validation,
    }
