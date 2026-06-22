"""Extended daily sales summary with new vs returning customers."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.gold.daily_sales import delivered_orders_with_revenue
from src.joins.business_questions import SilverJoinTables


@dataclass
class DailySalesSummaryConfig:
    source: SilverJoinTables | None = None
    target_table: str = "globalmart.gold.daily_sales_summary"


def build_daily_sales_summary(
    spark: SparkSession,
    config: DailySalesSummaryConfig | None = None,
) -> DataFrame:
    config = config or DailySalesSummaryConfig()
    tables = config.source or SilverJoinTables()

    orders = delivered_orders_with_revenue(spark, tables).select(
        "order_id",
        "customer_id",
        "order_date",
        "order_revenue",
        "order_freight",
        "item_count",
    )

    first_order = orders.groupBy("customer_id").agg(F.min("order_date").alias("first_order_date"))
    enriched = orders.join(first_order, "customer_id", "inner").withColumn(
        "is_new_customer",
        F.col("order_date") == F.col("first_order_date"),
    )

    daily = (
        enriched.groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.round(F.sum("order_revenue"), 2).alias("daily_revenue"),
            F.round(F.sum("order_freight"), 2).alias("daily_freight"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct(
                F.when(F.col("is_new_customer"), F.col("customer_id"))
            ).alias("new_customers"),
            F.countDistinct(
                F.when(~F.col("is_new_customer"), F.col("customer_id"))
            ).alias("returning_customers"),
            F.sum(F.col("item_count")).alias("item_count"),
        )
        .withColumn("processed_at", F.current_timestamp())
    )
    return daily


def validate_customer_counts(summary_df: DataFrame) -> dict:
    check = summary_df.withColumn(
        "customer_sum_ok",
        F.col("new_customers") + F.col("returning_customers") == F.col("unique_customers"),
    )
    failures = check.filter(~F.col("customer_sum_ok")).count()
    return {
        "rows_checked": check.count(),
        "validation_failures": failures,
        "all_rows_valid": failures == 0,
    }


def run_daily_sales_summary(spark: SparkSession, config: DailySalesSummaryConfig | None = None) -> dict:
    config = config or DailySalesSummaryConfig()
    summary = build_daily_sales_summary(spark, config)
    summary.write.format("delta").mode("overwrite").saveAsTable(config.target_table)
    written = spark.table(config.target_table)
    validation = validate_customer_counts(written)
    return {
        "task": "daily_sales_summary",
        "target_table": config.target_table,
        "row_count": written.count(),
        "validation": validation,
    }
