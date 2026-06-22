"""Gold daily sales metrics for delivered orders."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.joins.business_questions import SilverJoinTables, load_all_orders

MA_SHORT = 7
MA_LONG = 30


@dataclass
class GoldDailySalesConfig:
    source: SilverJoinTables | None = None
    target_table: str = "globalmart.gold.daily_sales_metrics"


def delivered_orders_with_revenue(
    spark: SparkSession,
    tables: SilverJoinTables | None = None,
) -> DataFrame:
    """Delivered orders joined to line-item revenue totals."""
    tables = tables or SilverJoinTables()
    orders = (
        load_all_orders(spark, tables)
        .filter(F.col("order_status") == "delivered")
        .select("order_id", "customer_id", "order_purchase_timestamp_ts")
    )
    order_revenue = (
        spark.table(tables.order_items)
        .groupBy("order_id")
        .agg(
            F.sum("line_total_value").alias("order_revenue"),
            F.sum("price").alias("order_product_revenue"),
            F.sum("freight_value").alias("order_freight"),
            F.count("*").alias("item_count"),
        )
    )
    return (
        orders.join(order_revenue, "order_id", "inner")
        .withColumn("order_date", F.to_date(F.col("order_purchase_timestamp_ts")))
        .filter(F.col("order_date").isNotNull())
    )


def build_daily_sales_metrics(
    spark: SparkSession,
    config: GoldDailySalesConfig | None = None,
) -> DataFrame:
    """Aggregate delivered-order revenue by day with window metrics."""
    config = config or GoldDailySalesConfig()
    tables = config.source or SilverJoinTables()

    daily = (
        delivered_orders_with_revenue(spark, tables)
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("customer_id").alias("customer_count"),
            F.sum("order_revenue").alias("daily_revenue"),
            F.sum("order_freight").alias("daily_freight"),
            F.sum("item_count").alias("item_count"),
        )
    )

    by_date = Window.orderBy("order_date")
    by_month_rank = Window.partitionBy(
        F.year("order_date"),
        F.month("order_date"),
    ).orderBy(F.col("daily_revenue").desc())

    return (
        daily.withColumn(
            "cumulative_revenue",
            F.sum("daily_revenue").over(by_date),
        )
        .withColumn(
            f"ma_{MA_SHORT}d",
            F.avg("daily_revenue").over(
                by_date.rowsBetween(-(MA_SHORT - 1), Window.currentRow)
            ),
        )
        .withColumn(
            f"ma_{MA_LONG}d",
            F.avg("daily_revenue").over(
                by_date.rowsBetween(-(MA_LONG - 1), Window.currentRow)
            ),
        )
        .withColumn("prev_day_revenue", F.lag("daily_revenue").over(by_date))
        .withColumn(
            "revenue_dod_change",
            F.col("daily_revenue") - F.col("prev_day_revenue"),
        )
        .withColumn(
            "revenue_dod_pct",
            F.when(
                F.col("prev_day_revenue").isNull() | (F.col("prev_day_revenue") == 0),
                F.lit(None),
            ).otherwise(
                F.round(
                    (F.col("revenue_dod_change") / F.col("prev_day_revenue")) * 100,
                    2,
                )
            ),
        )
        .withColumn("revenue_rank_in_month", F.rank().over(by_month_rank))
        .withColumn("processed_at", F.current_timestamp())
        .orderBy("order_date")
    )


def sample_month(
    metrics_df: DataFrame,
    year: int,
    month: int,
) -> DataFrame:
    return (
        metrics_df.filter(
            (F.year("order_date") == year) & (F.month("order_date") == month)
        )
        .orderBy("order_date")
    )


def run_daily_sales_metrics(
    spark: SparkSession,
    config: GoldDailySalesConfig | None = None,
    sample_year: int = 2018,
    sample_month_num: int = 1,
) -> dict:
    config = config or GoldDailySalesConfig()
    tables = config.source or SilverJoinTables()

    metrics = build_daily_sales_metrics(spark, config)
    metrics.write.format("delta").mode("overwrite").saveAsTable(config.target_table)

    written = spark.table(config.target_table)
    month_df = sample_month(written, sample_year, sample_month_num)
    month_rows = [row.asDict() for row in month_df.collect()]

    delivered_count = delivered_orders_with_revenue(spark, tables).select("order_id").distinct().count()

    return {
        "task": "gold_daily_sales_metrics",
        "target_table": config.target_table,
        "delivered_orders_in_scope": delivered_count,
        "daily_row_count": written.count(),
        "date_range": {
            "min": str(written.agg(F.min("order_date")).collect()[0][0]),
            "max": str(written.agg(F.max("order_date")).collect()[0][0]),
        },
        "window_sizes": {"short_ma_days": MA_SHORT, "long_ma_days": MA_LONG},
        "sample_month": {"year": sample_year, "month": sample_month_num, "rows": month_rows},
        "top_revenue_day_in_sample_month": max(
            month_rows,
            key=lambda r: float(r["daily_revenue"] or 0),
            default=None,
        ),
    }
