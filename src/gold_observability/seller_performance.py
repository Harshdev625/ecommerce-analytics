"""Monthly seller performance rankings."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.joins.business_questions import SilverJoinTables, load_all_orders


@dataclass
class SellerPerformanceConfig:
    source: SilverJoinTables | None = None
    target_table: str = "globalmart.gold.seller_performance_monthly"


def build_seller_performance_monthly(
    spark: SparkSession,
    config: SellerPerformanceConfig | None = None,
) -> DataFrame:
    config = config or SellerPerformanceConfig()
    tables = config.source or SilverJoinTables()

    delivered = (
        load_all_orders(spark, tables)
        .filter(F.col("order_status") == "delivered")
        .select("order_id", "delivery_duration_days", "delivery_late", "order_purchase_timestamp_ts")
    )
    items = spark.table(tables.order_items).select(
        "order_id", "seller_id", "line_total_value", "freight_value"
    )
    sellers = spark.table(tables.sellers).select("seller_id", "seller_state")

    line_level = (
        delivered.join(items, "order_id", "inner")
        .join(sellers, "seller_id", "inner")
        .withColumn("order_month", F.date_trunc("month", F.col("order_purchase_timestamp_ts")))
    )

    monthly = line_level.groupBy("seller_id", "seller_state", "order_month").agg(
        F.round(F.sum("line_total_value"), 2).alias("monthly_revenue"),
        F.countDistinct("order_id").alias("order_count"),
        F.round(F.avg("delivery_duration_days"), 2).alias("avg_delivery_days"),
        F.round(F.avg(F.col("delivery_late").cast("int")), 4).alias("late_delivery_rate"),
    )

    overall_rank = Window.partitionBy("order_month").orderBy(F.col("monthly_revenue").desc())
    state_rank = Window.partitionBy("order_month", "seller_state").orderBy(F.col("monthly_revenue").desc())

    return (
        monthly.withColumn("revenue_rank_overall", F.rank().over(overall_rank))
        .withColumn("revenue_rank_in_state", F.rank().over(state_rank))
        .withColumn("processed_at", F.current_timestamp())
    )


def top_sellers_for_month(
    df: DataFrame,
    year: int,
    month: int,
    limit: int = 5,
) -> list[dict]:
    rows = (
        df.filter(
            (F.year("order_month") == year) & (F.month("order_month") == month)
        )
        .orderBy("revenue_rank_overall")
        .limit(limit)
        .collect()
    )
    return [r.asDict() for r in rows]


def run_seller_performance_monthly(
    spark: SparkSession,
    config: SellerPerformanceConfig | None = None,
    sample_year: int = 2018,
    sample_month: int = 1,
) -> dict:
    config = config or SellerPerformanceConfig()
    perf = build_seller_performance_monthly(spark, config)
    perf.write.format("delta").mode("overwrite").saveAsTable(config.target_table)
    written = spark.table(config.target_table)
    top = top_sellers_for_month(written, sample_year, sample_month)
    import json as _json

    top = _json.loads(_json.dumps(top, default=str))
    return {
        "task": "seller_performance_monthly",
        "target_table": config.target_table,
        "row_count": written.count(),
        "sample_top_sellers": {"year": sample_year, "month": sample_month, "rows": top},
    }
