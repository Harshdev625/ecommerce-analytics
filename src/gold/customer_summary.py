"""Gold customer summary with MERGE upsert and soft-delete."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.gold.daily_sales import delivered_orders_with_revenue
from src.joins.business_questions import SilverJoinTables

SUMMARY_COLUMNS = (
    "customer_id",
    "total_orders",
    "total_spend",
    "first_order_date",
    "last_order_date",
    "avg_order_value",
    "is_active",
)


@dataclass
class GoldCustomerSummaryConfig:
    source: SilverJoinTables | None = None
    target_table: str = "globalmart.gold.customer_summary"
    inactive_days: int = 180


def _reference_date(spark: SparkSession, tables: SilverJoinTables) -> str:
    ref = delivered_orders_with_revenue(spark, tables).agg(F.max("order_date")).collect()[0][0]
    return str(ref)


def build_customer_summary_source(
    spark: SparkSession,
    config: GoldCustomerSummaryConfig | None = None,
) -> tuple[DataFrame, str, str]:
    """Lifetime metrics per customer from delivered orders."""
    config = config or GoldCustomerSummaryConfig()
    tables = config.source or SilverJoinTables()
    reference_date = _reference_date(spark, tables)
    cutoff_date = F.date_sub(F.lit(reference_date), config.inactive_days)

    summary = (
        delivered_orders_with_revenue(spark, tables)
        .groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("order_revenue"), 2).alias("total_spend"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
        .withColumn(
            "avg_order_value",
            F.round(F.col("total_spend") / F.col("total_orders"), 2),
        )
        .withColumn("is_active", F.col("last_order_date") >= cutoff_date)
    )
    cutoff_str = str(
        spark.range(1)
        .select(F.date_sub(F.lit(reference_date), config.inactive_days).alias("cutoff"))
        .collect()[0]["cutoff"]
    )
    return summary, reference_date, cutoff_str


def _metrics_changed_condition() -> str:
    return """
        s.total_orders <> t.total_orders
        OR s.total_spend <> t.total_spend
        OR s.first_order_date <> t.first_order_date
        OR s.last_order_date <> t.last_order_date
        OR s.avg_order_value <> t.avg_order_value
        OR s.is_active <> t.is_active
    """.strip()


def plan_merge_counts(source: DataFrame, target: DataFrame) -> dict:
    inserts = source.join(target.select("customer_id"), "customer_id", "left_anti").count()

    joined = source.alias("s").join(target.alias("t"), "customer_id", "inner")
    updates = joined.filter(
        (F.col("s.total_orders") != F.col("t.total_orders"))
        | (F.col("s.total_spend") != F.col("t.total_spend"))
        | (F.col("s.first_order_date") != F.col("t.first_order_date"))
        | (F.col("s.last_order_date") != F.col("t.last_order_date"))
        | (F.col("s.avg_order_value") != F.col("t.avg_order_value"))
        | (F.col("s.is_active") != F.col("t.is_active"))
    ).count()

    soft_deleted = joined.filter(F.col("t.is_active") & (~F.col("s.is_active"))).count()
    return {"inserts": inserts, "updates": updates, "soft_deleted": soft_deleted}


def apply_customer_summary_merge(
    spark: SparkSession,
    source: DataFrame,
    target_table: str,
) -> None:
    from delta.tables import DeltaTable

    insert_values = {c: f"s.{c}" for c in SUMMARY_COLUMNS if c != "customer_id"}
    insert_values["processed_at"] = "current_timestamp()"

    update_set = {c: f"s.{c}" for c in SUMMARY_COLUMNS if c != "customer_id"}
    update_set["processed_at"] = "current_timestamp()"

    if not spark.catalog.tableExists(target_table):
        (
            source.withColumn("processed_at", F.current_timestamp())
            .write.format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )
        return

    DeltaTable.forName(spark, target_table).alias("t").merge(
        source.alias("s"),
        "t.customer_id = s.customer_id",
    ).whenMatchedUpdate(
        condition=_metrics_changed_condition(),
        set=update_set,
    ).whenNotMatchedInsert(
        values={"customer_id": "s.customer_id", **insert_values},
    ).execute()


def run_customer_summary_merge(
    spark: SparkSession,
    config: GoldCustomerSummaryConfig | None = None,
) -> dict:
    config = config or GoldCustomerSummaryConfig()
    source, reference_date, cutoff_date = build_customer_summary_source(spark, config)

    table_exists = spark.catalog.tableExists(config.target_table)
    target = spark.table(config.target_table) if table_exists else None
    counts = plan_merge_counts(source, target) if table_exists else {
        "inserts": source.count(),
        "updates": 0,
        "soft_deleted": 0,
    }

    apply_customer_summary_merge(spark, source, config.target_table)

    written = spark.table(config.target_table)
    active_count = written.filter(F.col("is_active")).count()

    return {
        "task": "gold_customer_summary_merge",
        "target_table": config.target_table,
        "reference_date": reference_date,
        "inactive_cutoff_date": cutoff_date,
        "inactive_days": config.inactive_days,
        "merge_counts": counts,
        "total_customers": written.count(),
        "active_customers": active_count,
        "inactive_customers": written.count() - active_count,
    }
