"""Delta time travel: modify, query old version, restore."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@dataclass
class TimeTravelConfig:
    source_table: str = "globalmart.gold.fact_sales"
    target_table: str = "globalmart.gold.fact_sales_timeline_demo"


def _total_revenue(spark: SparkSession, table: str, version: int | None = None) -> float:
    if version is None:
        df = spark.table(table)
    else:
        df = spark.sql(f"SELECT * FROM {table} VERSION AS OF {version}")
    value = df.agg(F.round(F.sum("total_amount"), 2).alias("total")).collect()[0]["total"]
    return float(value or 0)


def prepare_timeline_demo_table(
    spark: SparkSession,
    config: TimeTravelConfig | None = None,
) -> int:
    config = config or TimeTravelConfig()
    (
        spark.table(config.source_table)
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(config.target_table)
    )
    return spark.table(config.target_table).count()


def run_time_travel_demo(
    spark: SparkSession,
    config: TimeTravelConfig | None = None,
) -> dict:
    config = config or TimeTravelConfig()
    prepare_timeline_demo_table(spark, config)

    sample_item_id = (
        spark.table(config.target_table)
        .agg(F.min("order_item_id").alias("id"))
        .collect()[0]["id"]
    )

    history = spark.sql(f"DESCRIBE HISTORY {config.target_table} LIMIT 5").collect()
    baseline_version = history[0]["version"]
    baseline_revenue = _total_revenue(spark, config.target_table, baseline_version)

    spark.sql(
        f"""
        UPDATE {config.target_table}
        SET total_amount = total_amount + 100.0
        WHERE order_item_id = {sample_item_id}
        """
    )
    after_modify_revenue = _total_revenue(spark, config.target_table)

    from_version_revenue = _total_revenue(spark, config.target_table, baseline_version)

    spark.sql(
        f"RESTORE TABLE {config.target_table} TO VERSION AS OF {baseline_version}"
    )
    after_restore_revenue = _total_revenue(spark, config.target_table)

    return {
        "task": "time_travel_demo",
        "target_table": config.target_table,
        "baseline_version": int(baseline_version),
        "modified_order_item_id": int(sample_item_id),
        "revenue_baseline": baseline_revenue,
        "revenue_after_modification": after_modify_revenue,
        "revenue_from_old_version_query": from_version_revenue,
        "revenue_after_restore": after_restore_revenue,
        "restore_matches_baseline": after_restore_revenue == baseline_revenue,
    }
