"""Liquid auto clustering comparison."""

from __future__ import annotations

import time
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.delta_ops.table_stats import describe_detail_summary


@dataclass
class LiquidClusterConfig:
    source_table: str = "globalmart.gold.fact_sales"
    liquid_table: str = "globalmart.gold.fact_sales_liquid_cluster"
    partitioned_table: str = "globalmart.gold.fact_sales_partitioned"
    cluster_columns: tuple[str, ...] = ("date_key", "product_sk")
    filter_date_key: int = 20180315
    append_fraction: float = 0.05


def create_liquid_cluster_table(
    spark: SparkSession,
    config: LiquidClusterConfig | None = None,
) -> None:
    config = config or LiquidClusterConfig()
    cols = ", ".join(config.cluster_columns)
    spark.sql(f"DROP TABLE IF EXISTS {config.liquid_table}")
    spark.sql(
        f"""
        CREATE TABLE {config.liquid_table}
        USING DELTA
        CLUSTER BY ({cols})
        AS SELECT * FROM {config.source_table}
        """
    )


def append_sample_rows(
    spark: SparkSession,
    config: LiquidClusterConfig | None = None,
) -> int:
    config = config or LiquidClusterConfig()
    source = spark.table(config.source_table)
    sample_count = max(1, int(source.count() * config.append_fraction))
    (
        source.orderBy(F.rand()).limit(sample_count)
        .write.format("delta")
        .mode("append")
        .saveAsTable(config.liquid_table)
    )
    return sample_count


def _timed_filter_sum(spark: SparkSession, table: str, date_key: int) -> tuple[float, float]:
    start = time.perf_counter()
    revenue = (
        spark.table(table)
        .filter(F.col("date_key") == date_key)
        .agg(F.round(F.sum("total_amount"), 2).alias("total"))
        .collect()[0]["total"]
    )
    elapsed_ms = (time.perf_counter() - start) * 1000
    return float(revenue or 0), round(elapsed_ms, 2)


def run_liquid_cluster_comparison(
    spark: SparkSession,
    config: LiquidClusterConfig | None = None,
) -> dict:
    config = config or LiquidClusterConfig()
    create_liquid_cluster_table(spark, config)
    detail_before_append = describe_detail_summary(spark, config.liquid_table)

    partitioned_exists = spark.catalog.tableExists(config.partitioned_table)
    liquid_rev, liquid_ms = _timed_filter_sum(spark, config.liquid_table, config.filter_date_key)
    part_rev, part_ms = (None, None)
    if partitioned_exists:
        part_rev, part_ms = _timed_filter_sum(
            spark, config.partitioned_table, config.filter_date_key
        )

    appended = append_sample_rows(spark, config)
    spark.sql(f"OPTIMIZE {config.liquid_table}")
    detail_after_append = describe_detail_summary(spark, config.liquid_table)

    return {
        "task": "liquid_cluster_comparison",
        "liquid_table": config.liquid_table,
        "cluster_columns": list(config.cluster_columns),
        "partitioned_table_compared": config.partitioned_table if partitioned_exists else None,
        "filter_date_key": config.filter_date_key,
        "liquid_cluster_revenue_ms": {"revenue": liquid_rev, "elapsed_ms": liquid_ms},
        "partitioned_table_revenue_ms": (
            {"revenue": part_rev, "elapsed_ms": part_ms} if partitioned_exists else None
        ),
        "rows_appended": appended,
        "describe_detail_before_append": detail_before_append,
        "describe_detail_after_append_and_optimize": detail_after_append,
        "note": "Run notebook 02 first for partitioned_table comparison; timing varies by cluster.",
    }
