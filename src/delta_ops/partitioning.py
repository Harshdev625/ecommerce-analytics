"""Partitioning, Z-ORDER, and partition-pruning checks (Task 8.2)."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.delta_ops.table_stats import describe_detail_summary


@dataclass
class PartitionStrategyConfig:
    source_table: str = "globalmart.gold.fact_sales"
    dim_date: str = "globalmart.gold.dim_date"
    partitioned_table: str = "globalmart.gold.fact_sales_partitioned"
    partition_column: str = "order_year_month"
    zorder_columns: tuple[str, ...] = ("date_key", "product_sk", "seller_sk")
    prune_filter_year_month: str = "201803"


def enrich_fact_with_date(spark: SparkSession, config: PartitionStrategyConfig) -> DataFrame:
    fact = spark.table(config.source_table)
    dim_date = spark.table(config.dim_date).select("date_key", "year_month", "year")
    return fact.join(dim_date, "date_key", "inner").withColumnRenamed(
        "year_month", config.partition_column
    )


def analyze_partition_candidates(
    spark: SparkSession,
    config: PartitionStrategyConfig | None = None,
) -> dict:
    config = config or PartitionStrategyConfig()
    enriched = enrich_fact_with_date(spark, config)
    return {
        "year": enriched.select("year").distinct().count(),
        "order_year_month": enriched.select(config.partition_column).distinct().count(),
        "date_key": enriched.select("date_key").distinct().count(),
        "justification": (
            f"Partition on `{config.partition_column}` (~40 month buckets): "
            "enough for pruning without hundreds of tiny partitions. "
            "Year alone is too coarse; date_key is too granular for directory layout."
        ),
    }


def build_partitioned_fact_table(
    spark: SparkSession,
    config: PartitionStrategyConfig | None = None,
) -> int:
    config = config or PartitionStrategyConfig()
    enriched = enrich_fact_with_date(spark, config)
    (
        enriched.write.format("delta")
        .mode("overwrite")
        .partitionBy(config.partition_column)
        .saveAsTable(config.partitioned_table)
    )
    return spark.table(config.partitioned_table).count()


def optimize_zorder(
    spark: SparkSession,
    table: str,
    columns: tuple[str, ...],
) -> None:
    cols = ", ".join(columns)
    spark.sql(f"OPTIMIZE {table} ZORDER BY ({cols})")


def partition_prune_explain(
    spark: SparkSession,
    config: PartitionStrategyConfig | None = None,
) -> str:
    config = config or PartitionStrategyConfig()
    df = (
        spark.table(config.partitioned_table)
        .filter(F.col(config.partition_column) == config.prune_filter_year_month)
        .groupBy(config.partition_column)
        .agg(F.round(F.sum("total_amount"), 2).alias("revenue"))
    )
    return df._jdf.queryExecution().toString()


def run_partition_zorder_strategy(
    spark: SparkSession,
    config: PartitionStrategyConfig | None = None,
) -> dict:
    config = config or PartitionStrategyConfig()
    cardinality = analyze_partition_candidates(spark, config)
    row_count = build_partitioned_fact_table(spark, config)
    before_z = describe_detail_summary(spark, config.partitioned_table)
    optimize_zorder(spark, config.partitioned_table, config.zorder_columns)
    after_z = describe_detail_summary(spark, config.partitioned_table)
    plan = partition_prune_explain(spark, config)

    return {
        "task": "partition_zorder_strategy",
        "partitioned_table": config.partitioned_table,
        "partition_column": config.partition_column,
        "zorder_columns": list(config.zorder_columns),
        "zorder_justification": (
            "date_key, product_sk, and seller_sk are high-cardinality join/filter keys "
            "unsuitable as partition columns but benefit from Z-ORDER colocation."
        ),
        "cardinality_analysis": cardinality,
        "row_count": row_count,
        "describe_detail_before_zorder": before_z,
        "describe_detail_after_zorder": after_z,
        "partition_prune_filter": config.prune_filter_year_month,
        "partition_prune_explain_snippet": plan[:2000],
    }
