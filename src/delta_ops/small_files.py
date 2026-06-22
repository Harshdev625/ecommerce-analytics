"""Small-file fragmentation and OPTIMIZE (Task 8.1)."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession

from src.delta_ops.table_stats import describe_detail_summary


@dataclass
class SmallFilesConfig:
    source_table: str = "globalmart.gold.fact_sales"
    target_table: str = "globalmart.gold.fact_sales_fragmented"
    num_partitions: int = 100


def create_fragmented_fact_copy(
    spark: SparkSession,
    config: SmallFilesConfig | None = None,
) -> int:
    """Write fact_sales spread across many partitions to simulate small files."""
    config = config or SmallFilesConfig()
    (
        spark.table(config.source_table)
        .repartition(config.num_partitions)
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(config.target_table)
    )
    return spark.table(config.target_table).count()


def optimize_table(spark: SparkSession, table: str) -> None:
    spark.sql(f"OPTIMIZE {table}")


def run_small_files_optimize(
    spark: SparkSession,
    config: SmallFilesConfig | None = None,
) -> dict:
    config = config or SmallFilesConfig()
    row_count = create_fragmented_fact_copy(spark, config)
    before = describe_detail_summary(spark, config.target_table)
    optimize_table(spark, config.target_table)
    after = describe_detail_summary(spark, config.target_table)

    return {
        "task": "small_files_optimize",
        "source_table": config.source_table,
        "target_table": config.target_table,
        "num_partitions_written": config.num_partitions,
        "row_count": row_count,
        "describe_detail_before": before,
        "describe_detail_after": after,
        "files_reduced_by": before["num_files"] - after["num_files"],
        "avg_file_size_increase_bytes": after["avg_file_size_bytes"] - before["avg_file_size_bytes"],
    }
