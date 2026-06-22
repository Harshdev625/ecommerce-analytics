"""VACUUM dry-run and execution."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession


@dataclass
class VacuumConfig:
    target_table: str = "globalmart.gold.fact_sales_fragmented"
    retain_hours: int = 168


def get_table_history(spark: SparkSession, table: str, limit: int = 10) -> list[dict]:
    return [
        row.asDict()
        for row in spark.sql(f"DESCRIBE HISTORY {table} LIMIT {limit}").collect()
    ]


def vacuum_dry_run(spark: SparkSession, config: VacuumConfig | None = None) -> list[str]:
    config = config or VacuumConfig()
    df = spark.sql(
        f"VACUUM {config.target_table} RETAIN {config.retain_hours} HOURS DRY RUN"
    )
    return [row[0] for row in df.collect()]


def vacuum_execute(spark: SparkSession, config: VacuumConfig | None = None) -> None:
    config = config or VacuumConfig()
    spark.sql(f"VACUUM {config.target_table} RETAIN {config.retain_hours} HOURS")


def run_vacuum_demo(
    spark: SparkSession,
    config: VacuumConfig | None = None,
) -> dict:
    config = config or VacuumConfig()
    history_before = get_table_history(spark, config.target_table)
    dry_run_files = vacuum_dry_run(spark, config)
    vacuum_execute(spark, config)
    history_after = get_table_history(spark, config.target_table)

    return {
        "task": "vacuum_operations",
        "target_table": config.target_table,
        "retain_hours": config.retain_hours,
        "history_before_vacuum": history_before,
        "dry_run_files_to_delete_count": len(dry_run_files),
        "dry_run_sample_paths": dry_run_files[:5],
        "history_after_vacuum": history_after,
    }
