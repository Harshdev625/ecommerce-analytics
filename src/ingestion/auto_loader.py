"""Databricks Auto Loader ingestion helpers."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@dataclass
class AutoLoaderConfig:
    source_path: str = "/Volumes/globalmart/bronze/orders_autoloader_in"
    checkpoint_path: str = "/Volumes/globalmart/metadata/checkpoints/orders_autoloader"
    schema_path: str = "/Volumes/globalmart/metadata/checkpoints/orders_autoloader/_schema"
    target_table: str = "globalmart.bronze.orders_autoloader"
    source_orders_file: str = (
        "/Volumes/globalmart/bronze/raw_landing/olist_orders_dataset.csv"
    )


def stage_orders_file(dbutils, config: AutoLoaderConfig) -> None:
    """Copy orders CSV into the Auto Loader input folder (idempotent staging)."""
    dbutils.fs.mkdirs(config.source_path)
    target = f"{config.source_path}/olist_orders_dataset.csv"
    if not dbutils.fs.cp(config.source_orders_file, target, True):
        raise RuntimeError(f"Failed to copy orders file to {target}")


def run_orders_autoloader(spark: SparkSession, config: AutoLoaderConfig | None = None) -> dict:
    """
    Run Auto Loader once (availableNow trigger).
    Returns row count before/after for idempotency comparison.
    """
    config = config or AutoLoaderConfig()
    before = 0
    if spark.catalog.tableExists(config.target_table):
        before = spark.table(config.target_table).count()

    stream_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", config.schema_path)
        .load(config.source_path)
        .withColumn("_ingested_via", F.lit("autoloader"))
    )

    query = (
        stream_df.writeStream.format("delta")
        .option("checkpointLocation", config.checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .table(config.target_table)
    )
    query.awaitTermination()

    after = spark.table(config.target_table).count()
    return {
        "target_table": config.target_table,
        "rows_before": before,
        "rows_after": after,
        "rows_added": after - before,
    }
