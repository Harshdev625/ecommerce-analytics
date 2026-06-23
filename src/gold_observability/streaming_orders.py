"""Streaming orders table as alternative to batch bronze ingestion."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@dataclass
class StreamingOrdersConfig:
    source_path: str = "/Volumes/globalmart/bronze/raw_landing/olist_orders_dataset.csv"
    checkpoint: str = "/Volumes/globalmart/metadata/checkpoints/orders_stream"
    target_table: str = "globalmart.gold.orders_stream"
    bronze_orders: str = "globalmart.bronze.orders"


STREAM_ORDER_COLUMNS = (
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
)

ORDERS_SOURCE_FILE = "olist_orders_dataset.csv"
SCHEMA_EVOLUTION_SAMPLE_SIZE = 500


def _bronze_base_row_count(spark: SparkSession, bronze_table: str) -> int:
    """Rows from the original landing CSV (excludes schema-evolution append)."""
    bronze = spark.table(bronze_table)
    if "_source_file_name" in bronze.columns:
        return bronze.filter(F.col("_source_file_name") == F.lit(ORDERS_SOURCE_FILE)).count()
    if "order_channel" in bronze.columns:
        return bronze.filter(F.col("order_channel").isNull()).count()
    return bronze.count()


def ensure_orders_stream_table(spark: SparkSession, config: StreamingOrdersConfig | None = None) -> None:
    config = config or StreamingOrdersConfig()
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {config.target_table} (
          order_id STRING,
          customer_id STRING,
          order_status STRING,
          order_purchase_timestamp STRING,
          _stream_ingested_at TIMESTAMP
        )
        USING DELTA
        """
    )


def run_orders_stream_once(spark: SparkSession, config: StreamingOrdersConfig | None = None) -> dict:
    """One micro-batch read from landing CSV into the streaming target table."""
    config = config or StreamingOrdersConfig()
    ensure_orders_stream_table(spark, config)

    batch = (
        spark.read.format("csv")
        .option("header", True)
        .load(config.source_path)
        .select(*STREAM_ORDER_COLUMNS)
        .withColumn("_stream_ingested_at", F.current_timestamp())
    )
    (
        batch.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(config.target_table)
    )

    stream_count = spark.table(config.target_table).count()
    bronze_total = spark.table(config.bronze_orders).count()
    bronze_base = _bronze_base_row_count(spark, config.bronze_orders)
    evolved_delta = bronze_total - bronze_base
    return {
        "task": "orders_stream",
        "target_table": config.target_table,
        "source_path": config.source_path,
        "stream_row_count": stream_count,
        "bronze_orders_row_count": bronze_total,
        "bronze_base_row_count": bronze_base,
        "bronze_evolved_row_delta": evolved_delta,
        "counts_match": stream_count == bronze_base,
        "note": (
            "bronze.orders includes 500 schema-evolution demo rows; "
            "stream load compares to the original landing CSV only."
        )
        if evolved_delta == SCHEMA_EVOLUTION_SAMPLE_SIZE
        else None,
    }
