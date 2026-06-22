"""Watermark-based incremental bronze → silver loader."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from src.transformations.silver_orders import SilverOrdersConfig, build_silver_orders

WATERMARK_TABLE = "globalmart.metadata.pipeline_watermarks"

WATERMARK_SCHEMA = StructType(
    [
        StructField("pipeline_id", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("watermark_ts", TimestampType(), True),
        StructField("last_run_at", TimestampType(), False),
        StructField("records_processed", LongType(), True),
        StructField("run_id", StringType(), False),
    ]
)


@dataclass
class IncrementalLoadConfig:
    source_table: str = "globalmart.bronze.orders"
    target_table: str = "globalmart.silver.orders_incremental"
    watermark_table: str = WATERMARK_TABLE
    pipeline_id: str = "bronze_orders_to_silver_incremental"
    timestamp_col: str = "_ingested_at"
    key_column: str = "order_id"
    lookback_hours: int = 24


def ensure_watermark_table(spark: SparkSession, watermark_table: str = WATERMARK_TABLE) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {watermark_table} (
          pipeline_id STRING,
          source_table STRING,
          target_table STRING,
          watermark_ts TIMESTAMP,
          last_run_at TIMESTAMP,
          records_processed BIGINT,
          run_id STRING
        )
        USING DELTA
        COMMENT 'Per-pipeline high-water marks for incremental loads'
        """
    )


def get_watermark(spark: SparkSession, config: IncrementalLoadConfig) -> datetime | None:
    ensure_watermark_table(spark, config.watermark_table)
    rows = (
        spark.table(config.watermark_table)
        .filter(F.col("pipeline_id") == config.pipeline_id)
        .orderBy(F.col("last_run_at").desc())
        .limit(1)
        .collect()
    )
    if not rows:
        return None
    return rows[0]["watermark_ts"]


def fetch_incremental_batch(
    spark: SparkSession,
    config: IncrementalLoadConfig,
    watermark_ts: datetime | None,
) -> tuple[DataFrame, datetime | None]:
    source = spark.table(config.source_table)
    if watermark_ts is None:
        return source, None

    lookback_start_unix = F.unix_timestamp(F.lit(watermark_ts)) - (config.lookback_hours * 3600)
    batch = source.filter(F.unix_timestamp(F.col(config.timestamp_col)) > lookback_start_unix)
    return batch, watermark_ts


def _merge_columns(source: DataFrame, exclude: set[str]) -> list[str]:
    return [c for c in source.columns if c not in exclude]


def apply_incremental_merge(
    spark: SparkSession,
    silver_batch: DataFrame,
    config: IncrementalLoadConfig,
) -> None:
    from delta.tables import DeltaTable

    merge_cols = _merge_columns(silver_batch, {config.key_column})
    update_set = {c: f"s.{c}" for c in merge_cols}
    update_set["processed_at"] = "current_timestamp()"

    insert_values = {config.key_column: f"s.{config.key_column}"}
    insert_values.update({c: f"s.{c}" for c in merge_cols})
    insert_values["processed_at"] = "current_timestamp()"

    if not spark.catalog.tableExists(config.target_table):
        silver_batch.write.format("delta").mode("overwrite").saveAsTable(config.target_table)
        return

    DeltaTable.forName(spark, config.target_table).alias("t").merge(
        silver_batch.alias("s"),
        f"t.{config.key_column} = s.{config.key_column}",
    ).whenMatchedUpdate(set=update_set).whenNotMatchedInsert(values=insert_values).execute()


def write_watermark(
    spark: SparkSession,
    config: IncrementalLoadConfig,
    watermark_ts: datetime | None,
    records_processed: int,
    run_id: str,
) -> None:
    ensure_watermark_table(spark, config.watermark_table)
    row = spark.createDataFrame(
        [
            {
                "pipeline_id": config.pipeline_id,
                "source_table": config.source_table,
                "target_table": config.target_table,
                "watermark_ts": watermark_ts,
                "last_run_at": datetime.now(timezone.utc),
                "records_processed": records_processed,
                "run_id": run_id,
            }
        ],
        schema=WATERMARK_SCHEMA,
    )
    row.write.format("delta").mode("append").saveAsTable(config.watermark_table)


def _advance_watermark(
    batch: DataFrame,
    config: IncrementalLoadConfig,
    current: datetime | None,
) -> datetime | None:
    batch_max = batch.agg(F.max(config.timestamp_col).alias("max_ts")).collect()[0]["max_ts"]
    if batch_max is None:
        return current
    if current is None or batch_max > current:
        return batch_max
    return current


def run_incremental_load(
    spark: SparkSession,
    config: IncrementalLoadConfig | None = None,
) -> dict:
    config = config or IncrementalLoadConfig()
    run_id = str(uuid.uuid4())
    previous_watermark = get_watermark(spark, config)

    batch, _ = fetch_incremental_batch(spark, config, previous_watermark)
    records_in_batch = batch.count()

    if records_in_batch == 0:
        return {
            "task": "gold_incremental_loader",
            "pipeline_id": config.pipeline_id,
            "source_table": config.source_table,
            "target_table": config.target_table,
            "run_id": run_id,
            "lookback_hours": config.lookback_hours,
            "previous_watermark": str(previous_watermark) if previous_watermark else None,
            "new_watermark": str(previous_watermark) if previous_watermark else None,
            "records_in_batch": 0,
            "records_new_since_watermark": 0,
            "target_row_count": (
                spark.table(config.target_table).count()
                if spark.catalog.tableExists(config.target_table)
                else 0
            ),
            "watermark_advanced": False,
        }

    if previous_watermark is not None:
        records_new = batch.filter(F.col(config.timestamp_col) > F.lit(previous_watermark)).count()
    else:
        records_new = records_in_batch

    silver_batch = build_silver_orders(
        spark,
        SilverOrdersConfig(bronze_table=config.source_table, silver_table=config.target_table),
        source_df=batch,
    )
    apply_incremental_merge(spark, silver_batch, config)

    new_watermark = _advance_watermark(batch, config, previous_watermark)
    write_watermark(spark, config, new_watermark, records_in_batch, run_id)

    target_count = spark.table(config.target_table).count()

    return {
        "task": "gold_incremental_loader",
        "pipeline_id": config.pipeline_id,
        "source_table": config.source_table,
        "target_table": config.target_table,
        "run_id": run_id,
        "lookback_hours": config.lookback_hours,
        "previous_watermark": str(previous_watermark) if previous_watermark else None,
        "new_watermark": str(new_watermark) if new_watermark else None,
        "records_in_batch": records_in_batch,
        "records_new_since_watermark": records_new,
        "target_row_count": target_count,
        "watermark_advanced": (
            previous_watermark is None
            or (new_watermark is not None and new_watermark != previous_watermark)
        ),
    }
