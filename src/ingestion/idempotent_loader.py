"""Idempotent batch ingestion from a Unity Catalog volume landing zone."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

METADATA_TABLE = "globalmart.metadata.file_ingestion_log"

INGESTION_LOG_SCHEMA = StructType(
    [
        StructField("log_id", StringType(), False),
        StructField("ingestion_run_id", StringType(), False),
        StructField("source_file_name", StringType(), False),
        StructField("source_path", StringType(), False),
        StructField("file_fingerprint", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("records_ingested", LongType(), True),
        StructField("column_count", LongType(), True),
        StructField("status", StringType(), False),
        StructField("message", StringType(), True),
        StructField("ingested_at", TimestampType(), False),
    ]
)

# Required Olist source files → bronze Delta tables
FILE_TABLE_MAP: dict[str, str] = {
    "olist_orders_dataset.csv": "globalmart.bronze.orders",
    "olist_order_items_dataset.csv": "globalmart.bronze.order_items",
    "olist_order_payments_dataset.csv": "globalmart.bronze.order_payments",
    "olist_order_reviews_dataset.csv": "globalmart.bronze.order_reviews",
    "olist_products_dataset.csv": "globalmart.bronze.products",
    "olist_sellers_dataset.csv": "globalmart.bronze.sellers",
    "olist_customers_dataset.csv": "globalmart.bronze.customers",
    "product_category_name_translation.csv": "globalmart.bronze.product_category_translation",
}


@dataclass
class IngestionConfig:
    landing_path: str = "/Volumes/globalmart/bronze/raw_landing"
    metadata_table: str = METADATA_TABLE
    file_table_map: dict[str, str] = field(default_factory=lambda: FILE_TABLE_MAP.copy())


@dataclass
class IngestionResult:
    source_file_name: str
    target_table: str
    status: str
    records_ingested: int
    column_count: int
    message: str


def ensure_metadata_table(spark: SparkSession, metadata_table: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {metadata_table} (
          log_id STRING,
          ingestion_run_id STRING,
          source_file_name STRING,
          source_path STRING,
          file_fingerprint STRING,
          target_table STRING,
          records_ingested BIGINT,
          column_count BIGINT,
          status STRING,
          message STRING,
          ingested_at TIMESTAMP
        )
        USING DELTA
        COMMENT 'Tracks batch file ingestion for idempotent bronze loads'
        """
    )


def _file_fingerprint(path: str, size: int, modification_time: int) -> str:
    return f"{path}|{size}|{modification_time}"


def _list_landing_files(dbutils, landing_path: str) -> list[dict]:
    entries = dbutils.fs.ls(landing_path)
    return [
        {
            "name": entry.name,
            "path": entry.path,
            "size": entry.size,
            "modification_time": entry.modificationTime,
        }
        for entry in entries
        if entry.name.lower().endswith(".csv")
    ]


def _already_ingested(spark: SparkSession, metadata_table: str, fingerprint: str) -> bool:
    if not spark.catalog.tableExists(metadata_table):
        return False
    row = (
        spark.table(metadata_table)
        .filter(
            (F.col("file_fingerprint") == fingerprint)
            & (F.col("status") == F.lit("INGESTED"))
        )
        .limit(1)
        .collect()
    )
    return len(row) > 0


def _append_log(
    spark: SparkSession,
    metadata_table: str,
    rows: Iterable[dict],
) -> None:
    if not rows:
        return
    df = spark.createDataFrame(list(rows), schema=INGESTION_LOG_SCHEMA)
    df.write.format("delta").mode("append").saveAsTable(metadata_table)


def _enrich_with_metadata(
    df: DataFrame,
    *,
    source_file_name: str,
    source_path: str,
    ingestion_run_id: str,
    ingested_at: datetime,
) -> DataFrame:
    return (
        df.withColumn("_source_file_name", F.lit(source_file_name))
        .withColumn("_source_path", F.lit(source_path))
        .withColumn("_ingestion_run_id", F.lit(ingestion_run_id))
        .withColumn("_ingested_at", F.lit(ingested_at).cast(TimestampType()))
    )


def ingest_file(
    spark: SparkSession,
    dbutils,
    *,
    file_info: dict,
    target_table: str,
    config: IngestionConfig,
    ingestion_run_id: str,
) -> IngestionResult:
    name = file_info["name"]
    path = file_info["path"]
    fingerprint = _file_fingerprint(path, file_info["size"], file_info["modification_time"])
    ingested_at = datetime.now(timezone.utc)

    if _already_ingested(spark, config.metadata_table, fingerprint):
        _append_log(
            spark,
            config.metadata_table,
            [
                {
                    "log_id": str(uuid.uuid4()),
                    "ingestion_run_id": ingestion_run_id,
                    "source_file_name": name,
                    "source_path": path,
                    "file_fingerprint": fingerprint,
                    "target_table": target_table,
                    "records_ingested": 0,
                    "column_count": None,
                    "status": "SKIPPED",
                    "message": "File fingerprint already ingested",
                    "ingested_at": ingested_at,
                }
            ],
        )
        return IngestionResult(
            source_file_name=name,
            target_table=target_table,
            status="SKIPPED",
            records_ingested=0,
            column_count=0,
            message="Already ingested — skipped",
        )

    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("encoding", "UTF-8")
        .csv(path)
    )
    record_count = df.count()
    column_count = len(df.columns)
    enriched = _enrich_with_metadata(
        df,
        source_file_name=name,
        source_path=path,
        ingestion_run_id=ingestion_run_id,
        ingested_at=ingested_at,
    )
    enriched.write.format("delta").mode("append").saveAsTable(target_table)

    _append_log(
        spark,
        config.metadata_table,
        [
            {
                "log_id": str(uuid.uuid4()),
                "ingestion_run_id": ingestion_run_id,
                "source_file_name": name,
                "source_path": path,
                "file_fingerprint": fingerprint,
                "target_table": target_table,
                "records_ingested": record_count,
                "column_count": column_count,
                "status": "INGESTED",
                "message": "Successfully ingested",
                "ingested_at": ingested_at,
            }
        ],
    )
    return IngestionResult(
        source_file_name=name,
        target_table=target_table,
        status="INGESTED",
        records_ingested=record_count,
        column_count=column_count,
        message="Successfully ingested",
    )


def ingest_landing_zone(
    spark: SparkSession,
    dbutils,
    config: IngestionConfig | None = None,
) -> tuple[str, list[IngestionResult]]:
    config = config or IngestionConfig()
    ensure_metadata_table(spark, config.metadata_table)
    run_id = str(uuid.uuid4())
    results: list[IngestionResult] = []

    for file_info in _list_landing_files(dbutils, config.landing_path):
        name = file_info["name"]
        if name not in config.file_table_map:
            results.append(
                IngestionResult(
                    source_file_name=name,
                    target_table="—",
                    status="SKIPPED",
                    records_ingested=0,
                    column_count=0,
                    message="Not in ingestion manifest (optional file)",
                )
            )
            continue

        target = config.file_table_map[name]
        results.append(
            ingest_file(
                spark,
                dbutils,
                file_info=file_info,
                target_table=target,
                config=config,
                ingestion_run_id=run_id,
            )
        )

    return run_id, results


def build_ingestion_summary(spark: SparkSession, config: IngestionConfig | None = None) -> DataFrame:
    config = config or IngestionConfig()
    rows = []
    for file_name, table in config.file_table_map.items():
        if spark.catalog.tableExists(table):
            tbl = spark.table(table)
            rows.append(
                (
                    file_name,
                    table,
                    tbl.count(),
                    len(tbl.columns),
                    "LOADED",
                )
            )
        else:
            rows.append((file_name, table, 0, 0, "NOT_LOADED"))

    return spark.createDataFrame(
        rows,
        ["source_file", "bronze_table", "record_count", "column_count", "status"],
    )


def build_run_report(
    spark: SparkSession,
    *,
    config: IngestionConfig,
    first_run_id: str,
    first_results: list[IngestionResult],
    second_run_id: str,
    second_results: list[IngestionResult],
) -> dict:
    """Compact JSON-serializable report for local sync or copy-paste."""
    summary_rows = build_ingestion_summary(spark, config).collect()
    top_records = max(summary_rows, key=lambda r: r.record_count)
    top_columns = max(summary_rows, key=lambda r: r.column_count)

    return {
        "landing_path": config.landing_path,
        "metadata_table": config.metadata_table,
        "first_run_id": first_run_id,
        "first_run": [r.__dict__ for r in first_results],
        "second_run_id": second_run_id,
        "second_run": [r.__dict__ for r in second_results],
        "idempotency": {
            "files_skipped": sum(1 for r in second_results if r.status == "SKIPPED"),
            "new_records_on_second_run": sum(r.records_ingested for r in second_results),
        },
        "summary": [row.asDict() for row in summary_rows],
        "highlights": {
            "most_records_table": top_records.bronze_table,
            "most_records_count": top_records.record_count,
            "most_columns_table": top_columns.bronze_table,
            "most_columns_count": top_columns.column_count,
        },
    }


def _workspace_path(path: str) -> str:
    """Databricks Repos imports use /Repos/...; file I/O needs /Workspace/Repos/..."""
    if path.startswith("/Repos/"):
        return f"/Workspace{path}"
    return path


def write_run_report(report: dict, output_path: str, dbutils=None) -> str:
    """Write report JSON to repo path, Databricks Workspace, or local disk."""
    import json
    from pathlib import Path

    payload = json.dumps(report, indent=2)
    resolved = _workspace_path(output_path)

    if dbutils is not None:
        parent = "/".join(resolved.split("/")[:-1])
        dbutils.fs.mkdirs(f"file:{parent}")
        dbutils.fs.put(f"file:{resolved}", payload, True)
        return resolved

    path = Path(resolved)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")
    return str(path)
