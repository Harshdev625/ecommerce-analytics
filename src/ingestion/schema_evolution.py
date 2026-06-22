"""Schema evolution simulation and validation for bronze tables."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

VIOLATION_LOG_TABLE = "globalmart.metadata.schema_violation_log"

METADATA_COLS = (
    "_source_file_name",
    "_source_path",
    "_ingestion_run_id",
    "_ingested_at",
)

VIOLATION_LOG_SCHEMA = StructType(
    [
        StructField("violation_id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("violation_type", StringType(), False),
        StructField("column_name", StringType(), True),
        StructField("expected_type", StringType(), True),
        StructField("actual_type", StringType(), True),
        StructField("logged_at", TimestampType(), False),
    ]
)

ORDERS_BASE_CONTRACT: dict[str, str] = {
    "order_id": "string",
    "customer_id": "string",
    "order_status": "string",
    "order_purchase_timestamp": "string",
    "order_approved_at": "string",
    "order_delivered_carrier_date": "string",
    "order_delivered_customer_date": "string",
    "order_estimated_delivery_date": "string",
}


@dataclass
class SchemaEvolutionConfig:
    orders_table: str = "globalmart.bronze.orders"
    evolved_csv_dir: str = "/Volumes/globalmart/bronze/raw_landing/orders_evolved"
    violation_log_table: str = VIOLATION_LOG_TABLE
    new_columns: tuple[str, str] = ("order_channel", "customer_segment")
    sample_size: int = 500


def ensure_violation_log_table(spark: SparkSession, table: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          violation_id STRING,
          table_name STRING,
          violation_type STRING,
          column_name STRING,
          expected_type STRING,
          actual_type STRING,
          logged_at TIMESTAMP
        )
        USING DELTA
        COMMENT 'Schema contract violations detected during pipeline runs'
        """
    )


def _normalize_type(dtype: str) -> str:
    return dtype.lower().replace("type", "").strip()


def validate_schema(
    df: DataFrame,
    expected_contract: dict[str, str],
    *,
    table_name: str,
) -> list[dict[str, Any]]:
    """Detect missing columns, extra columns, and type mismatches."""
    actual = {f.name: _normalize_type(f.dataType.simpleString()) for f in df.schema.fields}
    expected_cols = set(expected_contract)
    actual_cols = set(actual)
    violations: list[dict[str, Any]] = []

    for col in sorted(expected_cols - actual_cols):
        violations.append(
            {
                "table_name": table_name,
                "violation_type": "MISSING_COLUMN",
                "column_name": col,
                "expected_type": expected_contract[col],
                "actual_type": None,
            }
        )

    for col in sorted(actual_cols - expected_cols):
        violations.append(
            {
                "table_name": table_name,
                "violation_type": "EXTRA_COLUMN",
                "column_name": col,
                "expected_type": None,
                "actual_type": actual[col],
            }
        )

    for col in sorted(expected_cols & actual_cols):
        exp = _normalize_type(expected_contract[col])
        act = actual[col]
        if exp != act and exp not in act and act not in exp:
            violations.append(
                {
                    "table_name": table_name,
                    "violation_type": "TYPE_MISMATCH",
                    "column_name": col,
                    "expected_type": exp,
                    "actual_type": act,
                }
            )

    return violations


def log_schema_violations(
    spark: SparkSession,
    violations: list[dict[str, Any]],
    table: str = VIOLATION_LOG_TABLE,
) -> int:
    ensure_violation_log_table(spark, table)
    if not violations:
        return 0

    logged_at = datetime.now(timezone.utc)
    rows = [
        {
            "violation_id": str(uuid.uuid4()),
            "logged_at": logged_at,
            **v,
        }
        for v in violations
    ]
    spark.createDataFrame(rows, schema=VIOLATION_LOG_SCHEMA).write.format("delta").mode(
        "append"
    ).saveAsTable(table)
    return len(rows)


def build_evolved_sample_df(
    spark: SparkSession,
    config: SchemaEvolutionConfig,
) -> DataFrame:
    """Sample orders with two new categorical columns simulating a source schema change."""
    channels = ["web", "mobile", "marketplace"]
    segments = ["retail", "wholesale", "premium"]

    base = spark.table(config.orders_table).limit(config.sample_size)
    drop_cols = [c for c in METADATA_COLS if c in base.columns]
    base = base.drop(*drop_cols) if drop_cols else base

    return (
        base.withColumn(
            config.new_columns[0],
            F.element_at(
                F.array(*[F.lit(c) for c in channels]),
                (F.abs(F.hash(F.col("order_id"))) % 3) + 1,
            ),
        )
        .withColumn(
            config.new_columns[1],
            F.element_at(
                F.array(*[F.lit(s) for s in segments]),
                (F.abs(F.hash(F.col("customer_id"))) % 3) + 1,
            ),
        )
    )


def ingest_evolved_sample(
    spark: SparkSession,
    evolved_df: DataFrame,
    config: SchemaEvolutionConfig,
) -> int:
    """Append evolved rows with Delta schema evolution enabled."""
    count = evolved_df.count()
    (
        evolved_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(config.orders_table)
    )
    return count


def build_evolution_report(
    spark: SparkSession,
    config: SchemaEvolutionConfig,
    rows_appended: int,
) -> dict:
    orders = spark.table(config.orders_table)
    ch, seg = config.new_columns

    return {
        "orders_table": config.orders_table,
        "new_columns": list(config.new_columns),
        "rows_appended_with_new_schema": rows_appended,
        "total_rows_after_evolution": orders.count(),
        "rows_with_null_new_columns": orders.filter(F.col(ch).isNull()).count(),
        "rows_with_populated_new_columns": orders.filter(F.col(ch).isNotNull()).count(),
    }
