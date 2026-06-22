"""Dead letter queue for records that fail quality checks."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

DLQ_TABLE = "globalmart.metadata.dead_letter_queue"

DLQ_SCHEMA = StructType(
    [
        StructField("dlq_id", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("record_key", StringType(), True),
        StructField("failure_reason", StringType(), False),
        StructField("record_json", StringType(), False),
        StructField("status", StringType(), False),
        StructField("ingested_at", TimestampType(), False),
    ]
)


def ensure_dlq_table(spark: SparkSession, table: str = DLQ_TABLE) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          dlq_id STRING,
          source_table STRING,
          record_key STRING,
          failure_reason STRING,
          record_json STRING,
          status STRING,
          ingested_at TIMESTAMP
        )
        USING DELTA
        COMMENT 'Failed records routed from quality gates and transforms'
        """
    )


def route_to_dlq(
    spark: SparkSession,
    failed_df: DataFrame,
    *,
    source_table: str,
    failure_reason: str,
    key_column: str = "order_id",
    dlq_table: str = DLQ_TABLE,
) -> int:
    ensure_dlq_table(spark, dlq_table)
    if failed_df.limit(1).count() == 0:
        return 0

    ingested_at = datetime.now(timezone.utc)
    key_expr = F.col(key_column) if key_column in failed_df.columns else F.lit(None)

    rows = (
        failed_df.withColumn("record_key", key_expr.cast(StringType()))
        .withColumn("record_json", F.to_json(F.struct(*failed_df.columns)))
        .select("record_key", "record_json")
        .distinct()
        .collect()
    )

    payload = [
        {
            "dlq_id": str(uuid.uuid4()),
            "source_table": source_table,
            "record_key": r["record_key"],
            "failure_reason": failure_reason,
            "record_json": r["record_json"],
            "status": "pending_review",
            "ingested_at": ingested_at,
        }
        for r in rows
    ]

    spark.createDataFrame(payload, schema=DLQ_SCHEMA).write.format("delta").mode(
        "append"
    ).saveAsTable(dlq_table)
    return len(payload)


def dlq_summary(spark: SparkSession, dlq_table: str = DLQ_TABLE) -> DataFrame:
    return (
        spark.table(dlq_table)
        .groupBy("source_table", "failure_reason", "status")
        .count()
        .orderBy(F.col("count").desc())
    )
