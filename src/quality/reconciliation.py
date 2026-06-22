"""Multi-level reconciliation between source and target tables."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RECONCILIATION_LOG_TABLE = "globalmart.metadata.reconciliation_log"

LOG_SCHEMA = StructType(
    [
        StructField("reconciliation_id", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("level", StringType(), False),
        StructField("key_column", StringType(), False),
        StructField("passed", BooleanType(), False),
        StructField("detail", StringType(), True),
        StructField("mismatched_buckets", IntegerType(), True),
        StructField("reconciled_at", TimestampType(), False),
    ]
)


def ensure_reconciliation_log(spark: SparkSession, table: str = RECONCILIATION_LOG_TABLE) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          reconciliation_id STRING,
          source_table STRING,
          target_table STRING,
          level STRING,
          key_column STRING,
          passed BOOLEAN,
          detail STRING,
          mismatched_buckets INT,
          reconciled_at TIMESTAMP
        )
        USING DELTA
        COMMENT 'Bronze-to-silver reconciliation outcomes by level'
        """
    )


def _log_entry(
    reconciliation_id: str,
    source_table: str,
    target_table: str,
    level: str,
    key_column: str,
    passed: bool,
    detail: str,
    mismatched_buckets: int | None = None,
) -> dict[str, Any]:
    return {
        "reconciliation_id": reconciliation_id,
        "source_table": source_table,
        "target_table": target_table,
        "level": level,
        "key_column": key_column,
        "passed": passed,
        "detail": detail,
        "mismatched_buckets": mismatched_buckets,
        "reconciled_at": datetime.now(timezone.utc),
    }


def level1_count_check(
    source: DataFrame,
    target: DataFrame,
    key_column: str,
) -> tuple[bool, str]:
    src_count = source.count()
    tgt_count = target.count()
    src_distinct = source.select(key_column).distinct().count()
    tgt_distinct = target.select(key_column).distinct().count()
    passed = src_distinct == tgt_distinct
    detail = (
        f"source_rows={src_count}, target_rows={tgt_count}, "
        f"source_distinct_{key_column}={src_distinct}, target_distinct_{key_column}={tgt_distinct}"
    )
    return passed, detail


def level2_bucket_hash_check(
    source: DataFrame,
    target: DataFrame,
    key_column: str,
    *,
    num_buckets: int = 64,
) -> tuple[bool, str, list[int]]:
    def bucketed(df: DataFrame) -> DataFrame:
        return (
            df.withColumn("_bucket", F.abs(F.hash(F.col(key_column))) % num_buckets)
            .groupBy("_bucket")
            .agg(
                F.count("*").alias("_row_cnt"),
                F.countDistinct(key_column).alias("_distinct_cnt"),
            )
        )

    src_buckets = bucketed(source)
    tgt_buckets = bucketed(target)
    mismatched = (
        src_buckets.alias("s")
        .join(tgt_buckets.alias("t"), on="_bucket", how="full_outer")
        .filter(
            F.col("s._row_cnt").isNull()
            | F.col("t._row_cnt").isNull()
            | (F.col("s._row_cnt") != F.col("t._row_cnt"))
            | (F.col("s._distinct_cnt") != F.col("t._distinct_cnt"))
        )
        .select(F.coalesce(F.col("s._bucket"), F.col("t._bucket")).alias("_bucket"))
        .collect()
    )
    bad_buckets = [int(r["_bucket"]) for r in mismatched]
    passed = len(bad_buckets) == 0
    detail = f"buckets_compared={num_buckets}, mismatched={len(bad_buckets)}"
    return passed, detail, bad_buckets


def level3_drill_down(
    source: DataFrame,
    target: DataFrame,
    key_column: str,
    mismatched_buckets: list[int],
    *,
    num_buckets: int = 64,
) -> DataFrame:
    if not mismatched_buckets:
        return source.limit(0)

    src_b = source.withColumn("_bucket", F.abs(F.hash(F.col(key_column))) % num_buckets).filter(
        F.col("_bucket").isin(mismatched_buckets)
    )
    tgt_b = target.withColumn("_bucket", F.abs(F.hash(F.col(key_column))) % num_buckets).filter(
        F.col("_bucket").isin(mismatched_buckets)
    )
    return (
        src_b.select(key_column, F.lit("source_only").alias("diff_side"))
        .join(tgt_b.select(key_column), on=key_column, how="left_anti")
        .unionByName(
            tgt_b.select(key_column, F.lit("target_only").alias("diff_side")).join(
                src_b.select(key_column), on=key_column, how="left_anti"
            )
        )
    )


def run_reconciliation(
    spark: SparkSession,
    source: DataFrame,
    target: DataFrame,
    *,
    source_table: str,
    target_table: str,
    key_column: str,
    compare_columns: list[str] | None = None,
    log_table: str = RECONCILIATION_LOG_TABLE,
) -> str:
    ensure_reconciliation_log(spark, log_table)
    reconciliation_id = str(uuid.uuid4())
    entries: list[dict[str, Any]] = []

    l1_passed, l1_detail = level1_count_check(source, target, key_column)
    entries.append(
        _log_entry(
            reconciliation_id, source_table, target_table, "LEVEL_1_COUNT",
            key_column, l1_passed, l1_detail,
        )
    )

    l2_passed, l2_detail, bad_buckets = level2_bucket_hash_check(
        source, target, key_column
    )
    entries.append(
        _log_entry(
            reconciliation_id, source_table, target_table, "LEVEL_2_BUCKET_HASH",
            key_column, l2_passed, l2_detail, mismatched_buckets=len(bad_buckets),
        )
    )

    drill = level3_drill_down(source, target, key_column, bad_buckets)
    l3_count = drill.count()
    l3_passed = l3_count == 0
    entries.append(
        _log_entry(
            reconciliation_id, source_table, target_table, "LEVEL_3_DRILL_DOWN",
            key_column, l3_passed, f"diff_keys={l3_count}", mismatched_buckets=len(bad_buckets),
        )
    )

    spark.createDataFrame(entries, schema=LOG_SCHEMA).write.format("delta").mode(
        "append"
    ).saveAsTable(log_table)
    return reconciliation_id
