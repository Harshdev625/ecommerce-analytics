"""Reusable, configuration-driven data quality engine."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RESULTS_TABLE = "globalmart.metadata.data_quality_results"

RESULTS_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("rule_id", StringType(), False),
        StructField("check_type", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("passed", BooleanType(), False),
        StructField("total_records", LongType(), False),
        StructField("failure_count", LongType(), False),
        StructField("failure_pct", DoubleType(), False),
        StructField("checked_at", TimestampType(), False),
    ]
)


@dataclass
class QualityRunResult:
    run_id: str
    table_name: str
    overall_status: str
    critical_passed: bool
    results: list[dict[str, Any]]


def ensure_results_table(spark: SparkSession, table: str = RESULTS_TABLE) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          run_id STRING,
          table_name STRING,
          rule_id STRING,
          check_type STRING,
          severity STRING,
          passed BOOLEAN,
          total_records BIGINT,
          failure_count BIGINT,
          failure_pct DOUBLE,
          checked_at TIMESTAMP
        )
        USING DELTA
        COMMENT 'Data quality check outcomes per rule and run'
        """
    )


def _failed_rows_for_rule(spark: SparkSession, df: DataFrame, rule: dict) -> DataFrame:
    check = rule["check_type"]
    col_name = rule["column"]
    params = rule.get("params", {})

    if check == "not_null":
        return df.filter(F.col(col_name).isNull())
    if check == "unique":
        dup_keys = (
            df.groupBy(col_name).count().filter(F.col("count") > 1).select(col_name)
        )
        return df.join(dup_keys, on=col_name, how="inner")
    if check == "accepted_values":
        allowed = params.get("values", [])
        return df.filter(~F.col(col_name).isin(allowed) | F.col(col_name).isNull())
    if check == "referential":
        ref = spark.table(params["reference_table"])
        return df.join(
            ref.select(params["reference_column"]).distinct(),
            on=col_name,
            how="left_anti",
        )
    if check == "range":
        cond = F.lit(False)
        if params.get("min") is not None:
            cond = cond | (F.col(col_name) < F.lit(params["min"]))
        if params.get("max") is not None:
            cond = cond | (F.col(col_name) > F.lit(params["max"]))
        return df.filter(cond)
    raise ValueError(f"Unsupported check_type: {check}")


def _check_rule(spark: SparkSession, df: DataFrame, rule: dict) -> dict[str, Any]:
    total = df.count()
    failure_count = _failed_rows_for_rule(spark, df, rule).count()
    failure_pct = round(100.0 * failure_count / total, 4) if total else 0.0
    return {
        "rule_id": rule["rule_id"],
        "check_type": rule["check_type"],
        "severity": rule["severity"],
        "passed": failure_count == 0,
        "total_records": total,
        "failure_count": failure_count,
        "failure_pct": failure_pct,
    }


def run_quality_checks(
    spark: SparkSession,
    df: DataFrame,
    rules: list[dict],
    *,
    table_name: str,
    results_table: str = RESULTS_TABLE,
) -> QualityRunResult:
    ensure_results_table(spark, results_table)
    run_id = str(uuid.uuid4())
    checked_at = datetime.now(timezone.utc)
    results: list[dict[str, Any]] = []

    for rule in rules:
        outcome = _check_rule(spark, df, rule)
        outcome.update(
            {"run_id": run_id, "table_name": table_name, "checked_at": checked_at}
        )
        results.append(outcome)

    critical_failed = any(
        not r["passed"] and r["severity"] == "critical" for r in results
    )
    overall = "FAILED" if critical_failed else "PASSED"

    spark.createDataFrame(results, schema=RESULTS_SCHEMA).write.format("delta").mode(
        "append"
    ).saveAsTable(results_table)

    return QualityRunResult(
        run_id=run_id,
        table_name=table_name,
        overall_status=overall,
        critical_passed=not critical_failed,
        results=results,
    )


def get_failed_records(
    spark: SparkSession,
    df: DataFrame,
    rules: list[dict],
    *,
    severities: tuple[str, ...] = ("critical",),
    key_column: str = "order_id",
) -> DataFrame:
    """Rows failing at least one rule at the given severity levels."""
    active = [r for r in rules if r["severity"] in severities]
    if not active:
        return df.limit(0)

    failed = _failed_rows_for_rule(spark, df, active[0])
    for rule in active[1:]:
        failed = failed.unionByName(
            _failed_rows_for_rule(spark, df, rule), allowMissingColumns=True
        )

    if key_column in failed.columns:
        return failed.dropDuplicates([key_column])
    return failed.distinct()
