"""Shared helpers for Delta table inspection."""

from __future__ import annotations

from pyspark.sql import SparkSession


def describe_detail_row(spark: SparkSession, table: str) -> dict:
    """Return DESCRIBE DETAIL as a plain dict."""
    row = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
    return row.asDict()


def describe_detail_summary(spark: SparkSession, table: str) -> dict:
    """Key file-layout metrics for run reports."""
    detail = describe_detail_row(spark, table)
    size_bytes = detail.get("sizeInBytes") or 0
    num_files = detail.get("numFiles") or 0
    avg_file_bytes = int(size_bytes / num_files) if num_files else 0
    return {
        "table": table,
        "format": detail.get("format"),
        "num_files": int(num_files),
        "size_in_bytes": int(size_bytes),
        "avg_file_size_bytes": avg_file_bytes,
        "partition_columns": detail.get("partitionColumns") or [],
        "clustering_columns": detail.get("clusteringColumns") or [],
        "min_reader_version": detail.get("minReaderVersion"),
        "min_writer_version": detail.get("minWriterVersion"),
    }
