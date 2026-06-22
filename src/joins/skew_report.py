"""Skew distribution reports on silver join keys."""

from __future__ import annotations

from pyspark.sql import SparkSession

from src.joins.business_questions import SilverJoinTables
from src.spark_performance.skew import (
    DEFAULT_SKEW_THRESHOLD,
    analyze_key_skew,
    top_skewed_keys,
)


def run_skew_distribution_report(
    spark: SparkSession,
    tables: SilverJoinTables | None = None,
    columns: tuple[str, ...] = ("seller_id", "product_id"),
    limit: int = 10,
    skew_threshold: float = DEFAULT_SKEW_THRESHOLD,
) -> dict:
    """Top-N skewed keys per column on silver.order_items."""
    tables = tables or SilverJoinTables()
    items = spark.table(tables.order_items)

    analyses = []
    for col in columns:
        analyses.append(
            {
                "table": tables.order_items,
                "column": col,
                "skew_threshold": skew_threshold,
                "top_keys": top_skewed_keys(items, col, limit=limit, skew_threshold=skew_threshold),
            }
        )

    return {
        "task": "skew_distribution_report",
        "row_count": items.count(),
        "analyses": analyses,
    }
