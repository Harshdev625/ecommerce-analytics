"""Broadcast vs sort-merge join control on silver tables."""

from __future__ import annotations

import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.joins.business_questions import SilverJoinTables
from src.spark_performance.execution_plans import benchmark_df, capture_explain


def join_items_sellers_default(spark: SparkSession, tables: SilverJoinTables | None = None) -> DataFrame:
    """Spark optimizer chooses join strategy (often broadcast for small sellers)."""
    tables = tables or SilverJoinTables()
    items = spark.table(tables.order_items)
    sellers = spark.table(tables.sellers)
    return items.join(sellers, "seller_id").select(
        "order_id", "seller_id", "seller_state", "line_total_value"
    )


def join_items_sellers_sort_merge(spark: SparkSession, tables: SilverJoinTables | None = None) -> DataFrame:
    """Force sort-merge via hint (stand-in for disabled auto-broadcast on Databricks Free)."""
    tables = tables or SilverJoinTables()
    items = spark.table(tables.order_items)
    sellers = spark.table(tables.sellers).hint("merge")
    return items.join(sellers, "seller_id").select(
        "order_id", "seller_id", "seller_state", "line_total_value"
    )


def join_items_sellers_broadcast(spark: SparkSession, tables: SilverJoinTables | None = None) -> DataFrame:
    """Explicit broadcast of the small sellers table (~3k rows)."""
    tables = tables or SilverJoinTables()
    items = spark.table(tables.order_items)
    sellers = spark.table(tables.sellers)
    return items.join(F.broadcast(sellers), "seller_id").select(
        "order_id", "seller_id", "seller_state", "line_total_value"
    )


def detect_join_strategy(explain_text: str) -> str:
    text = explain_text
    if re.search(r"BroadcastHashJoin|PhotonBroadcastHashJoin", text, re.I):
        return "broadcast_hash_join"
    if re.search(r"SortMergeJoin", text, re.I):
        return "sort_merge_join"
    if re.search(r"ShuffleHashJoin", text, re.I):
        return "shuffle_hash_join"
    return "unknown"


def has_shuffle(explain_text: str) -> bool:
    return bool(re.search(r"Exchange|ShuffleExchange", explain_text, re.I))


def analyze_join_variant(name: str, label: str, df: DataFrame) -> dict:
    explain_text = capture_explain(df)
    timing = benchmark_df(df)
    return {
        "name": name,
        "label": label,
        "rows": timing["rows"],
        "elapsed_ms": timing["elapsed_ms"],
        "detected_strategy": detect_join_strategy(explain_text),
        "shuffle_in_plan": has_shuffle(explain_text),
        "explain_snippet": explain_text.splitlines()[:14],
    }


def run_broadcast_join_comparison(
    spark: SparkSession,
    tables: SilverJoinTables | None = None,
) -> dict:
    tables = tables or SilverJoinTables()
    variants = [
        ("default", "Spark default (optimizer chooses)", join_items_sellers_default(spark, tables)),
        ("sort_merge", "Forced sort-merge via hint('merge')", join_items_sellers_sort_merge(spark, tables)),
        ("broadcast", "Explicit broadcast(sellers)", join_items_sellers_broadcast(spark, tables)),
    ]
    results = [analyze_join_variant(name, label, df) for name, label, df in variants]

    default_ms = results[0]["elapsed_ms"]
    for row in results[1:]:
        row["timing_vs_default_x"] = (
            round(default_ms / row["elapsed_ms"], 2) if row["elapsed_ms"] > 0 else None
        )

    return {
        "task": "broadcast_join_control",
        "pair": "silver.order_items (~112k) ⋈ silver.sellers (~3k)",
        "note": (
            "Databricks Free blocks spark.conf auto-broadcast toggles; "
            "hint('merge') substitutes for disabled auto-broadcast."
        ),
        "variants": results,
    }
