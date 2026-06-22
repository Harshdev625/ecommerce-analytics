"""Execution-plan anti-patterns and fixes for silver-table queries."""

from __future__ import annotations

import io
import time
from contextlib import redirect_stdout
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

@dataclass
class SilverTableRefs:
    order_items: str = "globalmart.silver.order_items"
    customers: str = "globalmart.silver.customers"
    sellers: str = "globalmart.silver.sellers"
    orders: str = "globalmart.silver.orders_late_arrivals"


def capture_explain(df: DataFrame, mode: str = "formatted") -> str:
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        df.explain(mode)
    return buffer.getvalue()


def benchmark_df(df: DataFrame) -> dict:
    start = time.perf_counter()
    rows = df.count()
    elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
    return {"rows": rows, "elapsed_ms": elapsed_ms}


def run_pattern_comparison(
    spark: SparkSession,
    bad_df: DataFrame,
    good_df: DataFrame,
    pattern_name: str,
) -> dict:
    return {
        "pattern": pattern_name,
        "bad": {
            "explain": capture_explain(bad_df),
            "timing": benchmark_df(bad_df),
        },
        "good": {
            "explain": capture_explain(good_df),
            "timing": benchmark_df(good_df),
        },
        "speedup_x": None,
    }


def finalize_comparison(result: dict) -> dict:
    bad_ms = result["bad"]["timing"]["elapsed_ms"]
    good_ms = result["good"]["timing"]["elapsed_ms"]
    if good_ms > 0:
        result["speedup_x"] = round(bad_ms / good_ms, 2)
    return result


# --- Anti-pattern 1: filter after expensive join + aggregation ---


def predicate_after_join_bad(spark: SparkSession, tables: SilverTableRefs | None = None) -> DataFrame:
    """Join large tables and aggregate before filtering to one state."""
    tables = tables or SilverTableRefs()
    items = spark.table(tables.order_items)
    orders = spark.table(tables.orders).select("order_id", "customer_id")
    customers = spark.table(tables.customers)

    return (
        items.join(orders, "order_id")
        .join(customers, "customer_id")
        .groupBy("customer_state")
        .agg(F.sum("line_total_value").alias("revenue"))
        .filter(F.col("customer_state") == "SP")
    )


def predicate_before_join_good(spark: SparkSession, tables: SilverTableRefs | None = None) -> DataFrame:
    """Filter customers first, then join only the rows needed."""
    tables = tables or SilverTableRefs()
    items = spark.table(tables.order_items)
    orders = spark.table(tables.orders).select("order_id", "customer_id")
    sp_customers = spark.table(tables.customers).filter(F.col("customer_state") == "SP")

    return (
        items.join(orders, "order_id")
        .join(sp_customers, "customer_id")
        .agg(F.sum("line_total_value").alias("revenue"))
    )


# --- Anti-pattern 2: wrong join strategy (small table not broadcast) ---


def join_without_broadcast_bad(
    spark: SparkSession,
    tables: SilverTableRefs | None = None,
) -> DataFrame:
    """Force sort-merge join via hint (no spark.conf — blocked on Databricks Free)."""
    tables = tables or SilverTableRefs()
    items = spark.table(tables.order_items)
    sellers = spark.table(tables.sellers).hint("merge")
    return items.join(sellers, "seller_id").select("order_id", "seller_state", "line_total_value")


def join_with_broadcast_good(spark: SparkSession, tables: SilverTableRefs | None = None) -> DataFrame:
    """Broadcast the small sellers dimension."""
    tables = tables or SilverTableRefs()
    items = spark.table(tables.order_items)
    sellers = spark.table(tables.sellers)
    return items.join(F.broadcast(sellers), "seller_id").select(
        "order_id", "seller_state", "line_total_value"
    )


# --- Anti-pattern 3: unnecessary shuffle from repartition ---


def repartition_before_filter_bad(spark: SparkSession, tables: SilverTableRefs | None = None) -> DataFrame:
    """Repartition on a high-cardinality key before filtering — extra shuffle."""
    tables = tables or SilverTableRefs()
    items = spark.table(tables.order_items)
    orders = spark.table(tables.orders).select("order_id", "customer_id")
    return (
        items.repartition(200, "order_id")
        .filter(F.col("price") >= 50)
        .join(orders, "order_id")
    )


def filter_before_join_good(spark: SparkSession, tables: SilverTableRefs | None = None) -> DataFrame:
    """Filter first to reduce rows, then join without forced repartition."""
    tables = tables or SilverTableRefs()
    items = spark.table(tables.order_items)
    orders = spark.table(tables.orders).select("order_id", "customer_id")
    return items.filter(F.col("price") >= 50).join(orders, "order_id")


def run_all_comparisons(spark: SparkSession, tables: SilverTableRefs | None = None) -> list[dict]:
    tables = tables or SilverTableRefs()
    results = []

    r1 = run_pattern_comparison(
        spark,
        predicate_after_join_bad(spark, tables),
        predicate_before_join_good(spark, tables),
        "predicate_after_expensive_ops",
    )
    results.append(finalize_comparison(r1))

    r2 = run_pattern_comparison(
        spark,
        join_without_broadcast_bad(spark, tables),
        join_with_broadcast_good(spark, tables),
        "wrong_join_strategy",
    )
    results.append(finalize_comparison(r2))

    r3 = run_pattern_comparison(
        spark,
        repartition_before_filter_bad(spark, tables),
        filter_before_join_good(spark, tables),
        "unnecessary_shuffle_repartition",
    )
    results.append(finalize_comparison(r3))

    return results
