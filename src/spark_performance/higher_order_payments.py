"""Explode vs higher-order function analytics on nested payments."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.spark_performance.execution_plans import benchmark_df, capture_explain


@dataclass
class NestedPaymentsRefs:
    nested_table: str = "globalmart.bronze.order_payments_nested"
    flattened_table: str = "globalmart.bronze.order_payments_flattened"


# --- Problem 1: filter — orders with any credit_card payment > 100 ---


def ho_orders_high_credit_card(nested: DataFrame, threshold: float = 100.0) -> DataFrame:
    high_cc = F.filter(
        F.col("payments"),
        lambda p: (p.payment_type == "credit_card") & (p.payment_value > threshold),
    )
    return (
        nested.withColumn("high_cc_payments", high_cc)
        .filter(F.size("high_cc_payments") > 0)
        .select("order_id", "num_payment_methods", F.size("high_cc_payments").alias("high_cc_count"))
    )


def explode_orders_high_credit_card(flattened: DataFrame, threshold: float = 100.0) -> DataFrame:
    return (
        flattened.filter(
            (F.col("payment_type") == "credit_card") & (F.col("payment_value") > threshold)
        )
        .groupBy("order_id")
        .agg(
            F.first("num_payment_methods").alias("num_payment_methods"),
            F.count("*").alias("high_cc_count"),
        )
    )


# --- Problem 2: aggregate — total credit_card value per order ---


def ho_total_credit_card_value(nested: DataFrame) -> DataFrame:
    cc_values = F.transform(
        F.filter(F.col("payments"), lambda p: p.payment_type == "credit_card"),
        lambda p: p.payment_value,
    )
    return nested.select(
        "order_id",
        F.aggregate(cc_values, F.lit(0.0), lambda acc, v: acc + v).alias("credit_card_total"),
    )


def explode_total_credit_card_value(flattened: DataFrame) -> DataFrame:
    return (
        flattened.filter(F.col("payment_type") == "credit_card")
        .groupBy("order_id")
        .agg(F.sum("payment_value").alias("credit_card_total"))
    )


# --- Problem 3: transform + aggregate — max installments on non-boleto payments ---


def ho_max_non_boleto_installments(nested: DataFrame) -> DataFrame:
    installments = F.transform(
        F.filter(F.col("payments"), lambda p: p.payment_type != "boleto"),
        lambda p: p.payment_installments.cast("int"),
    )
    return nested.select(
        "order_id",
        F.aggregate(
            installments,
            F.lit(0),
            lambda acc, n: F.greatest(acc, n),
        ).alias("max_non_boleto_installments"),
    )


def explode_max_non_boleto_installments(flattened: DataFrame) -> DataFrame:
    return (
        flattened.filter(F.col("payment_type") != "boleto")
        .groupBy("order_id")
        .agg(F.max("payment_installments").alias("max_non_boleto_installments"))
    )


def compare_approaches(ho_df: DataFrame, explode_df: DataFrame, problem_name: str) -> dict:
    ho_timing = benchmark_df(ho_df)
    explode_timing = benchmark_df(explode_df)
    return {
        "problem": problem_name,
        "ho_rows": ho_timing["rows"],
        "explode_rows": explode_timing["rows"],
        "ho_elapsed_ms": ho_timing["elapsed_ms"],
        "explode_elapsed_ms": explode_timing["elapsed_ms"],
        "ho_explain_snippet": capture_explain(ho_df).splitlines()[:12],
        "explode_explain_snippet": capture_explain(explode_df).splitlines()[:12],
        "speedup_ho_vs_explode_x": (
            round(explode_timing["elapsed_ms"] / ho_timing["elapsed_ms"], 2)
            if ho_timing["elapsed_ms"] > 0
            else None
        ),
    }


OBSERVATIONS = [
    "Higher-order functions keep one row per order — no shuffle-expanding the array, so plans often avoid an extra Exchange after explode.",
    "Explode paths scan the flattened table (more rows) or inline explode nested arrays, which can simplify SQL mentally but materializes one row per payment.",
    "For filter-only questions (Problem 1), both return the same order grain; HO preserves nested context in-place.",
    "Aggregate across array elements (Problems 2–3) maps cleanly to filter + transform + aggregate without a groupBy on exploded rows.",
    "On Photon, HO array ops may fuse into fewer stages; explode + groupBy often shows ShuffleExchangeSource before aggregation.",
]


def run_all_comparisons(spark: SparkSession, refs: NestedPaymentsRefs | None = None) -> dict:
    refs = refs or NestedPaymentsRefs()
    nested = spark.table(refs.nested_table)
    flattened = spark.table(refs.flattened_table)

    problems = [
        compare_approaches(
            ho_orders_high_credit_card(nested),
            explode_orders_high_credit_card(flattened),
            "filter_high_credit_card",
        ),
        compare_approaches(
            ho_total_credit_card_value(nested),
            explode_total_credit_card_value(flattened),
            "aggregate_credit_card_total",
        ),
        compare_approaches(
            ho_max_non_boleto_installments(nested),
            explode_max_non_boleto_installments(flattened),
            "transform_max_non_boleto_installments",
        ),
    ]

    return {
        "task": "2.3_higher_order_vs_explode",
        "nested_table": refs.nested_table,
        "flattened_table": refs.flattened_table,
        "problems": problems,
        "observations": OBSERVATIONS,
    }
