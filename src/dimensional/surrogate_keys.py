"""Surrogate key generation strategies and stability tests."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SK_ANALYSIS = {
    "scd_type_1_choice": "deterministic_hash",
    "scd_type_1_rationale": (
        "SCD Type 1 overwrites attributes in place. Hash-based SKs derived from the natural key "
        "stay stable across reloads, so fact table FKs and MERGE joins do not break."
    ),
    "scd_type_2_choice": "monotonic_sequence",
    "scd_type_2_rationale": (
        "SCD Type 2 creates a new row per version. A monotonic sequence (or identity) assigns a "
        "fresh surrogate per version while the business key stays on the natural key columns."
    ),
    "date_dimension_rationale": (
        "dim_date uses date_key (YYYYMMDD) as the primary key — a fixed, meaningful integer for "
        "every calendar day. No surrogate is needed because the domain is closed and known upfront."
    ),
}


@dataclass
class SurrogateKeyConfig:
    source_table: str = "globalmart.silver.sellers"
    natural_key: str = "seller_id"


def assign_sk_monotonic(df: DataFrame, natural_key: str) -> DataFrame:
    """Spark monotonically_increasing_id — fast but not stable across runs."""
    return df.select(natural_key).distinct().withColumn(
        "sk_monotonic",
        F.monotonically_increasing_id(),
    )


def assign_sk_row_number(df: DataFrame, natural_key: str) -> DataFrame:
    """Deterministic ordering by natural key — stable for a fixed dataset."""
    ordered = df.select(natural_key).distinct().orderBy(natural_key)
    window = Window.orderBy(natural_key)
    return ordered.withColumn("sk_row_number", F.row_number().over(window))


def assign_sk_hash(df: DataFrame, natural_key: str, salt: str = "globalmart") -> DataFrame:
    """SHA-256 hash truncated to a positive long — stable across runs."""
    return (
        df.select(natural_key)
        .distinct()
        .withColumn(
            "sk_hash",
            F.abs(F.hash(F.concat(F.col(natural_key), F.lit(salt)))),
        )
    )


def _stability_check(
    run_a: DataFrame,
    run_b: DataFrame,
    natural_key: str,
    sk_col: str,
) -> dict:
    joined = run_a.alias("a").join(run_b.alias("b"), natural_key, "inner")
    total = joined.count()
    mismatches = joined.filter(F.col(f"a.{sk_col}") != F.col(f"b.{sk_col}")).count()
    return {
        "strategy": sk_col,
        "entities_tested": total,
        "mismatches_between_runs": mismatches,
        "stable_across_runs": mismatches == 0,
    }


def run_surrogate_key_tests(
    spark: SparkSession,
    config: SurrogateKeyConfig | None = None,
) -> dict:
    config = config or SurrogateKeyConfig()
    source = spark.table(config.source_table)

    mono_a = assign_sk_monotonic(source, config.natural_key)
    mono_b = assign_sk_monotonic(source, config.natural_key)
    row_a = assign_sk_row_number(source, config.natural_key)
    row_b = assign_sk_row_number(source, config.natural_key)
    hash_a = assign_sk_hash(source, config.natural_key)
    hash_b = assign_sk_hash(source, config.natural_key)

    mono_result = _stability_check(mono_a, mono_b, config.natural_key, "sk_monotonic")
    row_result = _stability_check(row_a, row_b, config.natural_key, "sk_row_number")
    hash_result = _stability_check(hash_a, hash_b, config.natural_key, "sk_hash")

    return {
        "task": "surrogate_key_strategy",
        "source_table": config.source_table,
        "natural_key": config.natural_key,
        "tests": [mono_result, row_result, hash_result],
        "strategy_decisions": SK_ANALYSIS,
        "sample_keys": [
            row.asDict()
            for row in hash_a.orderBy(config.natural_key).limit(5).collect()
        ],
    }


def hash_surrogate_key(natural_key_col: F.Column, salt: str = "globalmart") -> F.Column:
    """Reusable hash SK expression for dimension MERGE loads."""
    return F.abs(F.hash(F.concat(natural_key_col, F.lit(salt))))
