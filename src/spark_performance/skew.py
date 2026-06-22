"""Skew analysis and remediation for silver-table joins."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.spark_performance.execution_plans import SilverTableRefs, benchmark_df, capture_explain

DEFAULT_SKEW_THRESHOLD = 3.0
DEFAULT_SALT_BUCKETS = 8
DEFAULT_REPLICATION_FACTOR = 40


@dataclass
class SkewKeyResult:
    column: str
    hot_key: str
    hot_key_count: int
    skew_factor: float
    pct_of_total: float


def analyze_key_skew(
    df: DataFrame,
    key_column: str,
    skew_threshold: float = DEFAULT_SKEW_THRESHOLD,
) -> DataFrame:
    """Per-key counts with skew factor (count / average). Reused in M4 Task 4.3."""
    total = df.count()
    distinct_keys = df.select(key_column).distinct().count()
    avg_count = total / distinct_keys if distinct_keys else 0.0

    return (
        df.groupBy(key_column)
        .count()
        .withColumn("pct_of_total", F.round(100.0 * F.col("count") / F.lit(total), 4))
        .withColumn(
            "skew_factor",
            F.round(F.col("count") / F.lit(avg_count), 2) if avg_count else F.lit(0.0),
        )
        .withColumn("is_skewed", F.col("skew_factor") >= F.lit(skew_threshold))
        .orderBy(F.col("count").desc())
    )


def top_skewed_keys(
    df: DataFrame,
    key_column: str,
    limit: int = 10,
    skew_threshold: float = DEFAULT_SKEW_THRESHOLD,
) -> list[dict]:
    rows = analyze_key_skew(df, key_column, skew_threshold).limit(limit).collect()
    return [row.asDict() for row in rows]


def find_most_skewed_join_key(
    spark: SparkSession,
    tables: SilverTableRefs | None = None,
    candidate_columns: tuple[str, ...] = ("seller_id", "order_id"),
) -> SkewKeyResult:
    """Pick the join column (with a silver partner table) with the highest skew factor."""
    tables = tables or SilverTableRefs()
    items = spark.table(tables.order_items)
    best: SkewKeyResult | None = None

    for col in candidate_columns:
        top = analyze_key_skew(items, col).first()
        if top is None:
            continue
        candidate = SkewKeyResult(
            column=col,
            hot_key=str(top[col]),
            hot_key_count=int(top["count"]),
            skew_factor=float(top["skew_factor"]),
            pct_of_total=float(top["pct_of_total"]),
        )
        if best is None or candidate.skew_factor > best.skew_factor:
            best = candidate

    if best is None:
        raise ValueError("No skew candidates found in silver.order_items")
    return best


def inflate_skew_key(
    df: DataFrame,
    key_col: str,
    hot_key: str,
    replication_factor: int = DEFAULT_REPLICATION_FACTOR,
) -> DataFrame:
    """Simulate worst-case skew by replicating all rows for the hot key."""
    normal = df.filter(F.col(key_col) != hot_key)
    hot = df.filter(F.col(key_col) == hot_key)
    if hot.head(1) == []:
        return df

    multiplier = df.sparkSession.range(replication_factor).withColumnRenamed("id", "_rep")
    inflated = hot.crossJoin(multiplier).drop("_rep")
    return normal.unionByName(inflated)


def get_join_partner(spark: SparkSession, tables: SilverTableRefs, key_col: str) -> DataFrame:
    if key_col == "seller_id":
        return spark.table(tables.sellers)
    if key_col == "order_id":
        return spark.table(tables.orders).select("order_id", "customer_id")
    raise ValueError(f"No silver join partner for key column: {key_col}")


def skewed_join_aggregate(
    items: DataFrame,
    partner: DataFrame,
    key_col: str = "seller_id",
) -> DataFrame:
    """Group-by after join so skew shows up at the shuffle join."""
    value_col = "line_total_value" if "line_total_value" in items.columns else None
    agg_exprs = [F.count("*").alias("line_count")]
    if value_col:
        agg_exprs.insert(0, F.sum(value_col).alias("revenue"))

    return (
        items.join(partner.hint("merge"), key_col)
        .groupBy(key_col)
        .agg(*agg_exprs)
    )


def salted_join_aggregate(
    items: DataFrame,
    partner: DataFrame,
    key_col: str,
    hot_keys: list[str],
    salt_buckets: int = DEFAULT_SALT_BUCKETS,
    row_id_cols: tuple[str, ...] = ("order_id", "order_item_id"),
) -> DataFrame:
    """Salt hot keys on both sides, then join on (key, salt)."""
    hot_set = list(hot_keys)
    hash_cols = [F.col(c) for c in row_id_cols if c in items.columns]
    if not hash_cols:
        hash_cols = [F.col(key_col)]

    left = items.withColumn(
        "salt",
        F.when(
            F.col(key_col).isin(hot_set),
            F.pmod(F.xxhash64(*hash_cols), F.lit(salt_buckets)),
        ).otherwise(F.lit(0)),
    )

    partner_cols = [key_col] + [c for c in partner.columns if c != key_col]
    partner_small = partner.select(*partner_cols)
    partner_normal = partner_small.filter(~F.col(key_col).isin(hot_set)).withColumn("salt", F.lit(0))
    partner_hot = (
        partner_small.filter(F.col(key_col).isin(hot_set))
        .withColumn("salt", F.explode(F.sequence(F.lit(0), F.lit(salt_buckets - 1))))
    )
    right = partner_normal.unionByName(partner_hot)

    joined = left.join(right.hint("merge"), [key_col, "salt"]).drop("salt")
    value_col = "line_total_value" if "line_total_value" in joined.columns else None
    agg_exprs = [F.count("*").alias("line_count")]
    if value_col:
        agg_exprs.insert(0, F.sum(value_col).alias("revenue"))
    return joined.groupBy(key_col).agg(*agg_exprs)


def set_skew_join_conf(spark: SparkSession, enabled: bool) -> dict[str, str]:
    """Toggle AQE skew join. Returns settings applied (or skip reason)."""
    flag = "true" if enabled else "false"
    settings = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": flag,
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "2",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256KB",
    }
    applied: dict[str, str] = {}
    for key, value in settings.items():
        try:
            spark.conf.set(key, value)
            applied[key] = value
        except Exception as exc:
            applied[key] = f"skipped: {exc}"
    return applied


def run_skew_remediation_comparison(
    spark: SparkSession,
    tables: SilverTableRefs | None = None,
    replication_factor: int = DEFAULT_REPLICATION_FACTOR,
    salt_buckets: int = DEFAULT_SALT_BUCKETS,
) -> dict:
    tables = tables or SilverTableRefs()
    items = spark.table(tables.order_items)

    skew_key = find_most_skewed_join_key(spark, tables)
    key_col = skew_key.column
    hot_key = skew_key.hot_key
    partner = get_join_partner(spark, tables, key_col)

    skewed_items = inflate_skew_key(items, key_col, hot_key, replication_factor)
    inflated_rows = skewed_items.count()

    approaches: dict[str, dict] = {}

    set_skew_join_conf(spark, enabled=False)
    baseline_df = skewed_join_aggregate(skewed_items, partner, key_col)
    approaches["baseline"] = {
        "explain_snippet": capture_explain(baseline_df).splitlines()[:14],
        "timing": benchmark_df(baseline_df),
    }

    salted_df = salted_join_aggregate(skewed_items, partner, key_col, [hot_key], salt_buckets)
    approaches["salted"] = {
        "explain_snippet": capture_explain(salted_df).splitlines()[:14],
        "timing": benchmark_df(salted_df),
        "salt_buckets": salt_buckets,
    }

    aqe_conf = set_skew_join_conf(spark, enabled=True)
    aqe_df = skewed_join_aggregate(skewed_items, partner, key_col)
    approaches["aqe_skew_join"] = {
        "explain_snippet": capture_explain(aqe_df).splitlines()[:14],
        "timing": benchmark_df(aqe_df),
        "spark_conf": aqe_conf,
    }

    baseline_ms = approaches["baseline"]["timing"]["elapsed_ms"]
    for name in ("salted", "aqe_skew_join"):
        good_ms = approaches[name]["timing"]["elapsed_ms"]
        approaches[name]["speedup_vs_baseline_x"] = (
            round(baseline_ms / good_ms, 2) if good_ms > 0 else None
        )

    return {
        "skew_key": skew_key,
        "replication_factor": replication_factor,
        "inflated_row_count": inflated_rows,
        "approaches": approaches,
    }
