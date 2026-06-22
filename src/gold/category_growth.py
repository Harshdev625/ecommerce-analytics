"""Category revenue growth streaks (3+ consecutive positive months)."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.gold.daily_sales import delivered_orders_with_revenue
from src.joins.business_questions import SilverJoinTables

MIN_STREAK_LENGTH = 3


@dataclass
class GoldCategoryGrowthConfig:
    source: SilverJoinTables | None = None
    target_table: str = "globalmart.gold.category_growth_streaks"


def _month_key_col() -> F.Column:
    return F.col("order_year") * F.lit(12) + F.col("order_month")


def _month_label_from_key(key_col: F.Column) -> F.Column:
    year = F.floor((key_col - 1) / 12)
    month = ((key_col - 1) % 12) + 1
    return F.concat(
        year.cast("string"),
        F.lit("-"),
        F.lpad(month.cast("string"), 2, "0"),
    )


def monthly_category_revenue(
    spark: SparkSession,
    tables: SilverJoinTables | None = None,
) -> DataFrame:
    """Monthly revenue by English category for delivered orders."""
    tables = tables or SilverJoinTables()
    delivered = delivered_orders_with_revenue(spark, tables).select("order_id", "order_date")
    items = spark.table(tables.order_items).select(
        "order_id",
        "category_name_en",
        "line_total_value",
    )

    return (
        items.join(delivered, "order_id", "inner")
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .groupBy("category_name_en", "order_year", "order_month")
        .agg(F.round(F.sum("line_total_value"), 2).alias("monthly_revenue"))
        .withColumn("month_key", _month_key_col())
    )


def build_category_growth_streaks(
    spark: SparkSession,
    config: GoldCategoryGrowthConfig | None = None,
    min_streak_length: int = MIN_STREAK_LENGTH,
) -> DataFrame:
    """Find categories with consecutive months of positive revenue growth."""
    config = config or GoldCategoryGrowthConfig()
    monthly = monthly_category_revenue(spark, config.source)

    w = Window.partitionBy("category_name_en").orderBy("month_key")
    flagged = (
        monthly.withColumn("prev_revenue", F.lag("monthly_revenue").over(w))
        .withColumn(
            "positive_growth",
            (F.col("monthly_revenue") > F.col("prev_revenue"))
            & F.col("prev_revenue").isNotNull(),
        )
    )

    growth_months = flagged.filter(F.col("positive_growth"))
    w_growth = Window.partitionBy("category_name_en").orderBy("month_key")
    runs = (
        growth_months.withColumn("prev_month_key", F.lag("month_key").over(w_growth))
        .withColumn(
            "gap_break",
            F.when(F.col("prev_month_key").isNull(), F.lit(1))
            .when(F.col("month_key") - F.col("prev_month_key") != 1, F.lit(1))
            .otherwise(F.lit(0)),
        )
        .withColumn(
            "run_id",
            F.sum("gap_break").over(
                w_growth.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
    )

    streaks = (
        runs.groupBy("category_name_en", "run_id")
        .agg(
            F.count("*").alias("streak_length"),
            F.min("month_key").alias("start_month_key"),
            F.max("month_key").alias("end_month_key"),
            F.min(
                F.struct("month_key", "monthly_revenue", "order_year", "order_month")
            ).alias("start_point"),
            F.max(
                F.struct("month_key", "monthly_revenue", "order_year", "order_month")
            ).alias("end_point"),
        )
        .filter(F.col("streak_length") >= min_streak_length)
        .withColumn("streak_start_month", _month_label_from_key(F.col("start_month_key")))
        .withColumn("streak_end_month", _month_label_from_key(F.col("end_month_key")))
        .withColumn("start_revenue", F.col("start_point.monthly_revenue"))
        .withColumn("end_revenue", F.col("end_point.monthly_revenue"))
        .withColumn(
            "total_growth_pct",
            F.round(
                ((F.col("end_revenue") - F.col("start_revenue")) / F.col("start_revenue"))
                * 100,
                2,
            ),
        )
        .select(
            F.col("category_name_en").alias("category"),
            "streak_start_month",
            "streak_end_month",
            "streak_length",
            "start_revenue",
            "end_revenue",
            "total_growth_pct",
        )
        .withColumn("processed_at", F.current_timestamp())
        .orderBy(F.col("streak_length").desc(), F.col("total_growth_pct").desc())
    )
    return streaks


def run_category_growth_streaks(
    spark: SparkSession,
    config: GoldCategoryGrowthConfig | None = None,
    min_streak_length: int = MIN_STREAK_LENGTH,
) -> dict:
    config = config or GoldCategoryGrowthConfig()
    streaks = build_category_growth_streaks(spark, config, min_streak_length)
    streaks.write.format("delta").mode("overwrite").saveAsTable(config.target_table)

    written = spark.table(config.target_table)
    rows = [row.asDict() for row in written.collect()]

    import json as _json

    rows = _json.loads(_json.dumps(rows, default=str))

    return {
        "task": "gold_category_growth_streaks",
        "target_table": config.target_table,
        "min_streak_length": min_streak_length,
        "qualifying_streak_count": len(rows),
        "qualifying_streaks": rows,
    }
