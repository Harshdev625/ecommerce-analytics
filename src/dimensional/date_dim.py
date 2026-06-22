"""Date dimension for the star schema (2016–2020, fiscal year from April 1)."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

FISCAL_YEAR_START_MONTH = 4


@dataclass
class DateDimConfig:
    start_date: str = "2016-01-01"
    end_date: str = "2020-12-31"
    target_table: str = "globalmart.gold.dim_date"
    fiscal_year_start_month: int = FISCAL_YEAR_START_MONTH


def build_date_dimension(
    spark: SparkSession,
    config: DateDimConfig | None = None,
) -> DataFrame:
    """Build calendar + fiscal attributes for each day in the configured range."""
    config = config or DateDimConfig()
    fiscal_start = config.fiscal_year_start_month

    spine = spark.sql(
        f"""
        SELECT explode(
          sequence(
            to_date('{config.start_date}'),
            to_date('{config.end_date}'),
            interval 1 day
          )
        ) AS full_date
        """
    )

    months_from_fiscal_start = (F.col("month") - fiscal_start + 12) % 12

    return (
        spine.withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("day_of_month", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("year_month", F.date_format("full_date", "yyyy-MM"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
        .withColumn("is_month_start", F.dayofmonth("full_date") == 1)
        .withColumn("is_month_end", F.col("full_date") == F.last_day("full_date"))
        .withColumn(
            "fiscal_year",
            F.when(F.col("month") >= fiscal_start, F.col("year")).otherwise(F.col("year") - 1),
        )
        .withColumn("fiscal_month", months_from_fiscal_start + 1)
        .withColumn("fiscal_quarter", F.ceil((months_from_fiscal_start + 1) / 3).cast("int"))
        .withColumn("processed_at", F.current_timestamp())
        .orderBy("date_key")
    )


def run_date_dimension(
    spark: SparkSession,
    config: DateDimConfig | None = None,
) -> dict:
    config = config or DateDimConfig()
    dim = build_date_dimension(spark, config)
    dim.write.format("delta").mode("overwrite").saveAsTable(config.target_table)

    written = spark.table(config.target_table)
    row_count = written.count()

    import json as _json

    sample = _json.loads(
        _json.dumps(
            [row.asDict() for row in written.orderBy("date_key").limit(3).collect()],
            default=str,
        )
    )
    fiscal_sample = _json.loads(
        _json.dumps(
            [
                row.asDict()
                for row in written.filter(
                    (F.col("month") == 4) & (F.col("day_of_month") == 1)
                )
                .orderBy("date_key")
                .limit(3)
                .collect()
            ],
            default=str,
        )
    )

    return {
        "task": "dim_date",
        "target_table": config.target_table,
        "date_range": {"start": config.start_date, "end": config.end_date},
        "fiscal_year_start_month": config.fiscal_year_start_month,
        "row_count": row_count,
        "expected_row_count": 1827,
        "row_count_matches_expected": row_count == 1827,
        "weekend_days": written.filter(F.col("is_weekend")).count(),
        "sample_rows": sample,
        "fiscal_year_start_samples": fiscal_sample,
    }
