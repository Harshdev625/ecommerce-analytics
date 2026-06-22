"""Star schema analytics query — fact joined to all four dimensions."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass
class StarSchemaQueryConfig:
    fact_table: str = "globalmart.gold.fact_sales"
    dim_date: str = "globalmart.gold.dim_date"
    dim_product: str = "globalmart.gold.dim_product"
    dim_seller: str = "globalmart.gold.dim_seller"
    dim_customer: str = "globalmart.gold.dim_customer"
    filter_year: int = 2018
    filter_states: tuple[str, ...] = ("SP", "RJ", "MG")
    top_n: int = 20


def build_star_schema_summary(
    spark: SparkSession,
    config: StarSchemaQueryConfig | None = None,
) -> DataFrame:
    """Join fact to all dims; aggregate by month, state, and product category."""
    config = config or StarSchemaQueryConfig()

    fact = spark.table(config.fact_table)
    dim_date = spark.table(config.dim_date)
    dim_product = spark.table(config.dim_product)
    dim_seller = spark.table(config.dim_seller)
    dim_customer = spark.table(config.dim_customer).filter(F.col("is_current"))

    enriched = (
        fact.join(dim_date, "date_key", "inner")
        .join(dim_product, "product_sk", "inner")
        .join(dim_seller, "seller_sk", "inner")
        .join(dim_customer, "customer_sk", "inner")
        .filter(F.col("year") == config.filter_year)
        .filter(F.col("customer_state").isin(*config.filter_states))
    )

    return (
        enriched.groupBy("year_month", "customer_state", "category_name_en")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.count("*").alias("item_count"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
            F.round(F.avg("delivery_duration_days"), 2).alias("avg_delivery_days"),
            F.round(
                F.avg(F.when(F.col("delivery_late"), F.lit(1.0)).otherwise(F.lit(0.0))) * 100,
                2,
            ).alias("late_delivery_pct"),
        )
        .select(
            F.col("year_month").alias("time_period"),
            "customer_state",
            F.col("category_name_en").alias("product_category"),
            "order_count",
            "item_count",
            "revenue",
            "avg_delivery_days",
            "late_delivery_pct",
        )
        .orderBy(F.col("revenue").desc())
    )


def run_star_schema_query(
    spark: SparkSession,
    config: StarSchemaQueryConfig | None = None,
) -> dict:
    config = config or StarSchemaQueryConfig()
    summary = build_star_schema_summary(spark, config)
    top_rows = summary.limit(config.top_n).collect()

    return {
        "task": "star_schema_query",
        "filter_year": config.filter_year,
        "filter_states": list(config.filter_states),
        "summary_group_count": summary.count(),
        "top_n": config.top_n,
        "top_rows_by_revenue": [row.asDict() for row in top_rows],
    }
