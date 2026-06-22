"""Transform bronze orders into enriched silver orders."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

LATE_ARRIVAL_DAYS = 7


@dataclass
class SilverOrdersConfig:
    bronze_table: str = "globalmart.bronze.orders"
    silver_table: str = "globalmart.silver.orders"
    late_arrivals_table: str = "globalmart.silver.orders_late_arrivals"


def classify_arrival_status(ingested_at_col: str, event_ts_col: str) -> F.Column:
    delay_days = F.datediff(F.col(ingested_at_col), F.col(event_ts_col))
    return (
        F.when(delay_days <= LATE_ARRIVAL_DAYS, F.lit("on_time"))
        .when(delay_days <= 30, F.lit("late_arriving"))
        .otherwise(F.lit("very_late"))
    )


def build_silver_orders(
    spark: SparkSession,
    config: SilverOrdersConfig | None = None,
    source_df: DataFrame | None = None,
) -> DataFrame:
    config = config or SilverOrdersConfig()
    bronze = source_df if source_df is not None else spark.table(config.bronze_table)

    # Keep latest version per order (handles schema-evolution re-appends)
    w = Window.partitionBy("order_id").orderBy(
        F.col("order_channel").isNull(),
        F.col("_ingested_at").desc_nulls_last(),
    )
    bronze = (
        bronze.withColumn("_dedupe_rank", F.row_number().over(w))
        .filter(F.col("_dedupe_rank") == 1)
        .drop("_dedupe_rank")
    )

    ts_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    parsed = bronze
    for c in ts_cols:
        if c in bronze.columns:
            parsed = parsed.withColumn(f"{c}_ts", F.to_timestamp(F.col(c)))

    return (
        parsed.withColumn(
            "delivery_duration_days",
            F.datediff(
                F.col("order_delivered_customer_date_ts"),
                F.col("order_delivered_carrier_date_ts"),
            ),
        )
        .withColumn(
            "approval_time_hours",
            F.round(
                (
                    F.col("order_approved_at_ts").cast("long")
                    - F.col("order_purchase_timestamp_ts").cast("long")
                )
                / 3600,
                2,
            ),
        )
        .withColumn(
            "delivery_late",
            F.col("order_delivered_customer_date_ts")
            > F.col("order_estimated_delivery_date_ts"),
        )
        .withColumn("order_year", F.year(F.col("order_purchase_timestamp_ts")))
        .withColumn("order_month", F.month(F.col("order_purchase_timestamp_ts")))
        .withColumn("order_day_of_week", F.dayofweek(F.col("order_purchase_timestamp_ts")))
        .withColumn(
            "arrival_status",
            classify_arrival_status("_ingested_at", "order_purchase_timestamp_ts"),
        )
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("silver_layer_version", F.lit("v1"))
    )


def split_late_arrivals(
    silver_df: DataFrame,
    config: SilverOrdersConfig,
) -> tuple[DataFrame, DataFrame]:
    late = silver_df.filter(F.col("arrival_status") != "on_time")
    on_time = silver_df.filter(F.col("arrival_status") == "on_time")
    return on_time, late
