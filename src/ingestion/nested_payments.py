"""Nested and flattened representations of order payments."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass
class NestedPaymentsConfig:
    source_table: str = "globalmart.bronze.order_payments"
    nested_table: str = "globalmart.bronze.order_payments_nested"
    flattened_table: str = "globalmart.bronze.order_payments_flattened"


def build_nested_payments(df: DataFrame) -> DataFrame:
    payment_struct = F.struct(
        F.col("payment_sequential"),
        F.col("payment_type"),
        F.col("payment_installments"),
        F.col("payment_value"),
    )
    return (
        df.groupBy("order_id")
        .agg(
            F.collect_list(payment_struct).alias("payments"),
            F.sum("payment_value").alias("total_payment_value"),
            F.count("*").alias("num_payment_methods"),
        )
    )


def build_flattened_payments(nested_df: DataFrame) -> DataFrame:
    return (
        nested_df.select(
            "order_id",
            "total_payment_value",
            "num_payment_methods",
            F.explode("payments").alias("payment"),
        )
        .select(
            "order_id",
            "total_payment_value",
            "num_payment_methods",
            F.col("payment.payment_sequential").alias("payment_sequential"),
            F.col("payment.payment_type").alias("payment_type"),
            F.col("payment.payment_installments").alias("payment_installments"),
            F.col("payment.payment_value").alias("payment_value"),
        )
    )


def build_payments_report(
    spark: SparkSession,
    config: NestedPaymentsConfig,
) -> dict:
    source = spark.table(config.source_table)
    nested = spark.table(config.nested_table)
    flattened = spark.table(config.flattened_table)

    source_count = source.count()
    distinct_orders = source.select("order_id").distinct().count()
    nested_count = nested.count()
    flattened_count = flattened.count()

    max_methods = nested.agg(F.max("num_payment_methods").alias("max")).collect()[0]["max"]
    multi_order_count = nested.filter(F.col("num_payment_methods") > 1).count()
    multi_pct = round(100.0 * multi_order_count / nested_count, 2) if nested_count else 0.0

    return {
        "source_table": config.source_table,
        "nested_table": config.nested_table,
        "flattened_table": config.flattened_table,
        "source_row_count": source_count,
        "distinct_order_count": distinct_orders,
        "nested_row_count": nested_count,
        "flattened_row_count": flattened_count,
        "nested_matches_distinct_orders": nested_count == distinct_orders,
        "flattened_matches_source": flattened_count == source_count,
        "max_payment_methods_per_order": int(max_methods),
        "orders_with_multiple_payment_methods": multi_order_count,
        "pct_orders_multiple_payment_methods": multi_pct,
    }


def write_nested_payments(
    spark: SparkSession,
    config: NestedPaymentsConfig | None = None,
) -> NestedPaymentsConfig:
    config = config or NestedPaymentsConfig()
    source = spark.table(config.source_table)

    nested = build_nested_payments(source)
    nested.write.format("delta").mode("overwrite").saveAsTable(config.nested_table)

    flattened = build_flattened_payments(nested)
    flattened.write.format("delta").mode("overwrite").saveAsTable(config.flattened_table)

    return config
