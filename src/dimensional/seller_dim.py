"""Seller dimension (SCD Type 1) with hash surrogate keys."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.dimensional.surrogate_keys import hash_surrogate_key

SELLER_DIM_COLUMNS = (
    "seller_sk",
    "seller_id",
    "seller_city",
    "seller_state",
    "seller_zip_code_prefix",
)


@dataclass
class SellerDimConfig:
    source_table: str = "globalmart.silver.sellers"
    target_table: str = "globalmart.gold.dim_seller"


def build_seller_dimension_source(
    spark: SparkSession,
    config: SellerDimConfig | None = None,
) -> DataFrame:
    config = config or SellerDimConfig()
    sellers = spark.table(config.source_table)

    return (
        sellers.withColumn("seller_sk", hash_surrogate_key(F.col("seller_id"), salt="seller"))
        .select(*SELLER_DIM_COLUMNS)
        .withColumn("processed_at", F.current_timestamp())
    )


def apply_seller_dimension_merge(
    spark: SparkSession,
    source: DataFrame,
    config: SellerDimConfig | None = None,
) -> None:
    from delta.tables import DeltaTable

    config = config or SellerDimConfig()
    data_cols = [c for c in SELLER_DIM_COLUMNS if c != "seller_id"]
    update_set = {c: f"s.{c}" for c in data_cols}
    update_set["processed_at"] = "current_timestamp()"

    insert_values = {"seller_id": "s.seller_id"}
    insert_values.update({c: f"s.{c}" for c in data_cols})
    insert_values["processed_at"] = "current_timestamp()"

    if not spark.catalog.tableExists(config.target_table):
        source.write.format("delta").mode("overwrite").saveAsTable(config.target_table)
        return

    DeltaTable.forName(spark, config.target_table).alias("t").merge(
        source.alias("s"),
        "t.seller_id = s.seller_id",
    ).whenMatchedUpdate(set=update_set).whenNotMatchedInsert(values=insert_values).execute()


def run_seller_dimension(
    spark: SparkSession,
    config: SellerDimConfig | None = None,
) -> dict:
    config = config or SellerDimConfig()
    source = build_seller_dimension_source(spark, config)
    row_count_before = (
        spark.table(config.target_table).count()
        if spark.catalog.tableExists(config.target_table)
        else 0
    )

    apply_seller_dimension_merge(spark, source, config)
    written = spark.table(config.target_table)

    return {
        "task": "dim_seller",
        "source_table": config.source_table,
        "target_table": config.target_table,
        "row_count_before": row_count_before,
        "row_count_after": written.count(),
        "distinct_sellers": written.select("seller_id").distinct().count(),
        "sk_strategy": "deterministic_hash",
    }
