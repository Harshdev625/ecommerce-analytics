"""Product dimension (SCD Type 1) with category conformance checks."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.dimensional.surrogate_keys import hash_surrogate_key

CATEGORY_CONFORMANCE_TABLE = "globalmart.metadata.category_conformance_log"

PRODUCT_DIM_COLUMNS = (
    "product_sk",
    "product_id",
    "product_category_name",
    "category_name_en",
    "product_name_length",
    "product_description_length",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
)


@dataclass
class ProductDimConfig:
    bronze_products: str = "globalmart.bronze.products"
    bronze_translation: str = "globalmart.bronze.product_category_translation"
    target_table: str = "globalmart.gold.dim_product"
    conformance_table: str = CATEGORY_CONFORMANCE_TABLE


def ensure_conformance_table(spark: SparkSession, table: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          violation_id STRING,
          check_name STRING,
          violation_detail STRING,
          record_count BIGINT,
          checked_at TIMESTAMP
        )
        USING DELTA
        COMMENT 'Category translation conformance violations'
        """
    )


def build_product_dimension_source(
    spark: SparkSession,
    config: ProductDimConfig | None = None,
) -> DataFrame:
    config = config or ProductDimConfig()
    products = spark.table(config.bronze_products)
    translation = spark.table(config.bronze_translation)

    # Olist source CSV misspells product_name_length as product_name_lenght.
    name_length = (
        F.col("product_name_length")
        if "product_name_length" in products.columns
        else F.col("product_name_lenght")
    )

    return (
        products.join(
            translation.select("product_category_name", "product_category_name_english"),
            on="product_category_name",
            how="left",
        )
        .withColumn(
            "category_name_en",
            F.coalesce(F.col("product_category_name_english"), F.lit("unknown")),
        )
        .withColumn("product_sk", hash_surrogate_key(F.col("product_id")))
        .withColumn("product_name_length", name_length)
        .select(*PRODUCT_DIM_COLUMNS)
        .withColumn("processed_at", F.current_timestamp())
    )


def validate_category_conformance(
    spark: SparkSession,
    dim_df: DataFrame,
    config: ProductDimConfig | None = None,
) -> tuple[DataFrame, dict]:
    """Check English categories vs reference and one-to-one PT→EN mapping."""
    config = config or ProductDimConfig()
    translation = spark.table(config.bronze_translation)
    checked_at = datetime.now(timezone.utc)

    english_in_dim = (
        dim_df.select("category_name_en")
        .distinct()
        .filter(F.col("category_name_en") != "unknown")
    )
    english_in_ref = translation.select("product_category_name_english").distinct()
    orphan_english = english_in_dim.join(
        english_in_ref,
        english_in_dim.category_name_en == english_in_ref.product_category_name_english,
        "left_anti",
    )

    multi_en = (
        translation.groupBy("product_category_name")
        .agg(F.countDistinct("product_category_name_english").alias("english_count"))
        .filter(F.col("english_count") > 1)
    )

    orphan_count = orphan_english.count()
    multi_count = multi_en.count()

    violations = []
    if orphan_count > 0:
        violations.append(
            {
                "violation_id": str(uuid.uuid4()),
                "check_name": "english_category_not_in_reference",
                "violation_detail": "English category in dim_product missing from translation table",
                "record_count": orphan_count,
                "checked_at": checked_at,
            }
        )
    if multi_count > 0:
        violations.append(
            {
                "violation_id": str(uuid.uuid4()),
                "check_name": "portuguese_maps_to_multiple_english",
                "violation_detail": "Portuguese product_category_name maps to >1 English translation",
                "record_count": multi_count,
                "checked_at": checked_at,
            }
        )

    if violations:
        ensure_conformance_table(spark, config.conformance_table)
        spark.createDataFrame(violations).write.format("delta").mode("append").saveAsTable(
            config.conformance_table
        )

    summary = {
        "english_categories_not_in_reference": orphan_count,
        "portuguese_with_multiple_english": multi_count,
        "conformance_passed": orphan_count == 0 and multi_count == 0,
        "orphan_english_samples": [r["category_name_en"] for r in orphan_english.limit(5).collect()],
        "multi_map_samples": [r.asDict() for r in multi_en.limit(5).collect()],
    }
    return dim_df, summary


def apply_product_dimension_merge(
    spark: SparkSession,
    source: DataFrame,
    config: ProductDimConfig | None = None,
) -> None:
    from delta.tables import DeltaTable

    config = config or ProductDimConfig()
    data_cols = [c for c in PRODUCT_DIM_COLUMNS if c != "product_id"]
    update_set = {c: f"s.{c}" for c in data_cols}
    update_set["processed_at"] = "current_timestamp()"

    insert_values = {c: f"s.{c}" for c in data_cols}
    insert_values["product_id"] = "s.product_id"
    insert_values["processed_at"] = "current_timestamp()"

    if not spark.catalog.tableExists(config.target_table):
        source.write.format("delta").mode("overwrite").saveAsTable(config.target_table)
        return

    DeltaTable.forName(spark, config.target_table).alias("t").merge(
        source.alias("s"),
        "t.product_id = s.product_id",
    ).whenMatchedUpdate(set=update_set).whenNotMatchedInsert(values=insert_values).execute()


def run_product_dimension(
    spark: SparkSession,
    config: ProductDimConfig | None = None,
) -> dict:
    config = config or ProductDimConfig()
    source = build_product_dimension_source(spark, config)
    row_count_before = (
        spark.table(config.target_table).count()
        if spark.catalog.tableExists(config.target_table)
        else 0
    )

    apply_product_dimension_merge(spark, source, config)
    written = spark.table(config.target_table)
    _, conformance = validate_category_conformance(spark, written, config)

    return {
        "task": "dim_product",
        "target_table": config.target_table,
        "conformance_table": config.conformance_table,
        "row_count_before": row_count_before,
        "row_count_after": written.count(),
        "distinct_products": written.select("product_id").distinct().count(),
        "category_conformance": conformance,
        "sk_strategy": "deterministic_hash",
    }
