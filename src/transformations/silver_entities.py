"""Silver order items, customers, and sellers."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dataclass
class SilverEntitiesConfig:
    bronze_order_items: str = "globalmart.bronze.order_items"
    bronze_products: str = "globalmart.bronze.products"
    bronze_translation: str = "globalmart.bronze.product_category_translation"
    bronze_customers: str = "globalmart.bronze.customers"
    bronze_sellers: str = "globalmart.bronze.sellers"
    silver_order_items: str = "globalmart.silver.order_items"
    silver_customers: str = "globalmart.silver.customers"
    silver_sellers: str = "globalmart.silver.sellers"


def build_silver_order_items(spark: SparkSession, config: SilverEntitiesConfig | None = None) -> DataFrame:
    config = config or SilverEntitiesConfig()
    items = spark.table(config.bronze_order_items)
    products = spark.table(config.bronze_products)
    translation = spark.table(config.bronze_translation)

    enriched = (
        items.withColumn("price", F.col("price").cast("double"))
        .withColumn("freight_value", F.coalesce(F.col("freight_value").cast("double"), F.lit(0.0)))
        .withColumn("line_total_value", F.col("price") + F.col("freight_value"))
        .join(products.select("product_id", "product_category_name"), on="product_id", how="left")
        .join(translation, on="product_category_name", how="left")
        .withColumn(
            "category_name_en",
            F.coalesce(F.col("product_category_name_english"), F.lit("unknown")),
        )
    )

    w = Window.partitionBy("order_id", "order_item_id").orderBy(F.col("price").desc())
    return (
        enriched.withColumn("_dup_rank", F.row_number().over(w))
        .filter(F.col("_dup_rank") == 1)
        .drop("_dup_rank")
        .withColumn("processed_at", F.current_timestamp())
    )


def invalid_price_items(df: DataFrame) -> DataFrame:
    return df.filter((F.col("price").isNull()) | (F.col("price") <= 0))


def build_silver_customers(spark: SparkSession, config: SilverEntitiesConfig | None = None) -> tuple[DataFrame, dict]:
    config = config or SilverEntitiesConfig()
    src = spark.table(config.bronze_customers)
    records_in = src.count()

    cleaned = (
        src.withColumn("customer_city", F.initcap(F.trim(F.col("customer_city"))))
        .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
        .withColumn("customer_zip_code_prefix", F.trim(F.col("customer_zip_code_prefix")))
    )
    w = Window.partitionBy("customer_id").orderBy(F.col("customer_city"))
    deduped = (
        cleaned.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("processed_at", F.current_timestamp())
    )
    stats = {
        "records_in": records_in,
        "records_out": deduped.count(),
        "duplicates_removed": records_in - deduped.count(),
    }
    return deduped, stats


def build_silver_sellers(spark: SparkSession, config: SilverEntitiesConfig | None = None) -> tuple[DataFrame, dict]:
    config = config or SilverEntitiesConfig()
    src = spark.table(config.bronze_sellers)
    records_in = src.count()

    cleaned = (
        src.withColumn("seller_city", F.initcap(F.trim(F.col("seller_city"))))
        .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
        .withColumn("seller_zip_code_prefix", F.trim(F.col("seller_zip_code_prefix")))
    )
    w = Window.partitionBy("seller_id").orderBy(F.col("seller_city"))
    deduped = (
        cleaned.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("processed_at", F.current_timestamp())
    )
    duplicate_seller_ids = (
        src.groupBy("seller_id").count().filter(F.col("count") > 1).count()
    )
    stats = {
        "records_in": records_in,
        "records_out": deduped.count(),
        "duplicate_seller_ids_in_source": duplicate_seller_ids,
        "seller_id_unique_in_silver": deduped.select("seller_id").distinct().count() == deduped.count(),
    }
    return deduped, stats
