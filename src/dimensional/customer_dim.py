"""Customer dimension (SCD Type 2) with versioned surrogate keys."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CUSTOMER_DIM_COLUMNS = (
    "customer_sk",
    "customer_id",
    "customer_city",
    "customer_state",
    "customer_zip_code_prefix",
    "version_number",
    "is_current",
    "effective_start_date",
    "effective_end_date",
)

DEFAULT_EFFECTIVE_START = "2016-01-01"


@dataclass
class CustomerDimConfig:
    source_table: str = "globalmart.silver.customers"
    target_table: str = "globalmart.gold.dim_customer"
    initial_effective_start: str = DEFAULT_EFFECTIVE_START


def build_initial_customer_dimension(
    spark: SparkSession,
    config: CustomerDimConfig | None = None,
) -> DataFrame:
    """Load all silver customers as version 1, current."""
    config = config or CustomerDimConfig()
    customers = spark.table(config.source_table)

    tracked = ["customer_id", "customer_city", "customer_state", "customer_zip_code_prefix"]
    available = [c for c in tracked if c in customers.columns]

    return (
        customers.select(*available)
        .withColumn("version_number", F.lit(1))
        .withColumn("is_current", F.lit(True))
        .withColumn("effective_start_date", F.lit(config.initial_effective_start).cast("date"))
        .withColumn("effective_end_date", F.lit(None).cast("date"))
        .withColumn(
            "customer_sk",
            F.row_number().over(Window.partitionBy(F.lit(1)).orderBy("customer_id")),
        )
        .select(*CUSTOMER_DIM_COLUMNS)
        .withColumn("processed_at", F.current_timestamp())
    )


def apply_scd2_customer_changes(
    spark: SparkSession,
    config: CustomerDimConfig | None = None,
    change_count: int = 6,
    city_suffix: str = " (SCD2 Updated)",
) -> dict:
    """Close current rows for N customers and insert new versions with updated city."""
    config = config or CustomerDimConfig()
    dim = spark.table(config.target_table)

    current_rows = (
        dim.filter(F.col("is_current"))
        .orderBy("customer_id")
        .limit(change_count)
        .cache()
    )
    change_ids = [r["customer_id"] for r in current_rows.select("customer_id").collect()]
    if not change_ids:
        current_rows.unpersist()
        return {"customers_changed": 0, "versions_added": 0, "change_ids": []}

    max_sk = dim.agg(F.max("customer_sk")).collect()[0][0] or 0

    close_keys = current_rows.select("customer_sk")

    from delta.tables import DeltaTable

    DeltaTable.forName(spark, config.target_table).alias("t").merge(
        close_keys.alias("s"),
        "t.customer_sk = s.customer_sk",
    ).whenMatchedUpdate(
        condition="t.is_current = true",
        set={
            "is_current": "false",
            "effective_end_date": "current_date()",
            "processed_at": "current_timestamp()",
        },
    ).execute()

    new_versions = (
        current_rows.withColumn("customer_city", F.concat(F.col("customer_city"), F.lit(city_suffix)))
        .withColumn("version_number", F.col("version_number") + 1)
        .withColumn("is_current", F.lit(True))
        .withColumn("effective_start_date", F.current_date())
        .withColumn("effective_end_date", F.lit(None).cast("date"))
        .withColumn(
            "customer_sk",
            F.row_number().over(Window.partitionBy(F.lit(1)).orderBy("customer_id")) + F.lit(max_sk),
        )
        .select(*CUSTOMER_DIM_COLUMNS)
        .withColumn("processed_at", F.current_timestamp())
    )

    new_versions.write.format("delta").mode("append").saveAsTable(config.target_table)
    current_rows.unpersist()

    return {
        "customers_changed": len(change_ids),
        "versions_added": len(change_ids),
        "change_ids": change_ids,
    }


def get_customer_version_history(
    spark: SparkSession,
    customer_id: str,
    config: CustomerDimConfig | None = None,
) -> DataFrame:
    config = config or CustomerDimConfig()
    return (
        spark.table(config.target_table)
        .filter(F.col("customer_id") == customer_id)
        .orderBy("version_number")
    )


def run_customer_dimension(
    spark: SparkSession,
    config: CustomerDimConfig | None = None,
    change_count: int = 6,
) -> dict:
    config = config or CustomerDimConfig()

    initial = build_initial_customer_dimension(spark, config)
    initial.write.format("delta").mode("overwrite").saveAsTable(config.target_table)

    row_count_after_initial = spark.table(config.target_table).count()
    scd2_meta = apply_scd2_customer_changes(spark, config, change_count=change_count)

    written = spark.table(config.target_table)
    sample_id = scd2_meta["change_ids"][0] if scd2_meta["change_ids"] else None
    version_history = []
    if sample_id:
        version_history = [
            row.asDict()
            for row in get_customer_version_history(spark, sample_id, config).collect()
        ]

    import json as _json

    version_history = _json.loads(_json.dumps(version_history, default=str))

    return {
        "task": "dim_customer_scd2",
        "source_table": config.source_table,
        "target_table": config.target_table,
        "row_count_after_initial": row_count_after_initial,
        "row_count_after_scd2": written.count(),
        "current_customers": written.filter(F.col("is_current")).count(),
        "customers_with_multiple_versions": (
            written.groupBy("customer_id")
            .agg(F.max("version_number").alias("max_version"))
            .filter(F.col("max_version") > 1)
            .count()
        ),
        "scd2_simulation": scd2_meta,
        "sample_customer_version_history": {
            "customer_id": sample_id,
            "versions": version_history,
        },
        "sk_strategy": "monotonic_sequence_per_version",
    }
