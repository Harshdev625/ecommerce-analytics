"""Customer dimension (SCD Type 2) with versioned surrogate keys."""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.joins.business_questions import SilverJoinTables, load_all_orders

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
    source_tables: SilverJoinTables = field(default_factory=SilverJoinTables)


def _delivered_customer_ids(
    spark: SparkSession,
    source_tables: SilverJoinTables,
) -> DataFrame:
    return (
        load_all_orders(spark, source_tables)
        .filter(F.col("order_status") == "delivered")
        .select("customer_id")
        .distinct()
    )


def _build_orphan_customer_rows(
    spark: SparkSession,
    orphan_ids: DataFrame,
    dim: DataFrame,
    config: CustomerDimConfig,
) -> DataFrame:
    """Version-1 current rows for customer_ids absent from dim_customer."""
    silver = spark.table(config.source_table)
    orphans = orphan_ids.join(silver, "customer_id", "left")

    max_sk = int(dim.agg(F.max("customer_sk")).collect()[0][0] or 0)
    sk_type = dim.schema["customer_sk"].dataType

    return (
        orphans.withColumn(
            "_rn",
            F.row_number().over(Window.partitionBy(F.lit(1)).orderBy("customer_id")),
        )
        .withColumn("customer_sk", (F.col("_rn") + F.lit(max_sk)).cast(sk_type))
        .withColumn("customer_city", F.coalesce(F.col("customer_city"), F.lit("UNKNOWN")))
        .withColumn("customer_state", F.coalesce(F.col("customer_state"), F.lit("UN")))
        .withColumn(
            "customer_zip_code_prefix",
            F.coalesce(F.col("customer_zip_code_prefix"), F.lit("00000")),
        )
        .withColumn("version_number", F.lit(1))
        .withColumn("is_current", F.lit(True))
        .withColumn("effective_start_date", F.lit(config.initial_effective_start).cast("date"))
        .withColumn("effective_end_date", F.lit(None).cast("date"))
        .select(*CUSTOMER_DIM_COLUMNS)
        .withColumn("processed_at", F.current_timestamp())
    )


def build_initial_customer_dimension(
    spark: SparkSession,
    config: CustomerDimConfig | None = None,
) -> DataFrame:
    """Load all silver customers as version 1, current."""
    config = config or CustomerDimConfig()
    customers = spark.table(config.source_table)

    tracked = ["customer_id", "customer_city", "customer_state", "customer_zip_code_prefix"]
    available = [c for c in tracked if c in customers.columns]

    base = customers.select(*available)
    delivered_ids = _delivered_customer_ids(spark, config.source_tables)
    orphan_ids = delivered_ids.join(base.select("customer_id"), "customer_id", "left_anti")
    if orphan_ids.limit(1).count():
        stubs = (
            orphan_ids.withColumn("customer_city", F.lit("UNKNOWN"))
            .withColumn("customer_state", F.lit("UN"))
            .withColumn("customer_zip_code_prefix", F.lit("00000"))
            .select(*available)
        )
        base = base.unionByName(stubs)

    return (
        base
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

    snapshot_cols = [
        "customer_sk",
        "customer_id",
        "customer_city",
        "customer_state",
        "customer_zip_code_prefix",
        "version_number",
    ]
    # Collect the small change set once (serverless does not support .cache()/PERSIST).
    rows = (
        dim.filter(F.col("is_current"))
        .orderBy("customer_id")
        .limit(change_count)
        .select(*snapshot_cols)
        .collect()
    )
    change_ids = [r["customer_id"] for r in rows]
    if not change_ids:
        return {"customers_changed": 0, "versions_added": 0, "change_ids": []}

    max_sk = int(dim.agg(F.max("customer_sk")).collect()[0][0] or 0)
    sk_type = spark.table(config.target_table).schema["customer_sk"].dataType

    close_keys = spark.createDataFrame(
        [(r["customer_sk"],) for r in rows],
        ["customer_sk"],
    ).withColumn("customer_sk", F.col("customer_sk").cast(sk_type))

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

    sorted_rows = sorted(rows, key=lambda r: r["customer_id"])
    new_version_rows = [
        (
            max_sk + idx,
            r["customer_id"],
            r["customer_city"] + city_suffix,
            r["customer_state"],
            r["customer_zip_code_prefix"],
            int(r["version_number"]) + 1,
            True,
        )
        for idx, r in enumerate(sorted_rows, start=1)
    ]
    new_versions = (
        spark.createDataFrame(
            new_version_rows,
            "customer_sk int, customer_id string, customer_city string, "
            "customer_state string, customer_zip_code_prefix string, "
            "version_number int, is_current boolean",
        )
        .withColumn("customer_sk", F.col("customer_sk").cast(sk_type))
        .withColumn("effective_start_date", F.current_date())
        .withColumn("effective_end_date", F.lit(None).cast("date"))
        .select(*CUSTOMER_DIM_COLUMNS)
        .withColumn("processed_at", F.current_timestamp())
    )

    new_versions.write.format("delta").mode("append").saveAsTable(config.target_table)

    return {
        "customers_changed": len(change_ids),
        "versions_added": len(change_ids),
        "change_ids": change_ids,
    }


def ensure_one_current_customer_version(
    spark: SparkSession,
    config: CustomerDimConfig | None = None,
) -> dict:
    """Ensure each customer_id has exactly one is_current=true row (max version wins)."""
    from delta.tables import DeltaTable

    config = config or CustomerDimConfig()
    if not spark.catalog.tableExists(config.target_table):
        return {"repaired_no_current": 0, "repaired_multi_current": 0}

    dim = spark.table(config.target_table)
    w_desc = Window.partitionBy("customer_id").orderBy(F.col("version_number").desc())

    no_current_ids = (
        dim.groupBy("customer_id")
        .agg(F.max(F.when(F.col("is_current"), 1).otherwise(0)).alias("has_current"))
        .filter(F.col("has_current") == 0)
        .select("customer_id")
    )
    promote_sk = (
        dim.join(no_current_ids, "customer_id", "inner")
        .withColumn("_rank", F.row_number().over(w_desc))
        .filter(F.col("_rank") == 1)
        .select("customer_sk")
    )

    demote_sk = (
        dim.filter(F.col("is_current"))
        .withColumn("_rank", F.row_number().over(w_desc))
        .filter(F.col("_rank") > 1)
        .select("customer_sk")
    )

    promote_count = promote_sk.count()
    demote_count = demote_sk.count()

    delta = DeltaTable.forName(spark, config.target_table).alias("t")
    if promote_count:
        delta.merge(
            promote_sk.alias("s"),
            "t.customer_sk = s.customer_sk",
        ).whenMatchedUpdate(
            set={
                "is_current": "true",
                "effective_end_date": "cast(null as date)",
                "processed_at": "current_timestamp()",
            }
        ).execute()

    if demote_count:
        delta.merge(
            demote_sk.alias("s"),
            "t.customer_sk = s.customer_sk",
        ).whenMatchedUpdate(
            set={
                "is_current": "false",
                "processed_at": "current_timestamp()",
            }
        ).execute()

    return {
        "repaired_no_current": promote_count,
        "repaired_multi_current": demote_count,
    }


def ensure_delivered_order_customers_in_dim(
    spark: SparkSession,
    config: CustomerDimConfig | None = None,
) -> dict:
    """Append dim rows for delivered-order customer_ids missing from dim_customer."""
    config = config or CustomerDimConfig()
    if not spark.catalog.tableExists(config.target_table):
        return {"orphan_customers_merged": 0}

    dim = spark.table(config.target_table)
    delivered_ids = _delivered_customer_ids(spark, config.source_tables)
    dim_ids = dim.select("customer_id").distinct()
    orphan_ids = delivered_ids.join(dim_ids, "customer_id", "left_anti")
    orphan_count = orphan_ids.count()
    if orphan_count:
        rows = _build_orphan_customer_rows(spark, orphan_ids, dim, config)
        rows.write.format("delta").mode("append").saveAsTable(config.target_table)

    return {"orphan_customers_merged": orphan_count}


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
    current_repair = ensure_one_current_customer_version(spark, config)

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
        "current_flag_repair": current_repair,
        "sample_customer_version_history": {
            "customer_id": sample_id,
            "versions": version_history,
        },
        "sk_strategy": "monotonic_sequence_per_version",
    }
