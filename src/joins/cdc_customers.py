"""CDC simulation via MERGE on silver.customers."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.joins.business_questions import SilverJoinTables

CDC_OPERATION_COL = "cdc_operation"


@dataclass
class CdcBatchCounts:
    inserts: int = 6
    updates: int = 6
    deletes: int = 6

    @property
    def total(self) -> int:
        return self.inserts + self.updates + self.deletes


def generate_cdc_batch(
    spark: SparkSession,
    customers_table: str,
    counts: CdcBatchCounts | None = None,
) -> tuple[DataFrame, dict]:
    """Build a CDC source batch: inserts, updates, and deletes (18 rows default)."""
    counts = counts or CdcBatchCounts()
    customers = spark.table(customers_table)
    ranked = customers.withColumn("_rn", F.row_number().over(Window.orderBy("customer_id")))

    update_src = (
        ranked.filter((F.col("_rn") > counts.deletes) & (F.col("_rn") <= counts.deletes + counts.updates))
        .drop("_rn")
        .withColumn("customer_city", F.concat(F.col("customer_city"), F.lit(" (CDC Updated)")))
        .withColumn(CDC_OPERATION_COL, F.lit("update"))
    )

    delete_src = (
        ranked.filter(F.col("_rn") <= counts.deletes)
        .select("customer_id")
        .withColumn(CDC_OPERATION_COL, F.lit("delete"))
        .withColumn("customer_city", F.lit(None).cast("string"))
        .withColumn("customer_state", F.lit(None).cast("string"))
        .withColumn("customer_zip_code_prefix", F.lit(None).cast("string"))
    )

    insert_rows = []
    for i in range(counts.inserts):
        row = {
            "customer_id": f"cdc-insert-{uuid.uuid4().hex[:12]}",
            "customer_zip_code_prefix": f"{10000 + i}",
            "customer_city": f"Cdc City {i}",
            "customer_state": "SP",
            CDC_OPERATION_COL: "insert",
        }
        if "customer_unique_id" in customers.columns:
            row["customer_unique_id"] = f"cdc-unique-{i:03d}"
        insert_rows.append(row)

    insert_src = spark.createDataFrame(insert_rows)

    target_cols = [c for c in customers.columns if c != "processed_at"]
    batch = (
        update_src.select(*target_cols, CDC_OPERATION_COL)
        .unionByName(delete_src.select(*target_cols, CDC_OPERATION_COL), allowMissingColumns=True)
        .unionByName(insert_src.select(*target_cols, CDC_OPERATION_COL), allowMissingColumns=True)
    )

    meta = {
        "batch_size": batch.count(),
        "inserts": counts.inserts,
        "updates": counts.updates,
        "deletes": counts.deletes,
        "update_ids": [r["customer_id"] for r in update_src.select("customer_id").collect()],
        "delete_ids": [r["customer_id"] for r in delete_src.select("customer_id").collect()],
        "insert_ids": [r["customer_id"] for r in insert_src.select("customer_id").collect()],
    }
    return batch, meta


def merge_sql_template(target_table: str) -> str:
    return f"""
MERGE INTO {target_table} AS t
USING cdc_batch AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.cdc_operation = 'delete' THEN DELETE
WHEN MATCHED AND s.cdc_operation = 'update' THEN UPDATE SET
  t.customer_city = s.customer_city,
  t.customer_state = s.customer_state,
  t.customer_zip_code_prefix = s.customer_zip_code_prefix,
  t.processed_at = current_timestamp()
WHEN NOT MATCHED AND s.cdc_operation = 'insert' THEN INSERT (
  customer_id, customer_unique_id, customer_zip_code_prefix,
  customer_city, customer_state, processed_at
) VALUES (
  s.customer_id, s.customer_unique_id, s.customer_zip_code_prefix,
  s.customer_city, s.customer_state, current_timestamp()
)
""".strip()


def apply_customer_cdc_merge(
    spark: SparkSession,
    cdc_batch: DataFrame,
    customers_table: str,
) -> None:
    from delta.tables import DeltaTable

    cdc_batch.createOrReplaceTempView("cdc_batch")
    DeltaTable.forName(spark, customers_table).alias("t").merge(
        cdc_batch.alias("s"),
        "t.customer_id = s.customer_id",
    ).whenMatchedDelete(
        condition=f"s.{CDC_OPERATION_COL} = 'delete'"
    ).whenMatchedUpdate(
        condition=f"s.{CDC_OPERATION_COL} = 'update'",
        set={
            "customer_city": "s.customer_city",
            "customer_state": "s.customer_state",
            "customer_zip_code_prefix": "s.customer_zip_code_prefix",
            "processed_at": "current_timestamp()",
        },
    ).whenNotMatchedInsert(
        condition=f"s.{CDC_OPERATION_COL} = 'insert'",
        values={
            "customer_id": "s.customer_id",
            "customer_unique_id": "s.customer_unique_id",
            "customer_zip_code_prefix": "s.customer_zip_code_prefix",
            "customer_city": "s.customer_city",
            "customer_state": "s.customer_state",
            "processed_at": "current_timestamp()",
        },
    ).execute()


def verify_cdc_merge(
    spark: SparkSession,
    customers_table: str,
    meta: dict,
    row_count_before: int,
) -> dict:
    customers = spark.table(customers_table)
    row_count_after = customers.count()

    insert_ids = meta["insert_ids"]
    update_ids = meta["update_ids"]
    delete_ids = meta["delete_ids"]

    inserts_found = customers.filter(F.col("customer_id").isin(insert_ids)).count()
    updates_found = customers.filter(
        F.col("customer_id").isin(update_ids) & F.col("customer_city").contains("(CDC Updated)")
    ).count()
    deletes_confirmed = customers.filter(F.col("customer_id").isin(delete_ids)).count()

    expected_after = row_count_before + meta["inserts"] - meta["deletes"]

    return {
        "row_count_before": row_count_before,
        "row_count_after": row_count_after,
        "expected_row_count_after": expected_after,
        "row_count_matches_expected": row_count_after == expected_after,
        "inserts_applied": inserts_found,
        "inserts_expected": meta["inserts"],
        "updates_applied": updates_found,
        "updates_expected": meta["updates"],
        "deletes_remaining": deletes_confirmed,
        "deletes_expected_removed": meta["deletes"],
        "all_verified": (
            inserts_found == meta["inserts"]
            and updates_found == meta["updates"]
            and deletes_confirmed == 0
            and row_count_after == expected_after
        ),
    }


def run_customer_cdc_simulation(
    spark: SparkSession,
    tables: SilverJoinTables | None = None,
    counts: CdcBatchCounts | None = None,
) -> dict:
    tables = tables or SilverJoinTables()
    counts = counts or CdcBatchCounts()

    row_count_before = spark.table(tables.customers).count()
    batch, meta = generate_cdc_batch(spark, tables.customers, counts)
    apply_customer_cdc_merge(spark, batch, tables.customers)
    verification = verify_cdc_merge(spark, tables.customers, meta, row_count_before)

    return {
        "task": "cdc_customer_merge",
        "target_table": tables.customers,
        "cdc_batch": {
            "total_records": meta["batch_size"],
            "inserts": meta["inserts"],
            "updates": meta["updates"],
            "deletes": meta["deletes"],
        },
        "merge_sql": merge_sql_template(tables.customers),
        "verification": verification,
    }
