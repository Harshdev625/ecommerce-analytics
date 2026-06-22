"""CDC simulation via MERGE on silver.customers."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

from src.joins.business_questions import SilverJoinTables

CDC_OPERATION_COL = "cdc_operation"
MUTABLE_COLS = ("customer_city", "customer_state", "customer_zip_code_prefix")
METADATA_COLS = frozenset(
    {CDC_OPERATION_COL, "processed_at", "_ingested_at", "ingested_at"}
)


@dataclass
class CdcBatchCounts:
    inserts: int = 6
    updates: int = 6
    deletes: int = 6

    @property
    def total(self) -> int:
        return self.inserts + self.updates + self.deletes


def _data_columns(table_columns: list[str]) -> list[str]:
    """Target data columns (exclude lineage metadata and CDC flag)."""
    return [c for c in table_columns if c not in METADATA_COLS]


def _schema_map(df: DataFrame) -> dict[str, DataType]:
    return {field.name: field.dataType for field in df.schema.fields}


def _with_null_columns(
    df: DataFrame, columns: list[str], schema_map: dict[str, DataType]
) -> DataFrame:
    out = df
    for col in columns:
        if col not in out.columns:
            out = out.withColumn(col, F.lit(None).cast(schema_map[col]))
    return out


def _insert_cell_value(name: str, dtype: DataType, index: int):
    if name == "customer_id":
        return f"cdc-insert-{uuid.uuid4().hex[:12]}"
    if name == "customer_zip_code_prefix":
        return str(10000 + index)
    if name == "customer_city":
        return f"Cdc City {index}"
    if name == "customer_state":
        return "SP"
    if isinstance(dtype, (TimestampType, DateType)):
        return None
    if isinstance(dtype, BooleanType):
        return None
    if isinstance(dtype, (IntegerType, LongType, ShortType)):
        return None
    if isinstance(dtype, (DoubleType, FloatType, DecimalType)):
        return None
    return None


def _cdc_batch_schema(customers: DataFrame, data_cols: list[str]) -> StructType:
    return StructType(
        [customers.schema[c] for c in data_cols]
        + [StructField(CDC_OPERATION_COL, StringType(), False)]
    )


def generate_cdc_batch(
    spark: SparkSession,
    customers_table: str,
    counts: CdcBatchCounts | None = None,
) -> tuple[DataFrame, dict]:
    """Build a CDC source batch: inserts, updates, and deletes (18 rows default)."""
    counts = counts or CdcBatchCounts()
    customers = spark.table(customers_table)
    data_cols = _data_columns(customers.columns)
    col_types = _schema_map(customers)

    ranked = customers.withColumn(
        "_rn",
        F.row_number().over(Window.partitionBy(F.lit(1)).orderBy("customer_id")),
    )

    update_src = (
        ranked.filter((F.col("_rn") > counts.deletes) & (F.col("_rn") <= counts.deletes + counts.updates))
        .drop("_rn")
        .withColumn("customer_city", F.concat(F.col("customer_city"), F.lit(" (CDC Updated)")))
        .withColumn(CDC_OPERATION_COL, F.lit("update"))
    )

    delete_src = (
        ranked.filter(F.col("_rn") <= counts.deletes)
        .select("customer_id")
        .transform(
            lambda df: _with_null_columns(
                df, [c for c in data_cols if c != "customer_id"], col_types
            )
        )
        .withColumn(CDC_OPERATION_COL, F.lit("delete"))
    )

    insert_rows: list[dict] = []
    for i in range(counts.inserts):
        row = {CDC_OPERATION_COL: "insert"}
        for col in data_cols:
            row[col] = _insert_cell_value(col, col_types[col], i)
        insert_rows.append(row)

    insert_src = spark.createDataFrame(insert_rows, schema=_cdc_batch_schema(customers, data_cols))

    batch = (
        update_src.select(*data_cols, CDC_OPERATION_COL)
        .unionByName(delete_src.select(*data_cols, CDC_OPERATION_COL))
        .unionByName(insert_src.select(*data_cols, CDC_OPERATION_COL))
    )

    meta = {
        "batch_size": batch.count(),
        "inserts": counts.inserts,
        "updates": counts.updates,
        "deletes": counts.deletes,
        "data_columns": data_cols,
        "update_ids": [r["customer_id"] for r in update_src.select("customer_id").collect()],
        "delete_ids": [r["customer_id"] for r in delete_src.select("customer_id").collect()],
        "insert_ids": [r["customer_id"] for r in insert_src.select("customer_id").collect()],
    }
    return batch, meta


def merge_sql_template(target_table: str, data_cols: list[str] | None = None) -> str:
    cols = data_cols or ["customer_id", "customer_zip_code_prefix", "customer_city", "customer_state"]
    update_sets = ",\n  ".join(
        f"t.{c} = s.{c}" for c in cols if c in MUTABLE_COLS
    )
    insert_col_list = ", ".join(cols + ["processed_at"])
    insert_val_list = ", ".join(f"s.{c}" for c in cols) + ", current_timestamp()"

    return f"""
MERGE INTO {target_table} AS t
USING cdc_batch AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.cdc_operation = 'delete' THEN DELETE
WHEN MATCHED AND s.cdc_operation = 'update' THEN UPDATE SET
  {update_sets},
  t.processed_at = current_timestamp()
WHEN NOT MATCHED AND s.cdc_operation = 'insert' THEN INSERT (
  {insert_col_list}
) VALUES (
  {insert_val_list}
)
""".strip()


def apply_customer_cdc_merge(
    spark: SparkSession,
    cdc_batch: DataFrame,
    customers_table: str,
) -> None:
    from delta.tables import DeltaTable

    data_cols = _data_columns(spark.table(customers_table).columns)
    update_set = {c: f"s.{c}" for c in data_cols if c in MUTABLE_COLS}
    update_set["processed_at"] = "current_timestamp()"

    insert_values = {c: f"s.{c}" for c in data_cols}
    insert_values["processed_at"] = "current_timestamp()"

    DeltaTable.forName(spark, customers_table).alias("t").merge(
        cdc_batch.alias("s"),
        "t.customer_id = s.customer_id",
    ).whenMatchedDelete(
        condition=f"s.{CDC_OPERATION_COL} = 'delete'"
    ).whenMatchedUpdate(
        condition=f"s.{CDC_OPERATION_COL} = 'update'",
        set=update_set,
    ).whenNotMatchedInsert(
        condition=f"s.{CDC_OPERATION_COL} = 'insert'",
        values=insert_values,
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
        "merge_sql": merge_sql_template(tables.customers, meta["data_columns"]),
        "verification": verification,
    }
