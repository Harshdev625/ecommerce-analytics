"""Business join questions on silver tables."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass
class SilverJoinTables:
    orders: str = "globalmart.silver.orders"
    orders_late_arrivals: str = "globalmart.silver.orders_late_arrivals"
    customers: str = "globalmart.silver.customers"
    sellers: str = "globalmart.silver.sellers"
    order_items: str = "globalmart.silver.order_items"


def load_all_orders(spark: SparkSession, tables: SilverJoinTables | None = None) -> DataFrame:
    tables = tables or SilverJoinTables()
    on_time = spark.table(tables.orders)
    late = spark.table(tables.orders_late_arrivals)
    return on_time.unionByName(late, allowMissingColumns=True)


def orders_without_customers(spark: SparkSession, tables: SilverJoinTables | None = None) -> tuple[int, str, str]:
    """Q1: count orders with no matching row in silver.customers."""
    tables = tables or SilverJoinTables()
    orders = load_all_orders(spark, tables).select("order_id", "customer_id")
    customers = spark.table(tables.customers).select("customer_id")

    orphan_orders = orders.join(customers, "customer_id", "left_anti")
    count = orphan_orders.select("order_id").distinct().count()
    join_type = "left_anti"
    justification = (
        "Left anti join keeps orders whose customer_id is absent from silver.customers "
        "without duplicating order rows from a inner/null-filter pattern."
    )
    return count, join_type, justification


def sellers_never_sold(spark: SparkSession, tables: SilverJoinTables | None = None) -> tuple[int, str, str]:
    """Q2: count sellers with no rows in silver.order_items."""
    tables = tables or SilverJoinTables()
    sellers = spark.table(tables.sellers).select("seller_id")
    active_sellers = spark.table(tables.order_items).select("seller_id").distinct()

    count = sellers.join(active_sellers, "seller_id", "left_anti").count()
    join_type = "left_anti"
    justification = (
        "Left anti join returns sellers that never appear in order_items — "
        "equivalent to NOT IN / NOT EXISTS but idiomatic in Spark."
    )
    return count, join_type, justification


def top_delivered_orders_by_value(
    spark: SparkSession,
    tables: SilverJoinTables | None = None,
    limit: int = 10,
) -> tuple[DataFrame, str, str]:
    """Q3: top delivered orders by line-item value with customer city and item count."""
    tables = tables or SilverJoinTables()
    orders = load_all_orders(spark, tables).filter(F.col("order_status") == "delivered")
    customers = spark.table(tables.customers).select("customer_id", "customer_city")
    order_values = (
        spark.table(tables.order_items)
        .groupBy("order_id")
        .agg(
            F.sum("line_total_value").alias("total_order_value"),
            F.count("*").alias("item_count"),
        )
    )

    result = (
        orders.select("order_id", "customer_id")
        .join(customers, "customer_id", "inner")
        .join(order_values, "order_id", "inner")
        .select("order_id", "customer_city", "total_order_value", "item_count")
        .orderBy(F.col("total_order_value").desc())
        .limit(limit)
    )
    join_type = "inner"
    justification = (
        "Inner joins ensure each result row has a matching customer and at least one line item; "
        "delivered orders without items or customer master data are excluded from the ranking."
    )
    return result, join_type, justification


def customers_in_main_and_late_arrivals(
    spark: SparkSession,
    tables: SilverJoinTables | None = None,
) -> tuple[DataFrame, int, str, str]:
    """Q4: customers present in silver.customers and in orders_late_arrivals."""
    tables = tables or SilverJoinTables()
    main_customers = spark.table(tables.customers).select("customer_id", "customer_city", "customer_state")
    late_order_customers = (
        spark.table(tables.orders_late_arrivals)
        .select("customer_id")
        .distinct()
    )

    result = main_customers.join(late_order_customers, "customer_id", "inner")
    count = result.count()
    join_type = "inner"
    justification = (
        "Inner join returns only customers who exist in the master table and also appear "
        "on at least one late-arriving order — the overlap set for both populations."
    )
    return result, count, join_type, justification


def run_all_business_questions(
    spark: SparkSession,
    tables: SilverJoinTables | None = None,
) -> dict:
    tables = tables or SilverJoinTables()

    q1_count, q1_join, q1_why = orders_without_customers(spark, tables)
    q2_count, q2_join, q2_why = sellers_never_sold(spark, tables)
    q3_df, q3_join, q3_why = top_delivered_orders_by_value(spark, tables)
    q4_df, q4_count, q4_join, q4_why = customers_in_main_and_late_arrivals(spark, tables)

    late_arrivals_rows = spark.table(tables.orders_late_arrivals).count()

    return {
        "task": "business_join_questions",
        "questions": [
            {
                "id": 1,
                "question": "How many orders have no matching customer?",
                "join_type": q1_join,
                "justification": q1_why,
                "result": {"orders_without_customer": q1_count},
            },
            {
                "id": 2,
                "question": "How many sellers have never sold anything?",
                "join_type": q2_join,
                "justification": q2_why,
                "result": {"sellers_never_sold": q2_count},
            },
            {
                "id": 3,
                "question": "Top 10 delivered orders by value (with city and item count)",
                "join_type": q3_join,
                "justification": q3_why,
                "result": [row.asDict() for row in q3_df.collect()],
            },
            {
                "id": 4,
                "question": "Customers in both silver.customers and orders_late_arrivals",
                "join_type": q4_join,
                "justification": q4_why,
                "late_arrivals_table_empty": late_arrivals_rows == 0,
                "result": {
                    "customer_count": q4_count,
                    "sample": [row.asDict() for row in q4_df.limit(5).collect()],
                },
            },
        ],
    }
