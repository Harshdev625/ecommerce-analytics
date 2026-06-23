"""Unit tests for silver layer transformation logic."""

from __future__ import annotations

import unittest
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from src.quality.engine import get_failed_records
from src.quality.rules import ORDERS_DQ_RULES
from src.transformations.silver_orders import (
    LATE_ARRIVAL_DAYS,
    SilverOrdersConfig,
    build_silver_orders,
    classify_arrival_status,
)


class TestSilverTransformations(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("silver-unit-tests")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_late_arrival_boundary_on_time(self) -> None:
        ingested = datetime(2024, 1, 10)
        event = datetime(2024, 1, 5)
        df = self.spark.createDataFrame(
            [(ingested, event)],
            ["_ingested_at", "order_purchase_timestamp_ts"],
        )
        result = df.select(
            classify_arrival_status("_ingested_at", "order_purchase_timestamp_ts").alias(
                "arrival_status"
            )
        ).collect()[0]["arrival_status"]
        self.assertEqual(result, "on_time")

    def test_late_arrival_boundary_very_late(self) -> None:
        ingested = datetime(2024, 3, 1)
        event = datetime(2024, 1, 1)
        df = self.spark.createDataFrame(
            [(ingested, event)],
            ["_ingested_at", "order_purchase_timestamp_ts"],
        )
        result = df.select(
            classify_arrival_status("_ingested_at", "order_purchase_timestamp_ts").alias(
                "arrival_status"
            )
        ).collect()[0]["arrival_status"]
        self.assertEqual(result, "very_late")
        self.assertGreater(
            (ingested - event).days,
            30,
            "fixture should be beyond very_late threshold",
        )
        self.assertLessEqual(LATE_ARRIVAL_DAYS, 7)

    def test_null_customer_id_fails_quality_gate(self) -> None:
        schema = StructType(
            [
                StructField("order_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("order_status", StringType(), True),
                StructField("order_purchase_timestamp", StringType(), True),
            ]
        )
        df = self.spark.createDataFrame(
            [
                ("o1", None, "delivered", "2018-01-01 00:00:00"),
                ("o2", "c2", "delivered", "2018-01-02 00:00:00"),
            ],
            schema,
        )
        failed = get_failed_records(
            self.spark, df, ORDERS_DQ_RULES, severities=("critical",)
        )
        failed_ids = {row.order_id for row in failed.collect()}
        self.assertIn("o1", failed_ids)
        self.assertNotIn("o2", failed_ids)

    def test_build_silver_orders_idempotent_row_count(self) -> None:
        schema = StructType(
            [
                StructField("order_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("order_status", StringType(), True),
                StructField("order_purchase_timestamp", StringType(), True),
                StructField("order_approved_at", StringType(), True),
                StructField("order_delivered_carrier_date", StringType(), True),
                StructField("order_delivered_customer_date", StringType(), True),
                StructField("order_estimated_delivery_date", StringType(), True),
                StructField("_ingested_at", TimestampType(), True),
            ]
        )
        ts = datetime(2018, 6, 1, 12, 0, 0)
        rows = [
            (
                "ord-1",
                "cust-1",
                "delivered",
                "2018-05-01 10:00:00",
                "2018-05-01 11:00:00",
                "2018-05-05 08:00:00",
                "2018-05-07 18:00:00",
                "2018-05-08 18:00:00",
                ts,
            )
        ]
        source = self.spark.createDataFrame(rows, schema)
        config = SilverOrdersConfig()
        first = build_silver_orders(self.spark, config, source_df=source)
        second = build_silver_orders(self.spark, config, source_df=source)
        self.assertEqual(first.count(), second.count())
        self.assertEqual(first.count(), 1)
        self.assertIn("delivery_late", first.columns)


if __name__ == "__main__":
    unittest.main()
