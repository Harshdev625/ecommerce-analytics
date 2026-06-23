"""Parameterized pipeline tasks for Databricks Workflows."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.dimensional.customer_dim import run_customer_dimension
from src.dimensional.date_dim import run_date_dimension
from src.dimensional.fact_sales import run_fact_sales
from src.dimensional.product_dim import run_product_dimension
from src.dimensional.seller_dim import run_seller_dimension
from src.dimensional.star_schema_query import run_star_schema_query
from src.gold_observability.daily_sales_summary import run_daily_sales_summary
from src.gold_observability.seller_performance import run_seller_performance_monthly
from src.ingestion.idempotent_loader import IngestionConfig, build_ingestion_summary, ingest_landing_zone
from src.orchestration.dashboard_queries import build_dashboard_datasets
from src.orchestration.params import PipelineParams
from src.quality.dlq import route_to_dlq
from src.quality.engine import get_failed_records, run_quality_checks
from src.quality.reconciliation import RECONCILIATION_LOG_TABLE, run_reconciliation
from src.quality.rules import ORDERS_DQ_RULES
from src.transformations.silver_entities import (
    SilverEntitiesConfig,
    build_silver_customers,
    build_silver_order_items,
    build_silver_sellers,
)
from src.transformations.silver_orders import SilverOrdersConfig, build_silver_orders, split_late_arrivals


def _dedupe_bronze_orders(spark: SparkSession, bronze_table: str):
    bronze = spark.table(bronze_table)
    window = Window.partitionBy("order_id").orderBy(
        F.col("order_channel").isNull(),
        F.col("_ingested_at").desc_nulls_last(),
    )
    return (
        bronze.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def run_bronze_ingestion_task(
    spark: SparkSession,
    dbutils,
    params: PipelineParams,
) -> dict:
    """Bronze ingestion requires dbutils for volume file listing."""
    config = IngestionConfig()
    if params.dry_run:
        summary = build_ingestion_summary(spark, config).collect()
        return {
            "landing_path": config.landing_path,
            "dry_run": True,
            "tables": [row.asDict() for row in summary],
        }

    run_id, results = ingest_landing_zone(spark, dbutils, config=config)
    return {
        "ingestion_run_id": run_id,
        "files_processed": len(results),
        "ingested": sum(1 for r in results if r.status == "INGESTED"),
        "skipped": sum(1 for r in results if r.status == "SKIPPED"),
        "results": [r.__dict__ for r in results],
    }


def run_quality_checks_task(spark: SparkSession, params: PipelineParams) -> dict:
    config = SilverOrdersConfig()
    bronze_deduped = _dedupe_bronze_orders(spark, config.bronze_table)
    outcome = run_quality_checks(
        spark,
        bronze_deduped,
        ORDERS_DQ_RULES,
        table_name=config.bronze_table,
    )
    return {
        "quality_run_id": outcome.run_id,
        "table_name": outcome.table_name,
        "overall_status": outcome.overall_status,
        "critical_passed": outcome.critical_passed,
        "rule_count": len(outcome.results),
        "failed_rules": [r["rule_id"] for r in outcome.results if not r["passed"]],
    }


def run_silver_transforms_task(spark: SparkSession, params: PipelineParams) -> dict:
    orders_cfg = SilverOrdersConfig()
    entities_cfg = SilverEntitiesConfig()

    bronze_deduped = _dedupe_bronze_orders(spark, orders_cfg.bronze_table)
    failed = get_failed_records(
        spark, bronze_deduped, ORDERS_DQ_RULES, severities=("critical",)
    )
    if not params.dry_run:
        route_to_dlq(
            spark,
            failed,
            source_table=orders_cfg.bronze_table,
            failure_reason="pipeline_silver_gate",
        )

    valid_keys = bronze_deduped.join(failed.select("order_id"), on="order_id", how="left_anti")
    silver = build_silver_orders(spark, orders_cfg, source_df=valid_keys)
    on_time, late = split_late_arrivals(silver, orders_cfg)

    order_items = build_silver_order_items(spark, entities_cfg)
    customers, cust_stats = build_silver_customers(spark, entities_cfg)
    sellers, seller_stats = build_silver_sellers(spark, entities_cfg)

    if not params.dry_run:
        on_time.write.format("delta").mode("overwrite").saveAsTable(orders_cfg.silver_table)
        late.write.format("delta").mode("overwrite").saveAsTable(orders_cfg.late_arrivals_table)
        order_items.write.format("delta").mode("overwrite").saveAsTable(entities_cfg.silver_order_items)
        customers.write.format("delta").mode("overwrite").saveAsTable(entities_cfg.silver_customers)
        sellers.write.format("delta").mode("overwrite").saveAsTable(entities_cfg.silver_sellers)

    return {
        "silver_orders": orders_cfg.silver_table,
        "silver_orders_count": on_time.count(),
        "late_arrivals_count": late.count(),
        "silver_order_items_count": order_items.count(),
        "customer_stats": cust_stats,
        "seller_stats": seller_stats,
    }


def run_reconciliation_task(spark: SparkSession, params: PipelineParams) -> dict:
    orders_cfg = SilverOrdersConfig()
    bronze_deduped = _dedupe_bronze_orders(spark, orders_cfg.bronze_table)
    silver = spark.table(orders_cfg.silver_table)

    source_keys = bronze_deduped.select("order_id").distinct()
    target_keys = silver.select("order_id").distinct()

    if params.dry_run:
        return {
            "dry_run": True,
            "source_distinct": source_keys.count(),
            "target_distinct": target_keys.count(),
        }

    reconciliation_id = run_reconciliation(
        spark,
        source_keys,
        target_keys,
        source_table=orders_cfg.bronze_table,
        target_table=orders_cfg.silver_table,
        key_column="order_id",
    )
    log_rows = (
        spark.table(RECONCILIATION_LOG_TABLE)
        .filter(F.col("reconciliation_id") == reconciliation_id)
        .collect()
    )
    return {
        "reconciliation_id": reconciliation_id,
        "levels": [row.asDict() for row in log_rows],
        "all_passed": all(row.passed for row in log_rows),
    }


def run_gold_aggregations_task(spark: SparkSession, params: PipelineParams) -> dict:
    if params.dry_run:
        return {
            "dry_run": True,
            "daily_sales_summary_table": "globalmart.gold.daily_sales_summary",
            "seller_performance_table": "globalmart.gold.seller_performance_monthly",
        }

    daily = run_daily_sales_summary(spark)
    sellers = run_seller_performance_monthly(spark, sample_year=2018, sample_month=1)
    return {
        "daily_sales_summary": daily,
        "seller_performance": sellers,
    }


def run_dimensional_refresh_task(spark: SparkSession, params: PipelineParams) -> dict:
    if params.dry_run:
        return {
            "dry_run": True,
            "dimensions": [
                "globalmart.gold.dim_date",
                "globalmart.gold.dim_product",
                "globalmart.gold.dim_seller",
                "globalmart.gold.dim_customer",
                "globalmart.gold.fact_sales",
            ],
        }

    date_report = run_date_dimension(spark)
    product_report = run_product_dimension(spark)
    seller_report = run_seller_dimension(spark)
    customer_report = run_customer_dimension(spark)
    fact_report = run_fact_sales(spark)

    return {
        "date_dimension": date_report,
        "product_dimension": product_report,
        "seller_dimension": seller_report,
        "customer_dimension": customer_report,
        "fact_sales": fact_report,
    }


def run_visualization_task(spark: SparkSession, params: PipelineParams) -> dict:
    star = run_star_schema_query(spark)
    charts = build_dashboard_datasets(spark)
    return {
        "star_schema_query": star,
        "dashboard_datasets": charts,
        "lakeview_note": "Import SQL from dashboard/lakeview_queries.sql into a Lakeview dashboard",
    }
