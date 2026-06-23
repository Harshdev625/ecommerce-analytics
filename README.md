# GlobalMart — E-Commerce Analytics

End-to-end medallion data pipeline on Databricks for the [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) dataset. Pipeline outputs and verification details are documented in [`Result.md`](Result.md).

## Architecture

```text
Bronze → Silver → Gold → Observability → Dimensional → Delta ops → dbt → Orchestration
```

JSON run summaries are stored at `/Volumes/globalmart/metadata/run_reports/`.

## Dashboard

[GlobalMart Sales Analytics (Databricks Lakeview)](https://dbc-a54a680a-a023.cloud.databricks.com/dashboardsv3/01f16f20d20b18a78431d3f7d22e6ccc/published?o=7474660156362188)

The dashboard covers revenue trends, geographic distribution, delivery performance, category mix, and seller rankings over the gold star schema. Query definitions and setup instructions are in [`dashboard/`](dashboard/).

## Getting started

1. Clone this repository into Databricks Repos.
2. Execute `config/catalog_setup.sql` to provision the `globalmart` catalog.
3. Upload the eight Olist CSV files to `/Volumes/globalmart/bronze/raw_landing/`.
4. Run notebooks in `01_bronze/` through `10_orchestration/` in order, or execute `00_run_full_pipeline.ipynb` for a full pipeline run.
5. Develop locally, push to the remote repository, and pull changes in Databricks Repos.

## Repository layout

```
ecommerce-analytics/
├── README.md
├── Result.md
├── config/catalog_setup.sql
├── notebooks/
│   ├── 01_bronze/
│   ├── 02_spark_performance/
│   ├── 03_silver_quality/
│   ├── 04_joins_cdc/
│   ├── 05_gold_analytics/
│   ├── 06_gold_observability/
│   ├── 07_dimensional/
│   ├── 08_delta_ops/
│   ├── 09_dbt/
│   └── 10_orchestration/
├── config/workflows/
├── airflow/
├── dashboard/
├── tests/
├── dbt/
└── src/
```

## Notebooks

### `01_bronze/` — ingestion

| Notebook | Purpose |
|----------|---------|
| `01_idempotent_ingestion.ipynb` | Batch load with fingerprint-based idempotency |
| `02_auto_loader_orders.ipynb` | Auto Loader ingestion for orders |
| `03_nested_payments.ipynb` | Nested and flattened payment structures |
| `04_schema_evolution.ipynb` | Schema evolution and violation logging |

### `02_spark_performance/` — Spark optimization

| Notebook | Purpose |
|----------|---------|
| `01_execution_plan_diagnostics.ipynb` | Predicate, join, and shuffle diagnostics |
| `02_skew_detection.ipynb` | Data skew analysis and remediation |
| `03_higher_order_functions.ipynb` | Higher-order functions versus explode patterns |

### `03_silver_quality/` — silver layer

| Notebook | Purpose |
|----------|---------|
| `01_data_quality_dlq.ipynb` | Data quality rules and dead-letter queue |
| `02_silver_orders.ipynb` | Orders, late arrivals, and reconciliation |
| `03_silver_order_items.ipynb` | Order item enrichment |
| `04_silver_customers_sellers.ipynb` | Customer and seller deduplication |

### `04_joins_cdc/` — joins and CDC

| Notebook | Purpose |
|----------|---------|
| `01_business_join_questions.ipynb` | Business analytics queries with join rationale |
| `02_broadcast_join_control.ipynb` | Join strategy comparison |
| `03_skew_distribution_report.ipynb` | Skew distribution reporting |
| `04_cdc_customers_merge.ipynb` | Customer CDC with Delta MERGE |

### `05_gold_analytics/` — gold metrics

| Notebook | Purpose |
|----------|---------|
| `01_daily_sales_metrics.ipynb` | Daily revenue, moving averages, and rankings |
| `02_customer_rfm.ipynb` | RFM segmentation |
| `03_category_growth_streaks.ipynb` | Category growth streak analysis |
| `04_customer_summary_merge.ipynb` | Customer lifetime summary with MERGE |
| `05_incremental_loader.ipynb` | Watermark-based incremental loading |

### `06_gold_observability/` — observability and security

| Notebook | Purpose |
|----------|---------|
| `01_materialized_views.ipynb` | View materialization performance |
| `02_gold_aggregations.ipynb` | Daily and monthly gold aggregations |
| `03_streaming_orders.ipynb` | Streaming orders table |
| `04_dynamic_views.ipynb` | Column masking and row-level security |

Supporting modules: `src/gold_observability/`

### `07_dimensional/` — dimensional model

| Notebook | Purpose |
|----------|---------|
| `01_date_dimension.ipynb` | Date dimension |
| `02_surrogate_key_strategy.ipynb` | Surrogate key strategy comparison |
| `03_product_dimension.ipynb` | Product dimension (SCD Type 1) |
| `04_seller_dimension.ipynb` | Seller dimension (SCD Type 1) |
| `05_customer_dimension_scd2.ipynb` | Customer dimension (SCD Type 2) |
| `06_fact_sales.ipynb` | Sales fact table and validation |
| `07_star_schema_query.ipynb` | Multi-dimensional analytics query |

### `08_delta_ops/` — Delta Lake operations

| Notebook | Purpose |
|----------|---------|
| `01_small_files_optimize.ipynb` | Small-file compaction and OPTIMIZE |
| `02_partition_zorder.ipynb` | Partitioning and Z-ORDER |
| `03_vacuum.ipynb` | VACUUM execution |
| `04_time_travel.ipynb` | Time travel and RESTORE |
| `05_liquid_clustering.ipynb` | Liquid clustering evaluation |

### `09_dbt/` — dbt transformations

| Item | Purpose |
|------|---------|
| `dbt/` | Staging models, marts, incremental fact, snapshots, and tests |
| `01_dbt_setup_and_run.ipynb` | dbt installation, execution, and testing |

The incremental fact model requires dimensional tables from `07_dimensional/`.

### `10_orchestration/` — pipeline orchestration

| Notebook | Purpose |
|----------|---------|
| `00_run_full_pipeline.ipynb` | End-to-end pipeline runner |
| `01_bronze_ingestion.ipynb` | Bronze ingestion task |
| `02_quality_checks.ipynb` | Bronze data quality checks |
| `03_silver_transforms.ipynb` | Silver transformations |
| `04_reconciliation.ipynb` | Bronze-to-silver reconciliation |
| `05_gold_aggregations.ipynb` | Gold aggregation refresh |
| `06_dimensional_refresh.ipynb` | Dimensional model refresh |
| `07_visualization.ipynb` | Dashboard dataset preparation |
| `08_workflow_runbook.ipynb` | Workflow deployment notes |

Additional assets: `config/workflows/globalmart_pipeline.job.json`, `airflow/`, `tests/`, and [`dashboard/`](dashboard/).

Supporting modules: `src/orchestration/`
