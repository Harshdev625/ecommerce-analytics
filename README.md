# GlobalMart — E-Commerce Analytics

End-to-end medallion data pipeline on **Databricks** for the [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) dataset.

Verified metrics, run reports, and test results: [`Result.md`](Result.md)

---

## Highlights

| Area | Result |
|------|--------|
| Star schema | `gold.fact_sales` — **110,197** rows · **R$15.4M** revenue |
| Reconciliation | **all_passed: true** (99,441 order keys) |
| dbt | **9/9** models · **26/26** tests |
| End-to-end pipeline | **7/7** tasks SUCCESS |
| Dashboard | [GlobalMart Sales Analytics (Lakeview)](https://dbc-a54a680a-a023.cloud.databricks.com/dashboardsv3/01f16f20d20b18a78431d3f7d22e6ccc/published?o=7474660156362188) |
| Local verification | **6/6** unit tests · Airflow pattern demos — [`docs/LOCAL_SETUP.md`](docs/LOCAL_SETUP.md) |

---

## Architecture

```text
Bronze → Silver → Gold → Observability → Dimensional → Delta ops → dbt → Orchestration
```

**Catalog:** `globalmart` · **Schemas:** `bronze`, `silver`, `gold`, `metadata`  
**Run reports:** `/Volumes/globalmart/metadata/run_reports/` (JSON per notebook/task)

---

## Pipeline orchestration

Production flow is implemented in `notebooks/10_orchestration/` with reusable code in `src/orchestration/`.

```text
00_run_full_pipeline
  ├── 01_bronze_ingestion      → idempotent CSV load
  ├── 02_quality_checks        → DQ on bronze.orders
  ├── 03_silver_transforms     → silver entities + DLQ gate
  ├── 04_reconciliation        → bronze vs silver (3-level)
  ├── 05_gold_aggregations     → daily summary + seller performance
  ├── 06_dimensional_refresh   → star schema rebuild
  └── 07_visualization         → dashboard datasets
```

**Run on Databricks:** open `00_run_full_pipeline.ipynb` (chains all seven tasks via `dbutils.notebook.run`).

**Widgets:** `pipeline_run_id`, `dry_run`, `simulate_failure` — set `simulate_failure=silver_transforms` to demo downstream skip behavior.

**Workflow job (optional):** `config/workflows/globalmart_pipeline.job.json`  
**Airflow (local):** `airflow/dags/` · pattern demo: `scripts/demo_airflow_patterns.py`

---

## Dashboard

[GlobalMart Sales Analytics (Databricks Lakeview)](https://dbc-a54a680a-a023.cloud.databricks.com/dashboardsv3/01f16f20d20b18a78431d3f7d22e6ccc/published?o=7474660156362188)

Five charts over the gold star schema: revenue trend, revenue by state, delivery performance, top categories, top sellers.

SQL and setup: [`dashboard/lakeview_queries.sql`](dashboard/lakeview_queries.sql) · [`dashboard/README.md`](dashboard/README.md)

---

## Databricks setup

1. Clone this repository into **Databricks Repos**.
2. Run [`config/catalog_setup.sql`](config/catalog_setup.sql) to create the `globalmart` catalog.
3. Upload the eight Olist CSV files to `/Volumes/globalmart/bronze/raw_landing/`.
4. Run notebooks **`01_bronze/` → `10_orchestration/`** in order, or **`00_run_full_pipeline.ipynb`** for end-to-end.
5. Configure `dbt/profiles.yml` locally (gitignored) before running `09_dbt/01_dbt_setup_and_run.ipynb`.
6. Develop on PC → `git push` → Databricks **Pull** (avoid Commit & Push from the Databricks UI).

---

## Local development

PC-only tests and demos live under `local/` (gitignored). Databricks tests remain in `tests/`.

```powershell
cd E:\Projects\ecommerce-analytics
.\scripts\setup_local.ps1
.\scripts\run_local_verification.ps1
```

Optional Airflow UI (Podman):

```powershell
podman compose -f podman/compose.yaml up
```

Full guide: [`docs/LOCAL_SETUP.md`](docs/LOCAL_SETUP.md)

---

## Documentation

| Document | Purpose |
|----------|---------|
| [`Result.md`](Result.md) | Verified run metrics and deliverable sign-off |
| [`docs/LINEAGE.md`](docs/LINEAGE.md) | Gold-layer upstream dependencies |
| [`docs/LOCAL_SETUP.md`](docs/LOCAL_SETUP.md) | venv, tests, Airflow patterns, Podman |
| [`dashboard/README.md`](dashboard/README.md) | Lakeview dashboard |
| [`dbt/README.md`](dbt/README.md) | dbt project commands |
| [`airflow/README.md`](airflow/README.md) | Airflow DAG setup |

---

## Repository layout

```
ecommerce-analytics/
├── README.md
├── Result.md
├── config/
│   ├── catalog_setup.sql
│   └── workflows/globalmart_pipeline.job.json
├── notebooks/          # 01_bronze … 10_orchestration
├── src/                # Python modules per layer
├── dbt/                # Staging, marts, incremental fact, snapshot
├── dashboard/          # Lakeview SQL
├── airflow/dags/       # Local orchestration DAGs
├── tests/              # Databricks unit tests (full DQ rules)
├── scripts/            # Local setup and verification
├── docs/               # Lineage and local setup guides
└── podman/             # Optional Airflow compose
```

---

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

Code: `src/gold_observability/`

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
| `01_dbt_setup_and_run.ipynb` | dbt install, run, test, snapshot, docs generate |

Requires `gold.dim_*` tables from `07_dimensional/`.

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
| `08_workflow_runbook.ipynb` | Workflow and Free Edition notes |

Code: `src/orchestration/`
