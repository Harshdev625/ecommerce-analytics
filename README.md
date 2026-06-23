# GlobalMart — E-Commerce Analytics

Medallion data pipeline on **Databricks** for the [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) dataset.

**Pipeline status:** Phases `01`–`10` orchestration verified on Databricks · Lakeview dashboard + `dbt test` pending  
**Run results:** [`Result.md`](Result.md)

---

## Repository structure

Notebook folders **`01_*` … `10_*`** — run in numeric order:

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
│   ├── 05_gold_analytics/        # window functions & gold metrics
│   ├── 06_gold_observability/    # materialized views, extended gold, streaming, secure views
│   ├── 07_dimensional/           # star schema
│   ├── 08_delta_ops/             # Delta Lake optimization
│   ├── 09_dbt/
│   └── 10_orchestration/         # workflow tasks + runbook
├── config/workflows/             # Databricks job JSON
├── airflow/                      # local Airflow DAGs
├── dashboard/                    # Lakeview SQL
├── tests/                        # silver unit tests
├── dbt/
└── src/
    ├── ingestion/, quality/, transformations/
    ├── spark_performance/, joins/, gold/
    ├── gold_observability/, dimensional/, delta_ops/
    └── orchestration/
```

---

## Dataset

Upload the 8 Olist CSVs to `/Volumes/globalmart/bronze/raw_landing/` after running `config/catalog_setup.sql`.

---

## Databricks workflow

1. Clone repo into **Databricks Repos**.
2. Run `config/catalog_setup.sql`.
3. Upload CSVs.
4. Run notebooks **`01_bronze` → `09_dbt`** in order (see sections below).
5. Edit on PC → `git push` → Databricks **Pull** (never Commit & Push from Databricks UI).

---

## Notebooks by folder

### `01_bronze/` — ingestion

| Notebook | Purpose |
|----------|---------|
| `01_idempotent_ingestion.ipynb` | Batch load 8 CSVs with fingerprint idempotency |
| `02_auto_loader_orders.ipynb` | Auto Loader for orders |
| `03_nested_payments.ipynb` | Nested + flattened payments |
| `04_schema_evolution.ipynb` | Schema evolution + violation log |

### `02_spark_performance/` — plans & optimization

| Notebook | Purpose |
|----------|---------|
| `01_execution_plan_diagnostics.ipynb` | Predicate, join, shuffle anti-patterns |
| `02_skew_detection.ipynb` | Skew analysis and remediation |
| `03_higher_order_functions.ipynb` | Higher-order functions vs explode |

### `03_silver_quality/` — cleaned entities

| Notebook | Purpose |
|----------|---------|
| `01_data_quality_dlq.ipynb` | DQ engine + dead letter queue |
| `02_silver_orders.ipynb` | Silver orders + late arrivals + reconciliation |
| `03_silver_order_items.ipynb` | Order items enrichment |
| `04_silver_customers_sellers.ipynb` | Customers & sellers deduplication |

### `04_joins_cdc/` — joins & change data capture

| Notebook | Purpose |
|----------|---------|
| `01_business_join_questions.ipynb` | Four analytics queries + join justification |
| `02_broadcast_join_control.ipynb` | Join strategy comparison |
| `03_skew_distribution_report.ipynb` | Top skewed keys report |
| `04_cdc_customers_merge.ipynb` | CDC + Delta MERGE on customers |

### `05_gold_analytics/` — window functions & gold metrics

| Notebook | Purpose |
|----------|---------|
| `01_daily_sales_metrics.ipynb` | Daily revenue, cumulative totals, MAs, DoD, rank |
| `02_customer_rfm.ipynb` | RFM quintiles and segments |
| `03_category_growth_streaks.ipynb` | Consecutive month growth streaks |
| `04_customer_summary_merge.ipynb` | Customer lifetime MERGE + soft delete |
| `05_incremental_loader.ipynb` | Watermark incremental bronze → silver |

### `06_gold_observability/` — extended gold & security *(run on Databricks)*

| Notebook | Purpose |
|----------|---------|
| `01_materialized_views.ipynb` | Regular vs materialized view timing |
| `02_gold_aggregations.ipynb` | Daily summary (new/returning) + monthly seller performance |
| `03_streaming_orders.ipynb` | Streaming-style orders table vs bronze count |
| `04_dynamic_views.ipynb` | Column masking + row-level secure views |

**Code:** `src/gold_observability/`

### `07_dimensional/` — star schema

| Notebook | Purpose |
|----------|---------|
| `01_date_dimension.ipynb` | Date dimension 2016–2020 |
| `02_surrogate_key_strategy.ipynb` | SK strategy comparison |
| `03_product_dimension.ipynb` | Product SCD Type 1 |
| `04_seller_dimension.ipynb` | Seller SCD Type 1 |
| `05_customer_dimension_scd2.ipynb` | Customer SCD Type 2 |
| `06_fact_sales.ipynb` | Fact table + validations |
| `07_star_schema_query.ipynb` | Multi-dimension analytics query |

### `08_delta_ops/` — Delta Lake

| Notebook | Purpose |
|----------|---------|
| `01_small_files_optimize.ipynb` | Small files + OPTIMIZE |
| `02_partition_zorder.ipynb` | Partition + Z-ORDER |
| `03_vacuum.ipynb` | VACUUM dry run + execute |
| `04_time_travel.ipynb` | Time travel + RESTORE |
| `05_liquid_clustering.ipynb` | Liquid clustering comparison |

### `09_dbt/` + `dbt/` — dbt on Databricks

| Item | Purpose |
|------|---------|
| `dbt/` | Staging, marts, incremental fact, snapshot, tests |
| `01_dbt_setup_and_run.ipynb` | Install, debug, run, test, snapshot |

Requires `gold.dim_*` tables from `07_dimensional/` for the incremental fact model.

### `10_orchestration/` — pipeline orchestration *(run on Databricks)*

| Notebook | Workflow task |
|----------|----------------|
| `01_bronze_ingestion.ipynb` | Idempotent bronze load |
| `02_quality_checks.ipynb` | DQ on `bronze.orders` |
| `03_silver_transforms.ipynb` | Silver entities + DLQ gate |
| `04_reconciliation.ipynb` | Bronze vs silver reconciliation |
| `05_gold_aggregations.ipynb` | Daily summary + seller performance |
| `06_dimensional_refresh.ipynb` | Star schema rebuild |
| `07_visualization.ipynb` | Dashboard datasets + star query |
| `08_workflow_runbook.ipynb` | Job setup + failure demo |

**Also:** `config/workflows/globalmart_pipeline.job.json` · `airflow/` · `tests/` · `dashboard/lakeview_queries.sql`

**Code:** `src/orchestration/`

---

## Architecture

```text
Bronze → Silver → Gold → Observability → Dimensional → Delta ops → dbt → Orchestration
```

JSON run summaries: `/Volumes/globalmart/metadata/run_reports/`

---

## What's next

| Area | Work |
|------|------|
| Lakeview | Publish dashboard from `dashboard/lakeview_queries.sql` |
| dbt | Confirm `dbt test` in `09_dbt/01` |
| Optional | Failure demo on `00` (`simulate_failure=silver_transforms`) |
| Optional | Local unit tests / Airflow (`airflow/README.md`) |
| Assignment | Final reflection essay |
