# GlobalMart — E-Commerce Analytics

Medallion data pipeline on **Databricks** for the [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) dataset.

**Pipeline status:** Bronze, Silver, Spark performance, Joins & CDC, and Gold analytics built · Dimensional model and orchestration not started yet  
**Run results:** [`Result.md`](Result.md)

---

## Repository structure

```
ecommerce-analytics/
├── README.md
├── Result.md
├── config/
│   └── catalog_setup.sql
├── notebooks/
│   ├── m01_bronze/              # Raw ingestion
│   ├── m03_silver_quality/      # Quality gates & silver entities
│   ├── m02_spark_performance/   # Query plan & skew analysis
│   ├── m04_joins_cdc/           # Business joins, broadcast control, CDC MERGE
│   └── m05_gold_analytics/      # Gold aggregations and analytics
├── src/
│   ├── ingestion/
│   ├── quality/
│   ├── transformations/
│   ├── spark_performance/
│   ├── joins/
│   └── gold/
└── data/
    ├── raw/
    └── extracted/
```

---

## Dataset

Upload the 8 Olist CSVs to Databricks after catalog setup:

```text
/Volumes/globalmart/bronze/raw_landing/
```

Required files: `olist_orders_dataset.csv`, `olist_order_items_dataset.csv`, `olist_order_payments_dataset.csv`, `olist_order_reviews_dataset.csv`, `olist_products_dataset.csv`, `olist_sellers_dataset.csv`, `olist_customers_dataset.csv`, `product_category_name_translation.csv`

---

## Databricks setup

1. Create a [Databricks Free Edition](https://www.databricks.com/learn/free-edition) workspace.
2. Clone this repo into **Databricks Repos**.
3. Run `config/catalog_setup.sql` — creates `globalmart` catalog, `bronze` / `silver` / `gold` / `metadata` schemas, and volumes.
4. Upload CSVs to `globalmart.bronze.raw_landing`.
5. Run notebooks in order: `m01_bronze` → `m03_silver_quality` → `m02_spark_performance` → `m04_joins_cdc` → `m05_gold_analytics`.

### Workflow

- Edit locally → `git push` → Databricks **Pull**
- Run on Databricks only — do not Commit & Push notebooks from the Databricks UI
- After Pull: close notebook tab, re-open from Repos; restart Python to reload `src/`

---

## Notebooks

### Bronze — `notebooks/m01_bronze/`

| Notebook | Purpose |
|----------|---------|
| `01_idempotent_ingestion.ipynb` | Batch load 8 CSVs with fingerprint idempotency |
| `02_auto_loader_orders.ipynb` | Auto Loader for orders with checkpoint idempotency |
| `03_nested_payments.ipynb` | Nested + flattened payment representations |
| `04_schema_evolution.ipynb` | Orders schema evolution + validation log |

### Silver — `notebooks/m03_silver_quality/`

| Notebook | Purpose |
|----------|---------|
| `01_data_quality_dlq.ipynb` | Config-driven DQ engine + dead letter queue |
| `02_silver_orders.ipynb` | Silver orders, late arrivals, reconciliation |
| `03_silver_order_items.ipynb` | Order items enrichment + invalid price DLQ |
| `04_silver_customers_sellers.ipynb` | Customers & sellers deduplication |

### Spark performance — `notebooks/m02_spark_performance/`

| Notebook | Purpose |
|----------|---------|
| `01_execution_plan_diagnostics.ipynb` | Anti-patterns: predicate, join strategy, shuffle |
| `02_skew_detection.ipynb` | Skew analysis, salting, adaptive optimizer |
| `03_higher_order_functions.ipynb` | Higher-order functions vs explode on nested payments |

### Joins & CDC — `notebooks/m04_joins_cdc/`

| Notebook | Purpose |
|----------|---------|
| `01_business_join_questions.ipynb` | Four analytics queries with join-type justification |
| `02_broadcast_join_control.ipynb` | Default vs sort-merge vs broadcast join comparison |
| `03_skew_distribution_report.ipynb` | Top-10 skewed keys on `seller_id` and `product_id` |
| `04_cdc_customers_merge.ipynb` | CDC batch + Delta MERGE on `silver.customers` |

### Gold analytics — `notebooks/m05_gold_analytics/`

| Notebook | Purpose |
|----------|---------|
| `01_daily_sales_metrics.ipynb` | Daily revenue, cumulative totals, MAs, DoD change, monthly rank |
| `02_customer_rfm.ipynb` | RFM quintile scores and named customer segments |
| `03_category_growth_streaks.ipynb` | Categories with 3+ months consecutive revenue growth |
| `04_customer_summary_merge.ipynb` | Customer lifetime metrics via Delta MERGE + soft delete |
| `05_incremental_loader.ipynb` | Watermark-based incremental `bronze.orders` → `silver.orders_incremental` |

---

## Architecture

```text
Bronze (raw) → Silver (quality & entities) → Gold (analytics) → Orchestration
```

| Layer | What's built |
|-------|----------------|
| **Bronze** | 8 source tables, Auto Loader orders, nested/flat payments, evolved orders |
| **Silver** | Orders, late arrivals, order items, customers, sellers, incremental orders |
| **Gold** | Daily sales, customer RFM, category growth streaks, customer summary |
| **Metadata** | Ingestion log, DQ results, DLQ, reconciliation log, schema violations, pipeline watermarks |

JSON run summaries: `/Volumes/globalmart/metadata/run_reports/`

---

## What's next

| Area | Planned work |
|------|----------------|
| **Dimensional model** | Star schema — date/product/seller/customer dims + fact table |
| **Delta ops** | OPTIMIZE, partitioning, Z-order, VACUUM, time travel |
| **dbt** | Staging and mart models on Databricks |
| **Orchestration** | Databricks Workflows, Airflow, unit tests, dashboard |
