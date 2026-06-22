# GlobalMart — E-Commerce Analytics

Medallion data pipeline on **Databricks** for the [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) dataset.

**Pipeline status:** Bronze, Silver, and Spark performance layers built · Gold and orchestration not started yet  
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
│   └── m02_spark_performance/   # Query plan & skew analysis
├── src/
│   ├── ingestion/
│   ├── quality/
│   ├── transformations/
│   └── spark_performance/
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
5. Run notebooks: `m01_bronze` → `m03_silver_quality` → `m02_spark_performance` (silver tables needed for performance notebooks).

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

---

## Architecture

```text
Bronze (raw) → Silver (quality & entities) → Gold (analytics) → Orchestration
```

| Layer | What's built |
|-------|----------------|
| **Bronze** | 8 source tables, Auto Loader orders, nested/flat payments, evolved orders |
| **Silver** | Orders, late arrivals, order items, customers, sellers |
| **Metadata** | Ingestion log, DQ results, DLQ, reconciliation log, schema violations |
| **Gold** | Not started |

JSON run summaries: `/Volumes/globalmart/metadata/run_reports/`

---

## What's next

| Area | Planned work |
|------|----------------|
| **Joins & CDC** | Business join queries, broadcast control, customer MERGE |
| **Gold analytics** | Daily sales, RFM, seller performance, aggregations |
| **Dimensional model** | Star schema — date/product/seller/customer dims + fact table |
| **Delta ops** | OPTIMIZE, partitioning, Z-order, VACUUM, time travel |
| **dbt** | Staging and mart models on Databricks |
| **Orchestration** | Databricks Workflows, Airflow, unit tests, dashboard |
