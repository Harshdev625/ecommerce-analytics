# GlobalMart dbt project (Milestone 9)

Rebuilds silver-style staging, gold marts, an incremental fact, and a customer snapshot on Databricks Unity Catalog.

## Prerequisites

- Bronze tables loaded (`globalmart.bronze.*`)
- Gold dimensions from M6 (`globalmart.gold.dim_*`) — required for `fact_sales_incremental`
- SQL warehouse or serverless endpoint HTTP path

## Setup (local or Databricks notebook)

```bash
pip install -r dbt/requirements.txt
cp dbt/profiles.yml.example dbt/profiles.yml
```

Set environment variables:

```bash
export DATABRICKS_HOST="https://<workspace>.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<warehouse-id>"
export DATABRICKS_TOKEN="<personal-access-token>"
```

## Task 9.1 — debug both targets

```bash
cd dbt
dbt debug --target dev
dbt debug --target prod
dbt source freshness --target dev
```

## Task 9.2 — staging + marts

```bash
dbt run --target dev
dbt docs generate --target dev
```

Models land in:

| Layer | Schema (dev target) |
|-------|---------------------|
| Staging views | `globalmart.dbt_dev_staging` |
| Marts / fact | `globalmart.dbt_dev_marts` |
| Snapshots | `globalmart.dbt_dev_snapshots` |

Prod uses `dbt_prod_*` schemas when `--target prod`.

## Task 9.3 — incremental fact

```bash
dbt run --select fact_sales_incremental --target dev
# Re-run with no new bronze data — row count unchanged (idempotent merge)
dbt run --select fact_sales_incremental --target dev
```

## Task 9.4 — snapshot

Simulate a customer attribute change in bronze, refresh staging, then:

```bash
dbt snapshot --target dev
```

Compare `dbt_dev_snapshots.snap_customers` to manual SCD2 in `gold.dim_customer`.

## Task 9.5 — tests

```bash
dbt test --target dev
```

Includes built-in schema tests, referential integrity to gold dims, and singular test `assert_delivered_orders_positive_revenue`.

## Macros

`normalize_city` and `normalize_state` in `macros/normalize_location.sql` — used in `stg_customers` and `stg_sellers`.
