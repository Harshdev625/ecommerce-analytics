# GlobalMart dbt project

Rebuilds staging, gold marts, an incremental fact, and a customer snapshot on Databricks Unity Catalog.

## Prerequisites

- Bronze tables loaded (`globalmart.bronze.*`)
- Gold dimensions from `07_dimensional/` (`globalmart.gold.dim_*`)
- SQL warehouse HTTP path + token in `dbt/profiles.yml`

## Setup

```bash
pip install -r dbt/requirements.txt
cp dbt/profiles.yml.example dbt/profiles.yml   # then add token
```

Host must be **without** `https://` prefix, e.g. `dbc-xxxxx.cloud.databricks.com`.

## Commands

```bash
cd dbt
dbt debug --target dev
dbt debug --target prod
dbt source freshness --target dev
dbt run --target dev
dbt test --target dev
dbt run --select fact_sales_incremental --target dev
dbt snapshot --target dev
```

Or run `notebooks/09_dbt/01_dbt_setup_and_run.ipynb` on Databricks.

**Sign-off:** run `dbt test --target dev` and record results in [`Result.md`](../Result.md#dbt).

## Output schemas (dev target)

| Layer | Schema |
|-------|--------|
| Staging | `globalmart.dbt_dev_staging` |
| Marts / fact | `globalmart.dbt_dev_marts` |
| Snapshots | `globalmart.dbt_dev` |

## Macros

`normalize_city` and `normalize_state` in `macros/normalize_location.sql` — used in `stg_customers` and `stg_sellers`.
