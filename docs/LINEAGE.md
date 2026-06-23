# Gold layer lineage

Documented from pipeline code and Unity Catalog structure (Catalog Explorer → **Lineage** tab shows the same graph).

## `globalmart.gold.fact_sales`

| Direction | Object |
|-----------|--------|
| Upstream | `silver.order_items`, `silver.orders` |
| Upstream | `gold.dim_date`, `gold.dim_product`, `gold.dim_seller`, `gold.dim_customer` |
| Downstream | Lakeview dashboard queries, `07_star_schema_query` |

Built in `07_dimensional/06_fact_sales.ipynb` via `src/dimensional/fact_sales.py`.

## `globalmart.gold.daily_sales_summary`

| Direction | Object |
|-----------|--------|
| Upstream | `silver.orders`, `silver.order_items`, `silver.customers`, `silver.sellers` |
| Downstream | Orchestration gold task, observability validation |

Built in `06_gold_observability/02_gold_aggregations.ipynb` via `src/gold_observability/daily_sales_summary.py`.

## `globalmart.gold.dim_customer`

| Direction | Object |
|-----------|--------|
| Upstream | `silver.customers` |
| Downstream | `gold.fact_sales`, secure views, Lakeview (customer state) |

SCD Type 2 history in `07_dimensional/05_customer_dimension_scd2.ipynb`.

## Additional gold objects

| Table | Primary upstream |
|-------|------------------|
| `seller_performance_monthly` | `silver.order_items`, `silver.sellers` |
| `orders_stream` | `bronze.orders` (streaming source) |
| `dim_product` | `silver.products`, category translation tables |
| `dim_seller` | `silver.sellers` |
| `dim_date` | Generated calendar (no upstream tables) |

## dbt lineage

Run on Databricks:

```bash
dbt docs generate --project-dir dbt --profiles-dir dbt --target dev
```

Open `dbt/target/index.html` for the model DAG: sources (`bronze.*`) → staging → intermediate → marts → incremental fact / snapshot.
