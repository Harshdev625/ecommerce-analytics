# GlobalMart — E-Commerce Analytics

Medallion data pipeline on **Databricks** for the [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) dataset.

## Repository structure

```
ecommerce-analytics/
├── README.md
├── config/
│   └── catalog_setup.sql    # Unity Catalog setup (run in Databricks)
└── data/
    ├── raw/                 # Source files (not committed)
    └── extracted/           # Extracted CSVs (not committed)
```

Additional folders (`notebooks/`, `src/`, `dbt/`, etc.) are added as the pipeline is built.

## Dataset

Download the dataset from Kaggle and place it locally:

| Location | Contents |
|----------|----------|
| `data/raw/` | Kaggle zip and/or CSV files |
| `data/extracted/` | Extracted CSVs (optional) |

The `data/` directory is gitignored. Upload the CSVs to Databricks after running the catalog setup:

```text
/Volumes/globalmart/bronze/raw_landing/
```

**Required source files:**

- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `olist_customers_dataset.csv`
- `product_category_name_translation.csv`

## Databricks setup

1. Create a [Databricks Free Edition](https://www.databricks.com/learn/free-edition) workspace.
2. Clone this repo into **Databricks Repos**.
3. Run `config/catalog_setup.sql` in the SQL editor to create the `globalmart` catalog, schemas (`bronze`, `silver`, `gold`, `metadata`), and the `raw_landing` volume.
4. Upload the 8 CSV files to `globalmart.bronze.raw_landing` via Catalog Explorer.
5. Begin bronze-layer ingestion in `notebooks/m01_bronze/` (added in the next phase).

## Architecture

Bronze (raw ingestion) → Silver (quality & entities) → Gold (analytics & star schema) → Orchestration (Workflows, dbt, Airflow).
