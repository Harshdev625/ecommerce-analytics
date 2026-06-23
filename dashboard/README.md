# Lakeview dashboard (`10_orchestration` visualization)

Publish **5 charts** on Databricks using SQL from `lakeview_queries.sql`.

## Steps (Databricks Free Edition)

1. Open **SQL** or **Lakeview** in your workspace.
2. For each query block in `lakeview_queries.sql`, run it against catalog `globalmart`.
3. **Lakeview** → **Create** → **Dashboard**.
4. Add one visualization per query:

| # | Chart type (suggested) | Query section |
|---|----------------------|---------------|
| 1 | Line | Monthly revenue trend |
| 2 | Bar / map | Revenue by customer state (top 10) |
| 3 | Line | Late delivery rate by month |
| 4 | Bar | Top categories by revenue |
| 5 | Bar | Top sellers by revenue |

5. Title the dashboard **GlobalMart Business Overview**.
6. **Publish** and copy the share link for your assignment.

Notebook `07_visualization.ipynb` already materializes the same aggregates to `pipeline_visualization.json` for verification.
