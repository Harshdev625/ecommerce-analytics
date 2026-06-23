# Lakeview Dashboard

## Overview

[GlobalMart Sales Analytics](https://dbc-a54a680a-a023.cloud.databricks.com/dashboardsv3/01f16f20d20b18a78431d3f7d22e6ccc/published?o=7474660156362188) is a Databricks Lakeview dashboard built on the `globalmart` gold layer.

## Visualizations

| # | Visualization | SQL source |
|---|---------------|------------|
| 1 | Monthly revenue trend | Query 1 in `lakeview_queries.sql` |
| 2 | Revenue by state (top 10) | Query 2 |
| 3 | Late delivery rate by month | Query 3 |
| 4 | Revenue by category (top 10) | Query 4 |
| 5 | Top sellers by revenue | Query 5 |

The orchestration notebook `07_visualization.ipynb` materializes equivalent aggregates to `pipeline_visualization.json`.

## Setup

1. Open Lakeview in the Databricks workspace.
2. Create a dataset for each query block in `lakeview_queries.sql` against catalog `globalmart`.
3. Configure visualizations and publish the dashboard.

Recommended chart types: line charts for trends, horizontal bar charts for rankings, and tables for seller metrics.
