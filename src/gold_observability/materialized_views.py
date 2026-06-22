"""Regular view vs pre-computed Delta table (materialized-view fallback on serverless)."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from pyspark.sql import SparkSession

from src.joins.business_questions import SilverJoinTables


@dataclass
class MaterializedViewsConfig:
    source: SilverJoinTables | None = None
    regular_view: str = "globalmart.gold.v_seller_daily_revenue"
    materialized_view: str = "globalmart.gold.mv_seller_daily_revenue"
    materialized_table: str = "globalmart.gold.t_seller_daily_revenue_materialized"
    materialization_mode: str = field(default="", repr=False)
    materialized_target: str = field(default="", repr=False)


def _seller_daily_revenue_sql(source: SilverJoinTables) -> str:
    return f"""
        WITH delivered AS (
            SELECT order_id, order_purchase_timestamp_ts
            FROM {source.orders}
            WHERE order_status = 'delivered'
            UNION ALL
            SELECT order_id, order_purchase_timestamp_ts
            FROM {source.orders_late_arrivals}
            WHERE order_status = 'delivered'
        )
        SELECT
            s.seller_id,
            s.seller_state,
            CAST(d.order_purchase_timestamp_ts AS DATE) AS order_date,
            ROUND(SUM(i.line_total_value), 2) AS daily_revenue
        FROM delivered d
        INNER JOIN {source.order_items} i ON d.order_id = i.order_id
        INNER JOIN {source.sellers} s ON i.seller_id = s.seller_id
        GROUP BY s.seller_id, s.seller_state, CAST(d.order_purchase_timestamp_ts AS DATE)
    """


def _is_mv_not_allowed(exc: BaseException) -> bool:
    msg = str(exc)
    return "MATERIALIZED_VIEW" in msg or "MV_NOT_ENABLED" in msg


def create_seller_revenue_views(
    spark: SparkSession,
    config: MaterializedViewsConfig | None = None,
) -> MaterializedViewsConfig:
    """Create regular view; use native MV or Delta table cache depending on compute."""
    config = config or MaterializedViewsConfig()
    tables = config.source or SilverJoinTables()
    body = _seller_daily_revenue_sql(tables)

    spark.sql(f"CREATE OR REPLACE VIEW {config.regular_view} AS {body}")

    config.materialization_mode = "materialized_view"
    config.materialized_target = config.materialized_view
    try:
        spark.sql(f"DROP MATERIALIZED VIEW IF EXISTS {config.materialized_view}")
        spark.sql(f"CREATE MATERIALIZED VIEW {config.materialized_view} AS {body}")
        spark.sql(f"REFRESH MATERIALIZED VIEW {config.materialized_view}")
    except Exception as exc:
        if not _is_mv_not_allowed(exc):
            raise
        config.materialization_mode = "delta_table_cache"
        config.materialized_target = config.materialized_table
        spark.sql(f"CREATE OR REPLACE TABLE {config.materialized_table} AS {body}")

    return config


def _refresh_materialized(
    spark: SparkSession,
    config: MaterializedViewsConfig,
    body: str,
) -> None:
    if config.materialization_mode == "materialized_view":
        spark.sql(f"REFRESH MATERIALIZED VIEW {config.materialized_target}")
    else:
        spark.sql(f"CREATE OR REPLACE TABLE {config.materialized_target} AS {body}")


def compare_view_query_times(
    spark: SparkSession,
    config: MaterializedViewsConfig | None = None,
    *,
    state_filter: str = "SP",
) -> dict:
    config = config or MaterializedViewsConfig()
    if not config.materialized_target:
        create_seller_revenue_views(spark, config)

    tables = config.source or SilverJoinTables()
    body = _seller_daily_revenue_sql(tables)
    filter_sql = "SELECT SUM(daily_revenue) AS total FROM {table} WHERE seller_state = '{state}'"

    def _timed(table: str) -> float:
        start = time.perf_counter()
        spark.sql(filter_sql.format(table=table, state=state_filter)).collect()
        return round((time.perf_counter() - start) * 1000, 2)

    regular_ms = _timed(config.regular_view)
    refresh_start = time.perf_counter()
    _refresh_materialized(spark, config, body)
    refresh_ms = round((time.perf_counter() - refresh_start) * 1000, 2)
    cached_ms = _timed(config.materialized_target)

    return {
        "task": "materialized_views",
        "regular_view": config.regular_view,
        "materialized_target": config.materialized_target,
        "materialization_mode": config.materialization_mode,
        "serverless_note": (
            "delta_table_cache used when CREATE MATERIALIZED VIEW is disabled "
            "(Databricks Serverless Free)."
        )
        if config.materialization_mode == "delta_table_cache"
        else None,
        "filter_state": state_filter,
        "regular_view_ms": regular_ms,
        "materialized_query_ms": cached_ms,
        "materialized_refresh_ms": refresh_ms,
    }
