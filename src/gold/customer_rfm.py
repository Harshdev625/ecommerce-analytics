"""Customer RFM segmentation on delivered orders."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.gold.daily_sales import delivered_orders_with_revenue
from src.joins.business_questions import SilverJoinTables

RFM_QUINTILES = 5

SEGMENT_THRESHOLDS = (
    "Scoring uses quintiles (1=lowest, 5=highest) per dimension. "
    "Recency score inverts days-since-last-order (recent buyers score higher). "
    "Champions: R>=4 and F>=4 and M>=4. "
    "Loyal: R>=3 and F>=3 and M>=3 (excluding Champions). "
    "Potential: R>=4 and F<=2. "
    "At Risk: R<=2 and F>=3. "
    "Lost: R<=2 and F<=2. "
    "Needs Attention: all other customers."
)


@dataclass
class GoldRfmConfig:
    source: SilverJoinTables | None = None
    target_table: str = "globalmart.gold.customer_rfm"


def _reference_order_date(spark: SparkSession, tables: SilverJoinTables) -> str:
    ref = delivered_orders_with_revenue(spark, tables).agg(F.max("order_date")).collect()[0][0]
    return str(ref)


def build_customer_rfm(
    spark: SparkSession,
    config: GoldRfmConfig | None = None,
) -> tuple[DataFrame, str]:
    """Compute RFM scores and named segments per customer."""
    config = config or GoldRfmConfig()
    tables = config.source or SilverJoinTables()

    orders = delivered_orders_with_revenue(spark, tables)
    reference_date = orders.agg(F.max("order_date")).collect()[0][0]

    customer_metrics = (
        orders.groupBy("customer_id")
        .agg(
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("order_id").alias("frequency"),
            F.round(F.sum("order_revenue"), 2).alias("monetary"),
        )
        .withColumn(
            "recency_days",
            F.datediff(F.lit(reference_date), F.col("last_order_date")),
        )
    )

    r_window = Window.orderBy(F.col("recency_days").asc())
    f_window = Window.orderBy(F.col("frequency").desc())
    m_window = Window.orderBy(F.col("monetary").desc())
    value_window = Window.orderBy(F.col("monetary").desc())

    scored = (
        customer_metrics.withColumn(
            "r_score",
            (RFM_QUINTILES + 1) - F.ntile(RFM_QUINTILES).over(r_window),
        )
        .withColumn("f_score", F.ntile(RFM_QUINTILES).over(f_window))
        .withColumn("m_score", F.ntile(RFM_QUINTILES).over(m_window))
        .withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score"))
        .withColumn(
            "rfm_segment",
            F.when(
                (F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4),
                F.lit("Champions"),
            )
            .when(
                (F.col("r_score") >= 3) & (F.col("f_score") >= 3) & (F.col("m_score") >= 3),
                F.lit("Loyal"),
            )
            .when((F.col("r_score") >= 4) & (F.col("f_score") <= 2), F.lit("Potential"))
            .when((F.col("r_score") <= 2) & (F.col("f_score") >= 3), F.lit("At Risk"))
            .when((F.col("r_score") <= 2) & (F.col("f_score") <= 2), F.lit("Lost"))
            .otherwise(F.lit("Needs Attention")),
        )
        .withColumn("value_rank", F.row_number().over(value_window))
        .withColumn("reference_date", F.lit(reference_date))
        .withColumn("processed_at", F.current_timestamp())
    )
    return scored, str(reference_date)


def segment_distribution(rfm_df: DataFrame) -> DataFrame:
    return (
        rfm_df.groupBy("rfm_segment")
        .agg(
            F.count("*").alias("customer_count"),
            F.round(F.avg("monetary"), 2).alias("avg_monetary"),
            F.round(F.avg("frequency"), 2).alias("avg_frequency"),
            F.round(F.avg("recency_days"), 2).alias("avg_recency_days"),
        )
        .orderBy(F.col("customer_count").desc())
    )


def run_customer_rfm(
    spark: SparkSession,
    config: GoldRfmConfig | None = None,
) -> dict:
    config = config or GoldRfmConfig()
    rfm, reference_date = build_customer_rfm(spark, config)
    rfm.write.format("delta").mode("overwrite").saveAsTable(config.target_table)

    written = spark.table(config.target_table)
    distribution = segment_distribution(written)
    dist_rows = [row.asDict() for row in distribution.collect()]

    import json as _json

    dist_rows = _json.loads(_json.dumps(dist_rows, default=str))
    top_rows = _json.loads(
        _json.dumps(
            [row.asDict() for row in written.orderBy("value_rank").limit(5).collect()],
            default=str,
        )
    )

    return {
        "task": "gold_customer_rfm",
        "target_table": config.target_table,
        "reference_date": reference_date,
        "customer_count": written.count(),
        "segment_thresholds": SEGMENT_THRESHOLDS,
        "segment_distribution": dist_rows,
        "top_value_customers": top_rows,
    }
