"""Batch CSV ingestion with file-level idempotency."""

from src.ingestion.auto_loader import AutoLoaderConfig, run_orders_autoloader
from src.ingestion.idempotent_loader import (
    IngestionConfig,
    IngestionResult,
    build_ingestion_summary,
    build_run_report,
    ingest_landing_zone,
    save_run_report_to_volume,
)

__all__ = [
    "AutoLoaderConfig",
    "IngestionConfig",
    "IngestionResult",
    "build_ingestion_summary",
    "build_run_report",
    "ingest_landing_zone",
    "run_orders_autoloader",
    "save_run_report_to_volume",
]
