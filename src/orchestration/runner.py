"""Execute a pipeline task with reporting and optional simulated failure."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from src.ingestion.idempotent_loader import save_run_report_to_volume
from src.orchestration.params import PipelineParams

REPORT_PREFIX = "/Volumes/globalmart/metadata/run_reports/pipeline"


def run_pipeline_task(
    spark,
    dbutils,
    *,
    task_name: str,
    params: PipelineParams,
    task_fn: Callable[[], dict[str, Any]],
    report_name: str | None = None,
) -> dict[str, Any]:
    started_at = datetime.now(timezone.utc).isoformat()
    report_path = f"{REPORT_PREFIX}_{report_name or task_name}.json"

    if params.simulate_failure == task_name:
        report = {
            "task": task_name,
            "pipeline_run_id": params.pipeline_run_id,
            "status": "FAILED",
            "dry_run": params.dry_run,
            "started_at": started_at,
            "error": "simulated_failure",
            "message": f"simulate_failure={task_name} — downstream workflow tasks will be skipped",
        }
        save_run_report_to_volume(report, dbutils, report_path)
        raise RuntimeError(report["message"])

    try:
        payload = task_fn()
        report = {
            "task": task_name,
            "pipeline_run_id": params.pipeline_run_id,
            "status": "SUCCESS",
            "dry_run": params.dry_run,
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            **payload,
        }
    except Exception as exc:
        report = {
            "task": task_name,
            "pipeline_run_id": params.pipeline_run_id,
            "status": "FAILED",
            "dry_run": params.dry_run,
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "error": str(exc),
        }
        save_run_report_to_volume(report, dbutils, report_path)
        raise

    save_run_report_to_volume(report, dbutils, report_path)
    return report
