"""Shared workflow parameters via Databricks widgets."""

from __future__ import annotations

import uuid
from dataclasses import dataclass


@dataclass
class PipelineParams:
    pipeline_run_id: str
    dry_run: bool = False
    simulate_failure: str = ""


def bind_pipeline_widgets(dbutils) -> None:
    dbutils.widgets.text("pipeline_run_id", str(uuid.uuid4()), "Pipeline run ID")
    dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry run (skip writes)")
    dbutils.widgets.text(
        "simulate_failure",
        "",
        "Simulate failure (task name: bronze_ingestion, quality_checks, ...)",
    )


def get_pipeline_params(dbutils) -> PipelineParams:
    run_id = dbutils.widgets.get("pipeline_run_id").strip() or str(uuid.uuid4())
    dry_run = dbutils.widgets.get("dry_run").strip().lower() == "true"
    simulate_failure = dbutils.widgets.get("simulate_failure").strip().lower()
    return PipelineParams(
        pipeline_run_id=run_id,
        dry_run=dry_run,
        simulate_failure=simulate_failure,
    )
