"""Airflow production patterns: sensors, branching, idempotency, backfill, recovery."""

from __future__ import annotations

import json
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.filesystem import FileSensor

from src.orchestration.daily_transactions import (
    choose_branch,
    inbox_path,
    load_daily_file,
)

DEFAULT_ARGS = {
    "owner": "globalmart",
    "retries": 0,
}


def report_load(**context) -> None:
    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="load_daily_file") or {"status": "skipped_invalid_file"}
    print(json.dumps(payload, indent=2))


with DAG(
    dag_id="globalmart_daily_transactions",
    default_args=DEFAULT_ARGS,
    description="Daily file sensor, branching, idempotent SQLite load",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 30),
    catchup=True,
    max_active_runs=1,
    tags=["globalmart", "production-patterns"],
) as dag:
    wait_for_file = FileSensor(
        task_id="wait_for_daily_file",
        filepath=str(inbox_path() / "transactions_{{ ds }}.csv"),
        poke_interval=30,
        timeout=3600,
    )
    branch = BranchPythonOperator(
        task_id="validate_file_branch",
        python_callable=choose_branch,
    )
    load = PythonOperator(
        task_id="load_daily_file",
        python_callable=load_daily_file,
    )
    invalid = EmptyOperator(task_id="handle_invalid_file")
    summarize = PythonOperator(
        task_id="report_load",
        python_callable=report_load,
        trigger_rule="none_failed_min_one_success",
    )

    wait_for_file >> branch >> [load, invalid] >> summarize
