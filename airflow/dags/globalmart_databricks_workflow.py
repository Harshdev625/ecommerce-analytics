"""Trigger the GlobalMart Databricks Workflow via REST API."""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

DEFAULT_ARGS = {
    "owner": "globalmart",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _databricks_session() -> tuple[str, dict[str, str]]:
    conn = BaseHook.get_connection("databricks_default")
    host = conn.host.replace("https://", "").rstrip("/")
    token = conn.password
    extra = json.loads(conn.extra or "{}")
    job_id = extra.get("job_id") or Variable.get("databricks_job_id")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    return job_id, {"base": f"https://{host}", "headers": headers}


def trigger_databricks_job(**context) -> str:
    job_id, cfg = _databricks_session()
    payload = {
        "job_id": int(job_id),
        "notebook_params": {
            "pipeline_run_id": context["run_id"],
            "dry_run": "false",
            "simulate_failure": "",
        },
    }
    resp = requests.post(
        f"{cfg['base']}/api/2.1/jobs/run-now",
        headers=cfg["headers"],
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    run_id = resp.json()["run_id"]
    context["ti"].xcom_push(key="databricks_run_id", value=run_id)
    return str(run_id)


def wait_for_databricks_job(**context) -> None:
    _, cfg = _databricks_session()
    run_id = context["ti"].xcom_pull(task_ids="trigger_databricks_job", key="databricks_run_id")
    terminal = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
    while True:
        resp = requests.get(
            f"{cfg['base']}/api/2.1/jobs/runs/get",
            headers=cfg["headers"],
            params={"run_id": run_id},
            timeout=60,
        )
        resp.raise_for_status()
        state = resp.json()["state"]
        life = state["life_cycle_state"]
        if life in terminal:
            if state.get("result_state") != "SUCCESS":
                raise RuntimeError(f"Databricks run {run_id} ended: {state}")
            return
        time.sleep(30)


with DAG(
    dag_id="globalmart_databricks_workflow",
    default_args=DEFAULT_ARGS,
    description="Trigger GlobalMart Databricks multi-task workflow",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["globalmart", "databricks"],
) as dag:
    trigger = PythonOperator(
        task_id="trigger_databricks_job",
        python_callable=trigger_databricks_job,
    )
    wait = PythonOperator(
        task_id="wait_for_databricks_job",
        python_callable=wait_for_databricks_job,
    )
    trigger >> wait
