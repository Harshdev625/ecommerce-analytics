"""Airflow production patterns: sensors, branching, idempotency, backfill, recovery."""

from __future__ import annotations

import csv
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.filesystem import FileSensor

DEFAULT_ARGS = {
    "owner": "globalmart",
    "retries": 0,
}

REQUIRED_COLUMNS = {"order_id", "transaction_date", "amount", "status"}
DB_PATH = Path(__file__).resolve().parents[1] / "data" / "pipeline_state.db"


def _inbox_path() -> Path:
    try:
        from airflow.models import Variable

        return Path(Variable.get("daily_transactions_inbox"))
    except Exception:
        return Path(__file__).resolve().parents[1] / "data" / "daily_transactions"


def _conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH)
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_transactions (
          order_id TEXT PRIMARY KEY,
          transaction_date TEXT,
          amount REAL,
          status TEXT,
          loaded_at TEXT
        )
        """
    )
    return connection


def _file_for_logical_date(logical_date: datetime) -> Path:
    return _inbox_path() / f"transactions_{logical_date.date().isoformat()}.csv"


def choose_branch(**context) -> str:
    logical_date = context["logical_date"]
    path = _file_for_logical_date(logical_date)
    if not path.exists() or path.stat().st_size == 0:
        return "handle_invalid_file"
    with path.open(encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not REQUIRED_COLUMNS.issubset(set(reader.fieldnames or [])):
            return "handle_invalid_file"
    return "load_daily_file"


def load_daily_file(**context) -> dict:
    if Variable.get("force_load_failure", default_var="false").lower() == "true":
        raise RuntimeError("Simulated mid-pipeline failure — clear this task and re-run")

    logical_date = context["logical_date"]
    path = _file_for_logical_date(logical_date)
    loaded_at = datetime.utcnow().isoformat()
    rows = []
    with path.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    connection = _conn()
    try:
        for row in rows:
            connection.execute(
                """
                INSERT INTO daily_transactions (order_id, transaction_date, amount, status, loaded_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                  transaction_date=excluded.transaction_date,
                  amount=excluded.amount,
                  status=excluded.status,
                  loaded_at=excluded.loaded_at
                """,
                (
                    row["order_id"],
                    row["transaction_date"],
                    float(row["amount"]),
                    row["status"],
                    loaded_at,
                ),
            )
        connection.commit()
        total = connection.execute("SELECT COUNT(*) FROM daily_transactions").fetchone()[0]
    finally:
        connection.close()

    return {"file": str(path), "rows_in_file": len(rows), "table_row_count": total}


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
        filepath=str(_inbox_path() / "transactions_{{ ds }}.csv"),
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
