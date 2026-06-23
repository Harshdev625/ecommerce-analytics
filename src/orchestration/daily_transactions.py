"""Daily transaction file loading for Airflow production-pattern demos."""

from __future__ import annotations

import csv
import os
import sqlite3
from datetime import datetime
from pathlib import Path

REQUIRED_COLUMNS = {"order_id", "transaction_date", "amount", "status"}
DEFAULT_INBOX = Path(__file__).resolve().parents[2] / "airflow" / "data" / "daily_transactions"
DB_PATH = DEFAULT_INBOX.parent / "pipeline_state.db"


def inbox_path() -> Path:
    env = os.environ.get("DAILY_TRANSACTIONS_INBOX")
    if env:
        return Path(env)
    try:
        from airflow.models import Variable

        return Path(Variable.get("daily_transactions_inbox"))
    except Exception:
        return DEFAULT_INBOX


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


def file_for_logical_date(logical_date: datetime, inbox: Path | None = None) -> Path:
    base = inbox or inbox_path()
    return base / f"transactions_{logical_date.date().isoformat()}.csv"


def choose_branch(**context) -> str:
    logical_date = context["logical_date"]
    path = file_for_logical_date(logical_date)
    if not path.exists() or path.stat().st_size == 0:
        return "handle_invalid_file"
    with path.open(encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not REQUIRED_COLUMNS.issubset(set(reader.fieldnames or [])):
            return "handle_invalid_file"
    return "load_daily_file"


def load_daily_file(**context) -> dict:
    force = os.environ.get("FORCE_LOAD_FAILURE", "").lower() == "true"
    if not force:
        try:
            from airflow.models import Variable

            force = Variable.get("force_load_failure", default_var="false").lower() == "true"
        except Exception:
            force = False
    if force:
        raise RuntimeError("Simulated mid-pipeline failure — clear this task and re-run")

    logical_date = context["logical_date"]
    path = file_for_logical_date(logical_date)
    loaded_at = datetime.utcnow().isoformat()
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
