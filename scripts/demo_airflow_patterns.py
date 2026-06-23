"""Run Airflow production-pattern logic locally (no Airflow install required)."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.orchestration.daily_transactions import (  # noqa: E402
    DB_PATH,
    choose_branch,
    load_daily_file,
)


def _context(logical_date: datetime) -> dict:
    return {"logical_date": logical_date}


def demo_idempotency(day: datetime) -> dict:
    first = load_daily_file(**_context(day))
    second = load_daily_file(**_context(day))
    return {
        "pattern": "idempotency",
        "first_load": first,
        "second_load": second,
        "row_count_unchanged": first["table_row_count"] == second["table_row_count"],
    }


def demo_branching(day: datetime) -> dict:
    return {
        "pattern": "sensors_and_branching",
        "valid_file_branch": choose_branch(**_context(day)),
        "missing_file_branch": choose_branch(**_context(datetime(2099, 12, 31))),
    }


def demo_backfill(start: datetime, days: int = 7) -> dict:
    loads = []
    for offset in range(days):
        day = start + timedelta(days=offset)
        payload = load_daily_file(**_context(day))
        loads.append({"date": day.date().isoformat(), "rows_loaded": payload["rows_in_file"]})
    return {"pattern": "backfill", "days_processed": len(loads), "loads": loads}


def demo_failure_recovery(day: datetime) -> dict:
    os.environ["FORCE_LOAD_FAILURE"] = "true"
    failed = False
    try:
        load_daily_file(**_context(day))
    except RuntimeError:
        failed = True
    os.environ["FORCE_LOAD_FAILURE"] = "false"
    recovered = load_daily_file(**_context(day))
    return {
        "pattern": "failure_and_recovery",
        "simulated_failure": failed,
        "recovery_load": recovered,
    }


def main() -> None:
    day = datetime(2024, 1, 5)

    if DB_PATH.exists():
        DB_PATH.unlink()

    results = [
        demo_idempotency(day),
        demo_branching(day),
        demo_backfill(datetime(2024, 1, 1), days=7),
        demo_failure_recovery(day),
    ]

    out_path = REPO_ROOT / "airflow" / "data" / "local_pattern_demo.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(json.dumps(results, indent=2))
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
