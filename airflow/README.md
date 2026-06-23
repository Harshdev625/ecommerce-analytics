# Airflow — local orchestration (`10_orchestration/`)

## Prerequisites

```powershell
cd E:\Projects\ecommerce-analytics
python -m venv .venv-airflow
.\.venv-airflow\Scripts\Activate.ps1
pip install "apache-airflow==2.10.4" requests
```

Generate simulated daily files:

```powershell
python scripts/generate_daily_transaction_files.py
```

Initialize Airflow (first time):

```powershell
$env:AIRFLOW_HOME = "$PWD\airflow"
airflow db init
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
```

Set **Connections** in the Airflow UI (`Admin` → `Connections`):

| Conn ID | Type | Fields |
|---------|------|--------|
| `databricks_default` | HTTP | Host: `<workspace-host>` (no `https://`), Password: PAT token, Extra: `{"job_id": "<YOUR_JOB_ID>"}` |

Set **Variables** (`Admin` → `Variables`):

| Key | Example |
|-----|---------|
| `databricks_job_id` | `123456789012345` |
| `daily_transactions_inbox` | `E:/Projects/ecommerce-analytics/airflow/data/daily_transactions` |

Copy DAGs:

```powershell
$env:AIRFLOW_HOME = "$PWD\airflow"
Copy-Item -Recurse airflow\dags $env:AIRFLOW_HOME\dags
airflow standalone
```

Open http://localhost:8080 — trigger `globalmart_databricks_workflow` or `globalmart_daily_transactions`.

## DAGs

| DAG | Purpose |
|-----|---------|
| `globalmart_databricks_workflow` | Trigger + poll Databricks job via REST API |
| `globalmart_daily_transactions` | FileSensor, branching, idempotent load, backfill, failure recovery demo |

## Local verification (no UI)

```powershell
python scripts/demo_airflow_patterns.py
```

Runs all four production patterns against SQLite. See [`docs/LOCAL_SETUP.md`](../docs/LOCAL_SETUP.md).

## Airflow UI (Podman)

```powershell
podman compose -f podman/compose.yaml up
```

Open http://localhost:8080 (admin / admin). Python 3.13 cannot run Airflow 2.x in a local venv — use Podman or Python 3.12.

## Production patterns (orchestration phase)

1. **Idempotency** — trigger `globalmart_daily_transactions` twice for the same `logical_date`; target row count unchanged.
2. **Sensors & branching** — empty or invalid file routes to `handle_invalid_file`, skips load.
3. **Backfill** — `airflow dags backfill globalmart_daily_transactions -s 2024-01-01 -e 2024-01-07`
4. **Failure & recovery** — set Airflow Variable `force_load_failure=true`, clear failed task only, re-run.
