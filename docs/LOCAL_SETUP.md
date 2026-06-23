# Local setup and verification

Verified on **Windows · Python 3.13 · PowerShell**.

## Quick start

```powershell
cd E:\Projects\ecommerce-analytics
.\scripts\setup_local.ps1
.\scripts\run_local_verification.ps1
```

**Last verified:** 6/6 pytest pass · 4/4 Airflow production patterns pass

---

## What runs locally

| Step | Command | Purpose |
|------|---------|---------|
| Setup | `scripts/setup_local.ps1` | Creates `.venv`, installs deps, generates demo CSVs |
| Verify | `scripts/run_local_verification.ps1` | Runs `local/tests/` + Airflow pattern demo |
| Patterns only | `python scripts/demo_airflow_patterns.py` | Idempotency, branching, backfill, failure recovery |

### Test layout

| Path | Environment |
|------|-------------|
| `local/tests/` | PC venv (gitignored parent `local/`) |
| `tests/test_silver_transformations.py` | Databricks — full DQ rules + catalog tables |

---

## Manual setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
$env:PYTHONPATH = (Get-Location)
python -m pytest local/tests/ -v
python scripts/generate_daily_transaction_files.py
python scripts/demo_airflow_patterns.py
```

---

## Airflow UI (Podman)

Python 3.13 cannot install `apache-airflow` 2.x in a local venv. Use Podman:

```powershell
podman compose -f podman/compose.yaml up
```

Open http://localhost:8080 — **admin** / **admin** — trigger `globalmart_daily_transactions`.

Stop: `Ctrl+C` then `podman compose -f podman/compose.yaml down`

To trigger Databricks from Airflow (`globalmart_databricks_workflow`), configure Connection `databricks_default` with workspace host and PAT. See [`airflow/README.md`](../airflow/README.md).

---

## Databricks-only steps

| Task | Where |
|------|-------|
| Full pipeline | `00_run_full_pipeline.ipynb` |
| Failure demo | Same notebook, `simulate_failure=silver_transforms` |
| dbt docs lineage | Last cell in `09_dbt/01_dbt_setup_and_run.ipynb` |
| Full unit tests | `pytest tests/test_silver_transformations.py` |
