param()

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

Write-Host "Creating .venv ..."
python -m venv .venv
& .\.venv\Scripts\pip install -q --upgrade pip
& .\.venv\Scripts\pip install -q -r requirements-dev.txt

Write-Host "Generating simulated daily transaction files ..."
& .\.venv\Scripts\python scripts/generate_daily_transaction_files.py

Write-Host "Running production-pattern demo (no Airflow required) ..."
& .\.venv\Scripts\python scripts/demo_airflow_patterns.py

Write-Host "Done. Activate with: .\.venv\Scripts\Activate.ps1"
Write-Host "Optional Airflow UI: podman compose -f podman/compose.yaml up"
