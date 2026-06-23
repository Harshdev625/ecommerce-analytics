$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    Write-Host "Run scripts/setup_local.ps1 first."
    exit 1
}

$env:PYTHONPATH = (Get-Location).Path
$py = ".\.venv\Scripts\python.exe"

Write-Host "`n=== Unit tests (M10.4, local) ==="
& $py -m pytest local/tests/ -v
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`n=== Airflow production patterns (M10.3) ==="
& $py scripts/demo_airflow_patterns.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`nAll local verification steps completed."
Write-Host "Optional: podman compose -f podman/compose.yaml up  (Airflow UI on :8080)"
