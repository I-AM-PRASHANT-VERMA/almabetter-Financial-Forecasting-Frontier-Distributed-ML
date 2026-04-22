# Running the Python checker from PowerShell keeps the workflow simple on Windows.
$projectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location $projectRoot

python .\ops\health_checks\check_project_health.py
