$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot

Write-Host "Triggering the Airflow pipeline health check..."
docker exec airflow airflow dags trigger pipeline_health_check

Write-Host ""
Write-Host "The DAG was triggered."
Write-Host "Open Airflow at http://localhost:8080 to review task status."
Write-Host "Health artifacts will be written to data\monitoring after the task completes."