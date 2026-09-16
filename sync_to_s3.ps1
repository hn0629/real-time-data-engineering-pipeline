$ErrorActionPreference = "Stop"

$Bucket = "hoang-real-time-data-pipeline-2026"
$Prefix = "real-time-pipeline"
$S3BaseUri = "s3://$Bucket/$Prefix"

# Local data root
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$DataRoot = Join-Path $ProjectRoot "data"

# Define the order to mirror: analytics first, then others
$SyncTargets = @(
    @{ Name = "Analytics"
       LocalPath = Join-Path $DataRoot "analytics"
       S3Path = "$S3BaseUri/analytics/" },
    @{ Name = "Raw"
       LocalPath = Join-Path $DataRoot "raw"
       S3Path = "$S3BaseUri/raw/" },
    @{ Name = "Clean"
       LocalPath = Join-Path $DataRoot "clean"
       S3Path = "$S3BaseUri/clean/" },
    @{ Name = "Quarantine"
       LocalPath = Join-Path $DataRoot "quarantine"
       S3Path = "$S3BaseUri/quarantine/" },
    @{ Name = "Metrics"
       LocalPath = Join-Path $DataRoot "metrics"
       S3Path = "$S3BaseUri/logs/" }
)

Write-Host ""
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "Real-Time Pipeline: Local Parquet -> Amazon S3" -ForegroundColor Cyan
Write-Host "Target: $S3BaseUri/" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Checking AWS identity..." -ForegroundColor Yellow
aws sts get-caller-identity

if ($LASTEXITCODE -ne 0) {
    throw "AWS identity check failed. Run 'aws configure' and confirm your credentials."
}

foreach ($Target in $SyncTargets) {
    $Name = $Target.Name
    $LocalPath = $Target.LocalPath
    $S3Path = $Target.S3Path

    if (-not (Test-Path $LocalPath)) {
        Write-Host "Skipping $Name: local directory does not exist: $LocalPath" -ForegroundColor DarkYellow
        continue
    }

    $FileCount = @(Get-ChildItem -Path $LocalPath -File -Recurse -ErrorAction SilentlyContinue).Count

    if ($FileCount -eq 0) {
        Write-Host "Skipping $Name: no files found in $LocalPath" -ForegroundColor DarkYellow
        continue
    }

    Write-Host ""
    Write-Host "Syncing $Name" -ForegroundColor Green
    Write-Host "  Local: $LocalPath"
    Write-Host "  S3:    $S3Path"
    Write-Host "  Files: $FileCount"

    # Dry-run first (Preview)
    aws s3 sync $LocalPath $S3Path `
        --exclude "*.crc" `
        --exclude "_SUCCESS" `
        --dryrun

    # Actual sync (non-destructive)
    aws s3 sync $LocalPath $S3Path `
        --exclude "*.crc" `
        --exclude "_SUCCESS" `
        --no-progress `
        --only-show-errors

    if ($LASTEXITCODE -ne 0) {
        throw "S3 sync failed for $Name."
    }
}

Write-Host ""
Write-Host "=============================================" -ForegroundColor Green
Write-Host "Step 1 complete: Analytics first, then others." -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Green
Write-Host ""

Write-Host "S3 prefixes now available:" -ForegroundColor Cyan
aws s3 ls "$S3BaseUri/" --recursive --human-readable --summarize