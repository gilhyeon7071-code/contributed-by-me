$ErrorActionPreference = "Stop"

$root = "E:\1_Data"
$py = Join-Path $root "_runtime\python312-embed\python.exe"
$script = Join-Path $root "tools\maintenance\offsite_backup_manifest.py"
$logPath = Join-Path $root "2_Logs\run_offsite_backup_dry_last.txt"

if (-not (Test-Path -LiteralPath $py)) {
    $py = "python"
}

& $py $script *> $logPath
exit $LASTEXITCODE
