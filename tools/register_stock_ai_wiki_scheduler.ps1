param(
    [switch]$Apply
)

$ErrorActionPreference = "Stop"

$TaskName = "VIBE_Stock_AI_Wiki_Update"
$Root = "E:\1_Data"
$Pipeline = Join-Path $Root "run_stock_ai_wiki_update.bat"
$HiddenRunner = Join-Path $Root "run_stock_ai_wiki_update_hidden.vbs"
$LogDir = Join-Path $Root "Stock-AI-Wiki\00_Inbox\logs"
$ReportPath = Join-Path $LogDir "scheduler_registration_latest.json"

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$existsBefore = $null -ne (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue)
$command = "wscript.exe"
$arguments = "`"$HiddenRunner`""

$payload = [ordered]@{
    generated_at = (Get-Date).ToString("s")
    task_name = $TaskName
    root = $Root
    pipeline = $Pipeline
    mode = $(if ($Apply) { "APPLY" } else { "DRY_RUN" })
    policy_effect = $false
    trading_effect = $false
    exists_before = $existsBefore
    schedule = "hourly"
    action = "$command $arguments"
    applied = $false
    exists_after = $existsBefore
    notes = @(
        "Registers only the Stock-AI-Wiki local update batch.",
        "Updates Google RSS observe artifacts, Stock-AI-Wiki markdown notes, article archive seed, wiki links, and progress dashboard.",
        "Does not change Gate, STOP, LOCK, risk, score, order, fill, ledger, stats, broker dispatch, or paper engine behavior."
    )
}

if ($Apply) {
    if (-not (Test-Path -LiteralPath $Pipeline)) {
        throw "Pipeline runner missing: $Pipeline"
    }
    if (-not (Test-Path -LiteralPath $HiddenRunner)) {
        throw "Hidden runner missing: $HiddenRunner"
    }
    $action = New-ScheduledTaskAction -Execute $command -Argument $arguments -WorkingDirectory $Root
    $trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date.AddMinutes(5) -RepetitionInterval (New-TimeSpan -Hours 1)
    $settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 15)
    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Description "Run RootA Stock-AI-Wiki local update pipeline hourly." -Force | Out-Null
    $payload.applied = $true
    $payload.exists_after = $null -ne (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue)
}

$payload | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $ReportPath -Encoding UTF8
Write-Output $ReportPath
Write-Output ("mode=" + $payload.mode)
Write-Output ("exists_after=" + $payload.exists_after)
