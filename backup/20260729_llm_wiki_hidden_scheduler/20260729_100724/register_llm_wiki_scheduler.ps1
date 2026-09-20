param(
    [switch]$Apply
)

$ErrorActionPreference = "Stop"

$TaskName = "VIBE_LLM_Wiki_Pipeline"
$Root = "E:\1_Data"
$Pipeline = Join-Path $Root "run_llm_wiki_pipeline.bat"
$LogDir = Join-Path $Root "docs\llm_wiki\05_Logs"
$ReportPath = Join-Path $LogDir "scheduler_registration_latest.json"

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$existsBefore = $null -ne (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue)
$command = "cmd.exe"
$arguments = "/c `"$Pipeline`""

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
        "Registers only the LLM Wiki local pipeline.",
        "Does not call Slack, create credentials, or change Gate, STOP, LOCK, risk, score, order, fill, ledger, or stats behavior."
    )
}

if ($Apply) {
    if (-not (Test-Path -LiteralPath $Pipeline)) {
        throw "Pipeline runner missing: $Pipeline"
    }
    $action = New-ScheduledTaskAction -Execute $command -Argument $arguments -WorkingDirectory $Root
    $trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date.AddMinutes(5) -RepetitionInterval (New-TimeSpan -Hours 1)
    $settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 10)
    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Description "Run RootA LLM Wiki local refresh pipeline hourly." -Force | Out-Null
    $payload.applied = $true
    $payload.exists_after = $null -ne (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue)
}

$payload | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $ReportPath -Encoding UTF8
Write-Output $ReportPath
Write-Output ("mode=" + $payload.mode)
Write-Output ("exists_after=" + $payload.exists_after)
