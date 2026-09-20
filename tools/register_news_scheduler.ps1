$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $root "run_news_pipeline_once.bat"
$hiddenRunner = Join-Path $root "run_news_pipeline_once_hidden.vbs"
if (-not (Test-Path $runner)) {
    throw "missing runner: $runner"
}
if (-not (Test-Path $hiddenRunner)) {
    throw "missing hidden runner: $hiddenRunner"
}

$taskPrefix = "Buffett-News"
$definitions = @(
    @{
        Name = "$taskPrefix-PreMarket"
        Session = "premarket"
        Start = "07:00"
        End = "09:00"
        IntervalMinutes = 5
        Extra = ""
    },
    @{
        Name = "$taskPrefix-Intraday"
        Session = "intraday"
        Start = "09:00"
        End = "15:30"
        IntervalMinutes = 2
        Extra = ""
    },
    @{
        Name = "$taskPrefix-AfterHours"
        Session = "afterhours"
        Start = "15:30"
        End = "18:00"
        IntervalMinutes = 15
        Extra = ""
    },
    @{
        Name = "$taskPrefix-Evening"
        Session = "evening"
        Start = "18:00"
        End = "23:59"
        IntervalMinutes = 120
        Extra = "--allow-weekend"
    },
    @{
        Name = "$taskPrefix-Overnight"
        Session = "overnight"
        Start = "00:00"
        End = "07:00"
        IntervalMinutes = 120
        Extra = "--allow-weekend"
    }
)

foreach ($def in $definitions) {
    $start = [datetime]::ParseExact($def.Start, "HH:mm", $null)
    $end = [datetime]::ParseExact($def.End, "HH:mm", $null)
    if ($end -lt $start) {
        $end = $end.AddDays(1)
    }
    $duration = $end - $start
    $du = "{0:00}:{1:00}" -f [int]$duration.TotalHours, $duration.Minutes

    $taskCommand = "wscript.exe `"$hiddenRunner`" `"$($def.Session)`" `"$($def.Start)`" `"$($def.End)`""
    if ($def.Extra) {
        $taskCommand += " $($def.Extra)"
    }

    & schtasks.exe /Create `
        /TN $def.Name `
        /TR $taskCommand `
        /SC DAILY `
        /ST $def.Start `
        /RI ([string]$def.IntervalMinutes) `
        /DU $du `
        /F | Out-Null

    Write-Host "[OK] registered $($def.Name)"
}
