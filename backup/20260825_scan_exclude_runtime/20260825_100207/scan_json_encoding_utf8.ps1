param(
  [string[]]$Roots = @('E:\1_Data','E:\vibe'),
  [string]$OutDir = 'E:\1_Data\2_Logs',
  [string]$ExcludePathRegex = '\\backup\\|\\backups\\|\\2_Logs\\|\\node_modules\\|\\dist\\|\\build\\|\\_bak\\|\\_archive\\|\\\.git\\|\\\.venv\\|\\venv\\',
  [switch]$FailOnFind
)

$ErrorActionPreference = 'Stop'

$patterns = @(
  'Get-Content\s+-LiteralPath\s+[^\n\r]*-Raw\s*\|\s*ConvertFrom-Json',
  'Get-Content\s+-Path\s+[^\n\r]*-Raw\s*\|\s*ConvertFrom-Json',
  'Get-Content\s+-Raw\s+[^\n\r]*\|\s*ConvertFrom-Json'
)

$allFiles = @()
$rg = Get-Command rg -ErrorAction SilentlyContinue
if($rg){
  foreach($r in $Roots){
    if(Test-Path -LiteralPath $r){
      $paths = & $rg.Source --files --no-messages $r -g '*.ps1' -g '*.bat' -g '*.cmd'
      foreach($p in $paths){
        if([string]::IsNullOrWhiteSpace($p)){ continue }
        $allFiles += [pscustomobject]@{ FullName = [string]$p }
      }
    }
  }
} else {
  function Get-FilesFiltered($Path) {
    try {
      $dirs = [System.IO.Directory]::EnumerateDirectories($Path)
      foreach($d in $dirs) {
        if ([string]$d -match $ExcludePathRegex) { continue }
        Get-FilesFiltered -Path $d
      }
      $f_ps1 = [System.IO.Directory]::EnumerateFiles($Path, "*.ps1")
      foreach($f in $f_ps1) { [pscustomobject]@{ FullName = $f } }
      $f_bat = [System.IO.Directory]::EnumerateFiles($Path, "*.bat")
      foreach($f in $f_bat) { [pscustomobject]@{ FullName = $f } }
      $f_cmd = [System.IO.Directory]::EnumerateFiles($Path, "*.cmd")
      foreach($f in $f_cmd) { [pscustomobject]@{ FullName = $f } }
    } catch {}
  }
  foreach($r in $Roots){
    if(Test-Path -LiteralPath $r){
      $allFiles += @(Get-FilesFiltered -Path $r)
    }
  }
}

$hits = @()
foreach($f in $allFiles){
  if([string]$f.FullName -match $ExcludePathRegex){ continue }
  foreach($pat in $patterns){
    $matches = Select-String -Path $f.FullName -Pattern $pat -ErrorAction SilentlyContinue
    foreach($m in $matches){
      $line = [string]$m.Line
      if($line -match 'ConvertFrom-Json' -and $line -match '-Encoding\s+UTF8'){
        continue
      }
      $hits += [pscustomobject]@{
        path = $f.FullName
        line = [int]$m.LineNumber
        text = $line.Trim()
      }
    }
  }
}

$hits = $hits | Sort-Object path,line -Unique

$ts = Get-Date -Format 'yyyyMMdd_HHmmss'
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$jsonPath = Join-Path $OutDir ("json_encoding_scan_{0}.json" -f $ts)
$txtPath = Join-Path $OutDir 'json_encoding_scan_latest.txt'

$payload = [ordered]@{
  generated_at = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
  roots = $Roots
  exclude_path_regex = $ExcludePathRegex
  files_scanned = $allFiles.Count
  issue_count = $hits.Count
  issues = $hits
}

$payload | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $jsonPath -Encoding UTF8

$lines = @()
$lines += "generated_at=$($payload.generated_at)"
$lines += "files_scanned=$($payload.files_scanned)"
$lines += "issue_count=$($payload.issue_count)"
foreach($h in $hits){
  $lines += ("{0}:{1} {2}" -f $h.path,$h.line,$h.text)
}
$lines | Set-Content -LiteralPath $txtPath -Encoding UTF8

Write-Output ("json={0}" -f $jsonPath)
Write-Output ("latest={0}" -f $txtPath)
Write-Output ("issues={0}" -f $hits.Count)

if($FailOnFind -and $hits.Count -gt 0){
  exit 2
}
