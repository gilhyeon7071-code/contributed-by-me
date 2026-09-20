param(
  [string[]]$Roots = @('E:\1_Data','E:\vibe'),
  [string]$OutDir = 'E:\1_Data\2_Logs',
  [string]$ExcludePathRegex = '\\backup\\|\\backups\\|\\2_Logs\\|\\node_modules\\|\\dist\\|\\build\\|\\_bak\\|\\_archive\\|\\\.git\\|\\\.venv\\|\\venv\\|\\_runtime\\|\\_cache\\|\\site-packages\\|\\__pycache__\\',
  [switch]$FailOnFind
)

$ErrorActionPreference = 'Stop'

$patterns = @(
  'Get-Content\s+-LiteralPath\s+[^\n\r]*-Raw\s*\|\s*ConvertFrom-Json',
  'Get-Content\s+-Path\s+[^\n\r]*-Raw\s*\|\s*ConvertFrom-Json',
  'Get-Content\s+-Raw\s+[^\n\r]*\|\s*ConvertFrom-Json'
)

# [2026-09-04] rg 경로가 ExcludePathRegex 를 **순회에 적용하지 않고** 순회 뒤 필터만 했다.
#   그래서 backup/ _archive/ 2_Logs/(파일 106,420개) node_modules/ 를 전부 걸어 다녔다.
#   실측: E:\1_Data 열거만 13.3s (전체 14.9s 의 89%). 아침 배치는 동시 부하가 겹쳐
#   600s 타임아웃(rc=124) -> post-chain strict -> [FAILED] -> integrated_ops FAIL
#   -> dashboard FAIL -> run_daily_auto_sync.ps1 exit 20 -> STOC_FullAuto rc=20.
#   09-01~09-04 아침 4연속 재현(저녁 배치는 한산해 통과, 스캔 리포트가 저녁분만 존재).
#   아래 glob 은 ExcludePathRegex 와 같은 목록이다. 53행 -match 필터를 그대로 두므로
#   glob 이 덜 걸러도 검사 대상 집합은 바뀌지 않는다. 실측 13.3s -> 0.1s.
$rgExcludeGlobs = @(
  '!**/backup/**','!**/backups/**','!**/2_Logs/**','!**/node_modules/**','!**/dist/**',
  '!**/build/**','!**/_bak/**','!**/_archive/**','!**/.git/**','!**/.venv/**','!**/venv/**',
  '!**/_runtime/**','!**/_cache/**','!**/site-packages/**','!**/__pycache__/**'
)

$allFiles = @()
$rg = Get-Command rg -ErrorAction SilentlyContinue
if($rg){
  foreach($r in $Roots){
    if(Test-Path -LiteralPath $r){
      $rgArgs = @('--files','--no-messages',$r,'-g','*.ps1','-g','*.bat','-g','*.cmd')
      foreach($g in $rgExcludeGlobs){ $rgArgs += @('-g',$g) }
      $paths = & $rg.Source @rgArgs
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

# [2026-09-04] @() 로 감싸지 않으면 결과가 **정확히 1건일 때** Sort-Object 가 스칼라를
#   돌려주고 $hits.Count 가 $null 이 된다. issue_count=null 로 기록되고
#   `-FailOnFind -and $hits.Count -gt 0` 이 거짓이라 exit 2 가 발동하지 않았다.
#   즉 위반 1건은 파일에 적히기만 하고 게이트는 통과시켰다(fail-open).
#   실측: 2건 -> issues=2 exit=2, 1건 -> issues=(빈칸) exit=0. 수정 전 원본도 동일.
$hits = @($hits | Sort-Object path,line -Unique)

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
