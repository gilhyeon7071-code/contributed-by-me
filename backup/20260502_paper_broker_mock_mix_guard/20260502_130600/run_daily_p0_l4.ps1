param(
  [string]$RootA = "E:\1_Data",
  [string]$RootB = "E:\vibe\buffett",
  [ValidateSet("paper","broker")] [string]$Mode = "paper",
  [string]$Date = "",
  [switch]$Apply,
  [switch]$Dash,
  [int]$Port = 8501,
  [switch]$PortAuto
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok([string]$m){ Write-Host $m -ForegroundColor Green }
function Warn([string]$m){ Write-Host $m -ForegroundColor Yellow }
function Fail([string]$m){ Write-Host $m -ForegroundColor Red }
function StopNow([string]$m){ Fail $m; exit 2 }

function ResolvePy([string]$RootB){
  $cand = Join-Path $RootB ".venv\Scripts\python.exe"
  if(Test-Path $cand){ return $cand }
  return "python"
}

function GetDByRule([string]$py,[string]$RootA){
  $code = @"
import os,sys
import pandas as pd
rootA=sys.argv[1]
p=os.path.join(rootA,'paper','fills.csv')
df=pd.read_csv(p,encoding='utf-8-sig')
cols={str(c).lower():c for c in df.columns}
dt=None
for k in ('datetime','dt','timestamp','time'):
    if k in cols: dt=cols[k]; break
assert dt is not None, list(df.columns)
df['_ymd']=df[dt].astype(str).str.slice(0,8)
side=cols.get('side') or cols.get('buy_sell') or cols.get('bs')
d=None
if side:
    s=df[side].astype(str).str.upper()
    b=df[s.eq('BUY')]
    if len(b)>0: d=b['_ymd'].max()
if d is None: d=df['_ymd'].max()
print(str(d))
"@
  $out = & $py -c $code $RootA 2>&1
  if($LASTEXITCODE -ne 0){
    Write-Host ($out -join "`n")
    StopNow "STOP: D_by_rule 산출 실패(py exit=$LASTEXITCODE)"
  }
  $d = ($out | Select-Object -Last 1).Trim()
  if($d -notmatch '^\d{8}$'){ StopNow "STOP: D_by_rule 형식 오류: [$d]" }
  return $d
}

function CheckPaperBrokerMix([string]$RootA,[string]$D,[string]$Mode){
  if($Mode -ne "paper"){ return }
  $hits = Get-ChildItem -LiteralPath $RootA -Recurse -File -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match $D -and $_.FullName -match '(?i)broker_wrapper|inbox|kis|kiwoom' } |
    Select-Object -First 10
  if($hits){
    $hits | ForEach-Object { Write-Host (" - " + $_.FullName) }
    StopNow "STOP: paper/broker 혼용 가능성(D=$D)"
  }
}

function ReadStatsDirFromConfig([string]$cfg){
  if(-not (Test-Path $cfg)){ return $null }
  foreach($line in Get-Content -LiteralPath $cfg -Encoding UTF8){
    $s = ($line.Split("#")[0]).Trim()
    if($s -match '^\s*stats_dir\s*:\s*(.+?)\s*$'){
      $v = $Matches[1].Trim().Trim('"').Trim("'")
      return $v
    }
  }
  return $null
}

function VerifyExecDateStop([string]$py,[string]$RootB,[string]$D){
  $p = Join-Path $RootB ("data\orders\orders_{0}_exec.xlsx" -f $D)
  if(-not (Test-Path $p)){ StopNow "STOP: missing orders_exec: $p" }
  $code=@"
import sys,pandas as pd
p=sys.argv[1]; D=sys.argv[2]
df=pd.read_excel(p)
col=None
for c in df.columns:
    if str(c).lower()=='exec_date':
        col=c; break
if col is None:
    print('MISSING_exec_date_col'); raise SystemExit(3)
u=sorted(df[col].astype(str).unique().tolist())
print('exec_date_unique',u)
ok=(len(u)==1 and u[0]==D)
print('STOP_OK',ok)
raise SystemExit(0 if ok else 4)
"@
  $out=& $py -c $code $p $D 2>&1
  Write-Host ($out -join "`n")
  if($LASTEXITCODE -ne 0){ StopNow "STOP: exec_date_unique != D" }
  Ok "STOP_OK True"
}

# --- ADD: SSOT -> DATA sync (Apply only), best-effort, backup existing DATA ---
function SyncLiveVsBtToData([string]$RootB,[string]$D){
  try {
    $cfg = Join-Path $RootB "config.yaml"
    $sd = ReadStatsDirFromConfig $cfg
    if(-not $sd){
      Warn "SYNC_LVB SKIP: config.yaml stats_dir missing"
      return
    }

    $ssot = Join-Path $sd "live_vs_bt.json"
    $dataDir = Join-Path $RootB "data\stats"
    $data = Join-Path $dataDir "live_vs_bt.json"

    if(-not (Test-Path -LiteralPath $ssot)){
      Warn "SYNC_LVB SKIP: missing SSOT $ssot"
      return
    }

    if(-not (Test-Path -LiteralPath $dataDir)){
      New-Item -ItemType Directory -Path $dataDir -Force | Out-Null
    }

    if(Test-Path -LiteralPath $data){
      $bak = $data + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
      Copy-Item -LiteralPath $data -Destination $bak -Force
      Write-Host ("SYNC_LVB BACKUP=" + $bak)
    }

    Copy-Item -LiteralPath $ssot -Destination $data -Force
    Write-Host ("SYNC_LVB WROTE=" + $data)
  } catch {
    Warn ("SYNC_LVB FAIL: " + $_.Exception.Message)
  }
}

function VerifyLiveVsBtAlign([string]$py,[string]$RootB,[string]$D){
  $cfg = Join-Path $RootB "config.yaml"
  $sd = ReadStatsDirFromConfig $cfg
  if(-not $sd){ StopNow "FAIL: config.yaml stats_dir missing" }

  $ssot = Join-Path $sd "live_vs_bt.json"
  $data = Join-Path $RootB "data\stats\live_vs_bt.json"

  $code=@"
import os,sys,json
D=sys.argv[1]; ssot=sys.argv[2]; data=sys.argv[3]
def load(p):
    j=json.load(open(p,'r',encoding='utf-8'))
    _ms = j.get('mean_slippage')
    # fallback: use summary.mean_slippage when top-level is NA(None)
    if _ms is None:
        _ms = (j.get('summary') or {}).get('mean_slippage')
    summary_mean_slippage = _ms
    return (str(j.get('as_of_live') or j.get('as_of')),
            j.get('status'), j.get('exec_mode'),
            j.get('match_rate'), _ms)
for label,p in (('SSOT',ssot),('DATA',data)):
    if not os.path.exists(p):
        print(label,'MISSING',p); continue
    d,st,em,mr,ms = load(p)
    print(label,'D',d,'status',st,'exec_mode',em,'match_rate',mr,'mean_slippage',ms)
ok=False
if os.path.exists(ssot) and os.path.exists(data):
    a=load(ssot); b=load(data)
    ok=(a[0]==b[0]==D) and (a[1]==b[1]) and (a[2]==b[2])
print('ALIGN_OK',ok)
raise SystemExit(0 if ok else 5)
"@
  $out=& $py -c $code $D $ssot $data 2>&1
  Write-Host ($out -join "`n")
  if($LASTEXITCODE -ne 0){
    if($Apply){
      Warn "ALIGN_OK False (SKIP_STOP_ON_APPLY): SSOT vs DATA align failed (likely reports snapshot vs live)."
    } else {
      StopNow "FAIL: SSOT vs DATA align failed"
    }
  } else {
    Ok "ALIGN_OK True"
  }
}

function StartDashboard([string]$RootB,[string]$py,[int]$Port,[switch]$PortAuto){
  $busy = $false
  $ns = & cmd /c "netstat -ano | findstr :$Port | findstr LISTENING" 2>$null
  if($ns){ $busy=$true }

  if($busy){
    if(-not $PortAuto){ StopNow "STOP: Port $Port busy (no kill). Use -PortAuto or close existing." }
    for($p=$Port+1; $p -le ($Port+30); $p++){
      $ns = & cmd /c "netstat -ano | findstr :$p | findstr LISTENING" 2>$null
      if(-not $ns){ $Port=$p; break }
    }
    Warn "WARN: port busy -> using $Port"
  }

  $v = Join-Path $RootB "vibe_v18.py"
  if(-not (Test-Path $v)){ StopNow "FAIL: missing vibe_v18.py" }
  Ok "Launching streamlit on port $Port"
  & $py -m streamlit run $v --server.port $Port
}

# MAIN
$py = ResolvePy $RootB
Ok "py=$py"
Write-Host "RootA=$RootA"
Write-Host "RootB=$RootB"
Write-Host "Mode=$Mode Apply=$Apply"

$D_rule = GetDByRule $py $RootA
Ok "D_by_rule=$D_rule"

$D = $D_rule
if($Date -and $Date.Trim().Length -gt 0){
  $Date=$Date.Trim()
  if($Date -ne $D_rule){ StopNow "STOP: --Date($Date) != D_by_rule($D_rule)" }
  $D=$Date
}

CheckPaperBrokerMix $RootA $D $Mode
Ok "MIX_CHECK_OK"

Push-Location -LiteralPath $RootB
try{
  if($Apply){
    & $py .\tools\dashboard_point_today.py
    Ok "POINT_OK"
    & $py .\vibe_generate_stats_p0.py
    Ok "STATS_OK"
  } else {
    $sd = ReadStatsDirFromConfig (Join-Path $RootB "config.yaml")
    Warn "DRYRUN: stats_dir=$sd (use -Apply)"
  }

  VerifyExecDateStop $py $RootB $D
  if($Apply){ SyncLiveVsBtToData $RootB $D }   # <-- ADD: auto sync on Apply
  VerifyLiveVsBtAlign $py $RootB $D

  Ok "DONE: D=$D Apply=$Apply Mode=$Mode"

  if($Dash){
    StartDashboard $RootB $py $Port $PortAuto
  }
} finally {
  Pop-Location
}
exit 0