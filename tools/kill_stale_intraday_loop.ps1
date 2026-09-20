# kill_stale_intraday_loop.ps1
#
# 인트라데이 루프를 죽이고 락을 지운다. 워치독이 새 런처로 다시 띄운다.
#
# 왜 필요한가 (2026-09-10, BROKEN_WINDOW_REGISTER D7):
#   2026-09-08 08:36 에 뜬 프로세스가 그날 16:58 의 "기본값 PROD -> MOCK" 전환보다
#   **8시간 22분 먼저** 떴다. 그래서 옛 기본값(KIS_MOCK=0)을 그대로 들고 돈다.
#   락(run_intraday_paper.lock)이 재기동을 막아 수정이 적용될 기회가 없고,
#   워치독은 프로세스가 살아 있으면 alive_skip 이라 **스스로 못 고친다.**
#
# 안전장치: PID 를 맹목적으로 죽이지 않는다. 커맨드라인에 intraday_paper 가
#   들어 있는 프로세스만 고른다(PID 는 재사용될 수 있다).
#
# 재기동: 워치독 창 08:30~21:30, stale 12분, 쿨다운 180초.
#   창 안에서 실행하면 12~15분 뒤 run_intraday_paper.bat 으로 다시 뜬다(기본 MOCK).
#   창 밖이면 다음 날 08:30 이후에 뜬다.

param([switch]$NoRestart)

$ErrorActionPreference = 'Stop'
$lock = 'E:\1_Data\2_Logs\run_intraday_paper.lock'

$procs = Get-CimInstance Win32_Process -Filter "Name='python.exe' OR Name='cmd.exe'" |
         Where-Object { $_.CommandLine -match 'intraday_paper' }

if (-not $procs) {
    Write-Host "[SKIP] intraday_paper 프로세스가 없습니다."
} else {
    Write-Host "[FOUND] $($procs.Count)개"
    foreach ($p in $procs) {
        $cl = $p.CommandLine
        if ($cl.Length -gt 80) { $cl = $cl.Substring(0, 80) }
        Write-Host ("  pid {0,-6} 시작 {1}  {2}" -f $p.ProcessId, $p.CreationDate.ToString('MM-dd HH:mm:ss'), $cl)
    }
    # 자식(python)을 먼저, 부모(cmd)를 나중에 죽인다
    $ordered = $procs | Sort-Object { if ($_.Name -eq 'python.exe') { 0 } else { 1 } }
    foreach ($p in $ordered) {
        try {
            Stop-Process -Id $p.ProcessId -Force -ErrorAction Stop
            Write-Host "  [KILL] pid $($p.ProcessId)"
        } catch {
            Write-Host "  [FAIL] pid $($p.ProcessId) - $($_.Exception.Message)"
        }
    }
}

Start-Sleep -Seconds 2

if (Test-Path $lock) {
    try { Remove-Item -LiteralPath $lock -Recurse -Force; Write-Host "[LOCK] 제거함" }
    catch { Write-Host "[LOCK] 제거 실패 - $($_.Exception.Message)" }
} else {
    Write-Host "[LOCK] 이미 없음"
}

$left = Get-CimInstance Win32_Process -Filter "Name='python.exe' OR Name='cmd.exe'" |
        Where-Object { $_.CommandLine -match 'intraday_paper' }
if ($left) {
    Write-Host "[WARN] 아직 남음: $($left.ProcessId -join ', ')"
} else {
    Write-Host "[OK] intraday_paper 프로세스 0개"
}

$hhmm = [int](Get-Date).ToString('HHmm')
$inWindow = ($hhmm -ge 830 -and $hhmm -lt 2130)

if ($NoRestart) {
    if ($inWindow) {
        Write-Host "[NEXT] 재기동 창 안입니다. 12~15분 뒤 워치독이 새 런처로 띄웁니다."
    } else {
        Write-Host "[NEXT] 재기동 창(08:30~21:30) 밖입니다. 다음 날 08:30 이후에 뜹니다."
    }
} elseif ($inWindow) {
    # 창 안에서는 워치독에 맡긴다. 여기서 또 띄우면 락 경합과 중복 기동이 된다.
    Write-Host "[NEXT] 재기동 창 안입니다. 12~15분 뒤 워치독이 띄웁니다. (직접 띄우지 않습니다)"
} else {
    # 창 밖이면 아무도 안 띄운다. 직접 띄운다.
    if (Test-Path 'E:\1_Data\2_Logs\run_intraday_paper.lock') {
        Write-Host "[ABORT] 락이 아직 있습니다. 띄우지 않습니다."
    } else {
        Write-Host "[START] 재기동 창 밖이라 직접 띄웁니다."
        Start-Process -FilePath 'cmd.exe' `
            -ArgumentList '/c','E:\1_Data\run_intraday_paper.bat' `
            -WorkingDirectory 'E:\1_Data' -WindowStyle Hidden
        Start-Sleep -Seconds 12
        $new = Get-CimInstance Win32_Process -Filter "Name='cmd.exe' OR Name='python.exe'" |
               Where-Object { $_.CommandLine -match 'intraday_paper' }
        if ($new) {
            Write-Host "[OK] 새 프로세스 $($new.ProcessId -join ', ')"
        } else {
            Write-Host "[WARN] 12초 뒤에도 프로세스가 안 보입니다. 로그를 확인하세요."
        }
    }
}
Write-Host "[CHECK] 재기동 뒤 2_Logs\run_intraday_paper_last.txt 에 KIS_MOCK=1 이 찍히면 성공입니다."
Write-Host "[CHECK] PAPER_EXIT_ONLY 는 2026-09-10 부터 0 입니다(배관시험). p1_entry_gate_status_latest.json 의 exit_only_mode 가 false 여야 합니다."
