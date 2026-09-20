@echo off
REM run_intraday_e2e_daily.bat - KIS 장중 E2E 를 매일 돌린다.
REM   2026-08-29: run_kis_intraday_e2e.bat 은 존재했지만 **이것을 부르는 예약작업이 0건**이라
REM   마지막 산출물이 2026-08-07 에 멈춰 있었다. trading_stage_validation 의
REM   live_e2e_freshness(<=7일) 가 그 뒤로 매일 FAIL 했다 - 검사는 도는데 대상이 안 돌았다.
REM
REM   두 가지가 더 걸렸다(실측):
REM   (1) 인자를 작업 스케줄러 문자열에 직접 넣으면 콤마/따옴표가 깨진다 (rc=255)
REM       -> 전용 bat 으로 고정하고 예약작업은 인자 없이 이 파일만 부른다
REM   (2) 스케줄러에서 stdin 이 콘솔이 아니면 러너가 시작 직후 멈춘다
REM       (실측: pid 8152, 18분간 CPU 0초 / 자식 프로세스 0개)
REM       -> `< NUL` 로 stdin 을 즉시 EOF 로 만든다
setlocal EnableExtensions

set "ROOT=%~dp0"
call "%ROOT%run_tool_with_alert.bat" kis_intraday_e2e tools\kis_intraday_e2e_runner.py ^
  --mock true --codes "005930,000660" --health-code 005930 ^
  --iterations 3 --interval-sec 120 --timeout-sec 180 --max-orders 1 --notify < NUL
set "RC=%ERRORLEVEL%"
endlocal & exit /b %RC%
