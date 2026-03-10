@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "ROOT=E:\1_Data"
cd /d "%ROOT%" || (echo [FATAL] cannot cd to %ROOT% & pause & exit /b 1)

REM ---- locate zip (either in E:\1_Data or Downloads) ----
set "ZIP1=%ROOT%\_fix2_run_paper_daily_after_close.zip"
set "ZIP2=%USERPROFILE%\Downloads\_fix2_run_paper_daily_after_close.zip"

if exist "%ZIP1%" (
  set "ZP=%ZIP1%"
) else if exist "%ZIP2%" (
  set "ZP=%ZIP2%"
) else (
  echo [FATAL] ZIP not found.
  echo   - %ZIP1%
  echo   - %ZIP2%
  echo Put _fix2_run_paper_daily_after_close.zip into one of those paths and retry.
  pause
  exit /b 1
)

REM ---- make timestamp (NO WMIC). Use PowerShell if available, fallback to date/time ----
set "DTS="
for /f "delims=" %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss" 2^>nul') do set "DTS=%%I"

if not defined DTS (
  REM fallback (locale-dependent)
  set "DTS=%date%_%time%"
  set "DTS=%DTS: =0%"
  set "DTS=%DTS:/=%"
  set "DTS=%DTS:.=%"
  set "DTS=%DTS::=%"
  set "DTS=%DTS:~0,15%"
)

if not defined DTS (
  echo [FATAL] timestamp(DTS) is empty.
  pause
  exit /b 1
)

REM ---- backup ----
if not exist "run_paper_daily.bat" (
  echo [FATAL] run_paper_daily.bat not found in %ROOT%
  pause
  exit /b 1
)

copy /y "run_paper_daily.bat" "run_paper_daily.bat.bak_%DTS%" >nul
if errorlevel 1 (
  echo [FATAL] backup failed.
  pause
  exit /b 1
)
echo OK: backup created run_paper_daily.bat.bak_%DTS%

REM ---- extract (replace run_paper_daily.bat) ----
where python >nul 2>&1
if errorlevel 1 (
  echo [FATAL] python not found in PATH.
  echo Run this in CMD: where python
  pause
  exit /b 1
)

python -c "import zipfile; z=zipfile.ZipFile(r'%ZP%'); z.extractall(r'E:\1_Data'); z.close(); print('OK: extracted to E:\\1_Data')"
if errorlevel 1 (
  echo [FATAL] extract failed.
  pause
  exit /b 1
)

REM ---- verify replacement ----
echo --- verify run_paper_daily.bat ---
findstr /n /c:"[9/9] after_close_summary.cmd" run_paper_daily.bat
findstr /n /c:"call after_close_summary.cmd" run_paper_daily.bat

echo OK: apply_fix2 done.
pause
exit /b 0
