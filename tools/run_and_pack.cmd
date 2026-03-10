@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ------------------------------------------------------------
REM run_and_pack.cmd
REM  - [1/3] backup key files
REM  - [2/3] run run_paper_daily.bat -> log
REM  - [3/3] pack run log + backup dir (+ config) -> zip
REM  - exit code:
REM      - if pack fails: 1
REM      - else: exit code from run_paper_daily.bat
REM ------------------------------------------------------------

for %%I in ("%~dp0..") do set "ROOT=%%~fI"
set "LOGDIR=%ROOT%\2_Logs"
if not exist "%LOGDIR%" mkdir "%LOGDIR%"

set "PY="
if exist "%ROOT%\.venv\Scripts\python.exe" set "PY=%ROOT%\.venv\Scripts\python.exe"
if not defined PY if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY (
  where python >nul 2>nul && set "PY=python"
)
if not defined PY (
  echo [FATAL] python runtime not found
  exit /b 9009
)

set "TS="
for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%I"
if not defined TS (
  for /f %%I in ('"%PY%" -c "import datetime; print(datetime.datetime.now().strftime('%%Y%%m%%d_%%H%%M%%S'))" 2^>nul') do set "TS=%%I"
)
if not defined TS set "TS=ts_fallback"

set "BACKUPDIR=%LOGDIR%\backup_%TS%"
set "RUNLOG=%LOGDIR%\run_paper_daily_%TS%.log"
set "ZIPOUT=%LOGDIR%\pack_%TS%.zip"
set "PYTMP=%LOGDIR%\_pack_%TS%.py"

echo [1/3] backup key files to "%BACKUPDIR%" ...
if not exist "%BACKUPDIR%" mkdir "%BACKUPDIR%"

if exist "%ROOT%\gate_daily.py" copy /y "%ROOT%\gate_daily.py" "%BACKUPDIR%\gate_daily.py" >nul
if exist "%ROOT%\p0_daily_check.py" copy /y "%ROOT%\p0_daily_check.py" "%BACKUPDIR%\p0_daily_check.py" >nul
if exist "%ROOT%\paper_engine.py" copy /y "%ROOT%\paper_engine.py" "%BACKUPDIR%\paper_engine.py" >nul
if exist "%ROOT%\crash_risk_off.py" copy /y "%ROOT%\crash_risk_off.py" "%BACKUPDIR%\crash_risk_off.py" >nul

if exist "%ROOT%\paper\paper_engine_config.json" (
  if not exist "%BACKUPDIR%\paper" mkdir "%BACKUPDIR%\paper"
  copy /y "%ROOT%\paper\paper_engine_config.json" "%BACKUPDIR%\paper\paper_engine_config.json" >nul
)

echo [2/3] run_paper_daily.bat ^> "%RUNLOG%" ...
pushd "%ROOT%" >nul
call "%ROOT%\run_paper_daily.bat" > "%RUNLOG%" 2>&1
set "PIPE_EXIT_CODE=%ERRORLEVEL%"
popd >nul
echo PIPE_EXIT_CODE=%PIPE_EXIT_CODE%

echo [3/3] pack logs + backup ^> "%ZIPOUT%" ...

> "%PYTMP%" echo import os, zipfile, sys
>> "%PYTMP%" echo root = r"%ROOT%"
>> "%PYTMP%" echo zipout = r"%ZIPOUT%"
>> "%PYTMP%" echo runlog = r"%RUNLOG%"
>> "%PYTMP%" echo backupdir = r"%BACKUPDIR%"
>> "%PYTMP%" echo cfg = os.path.join(root, "paper", "paper_engine_config.json")
>> "%PYTMP%" echo(
>> "%PYTMP%" echo def add_file(zf, src, arc):
>> "%PYTMP%" echo^    if os.path.isfile(src):
>> "%PYTMP%" echo^        zf.write(src, arc)
>> "%PYTMP%" echo^        return 1
>> "%PYTMP%" echo^    return 0
>> "%PYTMP%" echo(
>> "%PYTMP%" echo def add_tree(zf, d, arcroot):
>> "%PYTMP%" echo^    n = 0
>> "%PYTMP%" echo^    if os.path.isdir(d):
>> "%PYTMP%" echo^        for base, _, files in os.walk(d):
>> "%PYTMP%" echo^            for fn in files:
>> "%PYTMP%" echo^                fp = os.path.join(base, fn)
>> "%PYTMP%" echo^                rel = os.path.relpath(fp, d)
>> "%PYTMP%" echo^                n += add_file(zf, fp, os.path.join(arcroot, rel))
>> "%PYTMP%" echo^    return n
>> "%PYTMP%" echo(
>> "%PYTMP%" echo os.makedirs(os.path.dirname(zipout), exist_ok=True)
>> "%PYTMP%" echo count = 0
>> "%PYTMP%" echo with zipfile.ZipFile(zipout, "w", compression=zipfile.ZIP_DEFLATED) as zf:
>> "%PYTMP%" echo^    count += add_file(zf, runlog, os.path.join("2_Logs", os.path.basename(runlog)))
>> "%PYTMP%" echo^    count += add_tree(zf, backupdir, os.path.join("2_Logs", os.path.basename(backupdir)))
>> "%PYTMP%" echo^    count += add_file(zf, cfg, os.path.join("paper", "paper_engine_config.json"))
>> "%PYTMP%" echo(
>> "%PYTMP%" echo print("PACKED_FILES=", count)
>> "%PYTMP%" echo print("ZIP=", zipout)
>> "%PYTMP%" echo sys.exit(0 if os.path.isfile(zipout) and count ^> 0 else 2)

"%PY%" "%PYTMP%"
set "PACK_EXIT_CODE=%ERRORLEVEL%"
del "%PYTMP%" >nul 2>&1

if not "%PACK_EXIT_CODE%"=="0" (
  if exist "%ZIPOUT%" del "%ZIPOUT%" >nul 2>&1
  echo PACK_EXIT_CODE=%PACK_EXIT_CODE%
  echo ZIPOUT="%ZIPOUT%"
  echo [FAILED] packing failed.
  exit /b 1
)

echo PACK_EXIT_CODE=%PACK_EXIT_CODE%
echo ZIPOUT="%ZIPOUT%"
exit /b %PIPE_EXIT_CODE%
