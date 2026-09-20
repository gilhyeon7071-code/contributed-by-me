@echo off
setlocal EnableExtensions
cd /d %~dp0

set "PY="
if exist "E:\1_Data\_runtime\python312-embed\python.exe" set "PY=E:\1_Data\_runtime\python312-embed\python.exe"
if not defined PY if exist "%~dp0.venv\Scripts\python.exe" set "PY=%~dp0.venv\Scripts\python.exe"
if not defined PY if exist "E:\1_Data\_runtime\python312-embed\python.exe" set "PY=E:\1_Data\_runtime\python312-embed\python.exe"
if not defined PY if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY set "PY=python"

echo [ARL_OBSERVER] PY=%PY%
"%PY%" "%~dp0tools\arl_threshold_bootstrap.py" ^
  --input "%~dp02_Logs\candidates_latest_data.with_final_score.csv" ^
  --value-col final_score ^
  --window-minutes 1 ^
  --bars-per-day 390 ^
  --targets soft:120,warning:300,critical:500 ^
  --lambdas 0.1,0.2,0.3 ^
  --limits 2.0:4.0:0.1 ^
  --bootstrap-runs 300 ^
  --block-size 5 ^
  --series-length 390 ^
  %*
set "RC=%ERRORLEVEL%"
echo [ARL_OBSERVER] exit=%RC%
exit /b %RC%
