@echo off
setlocal EnableExtensions
set "ROOT=%~dp0"
pushd "%ROOT%" || exit /b 2

if "%PY%"=="" (
  if exist "E:\1_Data\_runtime\python312-embed\python.exe" set "PY=E:\1_Data\_runtime\python312-embed\python.exe"
)
if "%PY%"=="" (
  if exist "E:\1_Data\.venv\Scripts\python.exe" set "PY=E:\1_Data\.venv\Scripts\python.exe"
)
if "%PY%"=="" (
  if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
)
if "%PY%"=="" (
  if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
)
if "%PY%"=="" (
  if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python314\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python314\python.exe"
)
if "%PY%"=="" set "PY=python"

echo [STEP] ssot_health_card
%PY% "%ROOT%tools\build_ssot_health_card.py" --config "%ROOT%config\ssot_health_card.json" --out-dir "%ROOT%2_Logs"
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" (
  echo [HARD_FAIL] ssot_health_card failed rc=%RC%
  popd
  endlocal & exit /b %RC%
)

echo [OK] ssot_health_card PASS
popd
endlocal & exit /b 0
