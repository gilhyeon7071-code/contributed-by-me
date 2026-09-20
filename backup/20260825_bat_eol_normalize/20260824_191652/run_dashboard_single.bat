@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "ROOT=E:\vibe\buffett"
set "PY=%ROOT%\.venv\Scripts\python.exe"
set "APP=%ROOT%\dashboard.py"
set "PORT=8501"
set "TRIAGE=E:\1_Data\tools\triage_new_entry_block.py"

if not exist "%PY%" (
  echo [ERROR] Missing python: %PY%
  exit /b 2
)
if not exist "%APP%" (
  echo [ERROR] Missing dashboard script: %APP%
  exit /b 3
)

powershell -NoProfile -ExecutionPolicy Bypass -Command "$ps=Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -like '*streamlit run*dashboard.py*8501*' }; foreach($p in $ps){ try{ taskkill /F /PID $p.ProcessId | Out-Null } catch {} }" >nul 2>nul

for /f "tokens=5" %%P in ('netstat -ano ^| findstr /R /C:":%PORT% .*LISTENING"') do (
  taskkill /F /PID %%P >nul 2>nul
)

start "" /min "%PY%" -m streamlit run "%APP%" --server.port %PORT% --server.headless true

set /a WAIT_SEC=0
:wait_loop
set "LISTENING="
for /f "tokens=5" %%P in ('netstat -ano ^| findstr /R /C:":%PORT% .*LISTENING"') do (
  set "LISTENING=1"
)
if defined LISTENING goto :ready
if %WAIT_SEC% GEQ 30 (
  echo [ERROR] Dashboard did not open port %PORT% within 30s.
  exit /b 4
)
set /a WAIT_SEC+=1
timeout /t 1 >nul
goto :wait_loop

:ready
echo [OK] Dashboard single-run ready on http://localhost:%PORT%
if exist "%TRIAGE%" (
  "%PY%" "%TRIAGE%"
)
exit /b 0
