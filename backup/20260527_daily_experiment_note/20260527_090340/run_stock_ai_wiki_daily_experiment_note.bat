@echo off
setlocal
set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
set "PROXY=E:\1_Data\tools\python_exec_proxy.cmd"
set "SCRIPT=E:\1_Data\Stock-AI-Wiki\tools\autofill_daily_experiment_note.py"

if exist "%PY%" (
  "%PY%" "%SCRIPT%" %*
  exit /b %ERRORLEVEL%
)

if exist "%PROXY%" (
  "%PROXY%" "%SCRIPT%" %*
  exit /b %ERRORLEVEL%
)

echo [ERROR] Python launcher not found.
exit /b 1
