@echo off
setlocal
set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
set "SCRIPT=E:\1_Data\Stock-AI-Wiki\tools\autofill_daily_experiment_note.py"

if not exist "%PY%" (
  echo [ERROR] Python not found: %PY%
  exit /b 1
)

"%PY%" "%SCRIPT%" %*
exit /b %ERRORLEVEL%
