@echo off
setlocal
set "PY_LOCAL=E:\1_Data\_runtime\python312-embed\python.exe"
set "PY_USER=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
set "SCRIPT=E:\1_Data\Stock-AI-Wiki\tools\autofill_daily_experiment_note.py"

if exist "%PY_LOCAL%" (
  goto RUN_LOCAL
)

if exist "%PY_USER%" (
  goto RUN_USER
)

echo [ERROR] Python launcher not found.
exit /b 1

:RUN_LOCAL
"%PY_LOCAL%" "%SCRIPT%" %*
exit /b %ERRORLEVEL%

:RUN_USER
"%PY_USER%" "%SCRIPT%" %*
exit /b %ERRORLEVEL%
