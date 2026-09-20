@echo off
setlocal
set "ROOT=%~dp0"
set "PY=%ROOT%_runtime\python312-embed\python.exe"
if exist "%PY%" (
  "%PY%" "%ROOT%tools\preopen_5min_check.py" %*
) else (
  python "%ROOT%tools\preopen_5min_check.py" %*
)
exit /b %ERRORLEVEL%
