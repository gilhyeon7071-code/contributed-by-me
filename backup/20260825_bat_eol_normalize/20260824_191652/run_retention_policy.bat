@echo off
setlocal

set "ROOT=%~dp0"
set "PY=%ROOT%_runtime\python312-embed\python.exe"

if exist "%PY%" (
  "%PY%" "%ROOT%tools\maintenance\retention_policy.py" %*
) else (
  python "%ROOT%tools\maintenance\retention_policy.py" %*
)

exit /b %ERRORLEVEL%
