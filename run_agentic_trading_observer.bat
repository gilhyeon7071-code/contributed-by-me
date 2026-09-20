@echo off
setlocal
cd /d "%~dp0"

set "PY=E:\1_Data\_runtime\python312-embed\python.exe"
if not exist "%PY%" set "PY=python"

"%PY%" "tools\build_agentic_trading_observer.py" %*
exit /b %ERRORLEVEL%
