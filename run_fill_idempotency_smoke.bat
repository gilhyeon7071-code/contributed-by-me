@echo off
setlocal
cd /d "%~dp0"
if not defined PY set "PY=E:\1_Data\_runtime\python312-embed\python.exe"
if not exist "%PY%" set "PY=python"
"%PY%" tools\fill_idempotency_smoke.py
exit /b %ERRORLEVEL%
