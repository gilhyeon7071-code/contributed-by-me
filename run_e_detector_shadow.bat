@echo off
setlocal
cd /d "%~dp0"
if exist "_runtime\python312-embed\python.exe" (
  set "PY=%CD%\_runtime\python312-embed\python.exe"
) else (
  set "PY=python"
)
"%PY%" -m tools.build_e_detector_shadow %*
set "RC=%ERRORLEVEL%"
echo [E_DETECTOR_SHADOW] rc=%RC%
exit /b %RC%
