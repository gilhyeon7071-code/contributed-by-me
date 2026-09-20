@echo off
setlocal
set "PS1=%~dp0python_exec_proxy.ps1"
if not exist "%PS1%" (
  echo [FAILED] missing proxy script: %PS1%
  endlocal & exit /b 9009
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%PS1%" %*
set "RC=%ERRORLEVEL%"
endlocal & exit /b %RC%

