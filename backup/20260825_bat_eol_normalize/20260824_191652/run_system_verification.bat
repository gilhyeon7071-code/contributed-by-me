@echo off
setlocal EnableExtensions

set "EASY=E:\1_Data\run_system_verification_easy.bat"
if not exist "%EASY%" (
  echo [ERROR] Missing easy launcher: %EASY%
  exit /b 2
)

cmd /c "%EASY%" %*
exit /b %ERRORLEVEL%
