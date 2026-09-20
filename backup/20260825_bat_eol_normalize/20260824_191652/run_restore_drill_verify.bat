@echo off
setlocal EnableExtensions
set "ROOT=%~dp0"
cd /d "%ROOT%"

set "PY_EXE="
call :TRY_PY "%ROOT%.venv\Scripts\python.exe"
call :TRY_PY "%ROOT%_runtime\python312-embed\python.exe"
call :TRY_PY "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY_EXE (
  for /f "usebackq delims=" %%P in (`where python 2^>nul`) do (
    if not defined PY_EXE call :TRY_PY "%%P"
  )
)
if not defined PY_EXE (
  echo [RESTORE_DRILL] FAILED python runtime not found
  exit /b 9009
)

echo [RESTORE_DRILL] PY=%PY_EXE%
"%PY_EXE%" "%ROOT%tools\restore_drill_verify.py" %*
set "RC=%ERRORLEVEL%"
echo [RESTORE_DRILL] exit=%RC%
exit /b %RC%

:TRY_PY
if defined PY_EXE goto :eof
if not exist "%~1" goto :eof
"%~1" -V >nul 2>nul
if errorlevel 1 goto :eof
set "PY_EXE=%~1"
goto :eof
