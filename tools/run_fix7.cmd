@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM run_fix7.cmd
REM - Extract fix zip package under E:\1_Data

cd /d E:\1_Data || exit /b 1
if not exist 2_Logs md 2_Logs

set "ZIP=E:\1_Data\_fix7_crash_risk_off_index_based.zip"
if not exist "%ZIP%" (
  echo [FATAL] ZIP not found: %ZIP%
  echo Put the zip into E:\1_Data then rerun.
  exit /b 1
)

set "PY="
if exist "E:\1_Data\.venv\Scripts\python.exe" set "PY=E:\1_Data\.venv\Scripts\python.exe"
if not defined PY if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY (
  where python >nul 2>nul && set "PY=python"
)
if not defined PY (
  echo [FATAL] python runtime not found
  exit /b 9009
)

"%PY%" -c "import zipfile,sys; p=r'E:\1_Data\\_fix7_crash_risk_off_index_based.zip'; z=zipfile.ZipFile(p); z.extractall(r'E:\1_Data'); n=len(z.namelist()); z.close(); print('OK: extracted files=',n)"
set "EC=%ERRORLEVEL%"
if not "%EC%"=="0" (
  echo [FATAL] unzip failed EC=%EC%
  exit /b %EC%
)

echo [OK] fix7 package extracted.
exit /b 0
