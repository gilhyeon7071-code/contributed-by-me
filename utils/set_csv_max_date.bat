@echo off
setlocal EnableExtensions

if "%~1"=="" exit /b 1
if "%~2"=="" exit /b 1
if "%~3"=="" exit /b 1

set "CSV_PATH=%~1"
set "DATETIME_COL=%~2"
set "OUT_VAR=%~3"
set "TMP_PATH=%ROOT%2_Logs\_csv_max_date.tmp"
set "RESULT="

if exist "%TMP_PATH%" del /q "%TMP_PATH%" >nul 2>nul
"%PY_RAW%" "%ROOT%batch_utils.py" csv-max-date "%CSV_PATH%" "%DATETIME_COL%" > "%TMP_PATH%" 2>nul
if exist "%TMP_PATH%" set /p RESULT=<"%TMP_PATH%"
if exist "%TMP_PATH%" del /q "%TMP_PATH%" >nul 2>nul

if "%RESULT%"=="" exit /b 1

endlocal & set "%~3=%RESULT%" & exit /b 0
