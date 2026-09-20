@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d %~dp0

set "_PY_ENV=%PY%"
set "PY="
if defined _PY_ENV set "PY=%_PY_ENV%"
if defined PY (
  %PY% -V >nul 2>nul
  if errorlevel 1 set "PY="
)
if not defined PY if exist "%~dp0.venv\Scripts\python.exe" set "PY=%~dp0.venv\Scripts\python.exe"
if not defined PY if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY (
  where python >nul 2>nul && set "PY=python"
)
if defined PY (
  %PY% -V >nul 2>nul
  if errorlevel 1 set "PY="
)
if not defined PY (
  where py >nul 2>nul && set "PY=py -3"
)
if defined PY (
  %PY% -V >nul 2>nul
  if errorlevel 1 set "PY="
)
if not defined PY (
  echo [FAILED] python runtime not found
  exit /b 9009
)

if "!PAPER_FIX_MAX_CYCLES!"=="" set "PAPER_FIX_MAX_CYCLES=1"
if "!PAPER_FIX_SLEEP_SEC!"=="" set "PAPER_FIX_SLEEP_SEC=5"
if "!PAPER_FIX_RUN_PAPER!"=="" set "PAPER_FIX_RUN_PAPER=1"
if "!PAPER_FIX_OPEN!"=="" set "PAPER_FIX_OPEN=1"
if "!PAPER_FIX_ALIGN_QUALITY_AUTO!"=="" set "PAPER_FIX_ALIGN_QUALITY_AUTO=1"
if "!PAPER_FIX_ALIGN_QUALITY_FORCE_OPT!"=="" set "PAPER_FIX_ALIGN_QUALITY_FORCE_OPT=1"
if "!PAPER_FIX_ALIGN_WINDOW_TRADES!"=="" set "PAPER_FIX_ALIGN_WINDOW_TRADES=30"
if "!PAPER_FIX_ALIGN_MIN_SHARED_STRICT!"=="" set "PAPER_FIX_ALIGN_MIN_SHARED_STRICT=10"
if "!PAPER_FIX_ALIGN_MIN_SHARED_RELAX!"=="" set "PAPER_FIX_ALIGN_MIN_SHARED_RELAX=5"
if "!PAPER_FIX_ALIGN_ALLOW_SUMMARY_FALLBACK!"=="" set "PAPER_FIX_ALIGN_ALLOW_SUMMARY_FALLBACK=0"

set /a _MAX=!PAPER_FIX_MAX_CYCLES!
if !_MAX! LSS 1 set /a _MAX=1

echo [PAPER_FIX_LOOP] PY=!PY! max_cycles=!_MAX! run_paper=!PAPER_FIX_RUN_PAPER! align_auto=!PAPER_FIX_ALIGN_QUALITY_AUTO! force_opt=!PAPER_FIX_ALIGN_QUALITY_FORCE_OPT!

set "RC=0"
for /L %%I in (1,1,!_MAX!) do (
  set "CYCLE=%%I"
  echo.
  echo [PAPER_FIX_LOOP] cycle !CYCLE!/!_MAX!

  if "!PAPER_FIX_RUN_PAPER!"=="1" (
    call "%~dp0run_paper_daily.bat"
    if errorlevel 1 (
      echo [PAPER_FIX_LOOP][FAILED] run_paper_daily.bat failed
      set "RC=1"
      goto :END
    )
  ) else (
    echo [PAPER_FIX_LOOP] skip run_paper_daily.bat
  )

  set "TRVAL_OPEN=0"
  call "%~dp0run_trading_stage_validation_report.bat"
  if errorlevel 1 (
    echo [PAPER_FIX_LOOP][FAILED] run_trading_stage_validation_report.bat failed
    set "RC=1"
    goto :END
  )

  "!PY!" "%~dp0tools\build_paper_fix_cycle_report.py" --cycle-index !CYCLE!
  set "REP_RC=!ERRORLEVEL!"

  if "!REP_RC!"=="0" (
    echo [PAPER_FIX_LOOP] cleared paper_fix gate
    set "RC=0"
    goto :END
  )

  if "!REP_RC!"=="10" (
    echo [PAPER_FIX_LOOP] still paper_fix cycle=!CYCLE!

    if "!PAPER_FIX_ALIGN_QUALITY_AUTO!"=="1" (
      set "AQ_ARGS=--cycle-index !CYCLE! --align-window-trades !PAPER_FIX_ALIGN_WINDOW_TRADES! --min-shared-trades-strict !PAPER_FIX_ALIGN_MIN_SHARED_STRICT! --min-shared-trades-relaxed !PAPER_FIX_ALIGN_MIN_SHARED_RELAX!"
      if "!PAPER_FIX_ALIGN_QUALITY_FORCE_OPT!"=="1" set "AQ_ARGS=!AQ_ARGS! --force-optimize"
      if "!PAPER_FIX_ALIGN_ALLOW_SUMMARY_FALLBACK!"=="1" set "AQ_ARGS=!AQ_ARGS! --allow-summary-fallback"

      "!PY!" "%~dp0tools\run_alignment_quality_fix.py" !AQ_ARGS!
      set "AQ_RC=!ERRORLEVEL!"
      if not "!AQ_RC!"=="0" (
        echo [PAPER_FIX_LOOP][WARN] alignment/quality routine rc=!AQ_RC!
      )

      set "TRVAL_OPEN=0"
      call "%~dp0run_trading_stage_validation_report.bat"
      if errorlevel 1 (
        echo [PAPER_FIX_LOOP][WARN] post-fix validation rebuild failed
      ) else (
        "!PY!" "%~dp0tools\build_paper_fix_cycle_report.py" --cycle-index !CYCLE!
        set "REP_RC=!ERRORLEVEL!"

        if "!REP_RC!"=="0" (
          echo [PAPER_FIX_LOOP] cleared paper_fix gate after alignment/quality routine
          set "RC=0"
          goto :END
        )

        if "!REP_RC!"=="11" (
          echo [PAPER_FIX_LOOP] paper_recheck status reached after alignment/quality routine
          set "RC=11"
          goto :END
        )

        if not "!REP_RC!"=="10" (
          echo [PAPER_FIX_LOOP][WARN] post-fix report rc=!REP_RC!
          set "RC=!REP_RC!"
          goto :END
        )
      )
    ) else (
      echo [PAPER_FIX_LOOP] skip alignment/quality routine
    )

    if !CYCLE! LSS !_MAX! (
      timeout /t !PAPER_FIX_SLEEP_SEC! /nobreak >nul
    ) else (
      set "RC=10"
    )
  ) else (
    if "!REP_RC!"=="11" (
      echo [PAPER_FIX_LOOP] paper_recheck status reached
      set "RC=11"
      goto :END
    ) else (
      echo [PAPER_FIX_LOOP][WARN] report rc=!REP_RC!
      set "RC=!REP_RC!"
      goto :END
    )
  )
)

:END
if "!PAPER_FIX_OPEN!"=="1" (
  set "HTML=%~dp02_Logs\trading_stage_validation_latest.html"
  if exist "!HTML!" start "" "!HTML!"
)

echo [PAPER_FIX_LOOP] exit=!RC!
exit /b !RC!
