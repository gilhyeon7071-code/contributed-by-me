@echo off
setlocal EnableExtensions

set "ROOT=%~dp0"
cd /d "%ROOT%"

call "%ROOT%run_build_backtest_market_csv.bat"
if errorlevel 1 (
  echo [BTREAL][ERR] market csv build failed
  exit /b %ERRORLEVEL%
)

call "%ROOT%run_build_rate_series_csv.bat"
if errorlevel 1 (
  echo [BTREAL][WARN] rate series build failed
)

call "%ROOT%run_build_backtest_symbol_panel.bat"
if errorlevel 1 (
  echo [BTREAL][WARN] symbol panel build failed
)

call "%ROOT%run_backtest_validation_framework.bat" --market-csv "%ROOT%2_Logs\backtest_market_ohlc_latest.csv" --date-col date --enable-cpcv --auto-fetch-inflation --inflation-country-code KR --max-tolerable-mdd 0.30
set "BTVAL_RC=%ERRORLEVEL%"

call "%ROOT%run_backtest_validation_checklist.bat" --report-json "%ROOT%2_Logs\backtest_validation_latest.json"
if errorlevel 1 (
  echo [BTREAL][WARN] checklist report build failed
)

call "%ROOT%run_backtest_validation_final_output.bat" --checklist-json "%ROOT%2_Logs\backtest_validation_checklist_latest.json" --report-json "%ROOT%2_Logs\backtest_validation_latest.json"
if errorlevel 1 (
  echo [BTREAL][WARN] final output build failed
)

call "%ROOT%run_backtest_analysis_structure_check.bat" --checklist-json "%ROOT%2_Logs\backtest_validation_checklist_latest.json" --report-json "%ROOT%2_Logs\backtest_validation_latest.json" --market-csv "%ROOT%2_Logs\backtest_market_ohlc_latest.csv" --date-col date
if errorlevel 1 (
  echo [BTREAL][WARN] analysis structure check reported fail/ne
)

call "%ROOT%run_trading_stage_validation_report.bat"
if errorlevel 1 (
  echo [BTREAL][WARN] trading stage validation report build failed
)

call "%ROOT%run_backtest_validation_screen.bat" --checklist-json "%ROOT%2_Logs\backtest_validation_checklist_latest.json" --report-json "%ROOT%2_Logs\backtest_validation_latest.json" --final-json "%ROOT%2_Logs\backtest_final_output_latest.json" --trading-stage-json "%ROOT%2_Logs\trading_stage_validation_latest.json"
if errorlevel 1 (
  echo [BTREAL][WARN] screen html build failed
)

set "SCREEN=%ROOT%2_Logs\backtest_validation_screen_latest.html"
if exist "%SCREEN%" (
  if not "%BT_SCREEN_OPEN%"=="0" start "" "%SCREEN%"
)

set "RC=%BTVAL_RC%"
echo [BTREAL] exit=%RC%
exit /b %RC%



