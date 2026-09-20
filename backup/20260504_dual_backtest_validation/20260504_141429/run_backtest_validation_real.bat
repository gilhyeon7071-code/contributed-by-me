@echo off
setlocal EnableExtensions

set "ROOT=%~dp0"
cd /d "%ROOT%"

set "BTCSV_USE_CORE6=0"
call "%ROOT%run_build_backtest_market_csv.bat" --max-files 999
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

set "LEDGER_CSV=E:\vibe\buffett\data\ledger\paper_fills_ledger.csv"
set "REAL_MARKET_CSV=%ROOT%2_Logs\backtest_market_ohlc_real_overlap_latest.csv"
set "BT_EVAL_START_YMD=20260301"
powershell -NoProfile -Command "$market = '%ROOT%2_Logs\backtest_market_ohlc_latest.csv'; $ledger = '%LEDGER_CSV%'; $out = '%REAL_MARKET_CSV%'; $evalStartYmd = '%BT_EVAL_START_YMD%'; if (!(Test-Path -LiteralPath $market)) { exit 71 }; if (!(Test-Path -LiteralPath $ledger)) { exit 72 }; $ledgerRows = Import-Csv -LiteralPath $ledger; $minDay = ($ledgerRows | ForEach-Object { $_.date } | Where-Object { $_ } | Sort-Object | Select-Object -First 1); if (-not $minDay) { exit 73 }; $ledgerMinDate = [datetime]::ParseExact($minDay, 'yyyyMMdd', $null); $evalStartDate = [datetime]::ParseExact($evalStartYmd, 'yyyyMMdd', $null); $minDate = if ($ledgerMinDate -gt $evalStartDate) { $ledgerMinDate } else { $evalStartDate }; $rows = Import-Csv -LiteralPath $market | Where-Object { $_.date -and ([datetime]$_.date) -ge $minDate }; if (($rows | Measure-Object).Count -eq 0) { exit 74 }; $rows | Export-Csv -LiteralPath $out -NoTypeInformation -Encoding UTF8"
if errorlevel 1 (
  echo [BTREAL][ERR] real overlap market csv build failed
  exit /b %ERRORLEVEL%
)

call "%ROOT%run_backtest_validation_framework.bat" --market-csv "%REAL_MARKET_CSV%" --date-col date --enable-cpcv --auto-fetch-inflation --inflation-country-code KR --max-tolerable-mdd 0.30 --strategy-spec "%ROOT%tools\backtest_real_strategy_adapter.py:real_strategy_signal" --backtest-spec "%ROOT%tools\backtest_real_strategy_adapter.py:real_strategy_backtest" --params-json "{\"fast\":8,\"slow\":100,\"allow_short\":true,\"position_scale\":0.7,\"daily_gross_turnover_cap_pct\":0.01}" --grid-spec-json "{\"fast\":[6,8,10],\"slow\":[95,100,105],\"allow_short\":[true],\"position_scale\":[0.6,0.7,0.8]}" --cost-model-json "{\"commission_bps\":5.0,\"slippage_bps\":5.0,\"spread_bps\":5.0}"
set "BTVAL_RC=%ERRORLEVEL%"
if not exist "%ROOT%2_Logs\backtest_validation_latest.json" (
  echo [BTREAL][ERR] validation report missing after framework run
  exit /b 61
)

call "%ROOT%run_backtest_validation_checklist.bat" --report-json "%ROOT%2_Logs\backtest_validation_latest.json"
if errorlevel 1 (
  echo [BTREAL][ERR] checklist report build failed
  exit /b 62
)
call :require_fresh "%ROOT%2_Logs\backtest_validation_checklist_latest.json" "%ROOT%2_Logs\backtest_validation_latest.json" checklist
if errorlevel 1 exit /b %ERRORLEVEL%

call "%ROOT%run_backtest_validation_final_output.bat" --checklist-json "%ROOT%2_Logs\backtest_validation_checklist_latest.json" --report-json "%ROOT%2_Logs\backtest_validation_latest.json"
if errorlevel 1 (
  echo [BTREAL][ERR] final output build failed
  exit /b 63
)
call :require_fresh "%ROOT%2_Logs\backtest_final_output_latest.json" "%ROOT%2_Logs\backtest_validation_checklist_latest.json" final_output
if errorlevel 1 exit /b %ERRORLEVEL%

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
  echo [BTREAL][ERR] screen html build failed
  exit /b 64
)
call :require_fresh "%ROOT%2_Logs\backtest_validation_screen_latest.html" "%ROOT%2_Logs\backtest_final_output_latest.json" screen
if errorlevel 1 exit /b %ERRORLEVEL%

set "SCREEN=%ROOT%2_Logs\backtest_validation_screen_latest.html"
if exist "%SCREEN%" (
  if not "%BT_SCREEN_OPEN%"=="0" start "" "%SCREEN%"
)

set "RC=%BTVAL_RC%"
echo [BTREAL] exit=%RC%
exit /b %RC%

:require_fresh
set "TARGET=%~1"
set "SOURCE=%~2"
set "LABEL=%~3"
if not exist "%TARGET%" (
  echo [BTREAL][ERR] %LABEL% output missing: %TARGET%
  exit /b 65
)
if not exist "%SOURCE%" (
  echo [BTREAL][ERR] %LABEL% source missing: %SOURCE%
  exit /b 66
)
powershell -NoProfile -Command "$target = Get-Item -LiteralPath '%TARGET%'; $source = Get-Item -LiteralPath '%SOURCE%'; if ($target.LastWriteTime -lt $source.LastWriteTime) { exit 1 }"
if errorlevel 1 (
  echo [BTREAL][ERR] %LABEL% output is stale: %TARGET%
  echo [BTREAL][ERR] source=%SOURCE%
  exit /b 67
)
exit /b 0



