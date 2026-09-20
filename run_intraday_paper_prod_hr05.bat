@echo off
setlocal EnableExtensions
REM Temporary launcher: PROD mode, paper data collection OFF, HIGH_REJECTION 0.05, dispatch dry-run
set "KIS_MOCK=0"
set "SURGE_RT_PAPER_DATA_COLLECTION=0"
set "SURGE_RT_PAPER_HIGH_REJECTION_ENTRY_BLOCK_PCT=0.05"
set "LOOP_DISPATCH_APPLY=0"
call "E:\1_Data\run_intraday_paper.bat"
