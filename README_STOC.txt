STOC runner (Windows)

Main entry
- run_daily.bat: main entry (Market Guard -> Storage Check -> Candidate Generator -> Ledger Sync -> Cleanup)

Market / Holiday
- market_guard.py: weekend/holiday cache gate
- holiday_manager.py: weekend + holidays.json cache
- holidays.json: put YYYYMMDD strings in holidays list

Candidate generation
- generate_candidates_v41_1.py: v41.1-compatible Top-N candidate generator
  - Input: krx_daily_*_clean.parquet (preferred) or krx_daily_*.parquet
  - Search scope: recursively under BASE_DIR
  - Required columns: date, code, close, value
  - Output:
    - 2_Logs/candidates_v41_1_YYYYMMDD.csv
    - 2_Logs/candidates_latest.csv (pointer file)

Ledger
- main_analysis.py: ledger normalization (virtual_ledger.csv)
- virtual_ledger.csv: ledger data (ticker should be 6 digits)

Cleanup
- cleanup_manager.py: move non-kept ROOT files to _archive (folders are not moved)

Dependencies
- Python 3.10+
- pyarrow (required for parquet reading):
  - pip install pyarrow

Environment variables (optional)
- STOC_BASE_DIR: base directory (default: this folder)
- STOC_ARCHIVE_DIR: archive root (default: <base>\_archive)
