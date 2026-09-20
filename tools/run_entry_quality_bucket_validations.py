from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
AUDIT_DIR = LOG_DIR / "entry_quality_bucket_audits"
BUCKET_REPORT = LOG_DIR / "entry_quality_cause_bucket_report_latest.json"
FRAMEWORK_BAT = ROOT / "run_backtest_validation_framework.bat"
MARKET_CSV = LOG_DIR / "backtest_market_ohlc_real_overlap_latest.csv"
STRATEGY_SPEC = ROOT / "tools" / "backtest_real_strategy_adapter.py:real_strategy_signal"
BACKTEST_SPEC = ROOT / "tools" / "backtest_real_strategy_adapter.py:real_strategy_backtest"


def _bucket_name(path: Path) -> str:
    return re.sub(r"_audit$", "", path.stem)


def _run_bucket(audit_path: Path) -> dict[str, object]:
    bucket = _bucket_name(audit_path)
    out_json = LOG_DIR / f"backtest_validation_bucket_{bucket}.json"
    out_csv = LOG_DIR / f"backtest_validation_bucket_{bucket}_gates.csv"
    env = os.environ.copy()
    env["BT_ENTRY_QUALITY_AUDIT_SCENARIO"] = "block_flagged_orders"
    env["BT_ENTRY_QUALITY_AUDIT_PATH"] = str(audit_path)
    args = [
        str(FRAMEWORK_BAT),
        "--market-csv",
        str(MARKET_CSV),
        "--date-col",
        "date",
        "--enable-cpcv",
        "--auto-fetch-inflation",
        "--inflation-country-code",
        "KR",
        "--max-tolerable-mdd",
        "0.30",
        "--strategy-spec",
        str(STRATEGY_SPEC),
        "--backtest-spec",
        str(BACKTEST_SPEC),
        "--params-json",
        '{"fast":8,"slow":100,"allow_short":true,"position_scale":0.7,"daily_gross_turnover_cap_pct":0.008}',
        "--grid-spec-json",
        '{"daily_gross_turnover_cap_pct":[0.006,0.008,0.01,0.012]}',
        "--cost-model-json",
        '{"commission_bps":5.0,"slippage_bps":5.0,"spread_bps":5.0}',
        "--out-json",
        str(out_json),
        "--out-csv",
        str(out_csv),
    ]
    proc = subprocess.run(args, cwd=str(ROOT), env=env)
    return {
        "bucket": bucket,
        "audit_path": str(audit_path),
        "returncode": proc.returncode,
        "out_json": str(out_json),
        "out_csv": str(out_csv),
    }


def main() -> int:
    report = json.loads(BUCKET_REPORT.read_text(encoding="utf-8-sig"))
    active = {
        str(row.get("cause_bucket", "")).lower()
        for row in report.get("bucket_summary", [])
        if isinstance(row, dict)
    }
    audits = [
        path for path in sorted(AUDIT_DIR.glob("*_audit.json"))
        if _bucket_name(path).lower() in active
    ]
    results = [_run_bucket(path) for path in audits]
    print(json.dumps({"active_buckets": sorted(active), "results": results}, ensure_ascii=False, indent=2))
    return 0 if all(int(r["returncode"]) == 0 for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
