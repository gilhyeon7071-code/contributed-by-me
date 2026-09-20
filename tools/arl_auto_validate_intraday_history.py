from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
BOOTSTRAP = Path(__file__).resolve().parent / "arl_threshold_bootstrap.py"
LATEST_JSON = LOG_DIR / "arl_auto_intraday_validation_latest.json"
RETURNS_CSV = LOG_DIR / "arl_intraday_history_returns_latest.csv"


def _now_ts() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _today_ymd() -> str:
    return datetime.now().strftime("%Y%m%d")


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc, dtype={"code": str})
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype={"code": str})


def _prepare_returns(history_path: Path, returns_path: Path) -> Dict[str, Any]:
    df = _read_csv(history_path)
    required = {"ts", "code", "current_price"}
    missing = sorted(required - set(df.columns))
    if missing:
        return {"ok": False, "reason": "MISSING_COLUMNS", "missing": missing}
    work = df.copy()
    work["ts"] = pd.to_datetime(work["ts"], errors="coerce")
    work["code"] = work["code"].astype(str).str.zfill(6)
    work["current_price"] = pd.to_numeric(work["current_price"], errors="coerce")
    work = work.dropna(subset=["ts", "code", "current_price"])
    work = work[work["current_price"] > 0].copy()
    if work.empty:
        return {"ok": False, "reason": "NO_VALID_PRICE_ROWS"}
    work = work.sort_values(["code", "ts"]).copy()
    work["ret_1m_pct"] = work.groupby("code", sort=False)["current_price"].pct_change() * 100.0
    work["ret_1m_pct"] = pd.to_numeric(work["ret_1m_pct"], errors="coerce").fillna(0.0)
    work["status"] = "OK"
    returns_path.parent.mkdir(parents=True, exist_ok=True)
    work.to_csv(returns_path, index=False, encoding="utf-8-sig")
    return {
        "ok": True,
        "rows": int(len(work)),
        "unique_codes": int(work["code"].nunique()),
        "unique_timestamps": int(work["ts"].nunique()),
        "ts_min": work["ts"].min().isoformat(),
        "ts_max": work["ts"].max().isoformat(),
    }


def run(args: argparse.Namespace) -> Dict[str, Any]:
    history_path = Path(args.history_csv) if args.history_csv else (LOG_DIR / f"intraday_prices_history_{_today_ymd()}.csv")
    output_json = Path(args.output_json)
    returns_path = Path(args.returns_csv)
    payload_base: Dict[str, Any] = {
        "generated_at": _now_ts(),
        "status": "NOT_READY",
        "reason": "",
        "history_csv": str(history_path),
        "returns_csv": str(returns_path),
        "thresholds": {
            "min_bars_soft": int(args.min_bars_soft),
            "min_bars_critical": int(args.min_bars_critical),
            "max_critical_censored_rate": float(args.max_critical_censored_rate),
        },
        "policy": {
            "auto_apply": False,
            "threshold_applied": False,
            "orders_modified": False,
            "fills_modified": False,
            "ledger_modified": False,
            "stats_modified": False,
            "gate_modified": False,
            "risk_lock_modified": False,
            "stop_meaning_modified": False,
            "runtime_config_modified": False,
        },
    }
    if not history_path.exists():
        payload = {**payload_base, "reason": "HISTORY_MISSING"}
        _write_json(output_json, payload)
        return payload

    prep = _prepare_returns(history_path, returns_path)
    payload_base["history"] = prep
    if not prep.get("ok"):
        payload = {**payload_base, "reason": str(prep.get("reason") or "HISTORY_INVALID")}
        _write_json(output_json, payload)
        return payload

    unique_bars = int(prep.get("unique_timestamps") or 0)
    if unique_bars < int(args.min_bars_soft):
        payload = {**payload_base, "reason": "BELOW_SOFT_MIN_BARS", "ready_level": "NOT_READY"}
        _write_json(output_json, payload)
        return payload

    arl_json = Path(args.arl_output_json)
    arl_csv = Path(args.arl_output_csv)
    cmd = [
        sys.executable,
        str(BOOTSTRAP),
        "--input",
        str(returns_path),
        "--value-col",
        "ret_1m_pct",
        "--symbol-col",
        "code",
        "--normal-filter-col",
        "status",
        "--normal-filter-value",
        "OK",
        "--bootstrap-runs",
        str(int(args.bootstrap_runs)),
        "--block-size",
        str(int(args.block_size)),
        "--series-length",
        str(int(args.series_length)),
        "--window-minutes",
        "1",
        "--bars-per-day",
        "390",
        "--limits",
        str(args.limits),
        "--targets",
        "soft:120,warning:300,critical:500",
        "--output-json",
        str(arl_json),
        "--output-csv",
        str(arl_csv),
    ]
    cp = subprocess.run(cmd, cwd=str(ROOT), text=True, capture_output=True, encoding="utf-8", errors="replace", check=False)
    arl_payload: Dict[str, Any] = {}
    if arl_json.exists():
        try:
            arl_payload = json.loads(arl_json.read_text(encoding="utf-8"))
        except Exception:
            arl_payload = {}
    critical = (((arl_payload.get("recommendations") or {}).get("critical") or {}).get("by_lambda") or {}).get("0.3")
    critical_censored = None
    if isinstance(critical, dict):
        critical_censored = critical.get("censored_rate")

    ready_level = "READY_SOFT"
    if unique_bars >= int(args.min_bars_critical):
        ready_level = "REVIEW_ONLY"
        try:
            if critical_censored is not None and float(critical_censored) <= float(args.max_critical_censored_rate):
                ready_level = "READY_CRITICAL_REVIEW"
        except Exception:
            ready_level = "REVIEW_ONLY"

    payload = {
        **payload_base,
        "status": "PASS" if cp.returncode == 0 else "FAIL",
        "reason": "ARL_VALIDATION_RAN" if cp.returncode == 0 else "ARL_VALIDATION_FAILED",
        "ready_level": ready_level,
        "arl_outputs": {"json": str(arl_json), "csv": str(arl_csv)},
        "arl_returncode": int(cp.returncode),
        "arl_stdout_tail": (cp.stdout or "").strip().splitlines()[-5:],
        "arl_stderr_tail": (cp.stderr or "").strip().splitlines()[-5:],
        "critical_lambda_0_3": critical,
        "blocked_for_auto_apply": [
            "Gate/STOP auto-apply is disabled by policy",
            "READY means review candidate only",
        ],
    }
    _write_json(output_json, payload)
    return payload


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Auto-run advisory ARL validation when intraday history is large enough.")
    p.add_argument("--history-csv", default="")
    p.add_argument("--returns-csv", default=str(RETURNS_CSV))
    p.add_argument("--output-json", default=str(LATEST_JSON))
    p.add_argument("--arl-output-json", default=str(LOG_DIR / "arl_auto_intraday_bootstrap_latest.json"))
    p.add_argument("--arl-output-csv", default=str(LOG_DIR / "arl_auto_intraday_bootstrap_latest.csv"))
    p.add_argument("--min-bars-soft", type=int, default=390)
    p.add_argument("--min-bars-critical", type=int, default=1950)
    p.add_argument("--max-critical-censored-rate", type=float, default=0.20)
    p.add_argument("--bootstrap-runs", type=int, default=300)
    p.add_argument("--block-size", type=int, default=30)
    p.add_argument("--series-length", type=int, default=1000)
    p.add_argument("--limits", default="2.5:8.0:0.25")
    return p


def main() -> int:
    payload = run(build_parser().parse_args())
    print(json.dumps({"status": payload.get("status"), "reason": payload.get("reason"), "ready_level": payload.get("ready_level")}, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
