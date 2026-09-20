from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"

DEFAULT_INPUT = LOGS / "candidates_latest_data.with_final_score.csv"
LATEST_JSON = LOGS / "future_signal_features_latest.json"
SSOT_HEALTH = LOGS / "ssot_health_card_latest.json"

FEATURE_VERSION = "FUTURE_SIGNAL_FEATURES_V1"
FEATURE_COLUMNS = [
    "D",
    "code",
    "name",
    "feature_asof",
    "feature_version",
    "close",
    "trading_value",
    "ret1_pct",
    "day_ret_pct",
    "rs",
    "rs_slope",
    "v_accel",
    "atr14_pct",
    "adx14",
    "rsi14",
    "stoch_k",
    "bb_width",
    "obv_slope",
    "disparity20",
    "disparity60",
    "foreign_net_20d",
    "institution_net_20d",
    "personal_net_20d",
    "foreign_net",
    "institution_net",
    "personal_net",
    "score",
    "sector_score",
    "regime_score",
    "news_score",
    "flow_score",
    "fundamental_score_01",
    "fx_score",
    "execution_lob_score",
    "forecast_score",
    "final_score",
]


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path)


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _norm_date8(v: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s[:8] if len(s) >= 8 else ""


def _norm_code(v: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s.zfill(6)[-6:] if s else ""


def _latest_date8(df: pd.DataFrame) -> str:
    for col in ("date_yyyymmdd", "D", "as_of_ymd", "date"):
        if col not in df.columns:
            continue
        vals = df[col].map(_norm_date8)
        vals = vals[vals.str.len() == 8]
        if len(vals):
            return str(vals.max())
    return ""


def _operational_date8() -> str:
    try:
        obj = json.loads(SSOT_HEALTH.read_text(encoding="utf-8"))
    except Exception:
        return datetime.now().strftime("%Y%m%d")
    for key in ("as_of_ymd", "today_ymd", "run_ymd", "D"):
        d = _norm_date8(obj.get(key))
        if d:
            return d
    return datetime.now().strftime("%Y%m%d")


def _schema_sha(columns: List[str]) -> str:
    return hashlib.sha256("|".join(columns).encode("utf-8")).hexdigest()


def _num(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([0.0] * len(df), index=df.index)
    return pd.to_numeric(df[col], errors="coerce").fillna(0.0)


def build_features(*, input_path: Path, output_dir: Path, d_override: str = "") -> Dict[str, Any]:
    if not input_path.exists():
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "status": "FAIL",
            "reason": "INPUT_MISSING",
            "input": str(input_path),
            "rows": 0,
            "outputs": {},
        }

    src = _read_csv(input_path)
    empty_input = len(src) == 0
    d = _norm_date8(d_override) or _latest_date8(src) or (_operational_date8() if empty_input else "")
    reasons: List[str] = []
    if not d:
        reasons.append("D_MISSING")

    out = pd.DataFrame(index=src.index)
    out["D"] = d
    out["code"] = src["code"].map(_norm_code) if "code" in src.columns else ""
    out["name"] = src["name"].fillna("").astype(str) if "name" in src.columns else ""
    out["feature_asof"] = d
    out["feature_version"] = FEATURE_VERSION

    for col in FEATURE_COLUMNS:
        if col in {"D", "code", "name", "feature_asof", "feature_version"}:
            continue
        out[col] = _num(src, col)

    out = out[out["code"].astype(str).str.len() == 6].copy()
    dup_count = int(out.duplicated(subset=["D", "code"], keep=False).sum()) if len(out) else 0
    if len(out) == 0 and not empty_input:
        reasons.append("FEATURE_ROW_COUNT_ZERO")
    if dup_count > 0:
        reasons.append("DUPLICATE_D_CODE")
    if d and (out["feature_asof"].astype(str) > d).any():
        reasons.append("FEATURE_ASOF_AFTER_D")

    out = out[FEATURE_COLUMNS]
    schema_hash = _schema_sha(FEATURE_COLUMNS)
    out_csv = output_dir / f"future_signal_features_{d or 'unknown'}.csv"
    out_json = output_dir / f"future_signal_features_{d or 'unknown'}.json"
    _write_csv(out_csv, out)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "FAIL" if reasons else "PASS",
        "reason": "|".join(dict.fromkeys(reasons)) if reasons else ("empty_candidate_input" if empty_input else "ok"),
        "D": d,
        "feature_asof": d,
        "feature_version": FEATURE_VERSION,
        "feature_schema_sha256": schema_hash,
        "input": str(input_path),
        "outputs": {
            "csv": str(out_csv),
            "json": str(out_json),
            "latest_json": str(LATEST_JSON),
        },
        "input_rows": int(len(src)),
        "rows": int(len(out)),
        "duplicate_d_code_rows": dup_count,
        "columns": FEATURE_COLUMNS,
        "policy": {
            "read_only_for_trading": True,
            "orders_modified": False,
            "fills_modified": False,
            "ledger_modified": False,
            "stats_modified": False,
            "gate_modified": False,
            "risk_lock_modified": False,
        },
    }
    _write_json(out_json, payload)
    _write_json(LATEST_JSON, payload)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Build read-only future signal feature artifacts.")
    ap.add_argument("--input", default=str(DEFAULT_INPUT))
    ap.add_argument("--output-dir", default=str(LOGS))
    ap.add_argument("--D", default="")
    args = ap.parse_args()

    payload = build_features(input_path=Path(args.input), output_dir=Path(args.output_dir), d_override=args.D)
    print(
        f"[FUTURE_FEATURES] status={payload.get('status')} reason={payload.get('reason')} "
        f"rows={payload.get('rows')} csv={(payload.get('outputs') or {}).get('csv', '')}"
    )
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
