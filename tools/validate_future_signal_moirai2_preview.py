from __future__ import annotations

import argparse
import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
KRX_ARCHIVE = ROOT / "krx_daily_archive"

DEFAULT_PREVIEW_META = LOGS / "future_signal_moirai2_preview_latest.json"
LATEST_JSON = LOGS / "future_signal_moirai2_validation_latest.json"


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_safe(payload), ensure_ascii=True, indent=2), encoding="utf-8")


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path)


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _norm_date8(value: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(value or ""))
    return s[:8] if len(s) >= 8 else ""


def _norm_code(value: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(value or ""))
    return s.zfill(6)[-6:] if s else ""


def _archive_files() -> List[Path]:
    return sorted(KRX_ARCHIVE.glob("krx_daily_*_clean.parquet"))


def _available_dates() -> List[str]:
    dates = set()
    for path in _archive_files():
        for d in re.findall(r"(\d{8})", path.name):
            dates.add(d)
    return sorted(dates)


def _target_date(d: str, horizon: int, dates: List[str]) -> Optional[str]:
    ordered = [x for x in sorted(set(dates)) if x >= d]
    if d not in ordered:
        return None
    idx = ordered.index(d) + int(horizon)
    if idx >= len(ordered):
        return None
    return ordered[idx]


def _load_close_map(codes: List[str], target_d: str) -> Dict[str, float]:
    out: Dict[str, float] = {}
    code_set = set(codes)
    for path in _archive_files():
        dates = re.findall(r"(\d{8})", path.name)
        if target_d not in dates:
            continue
        df = pd.read_parquet(path, columns=["date", "code", "close"])
        df["date"] = df["date"].map(_norm_date8)
        df["code"] = df["code"].map(_norm_code)
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df[(df["date"] == target_d) & (df["code"].isin(code_set)) & (df["close"] > 0)]
        for _, row in df.iterrows():
            out[str(row["code"])] = float(row["close"])
    return out


def _safe_corr(df: pd.DataFrame, left: str, right: str) -> Optional[float]:
    if len(df) < 3:
        return None
    x = pd.to_numeric(df[left], errors="coerce")
    y = pd.to_numeric(df[right], errors="coerce")
    work = pd.DataFrame({"x": x, "y": y}).dropna()
    if len(work) < 3:
        return None
    val = float(work["x"].corr(work["y"], method="spearman"))
    return round(val, 6) if math.isfinite(val) else None


def validate_moirai2_preview(*, preview_meta_path: Path, output_dir: Path) -> Dict[str, Any]:
    preview_meta = _load_json(preview_meta_path)
    generated_at = datetime.now().isoformat(timespec="seconds")
    d = _norm_date8(preview_meta.get("D"))
    out_json = output_dir / f"future_signal_moirai2_validation_{d or 'unknown'}.json"
    out_rows = output_dir / f"future_signal_moirai2_validation_rows_{d or 'unknown'}.csv"
    policy = {
        "read_only_for_trading": True,
        "trading_approved": False,
        "orders_modified": False,
        "fills_modified": False,
        "ledger_modified": False,
        "stats_modified": False,
        "gate_modified": False,
        "risk_lock_modified": False,
    }
    base = {
        "generated_at": generated_at,
        "D": d,
        "preview_meta": str(preview_meta_path),
        "model_version": str(preview_meta.get("model_version") or ""),
        "model_id": str(preview_meta.get("model_id") or ""),
        "outputs": {"json": str(out_json), "latest_json": str(LATEST_JSON), "rows_csv": str(out_rows)},
        "policy": policy,
    }

    if not preview_meta:
        payload = {**base, "status": "FAIL", "validation_state": "BLOCKED", "reason": "PREVIEW_META_MISSING", "rows": 0}
        _write_json(out_json, payload)
        _write_json(LATEST_JSON, payload)
        return payload
    preview_csv = Path(str((preview_meta.get("outputs") or {}).get("csv", "")))
    if not preview_csv.exists():
        payload = {**base, "status": "FAIL", "validation_state": "BLOCKED", "reason": "PREVIEW_CSV_MISSING", "rows": 0}
        _write_json(out_json, payload)
        _write_json(LATEST_JSON, payload)
        return payload

    preview = _read_csv(preview_csv)
    horizon_vals = pd.to_numeric(preview.get("horizon", pd.Series([5])), errors="coerce").dropna()
    horizon = int(horizon_vals.iloc[0]) if len(horizon_vals) else int(preview_meta.get("prediction_length") or 5)
    dates = _available_dates()
    target_d = _target_date(d, horizon, dates)
    base = {
        **base,
        "horizon": horizon,
        "available_price_dates_min": min(dates) if dates else "",
        "available_price_dates_max": max(dates) if dates else "",
        "target_date": target_d or "",
    }
    if not target_d:
        payload = {
            **base,
            "status": "FAIL",
            "validation_state": "WAITING",
            "reason": "TARGET_DATE_NOT_AVAILABLE",
            "rows": int(len(preview)),
            "realized_rows": 0,
        }
        _write_json(out_json, payload)
        _write_json(LATEST_JSON, payload)
        return payload

    preview["code"] = preview["code"].map(_norm_code)
    codes = preview["code"].dropna().astype(str).tolist()
    closes = _load_close_map(codes, target_d)
    rows: List[Dict[str, Any]] = []
    for _, row in preview.iterrows():
        code = str(row.get("code", ""))
        target_close = closes.get(code)
        current_close = pd.to_numeric(pd.Series([row.get("current_close")]), errors="coerce").iloc[0]
        expected = pd.to_numeric(pd.Series([row.get("expected_return")]), errors="coerce").iloc[0]
        if target_close is None or not math.isfinite(float(current_close)) or float(current_close) <= 0:
            continue
        realized = (float(target_close) / float(current_close)) - 1.0
        rows.append(
            {
                "D": d,
                "target_date": target_d,
                "code": code,
                "name": str(row.get("name", "") or ""),
                "current_close": float(current_close),
                "target_close": float(target_close),
                "expected_return": float(expected) if math.isfinite(float(expected)) else None,
                "realized_return": round(realized, 8),
                "actual_up": bool(realized > 0),
            }
        )

    realized = pd.DataFrame(rows)
    _write_csv(out_rows, realized)
    if len(realized) == 0:
        payload = {
            **base,
            "status": "FAIL",
            "validation_state": "BLOCKED",
            "reason": "REALIZED_ROWS_ZERO",
            "rows": int(len(preview)),
            "realized_rows": 0,
        }
        _write_json(out_json, payload)
        _write_json(LATEST_JSON, payload)
        return payload

    realized["expected_return"] = pd.to_numeric(realized["expected_return"], errors="coerce")
    realized["realized_return"] = pd.to_numeric(realized["realized_return"], errors="coerce")
    mae = float((realized["expected_return"] - realized["realized_return"]).abs().mean())
    direction_hit = float(((realized["expected_return"] > 0) == (realized["realized_return"] > 0)).mean())
    payload = {
        **base,
        "status": "PASS",
        "validation_state": "EVALUATED",
        "reason": "ok",
        "rows": int(len(preview)),
        "realized_rows": int(len(realized)),
        "mean_abs_error": round(mae, 8),
        "direction_hit_rate": round(direction_hit, 6),
        "spearman_expected_vs_realized": _safe_corr(realized, "expected_return", "realized_return"),
    }
    _write_json(out_json, payload)
    _write_json(LATEST_JSON, payload)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate read-only Moirai2 future-signal shadow preview.")
    ap.add_argument("--preview-meta", default=str(DEFAULT_PREVIEW_META))
    ap.add_argument("--output-dir", default=str(LOGS))
    args = ap.parse_args()
    payload = validate_moirai2_preview(preview_meta_path=Path(args.preview_meta), output_dir=Path(args.output_dir))
    print(
        f"[FUTURE_MOIRAI2_VALIDATION] status={payload.get('status')} state={payload.get('validation_state')} "
        f"reason={payload.get('reason')} D={payload.get('D')} target={payload.get('target_date', '')}"
    )
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
