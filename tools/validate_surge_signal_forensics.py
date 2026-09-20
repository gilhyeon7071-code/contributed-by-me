from __future__ import annotations

import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER_DIR = ROOT / "paper"
PRICES_PATH = PAPER_DIR / "prices" / "ohlcv_paper.parquet"
TRADES_PATH = PAPER_DIR / "trades.csv"
JOIN_PATH = LOG_DIR / "pnl_trade_signal_join_latest.csv"
OUT_LATEST = LOG_DIR / "surge_signal_forensics_latest.json"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _dated_path() -> Path:
    return LOG_DIR / f"surge_signal_forensics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc, dtype=str)
        except Exception:
            continue
    return pd.DataFrame()


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None or str(v).strip() == "":
            return None
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def _pct(v: Optional[float]) -> Optional[float]:
    return None if v is None else round(float(v) * 100.0, 4)


def _norm_code(v: Any) -> str:
    digits = re.sub(r"[^0-9]", "", str(v or ""))
    return digits[-6:].zfill(6) if digits else ""


def _ymd(v: Any) -> str:
    digits = re.sub(r"[^0-9]", "", str(v or ""))
    return digits[:8] if len(digits) >= 8 else ""


def _note_fields(note: Any) -> Dict[str, str]:
    out: Dict[str, str] = {}
    text = str(note or "").replace("|", ";")
    for part in text.split(";"):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        k = k.strip()
        if k:
            out[k] = v.strip()
    return out


def _segment(df: pd.DataFrame, ret_col: str) -> Dict[str, Any]:
    vals = pd.to_numeric(df.get(ret_col), errors="coerce").dropna()
    if vals.empty:
        return {"rows": int(len(df)), "valid_returns": 0}
    wins = vals > 0
    return {
        "rows": int(len(df)),
        "valid_returns": int(len(vals)),
        "avg_return_pct": _pct(float(vals.mean())),
        "median_return_pct": _pct(float(vals.median())),
        "win_rate_pct": round(float(wins.mean()) * 100.0, 2),
        "min_return_pct": _pct(float(vals.min())),
        "max_return_pct": _pct(float(vals.max())),
    }


def _load_prices() -> pd.DataFrame:
    if not PRICES_PATH.exists():
        return pd.DataFrame()
    try:
        px = pd.read_parquet(PRICES_PATH)
    except Exception:
        return pd.DataFrame()
    need = {"code", "date", "close"}
    if not need.issubset(set(px.columns)):
        return pd.DataFrame()
    out = px[["code", "date", "close"]].copy()
    out["code"] = out["code"].map(_norm_code)
    out["date"] = out["date"].map(_ymd)
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    return out.dropna(subset=["close"]).sort_values(["code", "date"])


def _load_surge_signal_history() -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for path in sorted(LOG_DIR.glob("surge_realtime_*.csv")):
        if path.name == "surge_realtime_latest.csv":
            continue
        df = _read_csv(path)
        if df.empty:
            continue
        df["_source_path"] = str(path)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    if "code" in out.columns:
        out["code"] = out["code"].map(_norm_code)
    if "date" in out.columns:
        out["date"] = out["date"].map(_ymd)
    return out


def _forward_returns_by_type(signals: pd.DataFrame, prices: pd.DataFrame) -> Dict[str, Any]:
    if signals.empty:
        return {"status": "NOT_EVALUABLE", "reason": "surge_signal_history_missing", "rows": 0}
    if prices.empty:
        return {"status": "NOT_EVALUABLE", "reason": "price_history_missing", "rows": int(len(signals))}
    required = {"code", "date", "surge_type"}
    if not required.issubset(set(signals.columns)):
        return {"status": "NOT_EVALUABLE", "reason": "required_columns_missing", "required": sorted(required)}

    sig = signals.copy()
    if "is_realtime_surge" in sig.columns:
        sig = sig[sig["is_realtime_surge"].astype(str).str.lower().isin(["true", "1", "yes"])]
    if "surge_flag" in sig.columns:
        sig = sig[sig["surge_flag"].astype(str).str.lower().isin(["true", "1", "yes"])]
    sig = sig[sig["code"].astype(str).str.len().eq(6) & sig["date"].astype(str).str.len().eq(8)]

    rows: List[Dict[str, Any]] = []
    price_groups = {code: g.reset_index(drop=True) for code, g in prices.groupby("code")}
    for _, r in sig.iterrows():
        code = str(r.get("code", ""))
        date = str(r.get("date", ""))
        g = price_groups.get(code)
        if g is None or g.empty:
            continue
        idxs = g.index[g["date"] >= date].tolist()
        if not idxs:
            continue
        idx = int(idxs[0])
        entry = _safe_float(r.get("current_price")) or float(g.loc[idx, "close"])
        row = {
            "code": code,
            "date": date,
            "surge_type": str(r.get("surge_type", "") or "UNKNOWN"),
            "entry_price": float(entry),
        }
        valid_any = False
        for n in (1, 3, 5):
            j = idx + n
            if j < len(g):
                row[f"ret_{n}d"] = float(g.loc[j, "close"] / entry - 1.0)
                valid_any = True
        if valid_any:
            rows.append(row)
    fwd = pd.DataFrame(rows)
    if fwd.empty:
        return {
            "status": "NOT_EVALUABLE",
            "reason": "no_signal_has_future_price_yet",
            "signal_rows": int(len(sig)),
        }
    summary: Dict[str, Any] = {}
    for typ, g in fwd.groupby("surge_type"):
        summary[str(typ)] = {f"{n}d": _segment(g, f"ret_{n}d") for n in (1, 3, 5)}
    return {"status": "EVALUATED", "signal_rows": int(len(sig)), "forward_rows": int(len(fwd)), "by_type": summary}


def _score_bin_for_column(df: pd.DataFrame, score_col: str) -> Dict[str, Any]:
    work = df.copy()
    work["_score"] = pd.to_numeric(work[score_col], errors="coerce")
    if work["_score"].max(skipna=True) is not None and float(work["_score"].max(skipna=True) or 0.0) <= 1.5:
        work["_score"] = work["_score"] * 100.0
    work["_net_ret"] = pd.to_numeric(work["net_ret"], errors="coerce")
    work = work.dropna(subset=["_score", "_net_ret"])
    if work.empty:
        return {"status": "NOT_EVALUABLE", "reason": "no_valid_score_return_rows", "rows": int(len(df))}
    bins = [
        ("lt75", work["_score"] < 75.0),
        ("75_85", (work["_score"] >= 75.0) & (work["_score"] < 85.0)),
        ("85_95", (work["_score"] >= 85.0) & (work["_score"] < 95.0)),
        ("gte95", work["_score"] >= 95.0),
    ]
    return {
        "status": "EVALUATED",
        "source": str(JOIN_PATH),
        "score_col": score_col,
        "valid_rows": int(len(work)),
        "correlation_score_net_ret": round(float(work["_score"].corr(work["_net_ret"])), 6) if len(work) >= 2 else None,
        "bins": {name: _segment(work.loc[mask], "_net_ret") for name, mask in bins},
    }


def _score_bins() -> Dict[str, Any]:
    df = _read_csv(JOIN_PATH)
    if df.empty:
        return {"status": "NOT_EVALUABLE", "reason": "pnl_trade_signal_join_missing"}
    if "net_ret" not in df.columns:
        return {"status": "NOT_EVALUABLE", "reason": "net_ret_missing", "rows": int(len(df))}
    candidates = [c for c in ("surge_score_final", "final_score", "score") if c in df.columns]
    if not candidates:
        return {"status": "NOT_EVALUABLE", "reason": "score_column_missing", "rows": int(len(df))}
    by_column = {c: _score_bin_for_column(df, c) for c in candidates}
    preferred = "score" if "score" in by_column else candidates[0]
    out = dict(by_column[preferred])
    out["rows"] = int(len(df))
    out["preferred_score_col"] = preferred
    out["by_column"] = by_column
    return out


def _stop_loss_structure(prices: pd.DataFrame) -> Dict[str, Any]:
    trades = _read_csv(TRADES_PATH)
    if trades.empty:
        return {"status": "NOT_EVALUABLE", "reason": "trades_missing"}
    if "exit_reason" not in trades.columns:
        return {"status": "NOT_EVALUABLE", "reason": "exit_reason_missing", "rows": int(len(trades))}
    work = trades.copy()
    work["exit_reason"] = work["exit_reason"].astype(str)
    stop = work[work["exit_reason"].str.contains("STOP", na=False)].copy()
    if stop.empty:
        return {"status": "EVALUATED", "rows": int(len(work)), "stop_rows": 0}
    stop["pnl_pct_num"] = pd.to_numeric(stop.get("pnl_pct"), errors="coerce")
    stop["entry_ymd"] = stop.get("entry_date", "").map(_ymd)
    stop["exit_ymd"] = stop.get("exit_date", "").map(_ymd)
    entry_dt = pd.to_datetime(stop["entry_ymd"], format="%Y%m%d", errors="coerce")
    exit_dt = pd.to_datetime(stop["exit_ymd"], format="%Y%m%d", errors="coerce")
    stop["hold_days_calendar"] = (exit_dt - entry_dt).dt.days

    rebound_rows: List[Dict[str, Any]] = []
    if not prices.empty:
        groups = {code: g.reset_index(drop=True) for code, g in prices.groupby("code")}
        for _, r in stop.iterrows():
            code = _norm_code(r.get("code"))
            g = groups.get(code)
            exit_ymd = str(r.get("exit_ymd", ""))
            exit_price = _safe_float(r.get("exit_price"))
            if g is None or not exit_ymd or exit_price is None:
                continue
            idxs = g.index[g["date"] >= exit_ymd].tolist()
            if not idxs:
                continue
            idx = int(idxs[0])
            row = {"code": code, "exit_ymd": exit_ymd}
            for n in (1, 3):
                j = idx + n
                if j < len(g):
                    row[f"after_exit_{n}d_ret"] = float(g.loc[j, "close"] / exit_price - 1.0)
            rebound_rows.append(row)
    rebound = pd.DataFrame(rebound_rows)
    return {
        "status": "EVALUATED",
        "source": str(TRADES_PATH),
        "rows": int(len(work)),
        "stop_rows": int(len(stop)),
        "stop_ratio_pct": round(float(len(stop) / max(len(work), 1)) * 100.0, 2),
        "avg_stop_pnl_pct": _pct(float(stop["pnl_pct_num"].mean())) if stop["pnl_pct_num"].notna().any() else None,
        "hold_le_2d_rows": int((stop["hold_days_calendar"] <= 2).sum()),
        "hold_le_2d_ratio_pct": round(float((stop["hold_days_calendar"] <= 2).mean()) * 100.0, 2),
        "by_exit_reason": {
            str(reason): _segment(g, "pnl_pct_num") for reason, g in stop.groupby("exit_reason")
        },
        "post_stop_rebound": {
            "status": "EVALUATED" if not rebound.empty else "NOT_EVALUABLE",
            "rows": int(len(rebound)),
            "after_exit_1d": _segment(rebound, "after_exit_1d_ret") if not rebound.empty else {},
            "after_exit_3d": _segment(rebound, "after_exit_3d_ret") if not rebound.empty else {},
        },
    }


def main() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    prices = _load_prices()
    signals = _load_surge_signal_history()
    payload = {
        "ts": _now(),
        "status": "OK",
        "scope": [
            "type_forward_returns_1d_3d_5d",
            "score_bin_performance",
            "stop_loss_structure",
            "surge_signal_history_availability",
        ],
        "inputs": {
            "surge_signal_history_rows": int(len(signals)),
            "prices_rows": int(len(prices)),
            "join_path": str(JOIN_PATH),
            "trades_path": str(TRADES_PATH),
        },
        "type_forward_returns": _forward_returns_by_type(signals, prices),
        "score_bin_performance": _score_bins(),
        "stop_loss_structure": _stop_loss_structure(prices),
    }
    dated = _dated_path()
    payload["out_json"] = str(OUT_LATEST)
    payload["history_json"] = str(dated)
    OUT_LATEST.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    dated.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": payload["status"], "out_json": str(OUT_LATEST), "history_json": str(dated)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
