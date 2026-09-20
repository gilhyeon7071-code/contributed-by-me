from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


ROOT = Path("E:/1_Data")
LOGS = ROOT / "2_Logs"
DEFAULT_HISTORY = LOGS / "future_signal_intraday_overlay_watchlist_history.csv"
LATEST_JSON = LOGS / "future_signal_intraday_watchlist_markout_latest.json"
LATEST_CSV = LOGS / "future_signal_intraday_watchlist_markout_latest.csv"


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc, dtype={"code": str})
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype={"code": str})


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _norm_code(value: Any) -> str:
    text = str(value or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(6) if text else ""


def _norm_date(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text[:8] if len(text) >= 8 else ""


def _intraday_return_map(d: str) -> Dict[str, Dict[str, Any]]:
    path = LOGS / f"intraday_prices_history_{d}.csv"
    if not path.exists():
        return {}
    df = _read_csv(path)
    required = {"ts", "code", "current_price"}
    if not required.issubset(df.columns):
        return {}
    work = df.copy()
    work["code"] = work["code"].map(_norm_code)
    work["current_price"] = pd.to_numeric(work["current_price"], errors="coerce")
    work = work.dropna(subset=["code", "current_price"])
    work = work[work["current_price"] > 0].copy()
    if work.empty:
        return {}
    work = work.sort_values(["code", "ts"])
    out: Dict[str, Dict[str, Any]] = {}
    for code, g in work.groupby("code"):
        first = g.iloc[0]
        last = g.iloc[-1]
        first_price = float(first["current_price"])
        last_price = float(last["current_price"])
        if first_price <= 0:
            continue
        out[str(code)] = {
            "source": str(path),
            "first_ts": str(first.get("ts") or ""),
            "last_ts": str(last.get("ts") or ""),
            "first_price": first_price,
            "last_price": last_price,
            "realized_return": (last_price / first_price) - 1.0,
        }
    return out


def _metrics(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty or "realized_return" not in df.columns:
        return {"rows": 0}
    realized = pd.to_numeric(df["realized_return"], errors="coerce").dropna()
    if realized.empty:
        return {"rows": 0}
    return {
        "rows": int(len(realized)),
        "hit_rate": round(float((realized > 0).mean()), 6),
        "mean_return": round(float(realized.mean()), 6),
        "median_return": round(float(realized.median()), 6),
        "q10_return": round(float(realized.quantile(0.10)), 6),
        "q90_return": round(float(realized.quantile(0.90)), 6),
        "max_loss": round(float(realized.min()), 6),
        "max_gain": round(float(realized.max()), 6),
    }


def build_markout(history_csv: Path) -> Dict[str, Any]:
    if not history_csv.exists():
        payload = {"status": "FAIL", "reason": "WATCHLIST_HISTORY_MISSING", "path": str(history_csv)}
        _write_json(LATEST_JSON, payload)
        return payload
    hist = _read_csv(history_csv)
    required = {"D", "code", "condition"}
    missing = sorted(required - set(hist.columns))
    if missing:
        payload = {"status": "FAIL", "reason": "REQUIRED_COLUMN_MISSING", "missing": missing}
        _write_json(LATEST_JSON, payload)
        return payload
    work = hist.copy()
    work["D"] = work["D"].map(_norm_date)
    work["code"] = work["code"].map(_norm_code)
    work = work.dropna(subset=["D", "code"]).copy()
    work = work.drop_duplicates(subset=["D", "code", "condition"], keep="last")

    maps: Dict[str, Dict[str, Dict[str, Any]]] = {}
    rows: List[Dict[str, Any]] = []
    for _, row in work.iterrows():
        d = str(row.get("D") or "")
        code = str(row.get("code") or "")
        if d not in maps:
            maps[d] = _intraday_return_map(d)
        ret_info = maps.get(d, {}).get(code)
        out = row.to_dict()
        if ret_info:
            out.update(ret_info)
            out["actual_up"] = bool(float(ret_info["realized_return"]) > 0)
            out["markout_status"] = "EVALUATED"
        else:
            out.update(
                {
                    "source": str(LOGS / f"intraday_prices_history_{d}.csv"),
                    "first_ts": "",
                    "last_ts": "",
                    "first_price": None,
                    "last_price": None,
                    "realized_return": None,
                    "actual_up": None,
                    "markout_status": "WAITING_OR_MISSING_PRICE",
                }
            )
        out["used_for_trading"] = False
        out["policy_note"] = "SHADOW_MARKOUT_ONLY_NOT_ROUTED"
        rows.append(out)

    out_df = pd.DataFrame(rows)
    _write_csv(LATEST_CSV, out_df)
    evaluated = out_df[out_df["markout_status"] == "EVALUATED"].copy() if "markout_status" in out_df.columns else pd.DataFrame()
    by_condition: Dict[str, Any] = {}
    if len(evaluated) and "condition" in evaluated.columns:
        for condition, g in evaluated.groupby("condition"):
            by_condition[str(condition)] = _metrics(g)
    current_condition = ""
    if "condition" in work.columns and len(work):
        current_candidates = work[work["condition"].astype(str).str.contains("NON_ABSTAIN", na=False)]
        if len(current_candidates):
            current_condition = str(current_candidates.iloc[-1].get("condition") or "")
        else:
            current_condition = str(work.iloc[-1].get("condition") or "")
    current_rows = out_df[out_df["condition"].astype(str) == current_condition].copy() if current_condition else pd.DataFrame()
    current_evaluated = (
        current_rows[current_rows["markout_status"] == "EVALUATED"].copy()
        if len(current_rows) and "markout_status" in current_rows.columns
        else pd.DataFrame()
    )

    payload = {
        "status": "PASS",
        "reason": "ok",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "watchlist_rows": int(len(work)),
        "markout_rows": int(len(out_df)),
        "evaluated_rows": int(len(evaluated)),
        "waiting_or_missing_rows": int((out_df.get("markout_status", pd.Series(dtype=str)) == "WAITING_OR_MISSING_PRICE").sum()),
        "metrics": _metrics(evaluated),
        "metrics_by_condition": by_condition,
        "current_condition": current_condition,
        "current_condition_rows": int(len(current_rows)),
        "current_condition_evaluated_rows": int(len(current_evaluated)),
        "current_condition_metrics": _metrics(current_evaluated),
        "policy": {
            "shadow_only": True,
            "used_for_trading": False,
            "orders_modified": False,
            "fills_modified": False,
            "ledger_modified": False,
            "stats_modified": False,
            "gate_modified": False,
            "risk_lock_modified": False,
            "score_modified": False,
            "threshold_applied": False,
        },
        "outputs": {
            "json": str(LATEST_JSON),
            "csv": str(LATEST_CSV),
        },
        "source": str(history_csv),
    }
    _write_json(LATEST_JSON, payload)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Build read-only intraday markout for future-signal overlay watchlist.")
    ap.add_argument("--history-csv", default=str(DEFAULT_HISTORY))
    args = ap.parse_args()
    payload = build_markout(Path(args.history_csv))
    print(
        "[FUTURE_INTRADAY_WATCHLIST_MARKOUT] "
        f"status={payload.get('status')} reason={payload.get('reason')} "
        f"evaluated_rows={payload.get('evaluated_rows', 0)}"
    )
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
