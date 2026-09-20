from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
INPUT_CSV = LOG_DIR / "entry_trace_performance_latest.csv"
OUT_JSON = LOG_DIR / "normal_intraday_realtime_loss_breakdown_latest.json"
OUT_CSV = LOG_DIR / "normal_intraday_realtime_loss_breakdown_latest.csv"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype=str)


def _to_float_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _group_summary(df: pd.DataFrame, key: str) -> List[Dict[str, Any]]:
    if df.empty or key not in df.columns:
        return []
    rows: List[Dict[str, Any]] = []
    for value, grp in df.groupby(key, dropna=False):
        net = _to_float_series(grp["net_ret_sum"]).dropna()
        rows.append(
            {
                "key": str(value),
                "entries": int(len(grp)),
                "realized_entries": int(len(net)),
                "mean_net": float(net.mean()) if len(net) else 0.0,
                "median_net": float(net.median()) if len(net) else 0.0,
                "win_rate": float((net > 0).mean()) if len(net) else 0.0,
                "loss_sum": float(net[net < 0].sum()) if len(net) else 0.0,
                "gain_sum": float(net[net > 0].sum()) if len(net) else 0.0,
            }
        )
    rows.sort(key=lambda x: (float(x["mean_net"]), -int(x["entries"])))
    return rows


def run() -> Dict[str, Any]:
    df = _read_csv(INPUT_CSV)
    if df.empty:
        payload = {"generated_at": _now_ts(), "status": "NO_INPUT", "input": str(INPUT_CSV)}
        OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    work = df[~df["entry_trace_id"].astype(str).str.startswith("MISSING_TRACE_", na=False)].copy()
    target = work[work["entry_class"].astype(str) == "normal_intraday_realtime"].copy()
    if target.empty:
        payload = {"generated_at": _now_ts(), "status": "NO_TARGET_ROWS", "input": str(INPUT_CSV)}
        OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    target["net_ret_sum_num"] = _to_float_series(target["net_ret_sum"])
    target["closed_rows_num"] = pd.to_numeric(target["closed_rows"], errors="coerce").fillna(0).astype(int)
    realized = target[target["closed_rows_num"] > 0].copy()

    worst_cols = [
        "entry_trace_id",
        "code",
        "entry_ts",
        "entry_qty",
        "entry_price",
        "entry_notional",
        "signal_date",
        "horizon",
        "closed_rows",
        "closed_qty",
        "net_ret_sum",
        "net_ret_mean",
        "exit_reasons",
        "last_exit_ymd",
        "status",
    ]
    worst = realized.sort_values("net_ret_sum_num", ascending=True).head(20).copy()
    worst[[c for c in worst_cols if c in worst.columns]].to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    net = realized["net_ret_sum_num"].dropna()
    payload = {
        "generated_at": _now_ts(),
        "status": "OK",
        "input": str(INPUT_CSV),
        "outputs": {"worst_csv": str(OUT_CSV)},
        "counts": {
            "target_entries": int(len(target)),
            "realized_entries": int(len(realized)),
            "open_entries": int((target["closed_rows_num"] <= 0).sum()),
        },
        "metrics": {
            "win_rate": float((net > 0).mean()) if len(net) else 0.0,
            "mean_net": float(net.mean()) if len(net) else 0.0,
            "median_net": float(net.median()) if len(net) else 0.0,
            "loss_sum": float(net[net < 0].sum()) if len(net) else 0.0,
            "gain_sum": float(net[net > 0].sum()) if len(net) else 0.0,
        },
        "by_entry_ymd": _group_summary(realized, "entry_ymd"),
        "by_horizon": _group_summary(realized, "horizon"),
        "by_exit_reasons": _group_summary(realized, "exit_reasons"),
        "worst_rows": worst[[c for c in worst_cols if c in worst.columns]].to_dict(orient="records"),
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> int:
    print(json.dumps(run(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
