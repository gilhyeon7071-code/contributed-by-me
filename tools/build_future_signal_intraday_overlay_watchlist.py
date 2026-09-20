from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd


ROOT = Path("E:/1_Data")
LOGS = ROOT / "2_Logs"
DEFAULT_SHADOW_JOIN = LOGS / "future_signal_candidate_shadow_join_latest.csv"
LATEST_JSON = LOGS / "future_signal_intraday_overlay_watchlist_latest.json"
LATEST_CSV = LOGS / "future_signal_intraday_overlay_watchlist_latest.csv"
HISTORY_CSV = LOGS / "future_signal_intraday_overlay_watchlist_history.csv"


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


def build_watchlist(shadow_join_csv: Path, *, top_pct: float = 0.20, require_non_abstain: bool = True) -> Dict[str, Any]:
    if not shadow_join_csv.exists():
        payload = {"status": "FAIL", "reason": "SHADOW_JOIN_CSV_MISSING", "path": str(shadow_join_csv)}
        _write_json(LATEST_JSON, payload)
        return payload

    df = _read_csv(shadow_join_csv)
    required = {"D", "code", "horizon_type", "pred_up_prob"}
    missing = sorted(required - set(df.columns))
    if missing:
        payload = {"status": "FAIL", "reason": "REQUIRED_COLUMN_MISSING", "missing": missing}
        _write_json(LATEST_JSON, payload)
        return payload

    work = df.copy()
    work["code"] = work["code"].map(_norm_code)
    work["horizon_type"] = work["horizon_type"].astype(str).str.upper()
    work["pred_up_prob"] = pd.to_numeric(work["pred_up_prob"], errors="coerce")
    work["confidence"] = pd.to_numeric(work.get("confidence", 0.0), errors="coerce").fillna(0.0)
    work["expected_return"] = pd.to_numeric(work.get("expected_return", 0.0), errors="coerce").fillna(0.0)
    work["abstain_bool"] = work.get("abstain", False).astype(str).str.lower().isin({"true", "1", "yes", "y"})
    intraday = work[work["horizon_type"] == "INTRADAY"].dropna(subset=["pred_up_prob"]).copy()
    if require_non_abstain:
        intraday = intraday[~intraday["abstain_bool"]].copy()
    if intraday.empty:
        suffix = "_NON_ABSTAIN" if require_non_abstain else ""
        condition = f"INTRADAY_PRED_TOP{int(top_pct * 100)}{suffix}"
        empty_cols = [
            "recorded_at",
            "D",
            "code",
            "name",
            "condition",
            "pred_rank",
            "pred_count",
            "pred_rank_pct",
            "pred_up_prob",
            "expected_return",
            "confidence",
            "abstain",
            "final_score",
            "forecast_score",
            "score",
            "candidate_origin",
            "candidate_origin_hybrid",
            "used_for_trading",
            "policy_note",
        ]
        _write_csv(LATEST_CSV, pd.DataFrame(columns=empty_cols))
        history_rows = int(len(_read_csv(HISTORY_CSV))) if HISTORY_CSV.exists() else 0
        if len(work) == 0:
            payload = {
                "status": "PASS",
                "reason": "empty_candidate_input",
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "D_values": [],
                "condition": condition,
                "require_non_abstain": bool(require_non_abstain),
                "source_rows": 0,
                "intraday_rows": 0,
                "watchlist_rows": 0,
                "history_rows": history_rows,
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
                    "history_csv": str(HISTORY_CSV),
                },
                "source": str(shadow_join_csv),
            }
            _write_json(LATEST_JSON, payload)
            return payload
        payload = {"status": "FAIL", "reason": "INTRADAY_ROWS_ZERO", "source_rows": int(len(work))}
        _write_json(LATEST_JSON, payload)
        return payload

    intraday = intraday.sort_values(["D", "pred_up_prob", "confidence", "code"], ascending=[True, False, False, True])
    intraday["pred_rank"] = intraday.groupby("D").cumcount() + 1
    intraday["pred_count"] = intraday.groupby("D")["code"].transform("count")
    intraday["pred_rank_pct"] = 1.0 - ((intraday["pred_rank"] - 1) / intraday["pred_count"].clip(lower=1))
    cutoff_count = intraday.groupby("D")["code"].transform(lambda s: max(1, int(len(s) * top_pct + 0.999999)))
    watch = intraday[intraday["pred_rank"] <= cutoff_count].copy()
    suffix = "_NON_ABSTAIN" if require_non_abstain else ""
    watch["condition"] = f"INTRADAY_PRED_TOP{int(top_pct * 100)}{suffix}"
    watch["used_for_trading"] = False
    watch["policy_note"] = "SHADOW_WATCHLIST_ONLY_NOT_ROUTED"
    watch["recorded_at"] = datetime.now().isoformat(timespec="seconds")

    ordered_cols = [
        c
        for c in [
            "recorded_at",
            "D",
            "code",
            "name",
            "condition",
            "pred_rank",
            "pred_count",
            "pred_rank_pct",
            "pred_up_prob",
            "expected_return",
            "confidence",
            "abstain",
            "final_score",
            "forecast_score",
            "score",
            "candidate_origin",
            "candidate_origin_hybrid",
            "used_for_trading",
            "policy_note",
        ]
        if c in watch.columns
    ]
    watch = watch[ordered_cols].copy()
    _write_csv(LATEST_CSV, watch)

    if HISTORY_CSV.exists():
        history = _read_csv(HISTORY_CSV)
        combined = pd.concat([history, watch], ignore_index=True)
    else:
        combined = watch.copy()
    if {"D", "code", "condition"}.issubset(combined.columns):
        combined["code"] = combined["code"].map(_norm_code)
        combined = combined.drop_duplicates(subset=["D", "code", "condition"], keep="last")
    _write_csv(HISTORY_CSV, combined)

    payload = {
        "status": "PASS",
        "reason": "ok",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "D_values": sorted(str(x) for x in watch["D"].dropna().unique().tolist()),
        "condition": f"INTRADAY_PRED_TOP{int(top_pct * 100)}{suffix}",
        "require_non_abstain": bool(require_non_abstain),
        "source_rows": int(len(work)),
        "intraday_rows": int(len(intraday)),
        "watchlist_rows": int(len(watch)),
        "history_rows": int(len(combined)),
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
            "history_csv": str(HISTORY_CSV),
        },
        "source": str(shadow_join_csv),
    }
    _write_json(LATEST_JSON, payload)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Build read-only INTRADAY future-signal overlay watchlist.")
    ap.add_argument("--shadow-join-csv", default=str(DEFAULT_SHADOW_JOIN))
    ap.add_argument("--top-pct", type=float, default=0.20)
    ap.add_argument("--allow-abstain", action="store_true")
    args = ap.parse_args()
    payload = build_watchlist(
        Path(args.shadow_join_csv),
        top_pct=float(args.top_pct),
        require_non_abstain=not bool(args.allow_abstain),
    )
    print(
        "[FUTURE_INTRADAY_WATCHLIST] "
        f"status={payload.get('status')} reason={payload.get('reason')} "
        f"watchlist_rows={payload.get('watchlist_rows', 0)}"
    )
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
