from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
INPUT_CSV = LOG_DIR / "new_method_candidates_for_replay_latest.csv"
INTRADAY_CSV = LOG_DIR / "intraday_prices_latest.csv"
FOLLOWTHROUGH_SCRIPT = ROOT / "tools" / "followthrough_realtime.py"

OUT_SUMMARY_JSON = LOG_DIR / "new_method_followthrough_branch_replay_summary_latest.json"
OUT_SUMMARY_CSV = LOG_DIR / "new_method_followthrough_branch_replay_summary_latest.csv"
OUT_AUDIT_CSV = LOG_DIR / "new_method_followthrough_branch_replay_join_audit_latest.csv"
OUT_MD = LOG_DIR / "new_method_followthrough_branch_replay_summary_latest.md"

FOLLOWTHROUGH_COLUMNS = [
    "date",
    "code",
    "name",
    "market",
    "market_regime",
    "close",
    "value",
    "score",
    "final_score",
    "stretch",
    "atr14_pct",
    "rsi14",
    "high_52w_gap",
    "relax_level",
    "candidate_origin",
    "method_branch",
    "signal_reason",
    "horizon",
    "expected_holding_days",
    "observe_status",
    "promotion_blocker",
    "forward_return_status",
    "source",
    "new_method_rank_in_branch",
    "new_method_forward_return",
]

NUMERIC_COLUMNS = [
    "close",
    "value",
    "score",
    "final_score",
    "stretch",
    "atr14_pct",
    "rsi14",
    "high_52w_gap",
    "expected_holding_days",
    "new_method_rank_in_branch",
    "new_method_forward_return",
]

MIN_CURRENT_VS_OPEN_PCT = 0.003
MIN_CURRENT_VS_PREV_CLOSE_PCT = 0.003
MAX_LOW_FROM_OPEN_PCT = 0.04
MAX_ATR_PCT = 0.05192369520951628
MAX_STRETCH = 1.0719612229679145
MIN_VALUE_KRW = 15_000_000_000.0
MIN_INTRADAY_TRADING_VALUE_KRW = 5_000_000_000.0
MAX_SCORE = 0.8862005532171512


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_name(*parts: Any) -> str:
    raw = "_".join(str(p) for p in parts)
    safe = "".join(ch if ch.isalnum() else "_" for ch in raw)
    return "_".join(item for item in safe.split("_") if item).lower()


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _prepare_slice(group: pd.DataFrame, safe: str) -> Path:
    out = LOG_DIR / f"new_method_candidates_for_followthrough_{safe}_latest.csv"
    work = group.copy()
    work["code"] = work["code"].astype("string").str.zfill(6)
    for col in NUMERIC_COLUMNS:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")
    work = work.sort_values(["new_method_rank_in_branch", "code"], na_position="last")
    work[FOLLOWTHROUGH_COLUMNS].to_csv(out, index=False, encoding="utf-8-sig")
    return out


def _run_followthrough(candidate_path: Path, safe: str) -> tuple[int, Path, Path]:
    prefix = LOG_DIR / f"new_method_followthrough_branch_replay_{safe}"
    out_csv = prefix.with_name(prefix.name + "_latest.csv")
    out_json = prefix.with_name(prefix.name + "_latest.json")
    cmd = [
        sys.executable,
        str(FOLLOWTHROUGH_SCRIPT),
        "--candidates",
        str(candidate_path),
        "--intraday",
        str(INTRADAY_CSV),
        "--out-csv",
        str(out_csv),
        "--out-json",
        str(out_json),
        "--history-csv",
        str(prefix.with_name(prefix.name + "_history.csv")),
        "--history-jsonl",
        str(prefix.with_name(prefix.name + "_history.jsonl")),
        "--mid-track-csv",
        str(prefix.with_name(prefix.name + "_mid_track.csv")),
        "--mid-track-jsonl",
        str(prefix.with_name(prefix.name + "_mid_track.jsonl")),
        "--mid-first-seen-json",
        str(prefix.with_name(prefix.name + "_mid_first_seen.json")),
        "--mid-status-csv",
        str(prefix.with_name(prefix.name + "_mid_status.csv")),
        "--mid-status-jsonl",
        str(prefix.with_name(prefix.name + "_mid_status.jsonl")),
        "--observation-track-csv",
        str(prefix.with_name(prefix.name + "_observation_track.csv")),
        "--observation-track-jsonl",
        str(prefix.with_name(prefix.name + "_observation_track.jsonl")),
        "--observation-first-seen-json",
        str(prefix.with_name(prefix.name + "_observation_first_seen.json")),
    ]
    proc = subprocess.run(cmd, cwd=str(ROOT), text=True, capture_output=True)
    return proc.returncode, out_csv, out_json


def _audit_join(candidate_path: Path, payload: dict[str, Any], branch_meta: dict[str, Any]) -> pd.DataFrame:
    cand = pd.read_csv(candidate_path, dtype={"code": "string"})
    rt = pd.read_csv(INTRADAY_CSV, dtype={"code": "string"})
    cand["code"] = cand["code"].astype("string").str.zfill(6)
    rt["code"] = rt["code"].astype("string").str.zfill(6)
    cand = cand.rename(columns={"date": "candidate_date"})
    rt = rt.rename(columns={"date": "intraday_date"})

    for col in ("close", "value", "score", "final_score", "stretch", "atr14_pct", "high_52w_gap"):
        cand[col] = pd.to_numeric(cand.get(col, 0.0), errors="coerce").fillna(0.0)
    for col in ("current_price", "open", "high", "low", "volume", "trading_value"):
        rt[col] = pd.to_numeric(rt.get(col, 0.0), errors="coerce").fillna(0.0)

    keep = [
        "candidate_date",
        "code",
        "market_regime",
        "method_branch",
        "horizon",
        "close",
        "value",
        "score",
        "final_score",
        "stretch",
        "atr14_pct",
        "candidate_origin",
        "promotion_blocker",
        "forward_return_status",
    ]
    work = cand[keep].merge(rt, on="code", how="inner")
    if work.empty:
        return pd.DataFrame(
            [
                {
                    **branch_meta,
                    "code": "",
                    "audit_status": "NO_JOINED_ROWS",
                    "entry_allowed_validation": False,
                    "pseudo_intraday_candidate": False,
                }
            ]
        )

    elapsed = float(payload.get("diagnostics", {}).get("elapsed_market_frac", 1.0) or 1.0)
    open_denom = work["open"].where(work["open"] > 0)
    close_denom = work["close"].where(work["close"] > 0)
    current_denom = work["current_price"].where(work["current_price"] > 0)
    work["current_vs_open_pct"] = work["current_price"] / open_denom - 1.0
    work["current_vs_prev_close_pct"] = work["current_price"] / close_denom - 1.0
    work["low_from_open_pct"] = work["low"] / open_denom - 1.0
    work["high_from_open_pct"] = work["high"] / open_denom - 1.0
    work["day_range_pct"] = (work["high"] - work["low"]) / current_denom
    work = work.replace([float("inf"), float("-inf")], pd.NA).fillna(0.0)
    work["trading_value_projected"] = work["trading_value"] / elapsed
    work["buy_signal_price"] = (
        (work["current_price"] > 0)
        & (work["open"] > 0)
        & (work["close"] > 0)
        & (work["current_vs_open_pct"] >= MIN_CURRENT_VS_OPEN_PCT)
        & (work["current_vs_prev_close_pct"] >= MIN_CURRENT_VS_PREV_CLOSE_PCT)
    )
    work["buy_signal_liquidity"] = work["trading_value_projected"] >= MIN_INTRADAY_TRADING_VALUE_KRW
    work["buy_signal"] = work["buy_signal_price"] & work["buy_signal_liquidity"]
    work["block_low_from_open"] = work["low_from_open_pct"] < -abs(MAX_LOW_FROM_OPEN_PCT)
    work["block_high_atr"] = work["atr14_pct"] > MAX_ATR_PCT
    work["block_high_stretch"] = work["stretch"] > MAX_STRETCH
    work["block_low_daily_value"] = work["value"] < MIN_VALUE_KRW
    work["block_high_score"] = work["score"] > MAX_SCORE
    block_cols = [
        "block_low_from_open",
        "block_high_atr",
        "block_high_stretch",
        "block_low_daily_value",
        "block_high_score",
    ]
    work["blocked_by_risk"] = work[block_cols].any(axis=1)
    work["entry_allowed_validation"] = work["buy_signal"] & ~work["blocked_by_risk"]
    work["pseudo_intraday_core"] = (
        (work["current_vs_open_pct"] > 0.0)
        & (work["high_from_open_pct"] >= 0.03)
        & (work["low_from_open_pct"] >= -0.04)
    )
    work["pseudo_intraday_stretch_ok"] = work["stretch"] <= 1.15
    work["pseudo_intraday_candidate"] = work["pseudo_intraday_core"] & work["pseudo_intraday_stretch_ok"]
    work["audit_status"] = "JOINED"
    for key, value in branch_meta.items():
        work[key] = value
    cols = [
        "branch_key",
        "candidate_date",
        "intraday_date",
        "code",
        "market_regime",
        "method_branch",
        "horizon",
        "audit_status",
        "current_price",
        "open",
        "high",
        "low",
        "trading_value",
        "trading_value_projected",
        "close",
        "value",
        "score",
        "stretch",
        "atr14_pct",
        "current_vs_open_pct",
        "current_vs_prev_close_pct",
        "low_from_open_pct",
        "high_from_open_pct",
        "buy_signal_price",
        "buy_signal_liquidity",
        "buy_signal",
        "blocked_by_risk",
        "entry_allowed_validation",
        "pseudo_intraday_candidate",
        "promotion_blocker",
        "forward_return_status",
    ]
    return work[cols]


def main() -> int:
    if not INPUT_CSV.exists():
        raise FileNotFoundError(INPUT_CSV)
    if not INTRADAY_CSV.exists():
        raise FileNotFoundError(INTRADAY_CSV)

    df = pd.read_csv(INPUT_CSV, dtype={"code": "string"})
    missing = [c for c in FOLLOWTHROUGH_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError("missing columns: " + ", ".join(missing))

    summaries: list[dict[str, Any]] = []
    audits: list[pd.DataFrame] = []
    group_cols = ["date", "market_regime", "method_branch", "horizon"]
    for keys, group in df.groupby(group_cols, dropna=False, sort=True):
        date, regime, branch, horizon = keys
        safe = _safe_name(date, regime, branch, horizon)
        branch_key = f"{date}|{regime}|{branch}|{horizon}"
        branch_meta = {
            "branch_key": branch_key,
            "date": str(date),
            "market_regime": str(regime),
            "method_branch": str(branch),
            "horizon": str(horizon),
        }
        slice_path = _prepare_slice(group, safe)
        returncode, out_csv, out_json = _run_followthrough(slice_path, safe)
        payload = _read_json(out_json)
        diagnostics = payload.get("diagnostics", {})
        audit = _audit_join(slice_path, payload, branch_meta)
        audits.append(audit)
        raw_entry_allowed_count = (
            int(audit["entry_allowed_validation"].fillna(False).astype(bool).sum())
            if "entry_allowed_validation" in audit.columns
            else 0
        )
        raw_pseudo_intraday_candidate_count = (
            int(audit["pseudo_intraday_candidate"].fillna(False).astype(bool).sum())
            if "pseudo_intraday_candidate" in audit.columns
            else 0
        )

        duplicate_code_rows = int(group.duplicated(["code"], keep=False).sum())
        invalid_code_rows = int((group["code"].astype("string").str.zfill(6).str.len() != 6).sum())
        summaries.append(
            {
                **branch_meta,
                "safe": safe,
                "slice_path": str(slice_path),
                "followthrough_returncode": returncode,
                "followthrough_status": payload.get("status", "MISSING_PAYLOAD"),
                "candidate_rows": int(len(group)),
                "unique_codes": int(group["code"].nunique()),
                "duplicate_code_rows": duplicate_code_rows,
                "invalid_code_rows": invalid_code_rows,
                "closed_rows": int((group["forward_return_status"].astype(str) == "CLOSED").sum()),
                "pending_rows": int((group["forward_return_status"].astype(str) != "CLOSED").sum()),
                "intraday_rows": int(payload.get("intraday_rows", 0) or 0),
                "joined_rows": int(payload.get("joined_rows", 0) or 0),
                "alerts_count": int(payload.get("alerts_count", 0) or 0),
                "buy_signal_price_count": int(diagnostics.get("buy_signal_price_count", 0) or 0),
                "buy_signal_liquidity_count": int(diagnostics.get("buy_signal_liquidity_count", 0) or 0),
                "buy_signal_count": int(diagnostics.get("buy_signal_count", 0) or 0),
                "entry_allowed_validation_count": int(diagnostics.get("entry_allowed_validation_count", 0) or 0),
                "pseudo_intraday_candidate_count": int(diagnostics.get("pseudo_intraday_candidate_count", 0) or 0),
                "raw_entry_allowed_validation_count": raw_entry_allowed_count,
                "raw_pseudo_intraday_candidate_count": raw_pseudo_intraday_candidate_count,
                "out_csv": str(out_csv),
                "out_json": str(out_json),
                "promotion_blockers": ";".join(sorted(str(v) for v in group["promotion_blocker"].dropna().unique())),
            }
        )

    summary_df = pd.DataFrame(summaries).sort_values(["date", "method_branch"])
    audit_df = pd.concat(audits, ignore_index=True) if audits else pd.DataFrame()
    summary_df.to_csv(OUT_SUMMARY_CSV, index=False, encoding="utf-8-sig")
    audit_df.to_csv(OUT_AUDIT_CSV, index=False, encoding="utf-8-sig")

    payload = {
        "generated_at": _now(),
        "classification": "NEW_METHOD_FOLLOWTHROUGH_BRANCH_REPLAY_SUMMARY",
        "operation_effect": "READ_ONLY_BRANCH_REPLAY_ONLY",
        "input": str(INPUT_CSV),
        "intraday": str(INTRADAY_CSV),
        "branch_count": int(len(summary_df)),
        "total_candidate_rows": int(summary_df["candidate_rows"].sum()) if not summary_df.empty else 0,
        "total_joined_rows": int(summary_df["joined_rows"].sum()) if not summary_df.empty else 0,
        "total_alerts_count": int(summary_df["alerts_count"].sum()) if not summary_df.empty else 0,
        "total_entry_allowed_validation_count": int(summary_df["entry_allowed_validation_count"].sum()) if not summary_df.empty else 0,
        "total_pseudo_intraday_candidate_count": int(summary_df["pseudo_intraday_candidate_count"].sum()) if not summary_df.empty else 0,
        "total_raw_entry_allowed_validation_count": int(summary_df["raw_entry_allowed_validation_count"].sum()) if not summary_df.empty else 0,
        "total_raw_pseudo_intraday_candidate_count": int(summary_df["raw_pseudo_intraday_candidate_count"].sum()) if not summary_df.empty else 0,
        "operational_candidate_files_written": False,
        "summary_csv": str(OUT_SUMMARY_CSV),
        "audit_csv": str(OUT_AUDIT_CSV),
        "branches": summary_df.to_dict(orient="records"),
    }
    OUT_SUMMARY_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# New Method Followthrough Branch Replay Summary",
        "",
        f"- classification: {payload['classification']}",
        f"- operation_effect: {payload['operation_effect']}",
        f"- branch_count: {payload['branch_count']}",
        f"- total_candidate_rows: {payload['total_candidate_rows']}",
        f"- total_joined_rows: {payload['total_joined_rows']}",
        f"- total_alerts_count: {payload['total_alerts_count']}",
        f"- total_entry_allowed_validation_count: {payload['total_entry_allowed_validation_count']}",
        f"- total_pseudo_intraday_candidate_count: {payload['total_pseudo_intraday_candidate_count']}",
        f"- total_raw_entry_allowed_validation_count: {payload['total_raw_entry_allowed_validation_count']}",
        f"- total_raw_pseudo_intraday_candidate_count: {payload['total_raw_pseudo_intraday_candidate_count']}",
        f"- operational_candidate_files_written: {payload['operational_candidate_files_written']}",
        "",
        "## Branches",
    ]
    for row in summary_df.to_dict(orient="records"):
        lines.append(
            "- {date} / {market_regime} / {method_branch} / {horizon}: "
            "candidates={candidate_rows}, joined={joined_rows}, price={buy_signal_price_count}, "
            "liquidity={buy_signal_liquidity_count}, buy={buy_signal_count}, "
            "entry={entry_allowed_validation_count}, pseudo={pseudo_intraday_candidate_count}, "
            "raw_entry={raw_entry_allowed_validation_count}, raw_pseudo={raw_pseudo_intraday_candidate_count}, "
            "status={followthrough_status}".format(**row)
        )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
