"""Build a read-only shadow ledger for candidate bridge policy simulations.

The ledger records proposed observe-to-entry bridge decisions and fills outcome
columns only when later OHLCV rows are available. It does not alter candidates,
orders, fills, ledger, stats, gates, sizing, or trading eligibility.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


OUTCOME_DAYS = (1, 3, 5)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except Exception:
            continue
    return {}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _norm_date(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text.replace("-", "")[:8]


def _load_price_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["ymd"] = df["date"].astype(str).str.replace("-", "", regex=False).str[:8]
    for col in ("open", "high", "low", "close"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.sort_values(["code", "ymd"]).reset_index(drop=True)


def _outcome_for_row(price_df: pd.DataFrame, code: str, signal_ymd: str, ref_close: float) -> dict[str, Any]:
    out: dict[str, Any] = {
        "outcome_status": "PENDING_FUTURE_PRICE",
        "price_max_ymd": "",
        "available_future_sessions": 0,
        "max_up_5d_pct": None,
        "max_down_5d_pct": None,
    }
    if price_df.empty or not code or not signal_ymd or ref_close <= 0:
        out["outcome_status"] = "MISSING_PRICE_INPUT"
        return out
    code_df = price_df[price_df["code"] == code].copy()
    if code_df.empty:
        out["outcome_status"] = "NO_PRICE_ROWS_FOR_CODE"
        return out
    out["price_max_ymd"] = str(code_df["ymd"].max())
    future = code_df[code_df["ymd"] > signal_ymd].sort_values("ymd").reset_index(drop=True)
    out["available_future_sessions"] = int(len(future))
    if future.empty:
        return out

    highs = pd.to_numeric(future.head(max(OUTCOME_DAYS))["high"], errors="coerce").dropna()
    lows = pd.to_numeric(future.head(max(OUTCOME_DAYS))["low"], errors="coerce").dropna()
    if not highs.empty:
        out["max_up_5d_pct"] = float((highs.max() / ref_close - 1.0) * 100.0)
    if not lows.empty:
        out["max_down_5d_pct"] = float((lows.min() / ref_close - 1.0) * 100.0)

    for days in OUTCOME_DAYS:
        key = f"d{days}"
        if len(future) >= days:
            row = future.iloc[days - 1]
            close = _safe_float(row.get("close"), 0.0)
            out[f"{key}_ymd"] = str(row.get("ymd") or "")
            out[f"{key}_close"] = close if close > 0 else None
            out[f"{key}_close_return_pct"] = float((close / ref_close - 1.0) * 100.0) if close > 0 else None
        else:
            out[f"{key}_ymd"] = ""
            out[f"{key}_close"] = None
            out[f"{key}_close_return_pct"] = None

    out["outcome_status"] = "COMPLETE_D5" if len(future) >= 5 else f"PENDING_D{len(future) + 1}"
    return out


def _return_stats(values: pd.Series) -> dict[str, Any]:
    nums = pd.to_numeric(values, errors="coerce").dropna()
    if nums.empty:
        return {"count": 0}
    wins = int((nums > 0).sum())
    return {
        "count": int(nums.count()),
        "mean_pct": float(nums.mean()),
        "median_pct": float(nums.median()),
        "win_rate_pct": float(wins / int(nums.count()) * 100.0),
        "min_pct": float(nums.min()),
        "max_pct": float(nums.max()),
    }


def _validation_alert(out_df: pd.DataFrame) -> dict[str, Any]:
    if out_df.empty:
        return {
            "alert_status": "VALIDATION_NO_ROWS",
            "alert_message": "No bridge shadow rows are available.",
            "validation_summary": {},
        }

    summary: dict[str, Any] = {
        "completed_d1_rows": 0,
        "completed_d3_rows": 0,
        "completed_d5_rows": 0,
        "decision_return_stats": {},
        "allow_minus_block_d1_mean_pct": None,
        "allow_minus_block_d3_mean_pct": None,
        "allow_minus_block_d5_mean_pct": None,
    }
    decision_col = "proposed_decision_read_only"
    if decision_col not in out_df.columns:
        return {
            "alert_status": "VALIDATION_SCHEMA_GAP",
            "alert_message": "Bridge shadow ledger has no proposed decision column.",
            "validation_summary": summary,
        }

    horizon_cols = {
        "d1": "d1_close_return_pct",
        "d3": "d3_close_return_pct",
        "d5": "d5_close_return_pct",
    }
    for label, col in horizon_cols.items():
        if col in out_df.columns:
            summary[f"completed_{label}_rows"] = int(pd.to_numeric(out_df[col], errors="coerce").notna().sum())

    decisions = out_df[decision_col].fillna("").astype(str)
    for decision, group in out_df.groupby(decisions, dropna=False):
        if not str(decision).strip():
            continue
        decision_stats: dict[str, Any] = {"rows": int(len(group))}
        for label, col in horizon_cols.items():
            if col in group.columns:
                decision_stats[label] = _return_stats(group[col])
        if "max_up_5d_pct" in group.columns:
            decision_stats["max_up_5d"] = _return_stats(group["max_up_5d_pct"])
        if "max_down_5d_pct" in group.columns:
            decision_stats["max_down_5d"] = _return_stats(group["max_down_5d_pct"])
        summary["decision_return_stats"][str(decision)] = decision_stats

    def _mean(decision: str, col: str) -> float | None:
        if col not in out_df.columns:
            return None
        mask = decisions.eq(decision)
        nums = pd.to_numeric(out_df.loc[mask, col], errors="coerce").dropna()
        if nums.empty:
            return None
        return float(nums.mean())

    for label, col in horizon_cols.items():
        allow_mean = _mean("ALLOW_REDUCED", col)
        block_mean = _mean("BLOCK", col)
        key = f"allow_minus_block_{label}_mean_pct"
        if allow_mean is not None and block_mean is not None:
            summary[key] = float(allow_mean - block_mean)

    d1_count = int(summary.get("completed_d1_rows") or 0)
    d5_count = int(summary.get("completed_d5_rows") or 0)
    allow_rows = int((decisions == "ALLOW_REDUCED").sum())
    block_rows = int((decisions == "BLOCK").sum())
    if d1_count == 0:
        status = "VALIDATION_PENDING"
        message = "No future return is available yet; wait for OHLCV beyond the signal date."
    elif allow_rows < 3 or block_rows < 3 or d1_count < 6:
        status = "VALIDATION_SAMPLE_LOW"
        message = "Forward returns exist, but the sample is too small for a policy judgment."
    else:
        delta = summary.get("allow_minus_block_d5_mean_pct")
        if delta is None:
            delta = summary.get("allow_minus_block_d3_mean_pct")
        if delta is None:
            delta = summary.get("allow_minus_block_d1_mean_pct")
        if delta is None:
            status = "VALIDATION_SAMPLE_LOW"
            message = "Comparison sample is incomplete."
        elif float(delta) >= 0.5 and d5_count >= 6:
            status = "VALIDATION_IMPROVING"
            message = "ALLOW_REDUCED is outperforming BLOCK on completed forward returns."
        elif float(delta) <= -0.5 and d5_count >= 6:
            status = "VALIDATION_BAD"
            message = "ALLOW_REDUCED is underperforming BLOCK on completed forward returns."
        else:
            status = "VALIDATION_WEAK"
            message = "ALLOW_REDUCED does not yet show a clear advantage over BLOCK."

    return {
        "alert_status": status,
        "alert_message": message,
        "validation_summary": summary,
    }


def _rows_from_simulation(sim: dict[str, Any]) -> list[dict[str, Any]]:
    rows = sim.get("rows")
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    generated_at = str(sim.get("generated_at") or "")
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        row["simulation_generated_at"] = generated_at
        row["signal_ymd"] = _norm_date(row.get("date"))
        row["code"] = str(row.get("code") or "").zfill(6)
        out.append(row)
    return out


def _candidate_enrichment(root: Path) -> dict[str, dict[str, Any]]:
    path = root / "2_Logs" / "candidates_latest_data.csv"
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path, dtype={"code": str})
    except Exception:
        return {}
    if df.empty or "code" not in df.columns:
        return {}
    df = df.copy()
    df["code"] = df["code"].astype(str).str.zfill(6)
    enrich: dict[str, dict[str, Any]] = {}
    keep_cols = [
        "date",
        "name",
        "close",
        "market",
        "market_regime",
        "trading_value",
        "relax_level",
    ]
    for _, row in df.iterrows():
        code = str(row.get("code") or "").zfill(6)
        if not code:
            continue
        enrich[code] = {col: row.get(col) for col in keep_cols if col in df.columns}
    return enrich


def build_shadow_ledger(root: Path) -> dict[str, Any]:
    log_dir = root / "2_Logs"
    sim_path = log_dir / "candidate_bridge_policy_simulation_latest.json"
    price_path = root / "paper" / "prices" / "ohlcv_paper.parquet"
    history_path = log_dir / "candidate_bridge_shadow_ledger_history.csv"
    latest_csv = log_dir / "candidate_bridge_shadow_ledger_latest.csv"
    latest_json = log_dir / "candidate_bridge_shadow_ledger_latest.json"

    sim = _read_json(sim_path)
    current_rows = _rows_from_simulation(sim)
    enrichment = _candidate_enrichment(root)
    for row in current_rows:
        enrich = enrichment.get(str(row.get("code") or "").zfill(6), {})
        for key, value in enrich.items():
            if key not in row or str(row.get(key) or "").strip() == "":
                row[key] = value
        row["signal_ymd"] = _norm_date(row.get("signal_ymd") or row.get("date"))
    price_df = _load_price_frame(price_path)

    if history_path.exists():
        history = pd.read_csv(history_path, dtype={"code": str, "signal_ymd": str})
    else:
        history = pd.DataFrame()

    current_df = pd.DataFrame(current_rows)
    if not current_df.empty:
        current_df["ledger_key"] = (
            current_df["signal_ymd"].astype(str)
            + "|"
            + current_df["code"].astype(str).str.zfill(6)
            + "|"
            + current_df["proposed_decision_read_only"].fillna("").astype(str)
        )
    if not history.empty and "ledger_key" not in history.columns:
        history["ledger_key"] = (
            history["signal_ymd"].fillna("").astype(str)
            + "|"
            + history["code"].fillna("").astype(str).str.zfill(6)
            + "|"
            + history.get("proposed_decision_read_only", pd.Series([""] * len(history))).fillna("").astype(str)
        )

    if history.empty:
        combined = current_df.copy()
        appended = int(len(current_df))
    elif current_df.empty:
        combined = history.copy()
        appended = 0
    else:
        existing_keys = set(history["ledger_key"].fillna("").astype(str))
        add_df = current_df[~current_df["ledger_key"].fillna("").astype(str).isin(existing_keys)].copy()
        combined = pd.concat([history, add_df], ignore_index=True, sort=False)
        appended = int(len(add_df))

    outcome_rows: list[dict[str, Any]] = []
    if not combined.empty:
        for _, row in combined.iterrows():
            rec = row.to_dict()
            code = str(rec.get("code") or "").zfill(6)
            enrich = enrichment.get(code, {})
            for key, value in enrich.items():
                if key not in rec or str(rec.get(key) or "").strip() == "" or pd.isna(rec.get(key)):
                    rec[key] = value
            signal_ymd = _norm_date(rec.get("signal_ymd") or rec.get("date"))
            ref_close = _safe_float(rec.get("close"), 0.0)
            rec.update(_outcome_for_row(price_df, code, signal_ymd, ref_close))
            outcome_rows.append(rec)
    out_df = pd.DataFrame(outcome_rows)

    if not out_df.empty:
        sort_cols = [c for c in ["signal_ymd", "proposed_decision_read_only", "code"] if c in out_df.columns]
        out_df = out_df.sort_values(sort_cols).reset_index(drop=True) if sort_cols else out_df
        out_df.to_csv(history_path, index=False, encoding="utf-8")
        out_df.to_csv(latest_csv, index=False, encoding="utf-8")
    else:
        pd.DataFrame().to_csv(history_path, index=False, encoding="utf-8")
        pd.DataFrame().to_csv(latest_csv, index=False, encoding="utf-8")

    summary: dict[str, Any] = {}
    if not out_df.empty and "proposed_decision_read_only" in out_df.columns:
        summary["decision_counts"] = {
            str(k): int(v)
            for k, v in out_df["proposed_decision_read_only"].fillna("").value_counts().sort_index().items()
        }
    if not out_df.empty and "outcome_status" in out_df.columns:
        summary["outcome_status_counts"] = {
            str(k): int(v)
            for k, v in out_df["outcome_status"].fillna("").value_counts().sort_index().items()
        }
    if not out_df.empty and "entry_eligible_read_only" in out_df.columns:
        eligible = out_df["entry_eligible_read_only"].astype(str).str.lower().isin({"true", "1", "yes"})
        summary["entry_eligible_read_only_rows"] = int(eligible.sum())
    alert = _validation_alert(out_df)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_candidate_bridge_shadow_ledger",
        "policy_effect": "read_only_no_candidate_order_fill_ledger_stat_change",
        "source_simulation": str(sim_path),
        "source_price": str(price_path),
        "history_csv": str(history_path),
        "latest_csv": str(latest_csv),
        "rows_total": int(len(out_df)),
        "rows_appended": appended,
        "price_max_ymd": str(price_df["ymd"].max()) if not price_df.empty else "",
        "summary": summary,
        "alert_status": alert["alert_status"],
        "alert_message": alert["alert_message"],
        "validation_summary": alert["validation_summary"],
        "limitations": [
            "This ledger is not wired into paper_engine entry eligibility.",
            "Outcomes are filled only when future OHLCV sessions are available.",
            "No orders, fills, operational ledger, gates, or stats are modified.",
        ],
    }
    latest_json.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".", help="RootA path")
    args = parser.parse_args()
    payload = build_shadow_ledger(Path(args.root).resolve())
    print(json.dumps(payload, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
