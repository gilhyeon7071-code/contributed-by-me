#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Audit new-method candidate outcomes from actual post-signal KRX paths.

This is a read-only research artifact.  It intentionally does not use the
existing operating logic's buy/block/score outputs or the history file's
precalculated forward-return fields.  Every outcome in this audit is
recalculated from the KRX daily archive under PRICE_HISTORY_INTEGRITY_V1.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from utils.price_history_contract import (
    CONTRACT_VERSION,
    KEY_COL,
    SESSION_COL,
    add_exact_session_forward_returns,
    apply_price_history_contract,
)


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
KRX_ARCHIVE = ROOT / "krx_daily_archive"

INPUT_HISTORY = LOG_DIR / "new_method_candidates_history.csv"
OUT_DETAIL_CSV = LOG_DIR / "new_method_path_expectancy_audit_latest.csv"
OUT_SUMMARY_CSV = LOG_DIR / "new_method_path_expectancy_summary_latest.csv"
OUT_JSON = LOG_DIR / "new_method_path_expectancy_audit_latest.json"
OUT_MD = LOG_DIR / "new_method_path_expectancy_audit_latest.md"

# These fields are deliberately excluded from the input read.  This audit must
# not be conditioned on the existing operating logic or earlier replay verdicts.
BANNED_EXISTING_LOGIC_FIELDS = (
    "entry_allowed_validation",
    "block_reasons",
    "blocked_by_risk",
    "buy_signal",
    "high_score",
    "low_from_open",
    "forward_return",
    "forward_return_status",
    "status",
    "promotion_blocker",
    "operational_use",
)

ALLOWED_CANDIDATE_FIELDS = [
    "date",
    "code",
    "market",
    "regime",
    "method_branch",
    "horizon",
    "expected_holding_days",
    "candidate_score",
    "rank_in_branch",
    "close",
    "trade_value",
    "rs",
    "rsi14",
    "stretch",
    "atr_pct",
]


def _number(value: Any) -> float:
    return float(pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0])


def _normal_code(value: Any) -> str:
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(6)


def _archive_paths(start_ymd: str, end_ymd: str) -> list[Path]:
    selected: list[Path] = []
    for path in sorted(KRX_ARCHIVE.glob("krx_daily_*_clean.parquet")):
        parts = path.stem.replace("krx_daily_", "").replace("_clean", "").split("_")
        if len(parts) != 2:
            continue
        file_start, file_end = parts
        if file_end >= start_ymd and file_start <= end_ymd:
            selected.append(path)
    return selected


def _load_krx_window(start_ymd: str, end_ymd: str) -> tuple[pd.DataFrame, list[str]]:
    paths = _archive_paths(start_ymd, end_ymd)
    if not paths:
        raise ValueError(f"no KRX archive partitions overlap {start_ymd}..{end_ymd}")
    frames: list[pd.DataFrame] = []
    for path in paths:
        frame = pd.read_parquet(path, columns=["date", "code", "open", "high", "low", "close", "value"])
        frame["date"] = frame["date"].astype(str).str.replace("-", "", regex=False)
        frame = frame[frame["date"].between(start_ymd, end_ymd)].copy()
        frames.append(frame)
    prices = pd.concat(frames, ignore_index=True)
    prices["code"] = prices["code"].map(_normal_code)
    prices = prices.drop_duplicates(["date", "code"], keep="last")
    return prices, [str(path) for path in paths]


def _tercile(series: pd.Series, *, low: str, mid: str, high: str) -> pd.Series:
    ranks = pd.to_numeric(series, errors="coerce").rank(method="first", pct=True)
    return pd.Series(np.select([ranks.le(1 / 3), ranks.le(2 / 3)], [low, mid], default=high), index=series.index)


def _signal_state(row: pd.Series) -> str:
    rsi = _number(row.get("rsi14"))
    rs = _number(row.get("rs"))
    if np.isfinite(rsi) and rsi <= 30 and np.isfinite(rs) and rs >= 1:
        return "VERY_OVERSOLD_STRONG_RS"
    if np.isfinite(rsi) and rsi <= 40 and np.isfinite(rs) and rs >= 1:
        return "OVERSOLD_STRONG_RS"
    if np.isfinite(rsi) and rsi <= 40:
        return "OVERSOLD_OTHER_RS"
    if np.isfinite(rs) and rs >= 1:
        return "STRONG_RS_OTHER_RSI"
    return "OTHER_SIGNAL_PROFILE"


def _rank_state(value: Any) -> str:
    rank = _number(value)
    if np.isfinite(rank) and rank <= 3:
        return "TOP3"
    if np.isfinite(rank) and rank <= 7:
        return "MID4_7"
    return "TAIL8_PLUS"


def _path_state(row: pd.Series) -> str:
    terminal = _number(row.get("path_forward_return"))
    mfe = _number(row.get("mfe"))
    mae = _number(row.get("mae"))
    if not np.isfinite(terminal):
        return "PENDING_OR_INVALID_PATH"
    if terminal > 0 and mae >= 0:
        return "POSITIVE_NO_DRAWDOWN"
    if terminal > 0:
        return "POSITIVE_AFTER_DRAWDOWN"
    if terminal <= 0 and mfe > 0:
        return "NEGATIVE_AFTER_RALLY"
    return "NEGATIVE_NO_RALLY"


def _summarize(detail: pd.DataFrame, axis: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for value, group in detail.groupby(axis, dropna=False):
        closed = group[group["path_status"].eq("CLOSED_EXACT")].copy()
        returns = pd.to_numeric(closed["path_forward_return"], errors="coerce").dropna()
        mfes = pd.to_numeric(closed["mfe"], errors="coerce").dropna()
        maes = pd.to_numeric(closed["mae"], errors="coerce").dropna()
        reward_risk = pd.to_numeric(closed["mfe_mae_ratio"], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        rows.append(
            {
                "analysis_axis": axis,
                "axis_value": str(value),
                "rows": int(len(group)),
                "closed_rows": int(len(returns)),
                "pending_or_invalid_rows": int(len(group) - len(returns)),
                "win_rows": int((returns > 0).sum()) if len(returns) else 0,
                "win_rate": round(float((returns > 0).mean()), 6) if len(returns) else None,
                "avg_path_forward_return": round(float(returns.mean()), 6) if len(returns) else None,
                "median_path_forward_return": round(float(returns.median()), 6) if len(returns) else None,
                "avg_mfe": round(float(mfes.mean()), 6) if len(mfes) else None,
                "avg_mae": round(float(maes.mean()), 6) if len(maes) else None,
                "median_mfe_mae_ratio": round(float(reward_risk.median()), 6) if len(reward_risk) else None,
                "positive_no_drawdown_rows": int((closed["path_state"] == "POSITIVE_NO_DRAWDOWN").sum()),
                "positive_after_drawdown_rows": int((closed["path_state"] == "POSITIVE_AFTER_DRAWDOWN").sum()),
                "negative_after_rally_rows": int((closed["path_state"] == "NEGATIVE_AFTER_RALLY").sum()),
                "negative_no_rally_rows": int((closed["path_state"] == "NEGATIVE_NO_RALLY").sum()),
            }
        )
    return pd.DataFrame(rows)


def main() -> int:
    if not INPUT_HISTORY.exists():
        raise SystemExit(f"missing input: {INPUT_HISTORY}")

    schema = pd.read_csv(INPUT_HISTORY, nrows=0, encoding="utf-8-sig").columns.tolist()
    missing = [col for col in ALLOWED_CANDIDATE_FIELDS if col not in schema]
    if missing:
        raise SystemExit(f"input lacks required candidate fields: {missing}")
    # usecols is the enforcement point: banned fields never enter the analysis frame.
    candidates = pd.read_csv(INPUT_HISTORY, usecols=ALLOWED_CANDIDATE_FIELDS, dtype={"code": str}, encoding="utf-8-sig")
    candidates["code"] = candidates["code"].map(_normal_code)
    candidates["date"] = pd.to_datetime(candidates["date"], errors="coerce").dt.strftime("%Y%m%d")
    numeric_fields = ["expected_holding_days", "candidate_score", "rank_in_branch", "close", "trade_value", "rs", "rsi14", "stretch", "atr_pct"]
    for col in numeric_fields:
        candidates[col] = pd.to_numeric(candidates[col], errors="coerce")
    if candidates[["date", "code", "expected_holding_days"]].isna().any().any():
        raise SystemExit("candidate keys or holding days contain missing values")
    if candidates.duplicated(["date", "code", "method_branch", "horizon"]).any():
        raise SystemExit("duplicate candidate keys detected")

    start_ymd = (pd.to_datetime(candidates["date"].min()) - pd.Timedelta(days=7)).strftime("%Y%m%d")
    end_ymd = (pd.to_datetime(candidates["date"].max()) + pd.Timedelta(days=int(candidates["expected_holding_days"].max()) + 10)).strftime("%Y%m%d")
    raw_prices, archive_paths = _load_krx_window(start_ymd, end_ymd)
    prices, integrity = apply_price_history_contract(raw_prices)
    prices = add_exact_session_forward_returns(prices, horizons=sorted(candidates["expected_holding_days"].dropna().astype(int).unique()))
    prices["date_ymd"] = prices["date"].dt.strftime("%Y%m%d")

    signal_cols = ["date_ymd", "code", "close", KEY_COL, SESSION_COL]
    signal = prices[signal_cols].rename(columns={"date_ymd": "date", "close": "krx_signal_close", KEY_COL: "signal_key", SESSION_COL: "signal_session"})
    detail = candidates.merge(signal, on=["date", "code"], how="left", validate="many_to_one")
    detail["candidate_close_delta"] = detail["close"].div(detail["krx_signal_close"]).sub(1.0)
    detail["price_basis_status"] = np.select(
        [detail["krx_signal_close"].isna(), detail["candidate_close_delta"].abs().le(0.000001)],
        ["SIGNAL_PRICE_MISSING", "SIGNAL_CLOSE_MATCH"],
        default="SIGNAL_CLOSE_DIFFERENT",
    )

    path_records: list[dict[str, Any]] = []
    price_groups = {key: group.sort_values(SESSION_COL).copy() for key, group in prices.groupby(KEY_COL, sort=False)}
    for row in detail.itertuples(index=False):
        record: dict[str, Any] = {"path_status": "SIGNAL_PRICE_MISSING", "path_forward_return": np.nan, "mfe": np.nan, "mae": np.nan, "mfe_mae_ratio": np.nan, "exit_date": None, "krx_exit_close": np.nan, "observed_future_sessions": 0}
        if pd.notna(row.signal_key) and pd.notna(row.signal_session) and pd.notna(row.krx_signal_close):
            group = price_groups.get(row.signal_key)
            horizon = int(row.expected_holding_days)
            future = group[(group[SESSION_COL] > int(row.signal_session)) & (group[SESSION_COL] <= int(row.signal_session) + horizon)] if group is not None else pd.DataFrame()
            exact = len(future) == horizon and (future[SESSION_COL].iloc[-1] - int(row.signal_session) == horizon if len(future) else False)
            record["observed_future_sessions"] = int(len(future))
            if exact:
                exit_row = future.iloc[-1]
                signal_close = float(row.krx_signal_close)
                mfe = float(future["high"].max() / signal_close - 1.0)
                mae = float(future["low"].min() / signal_close - 1.0)
                record.update(
                    {
                        "path_status": "CLOSED_EXACT",
                        "path_forward_return": float(exit_row["close"] / signal_close - 1.0),
                        "mfe": mfe,
                        "mae": mae,
                        "mfe_mae_ratio": float(mfe / abs(mae)) if mae < 0 else np.nan,
                        "exit_date": exit_row["date"].strftime("%Y%m%d"),
                        "krx_exit_close": float(exit_row["close"]),
                    }
                )
            else:
                record["path_status"] = "PENDING_OR_SEGMENT_BREAK"
        path_records.append(record)
    path = pd.DataFrame(path_records, index=detail.index)
    detail = pd.concat([detail, path], axis=1)

    detail["signal_state"] = detail.apply(_signal_state, axis=1)
    detail["liquidity_tercile"] = _tercile(detail["trade_value"], low="LOW_LIQUIDITY", mid="MID_LIQUIDITY", high="HIGH_LIQUIDITY")
    detail["volatility_tercile"] = _tercile(detail["atr_pct"], low="LOW_VOLATILITY", mid="MID_VOLATILITY", high="HIGH_VOLATILITY")
    detail["rank_state"] = detail["rank_in_branch"].map(_rank_state)
    detail["path_state"] = detail.apply(_path_state, axis=1)

    axes = ["regime", "method_branch", "signal_state", "liquidity_tercile", "volatility_tercile", "rank_state", "path_state"]
    summary = pd.concat([_summarize(detail, axis) for axis in axes], ignore_index=True)
    summary = summary.sort_values(["analysis_axis", "axis_value"]).reset_index(drop=True)

    detail_columns = [
        "date", "code", "market", "regime", "method_branch", "horizon", "expected_holding_days", "candidate_score", "rank_in_branch", "rank_state",
        "trade_value", "liquidity_tercile", "atr_pct", "volatility_tercile", "rsi14", "rs", "stretch", "signal_state",
        "close", "krx_signal_close", "candidate_close_delta", "price_basis_status", "path_status", "observed_future_sessions", "exit_date", "krx_exit_close",
        "path_forward_return", "mfe", "mae", "mfe_mae_ratio", "path_state",
    ]
    detail[detail_columns].to_csv(OUT_DETAIL_CSV, index=False, encoding="utf-8-sig")
    summary.to_csv(OUT_SUMMARY_CSV, index=False, encoding="utf-8-sig")

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_new_method_path_expectancy_audit",
        "question": "Which independent candidate axes describe post-signal price-path expectancy without using existing operating-logic verdicts?",
        "input_history": str(INPUT_HISTORY),
        "krx_archive_partitions": archive_paths,
        "price_history_contract": integrity,
        "candidate_fields_read": ALLOWED_CANDIDATE_FIELDS,
        "excluded_existing_logic_fields": list(BANNED_EXISTING_LOGIC_FIELDS),
        "rows": int(len(detail)),
        "unique_codes": int(detail["code"].nunique()),
        "date_min": str(detail["date"].min()),
        "date_max": str(detail["date"].max()),
        "path_status_counts": {str(k): int(v) for k, v in detail["path_status"].value_counts().to_dict().items()},
        "price_basis_status_counts": {str(k): int(v) for k, v in detail["price_basis_status"].value_counts().to_dict().items()},
        "summary_records": summary.astype(object).where(pd.notna(summary), None).to_dict(orient="records"),
        "methodology": {
            "outcome": "signal-day KRX close to exact h-th following global trading session close within the same integrity segment",
            "mfe": "maximum KRX high from D+1 through D+h divided by signal-day KRX close minus one",
            "mae": "minimum KRX low from D+1 through D+h divided by signal-day KRX close minus one",
            "analysis_axes": axes[:-1],
            "axis_rule": "Each axis is summarized independently; no full cross-product eligibility rule is imposed.",
        },
        "limitations": [
            "The candidate population is the existing 90-row new-method observe-only history, not the entire market universe.",
            "Method branches were created under regime-specific research hypotheses; independent summaries are descriptive, not causal proof.",
            "This daily-price path audit has no intraday execution, fill, slippage, commission, or market-impact model.",
            "Terciles are descriptive splits of this candidate sample, not approved operating thresholds.",
        ],
        "operational_change": False,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

    lines = [
        "# New Method Path Expectancy Audit",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- rows: {payload['rows']}",
        f"- unique_codes: {payload['unique_codes']}",
        f"- date_range: {payload['date_min']} to {payload['date_max']}",
        f"- path_status_counts: {payload['path_status_counts']}",
        f"- price_basis_status_counts: {payload['price_basis_status_counts']}",
        "- existing operating-logic fields: excluded from input read",
        "",
        "## Independent-axis summary",
    ]
    for _, item in summary.iterrows():
        avg = item["avg_path_forward_return"]
        win = item["win_rate"]
        lines.append(
            f"- {item['analysis_axis']} / {item['axis_value']}: rows={int(item['rows'])}, closed={int(item['closed_rows'])}, "
            f"pending_or_invalid={int(item['pending_or_invalid_rows'])}, win_rate={'-' if pd.isna(win) else f'{float(win):.3f}'}, "
            f"avg_ret={'-' if pd.isna(avg) else f'{float(avg):.4f}'}"
        )
    lines.extend(["", "## Caveats", *[f"- {text}" for text in payload["limitations"]]])
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "detail": str(OUT_DETAIL_CSV), "summary": str(OUT_SUMMARY_CSV), "json": str(OUT_JSON), "md": str(OUT_MD)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
