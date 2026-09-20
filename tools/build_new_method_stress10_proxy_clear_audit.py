#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Build a read-only audit table for the new-method STRESS10 proxy-clear replay."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

CANDIDATES = LOG_DIR / "new_method_candidates_for_followthrough_latest.csv"
INTRADAY = LOG_DIR / "new_method_intraday_price_probe_stress10_proxy_clear_latest.csv"
REPLAY_JSON = LOG_DIR / "new_method_followthrough_replay_stress10_proxy_clear_latest.json"

OUT_CSV = LOG_DIR / "new_method_stress10_proxy_clear_audit_latest.csv"
OUT_JSON = LOG_DIR / "new_method_stress10_proxy_clear_audit_latest.json"
OUT_MD = LOG_DIR / "new_method_stress10_proxy_clear_audit_latest.md"

MIN_CURRENT_VS_OPEN_PCT = 0.003
MIN_CURRENT_VS_PREV_CLOSE_PCT = 0.003
MAX_LOW_FROM_OPEN_PCT = 0.04
MAX_ATR_PCT = 0.05192369520951628
MAX_STRETCH = 1.0719612229679145
MIN_VALUE_KRW = 15_000_000_000.0
MIN_INTRADAY_TRADING_VALUE_KRW = 5_000_000_000.0
MAX_SCORE = 0.8862005532171512


def _num(s, default=0.0):
    return pd.to_numeric(s, errors="coerce").fillna(default)


def _norm_code(s: pd.Series) -> pd.Series:
    return s.astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)


def main() -> int:
    cand = pd.read_csv(CANDIDATES, dtype={"code": str}, encoding="utf-8-sig")
    rt = pd.read_csv(INTRADAY, dtype={"code": str}, encoding="utf-8-sig")
    cand["code"] = _norm_code(cand["code"])
    rt["code"] = _norm_code(rt["code"])

    for c in ["close", "value", "score", "final_score", "stretch", "atr14_pct"]:
        if c not in cand.columns:
            cand[c] = 0.0
        cand[c] = _num(cand[c])
    for c in ["current_price", "open", "high", "low", "volume", "trading_value"]:
        if c not in rt.columns:
            rt[c] = 0.0
        rt[c] = _num(rt[c])

    cols = ["code", "name", "close", "value", "score", "final_score", "stretch", "atr14_pct"]
    for c in cols:
        if c not in cand.columns:
            cand[c] = ""
    work = cand[cols].merge(rt, on="code", how="left", suffixes=("", "_rt"))

    open_denom = work["open"].where(work["open"] > 0)
    close_denom = work["close"].where(work["close"] > 0)
    work["current_vs_open_pct"] = _num(work["current_price"] / open_denom - 1.0)
    work["current_vs_prev_close_pct"] = _num(work["current_price"] / close_denom - 1.0)
    work["low_from_open_pct"] = _num(work["low"] / open_denom - 1.0)
    work["high_from_open_pct"] = _num(work["high"] / open_denom - 1.0)
    work["trading_value_projected"] = work["trading_value"]

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
    block_cols = ["block_low_from_open", "block_high_atr", "block_high_stretch", "block_low_daily_value", "block_high_score"]
    work["blocked_by_risk"] = work[block_cols].any(axis=1)
    work["entry_allowed_validation"] = work["buy_signal"] & ~work["blocked_by_risk"]
    work["block_reasons"] = work[block_cols].apply(
        lambda row: ",".join(name.replace("block_", "") for name, flag in row.items() if bool(flag)),
        axis=1,
    )

    def classify(row: pd.Series) -> str:
        if bool(row["entry_allowed_validation"]):
            return "BUY_POSSIBLE_OBSERVE_ONLY"
        if bool(row["buy_signal"]):
            return "EXPLORE_BLOCKED_RISK"
        if bool(row["buy_signal_price"]) and not bool(row["buy_signal_liquidity"]):
            return "EXCLUDE_LIQUIDITY"
        if bool(row["buy_signal_liquidity"]) and not bool(row["buy_signal_price"]):
            return "EXCLUDE_PRICE"
        return "EXCLUDE_PRICE_AND_LIQUIDITY"

    work["classification"] = work.apply(classify, axis=1)
    work["classification_kr"] = work["classification"].map(
        {
            "BUY_POSSIBLE_OBSERVE_ONLY": "매수 가능 후보(검증용)",
            "EXPLORE_BLOCKED_RISK": "탐색 후보(리스크 차단)",
            "EXCLUDE_LIQUIDITY": "진짜 제외 후보(유동성 미충족)",
            "EXCLUDE_PRICE": "진짜 제외 후보(가격 신호 미충족)",
            "EXCLUDE_PRICE_AND_LIQUIDITY": "진짜 제외 후보(가격·유동성 미충족)",
        }
    )

    replay = json.loads(REPLAY_JSON.read_text(encoding="utf-8")) if REPLAY_JSON.exists() else {}
    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_new_method_stress10_proxy_clear_audit",
        "candidates": str(CANDIDATES),
        "intraday": str(INTRADAY),
        "replay_json": str(REPLAY_JSON),
        "candidate_rows": int(len(cand)),
        "intraday_rows": int(len(rt)),
        "joined_rows": int(work["current_price"].gt(0).sum()),
        "replay_status": replay.get("status"),
        "replay_alerts_count": replay.get("alerts_count"),
        "counts": {str(k): int(v) for k, v in work["classification"].value_counts().to_dict().items()},
        "thresholds": {
            "min_current_vs_open_pct": MIN_CURRENT_VS_OPEN_PCT,
            "min_current_vs_prev_close_pct": MIN_CURRENT_VS_PREV_CLOSE_PCT,
            "max_low_from_open_pct": MAX_LOW_FROM_OPEN_PCT,
            "max_atr_pct": MAX_ATR_PCT,
            "max_stretch": MAX_STRETCH,
            "min_value_krw": MIN_VALUE_KRW,
            "min_intraday_trading_value_krw": MIN_INTRADAY_TRADING_VALUE_KRW,
            "max_score": MAX_SCORE,
        },
    }

    out_cols = [
        "classification_kr",
        "classification",
        "code",
        "name",
        "current_price",
        "open",
        "close",
        "current_vs_open_pct",
        "current_vs_prev_close_pct",
        "low_from_open_pct",
        "high_from_open_pct",
        "trading_value",
        "value",
        "score",
        "final_score",
        "stretch",
        "atr14_pct",
        "buy_signal_price",
        "buy_signal_liquidity",
        "buy_signal",
        "blocked_by_risk",
        "entry_allowed_validation",
        "block_reasons",
    ]
    work[out_cols].to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps({**summary, "rows": work[out_cols].to_dict(orient="records")}, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# New Method STRESS10 Proxy-Clear Audit",
        "",
        f"- generated_at: {summary['generated_at']}",
        f"- replay_status: {summary['replay_status']}",
        f"- replay_alerts_count: {summary['replay_alerts_count']}",
        f"- candidate_rows: {summary['candidate_rows']}",
        f"- intraday_rows: {summary['intraday_rows']}",
        f"- joined_rows: {summary['joined_rows']}",
        "",
        "## Classification counts",
    ]
    for key, value in summary["counts"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Rows"])
    for row in work[out_cols].to_dict(orient="records"):
        lines.append(
            f"- {row['code']} {row['classification_kr']} "
            f"price={row['current_price']} cur/open={row['current_vs_open_pct']:.4f} "
            f"cur/close={row['current_vs_prev_close_pct']:.4f} block={row['block_reasons']}"
        )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "csv": str(OUT_CSV), "json": str(OUT_JSON), "md": str(OUT_MD), "counts": summary["counts"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
