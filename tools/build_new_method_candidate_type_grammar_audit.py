#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Re-read STRESS10 candidates with a candidate-type grammar instead of pass/fail only.

This is observe-only. It does not change gates, scores, orders, or operational files.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

INPUT_CSV = LOG_DIR / "new_method_stress10_proxy_clear_audit_latest.csv"
INPUT_REPLAY_JSON = LOG_DIR / "new_method_followthrough_replay_stress10_proxy_clear_latest.json"

OUT_CSV = LOG_DIR / "new_method_candidate_type_grammar_audit_latest.csv"
OUT_JSON = LOG_DIR / "new_method_candidate_type_grammar_audit_latest.json"
OUT_MD = LOG_DIR / "new_method_candidate_type_grammar_audit_latest.md"

PRICE_MOMENTUM_OPEN_MIN = 0.03
PRICE_SIGNAL_OPEN_MIN = 0.003
PRICE_SIGNAL_CLOSE_MIN = 0.003
LOW_STRICT_LIMIT = -0.04
LOW_SOFT_LIMIT = -0.08
LIQUIDITY_MIN = 5_000_000_000.0


def _bool(v: Any) -> bool:
    return str(v).strip().lower() in {"1", "true", "y", "yes"}


def _float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or str(v).strip() == "":
            return default
        return float(v)
    except Exception:
        return default


def _price_state(row: pd.Series) -> str:
    cur_open = _float(row.get("current_vs_open_pct"))
    cur_close = _float(row.get("current_vs_prev_close_pct"))
    if cur_open >= PRICE_MOMENTUM_OPEN_MIN and cur_close >= PRICE_SIGNAL_CLOSE_MIN:
        return "MOMENTUM_UP"
    if cur_open >= PRICE_SIGNAL_OPEN_MIN and cur_close >= PRICE_SIGNAL_CLOSE_MIN:
        return "MILD_UP"
    if cur_open < 0 and cur_close < 0:
        return "PRICE_WEAK"
    return "MIXED_PRICE"


def _path_risk(row: pd.Series) -> str:
    low = _float(row.get("low_from_open_pct"))
    if low >= LOW_STRICT_LIMIT:
        return "LOW_DIP"
    if low >= LOW_SOFT_LIMIT:
        return "MID_DIP"
    return "DEEP_DIP"


def _liquidity_state(row: pd.Series) -> str:
    if _bool(row.get("buy_signal_liquidity")):
        return "LIQUID"
    return "LIQUIDITY_GAP"


def _score_state(row: pd.Series) -> str:
    reasons = str(row.get("block_reasons") or "")
    return "SCORE_ABOVE_CAP" if "high_score" in reasons else "SCORE_WITHIN_CAP"


def _type_and_action(row: pd.Series) -> tuple[str, str, str]:
    price = str(row["price_state"])
    path = str(row["path_risk"])
    liq = str(row["liquidity_state"])
    score = str(row["score_state"])
    entry_allowed = _bool(row.get("entry_allowed_validation"))

    if entry_allowed:
        return "STRICT_BUY_CANDIDATE", "BUY_POSSIBLE_OBSERVE_ONLY", "현 기준도 통과"

    if price == "MOMENTUM_UP" and liq == "LIQUID" and path == "MID_DIP" and score == "SCORE_ABOVE_CAP":
        return "RECOVERY_MOMENTUM_MID_DIP_SCORE_CAP", "OBSERVE_RECOVERY_RISK", "가격·유동성은 살아있지만 장중 흔들림과 점수상한 동시 차단"

    if price == "MOMENTUM_UP" and liq == "LIQUIDITY_GAP" and path in {"LOW_DIP", "MID_DIP"}:
        return "MOMENTUM_LIQUIDITY_GAP", "OBSERVE_LIQUIDITY_PENDING", "가격은 살아있지만 실매매 유동성 부족"

    if price == "MILD_UP" and liq == "LIQUIDITY_GAP" and path == "MID_DIP":
        return "MILD_UP_LIQUIDITY_GAP", "OBSERVE_WEAK_LIQUIDITY_PENDING", "약한 상승과 유동성 부족이 함께 있음"

    if price == "PRICE_WEAK" and path == "DEEP_DIP":
        return "WEAK_PRICE_DEEP_DIP", "TRUE_EXCLUDE_PRICE_FAILURE", "가격 약세와 깊은 장중 저점이 함께 확인됨"

    if price == "PRICE_WEAK":
        return "WEAK_PRICE", "TRUE_EXCLUDE_PRICE_FAILURE", "가격 신호가 약함"

    if liq == "LIQUIDITY_GAP":
        return "LIQUIDITY_GAP_OTHER", "TRUE_EXCLUDE_OR_LOW_PRIORITY", "유동성 부족"

    return "UNRESOLVED_MIXED", "OBSERVE_UNRESOLVED", "혼합 신호라 추가 표본 필요"


def main() -> int:
    if not INPUT_CSV.exists():
        raise SystemExit(f"missing input: {INPUT_CSV}")
    df = pd.read_csv(INPUT_CSV, dtype={"code": str}, encoding="utf-8-sig")
    df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)

    for col in [
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
    ]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["price_state"] = df.apply(_price_state, axis=1)
    df["path_risk"] = df.apply(_path_risk, axis=1)
    df["liquidity_state"] = df.apply(_liquidity_state, axis=1)
    df["score_state"] = df.apply(_score_state, axis=1)
    typed = df.apply(_type_and_action, axis=1, result_type="expand")
    df["candidate_type"] = typed[0]
    df["action_layer"] = typed[1]
    df["type_reason_kr"] = typed[2]

    replay = json.loads(INPUT_REPLAY_JSON.read_text(encoding="utf-8")) if INPUT_REPLAY_JSON.exists() else {}
    output_cols = [
        "action_layer",
        "candidate_type",
        "type_reason_kr",
        "code",
        "classification_kr",
        "current_price",
        "current_vs_open_pct",
        "current_vs_prev_close_pct",
        "low_from_open_pct",
        "high_from_open_pct",
        "trading_value",
        "value",
        "score",
        "final_score",
        "price_state",
        "path_risk",
        "liquidity_state",
        "score_state",
        "buy_signal_price",
        "buy_signal_liquidity",
        "buy_signal",
        "blocked_by_risk",
        "entry_allowed_validation",
        "block_reasons",
    ]
    for col in output_cols:
        if col not in df.columns:
            df[col] = ""

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_candidate_type_grammar_audit",
        "input_csv": str(INPUT_CSV),
        "input_replay_json": str(INPUT_REPLAY_JSON),
        "rows": int(len(df)),
        "unique_codes": int(df["code"].nunique()),
        "replay_status": replay.get("status"),
        "replay_alerts_count": replay.get("alerts_count"),
        "action_layer_counts": {str(k): int(v) for k, v in df["action_layer"].value_counts().to_dict().items()},
        "candidate_type_counts": {str(k): int(v) for k, v in df["candidate_type"].value_counts().to_dict().items()},
        "price_state_counts": {str(k): int(v) for k, v in df["price_state"].value_counts().to_dict().items()},
        "path_risk_counts": {str(k): int(v) for k, v in df["path_risk"].value_counts().to_dict().items()},
        "liquidity_state_counts": {str(k): int(v) for k, v in df["liquidity_state"].value_counts().to_dict().items()},
        "methodology_note": "This reclassifies the same STRESS10 rows by candidate behavior type; it is not an operational gate change.",
        "limitations": [
            "Only one STRESS branch snapshot with 10 rows.",
            "No forward return outcome is evaluated here.",
            "Thresholds are descriptive grammar cutoffs, not approved trading parameters.",
        ],
    }

    df[output_cols].to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(
        json.dumps({**summary, "rows_detail": df[output_cols].to_dict(orient="records")}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    lines = [
        "# New Method Candidate Type Grammar Audit",
        "",
        f"- generated_at: {summary['generated_at']}",
        f"- rows: {summary['rows']}",
        f"- unique_codes: {summary['unique_codes']}",
        f"- replay_status: {summary['replay_status']}",
        f"- replay_alerts_count: {summary['replay_alerts_count']}",
        "",
        "## Action layer counts",
    ]
    for key, value in summary["action_layer_counts"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Candidate type counts"])
    for key, value in summary["candidate_type_counts"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Rows"])
    for row in df[output_cols].to_dict(orient="records"):
        lines.append(
            f"- {row['code']} {row['action_layer']} / {row['candidate_type']} "
            f"price={row['price_state']} path={row['path_risk']} liq={row['liquidity_state']} "
            f"score={row['score_state']} reason={row['type_reason_kr']}"
        )
    lines.extend(["", "## Caveats"])
    for item in summary["limitations"]:
        lines.append(f"- {item}")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "csv": str(OUT_CSV), "json": str(OUT_JSON), "md": str(OUT_MD)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
