from __future__ import annotations

import csv
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_pullback_methodology_first_screener import _load_daily_history


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

FOUR_QUESTION_CSV = LOG_DIR / "event_theme_candidate_layer_latest.csv"
FINAL_CANDIDATES_CSV = LOG_DIR / "candidates_latest_data.with_final_score.csv"
MARKET_RISING_CSV = LOG_DIR / "market_rising_latest.csv"
NEWS_CANDIDATES_CSV = LOG_DIR / "news_candidates_latest.csv"

OUT_JSON = LOG_DIR / "four_question_readonly_validation_latest.json"
OUT_CSV = LOG_DIR / "four_question_readonly_validation_latest.csv"

WATCH_DECISIONS = {
    "READONLY_PRIORITY_WATCH",
    "READONLY_MONEY_PULLBACK_WATCH",
    "READONLY_EXPLORE",
}


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    return []


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _code(value: Any) -> str:
    text = str(value or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6)[-6:] if digits else ""


def _ymd(value: Any) -> str:
    text = str(value or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _float(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value if value is not None else "").strip()
        if not text or text.lower() in {"nan", "none", "<na>"}:
            return default
        return float(text)
    except Exception:
        return default


def _pct(num: float, den: float) -> float:
    return num / den if den > 0 else 0.0


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _codes(rows: list[dict[str, str]]) -> set[str]:
    return {code for row in rows if (code := _code(row.get("code")))}


def _markout_map(daily: pd.DataFrame, pairs: set[tuple[str, str]]) -> dict[tuple[str, str], dict[str, Any]]:
    if daily.empty or not pairs:
        return {}
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for code, group in daily.groupby("code", sort=False):
        sub = group.sort_values("date").reset_index(drop=True)
        pair_dates = {ymd for c, ymd in pairs if c == code}
        if not pair_dates:
            continue
        for idx, row in sub.iterrows():
            asof = str(row["date"])
            if asof not in pair_dates:
                continue
            close = _float(row.get("close"), 0.0)
            if close <= 0:
                continue
            future = sub.iloc[idx + 1 : idx + 6].copy()
            if future.empty:
                out[(code, asof)] = {
                    "markout_status": "NOT_EVALUABLE_NO_FORWARD_BARS",
                    "forward_bars": 0,
                }
                continue
            closes = future["close"].astype(float).tolist()
            highs = future["high"].astype(float).tolist() if "high" in future else closes
            lows = future["low"].astype(float).tolist() if "low" in future else closes
            out[(code, asof)] = {
                "markout_status": "EVALUATED",
                "forward_bars": len(future),
                "next_1d_return": round(_pct(closes[min(0, len(closes) - 1)] - close, close), 6),
                "next_3d_return": round(_pct(closes[min(2, len(closes) - 1)] - close, close), 6),
                "next_5d_return": round(_pct(closes[min(4, len(closes) - 1)] - close, close), 6),
                "max_up_5d": round(_pct(max(highs) - close, close), 6),
                "max_down_5d": round(_pct(min(lows) - close, close), 6),
                "followthrough_5d": max(highs) > close * 1.03,
                "stop_risk_5d": min(lows) < close * 0.95,
            }
    return out


def _summarize_markout(rows: list[dict[str, Any]]) -> dict[str, Any]:
    evaluated = [row for row in rows if row.get("markout_status") == "EVALUATED"]
    if not evaluated:
        return {
            "sample_count": 0,
            "status": "NOT_EVALUABLE",
            "avg_1d": 0.0,
            "avg_3d": 0.0,
            "avg_5d": 0.0,
            "win_rate_5d": 0.0,
            "followthrough_rate_5d": 0.0,
            "stop_risk_rate_5d": 0.0,
            "avg_max_down_5d": 0.0,
        }
    return {
        "sample_count": len(evaluated),
        "status": "EVALUATED",
        "avg_1d": round(_avg([_float(row.get("next_1d_return")) for row in evaluated]), 6),
        "avg_3d": round(_avg([_float(row.get("next_3d_return")) for row in evaluated]), 6),
        "avg_5d": round(_avg([_float(row.get("next_5d_return")) for row in evaluated]), 6),
        "win_rate_5d": round(_avg([1.0 if _float(row.get("next_5d_return")) > 0 else 0.0 for row in evaluated]), 6),
        "followthrough_rate_5d": round(_avg([1.0 if row.get("followthrough_5d") else 0.0 for row in evaluated]), 6),
        "stop_risk_rate_5d": round(_avg([1.0 if row.get("stop_risk_5d") else 0.0 for row in evaluated]), 6),
        "avg_max_down_5d": round(_avg([_float(row.get("max_down_5d")) for row in evaluated]), 6),
    }


def _comparison_bucket(is_watch: bool, current_selected: bool, decision: str) -> str:
    if decision == "READONLY_CHASE_BLOCK" and current_selected:
        return "CURRENT_SELECTED_BUT_READONLY_CHASE_BLOCK"
    if is_watch and current_selected:
        return "OVERLAP_CURRENT_AND_FOUR_QUESTION"
    if is_watch:
        return "FOUR_QUESTION_ONLY_WATCH"
    if current_selected:
        return "CURRENT_ONLY_NONWATCH"
    return "BOTH_NONWATCH_OR_DATA_LIMITED"


def build() -> dict[str, Any]:
    source_rows = _read_csv(FOUR_QUESTION_CSV)
    final_codes = _codes(_read_csv(FINAL_CANDIDATES_CSV))
    rising_codes = _codes(_read_csv(MARKET_RISING_CSV))
    news_codes = _codes(_read_csv(NEWS_CANDIDATES_CSV))
    current_selected_codes = final_codes | rising_codes | news_codes

    pairs = {(_code(row.get("code")), _ymd(row.get("asof"))) for row in source_rows}
    pairs = {(code, asof) for code, asof in pairs if code and asof}
    daily = _load_daily_history()
    markouts = _markout_map(daily, pairs)

    out_rows: list[dict[str, Any]] = []
    for row in source_rows:
        code = _code(row.get("code"))
        asof = _ymd(row.get("asof"))
        decision = str(row.get("four_question_decision") or "")
        is_watch = decision in WATCH_DECISIONS
        current_selected = code in current_selected_codes
        markout = markouts.get((code, asof), {"markout_status": "NOT_EVALUABLE_MISSING_ASOF", "forward_bars": 0})
        out_rows.append(
            {
                "asof": asof,
                "code": code,
                "name": row.get("name", ""),
                "four_question_decision": decision,
                "four_question_reason": row.get("four_question_reason", ""),
                "event_theme_decision": row.get("event_theme_decision", ""),
                "global_event_label": row.get("global_event_label", ""),
                "global_event_feed_label": row.get("global_event_feed_label", ""),
                "beneficiary_grade": row.get("beneficiary_grade", ""),
                "beneficiary_mapping_label": row.get("beneficiary_mapping_label", ""),
                "smart_money_quality": row.get("smart_money_quality", ""),
                "smart_money_continuity_label": row.get("smart_money_continuity_label", ""),
                "smart_money_continuity_source": row.get("smart_money_continuity_source", ""),
                "raw_supply_history_status": row.get("raw_supply_history_status", ""),
                "foreign_consecutive_buy_days": row.get("foreign_consecutive_buy_days", ""),
                "institution_consecutive_buy_days": row.get("institution_consecutive_buy_days", ""),
                "smart_money_consecutive_buy_days": row.get("smart_money_consecutive_buy_days", ""),
                "late_buy_risk": row.get("late_buy_risk", ""),
                "price_zone_label": row.get("price_zone_label", ""),
                "theme_scarcity_label": row.get("theme_scarcity_label", ""),
                "current_selected": current_selected,
                "current_sources": "|".join(
                    name
                    for name, exists in [
                        ("final_candidates", code in final_codes),
                        ("market_rising", code in rising_codes),
                        ("news_candidates", code in news_codes),
                    ]
                    if exists
                ),
                "comparison_bucket": _comparison_bucket(is_watch, current_selected, decision),
                "markout_status": markout.get("markout_status", ""),
                "forward_bars": markout.get("forward_bars", 0),
                "next_1d_return": markout.get("next_1d_return", ""),
                "next_3d_return": markout.get("next_3d_return", ""),
                "next_5d_return": markout.get("next_5d_return", ""),
                "max_up_5d": markout.get("max_up_5d", ""),
                "max_down_5d": markout.get("max_down_5d", ""),
                "followthrough_5d": markout.get("followthrough_5d", ""),
                "stop_risk_5d": markout.get("stop_risk_5d", ""),
                "research_only": True,
                "policy_change": False,
                "entry_approval_changed": False,
                "paper_order_route": False,
                "broker_order_route": False,
                "trading_route": False,
            }
        )

    out_rows.sort(key=lambda r: (str(r["comparison_bucket"]), str(r["four_question_decision"]), str(r["code"])))
    fields = list(out_rows[0].keys()) if out_rows else ["asof", "code", "four_question_decision"]
    _write_csv(OUT_CSV, out_rows, fields)

    four_watch = [row for row in out_rows if row["four_question_decision"] in WATCH_DECISIONS]
    chase_block = [row for row in out_rows if row["four_question_decision"] == "READONLY_CHASE_BLOCK"]
    current_selected = [row for row in out_rows if row["current_selected"]]
    current_selected_chase_block = [
        row for row in out_rows if row["comparison_bucket"] == "CURRENT_SELECTED_BUT_READONLY_CHASE_BLOCK"
    ]

    by_decision = {
        key: _summarize_markout([row for row in out_rows if row["four_question_decision"] == key])
        for key in sorted(set(str(row["four_question_decision"]) for row in out_rows))
    }
    summary = {
        "generated_at": _now_ts(),
        "research_only": True,
        "policy_change": False,
        "entry_approval_changed": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "trading_route": False,
        "scope": "read_only_four_question_validation",
        "source_files": {
            "four_question_layer": str(FOUR_QUESTION_CSV),
            "final_candidates": str(FINAL_CANDIDATES_CSV),
            "market_rising": str(MARKET_RISING_CSV),
            "news_candidates": str(NEWS_CANDIDATES_CSV),
            "daily_archive": str(ROOT / "krx_daily_archive"),
        },
        "source_counts": {
            "four_question_rows": len(source_rows),
            "final_candidate_codes": len(final_codes),
            "market_rising_codes": len(rising_codes),
            "news_candidate_codes": len(news_codes),
            "current_selected_union_codes": len(current_selected_codes),
            "daily_rows_loaded": int(len(daily)) if not daily.empty else 0,
        },
        "comparison_counts": dict(Counter(str(row["comparison_bucket"]) for row in out_rows)),
        "four_question_decision_counts": dict(Counter(str(row["four_question_decision"]) for row in out_rows)),
        "global_event_label_counts": dict(Counter(str(row["global_event_label"]) for row in out_rows)),
        "global_event_feed_label_counts": dict(Counter(str(row["global_event_feed_label"]) for row in out_rows)),
        "beneficiary_mapping_counts": dict(Counter(str(row["beneficiary_mapping_label"]) for row in out_rows)),
        "smart_money_continuity_counts": dict(Counter(str(row["smart_money_continuity_label"]) for row in out_rows)),
        "raw_supply_history_status_counts": dict(Counter(str(row["raw_supply_history_status"]) for row in out_rows)),
        "theme_scarcity_counts": dict(Counter(str(row["theme_scarcity_label"]) for row in out_rows)),
        "markout_status_counts": dict(Counter(str(row["markout_status"]) for row in out_rows)),
        "current_overlap": {
            "four_question_watch_rows": len(four_watch),
            "current_selected_rows": len(current_selected),
            "overlap_watch_and_current": len(
                [row for row in four_watch if row["comparison_bucket"] == "OVERLAP_CURRENT_AND_FOUR_QUESTION"]
            ),
            "four_question_only_watch": len(
                [row for row in four_watch if row["comparison_bucket"] == "FOUR_QUESTION_ONLY_WATCH"]
            ),
            "current_selected_but_nonwatch": len(
                [row for row in out_rows if row["comparison_bucket"] == "CURRENT_ONLY_NONWATCH"]
            ),
            "current_selected_but_chase_block": len(current_selected_chase_block),
            "readonly_chase_block_total": len(chase_block),
        },
        "forward_effect_summary": _summarize_markout(out_rows),
        "forward_effect_by_decision": by_decision,
        "top_watch_rows": four_watch[:20],
        "current_selected_chase_block_rows": current_selected_chase_block[:20],
        "access_issues": [
            "latest four-question layer is a current snapshot; forward effect is not evaluable until later daily bars exist",
            "current-selected comparison is a read-only overlap check, not an approval or rejection rule",
            "foreign/institution continuity uses raw pykrx supply cache when available and proxy fields only for missing codes",
        ],
    }
    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    return summary


def main() -> int:
    summary = build()
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
