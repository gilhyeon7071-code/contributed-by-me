from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SURGE_CSV = LOG_DIR / "surge_realtime_latest.csv"
BUYABLE_CSV = LOG_DIR / "surge_buyable_candidate_layer_latest.csv"
CANDIDATE_CSVS = [
    LOG_DIR / "candidates_latest_data.with_final_score.csv",
    LOG_DIR / "candidates_latest_data.with_news_score.csv",
    LOG_DIR / "candidates_latest_data.csv",
]

OUT_JSON = LOG_DIR / "surge_event_money_pullback_layer_latest.json"
OUT_CSV = LOG_DIR / "surge_event_money_pullback_layer_latest.csv"


HARD_EXCLUDE_TOKENS = {
    "KRX_ADMIN",
    "KRX_WARNING",
    "KRX_RISK",
    "NEWS_NEGATIVE",
    "NEWS_IMPLICATION_BLOCK",
    "ORDERFLOW_PAUSE",
    "ORDERFLOW_RISK_BLOCK",
    "TRADING_VALUE_FLOOR",
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
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        if value in ("", None):
            return default
        return float(value)
    except Exception:
        return default


def _int(value: Any, default: int = 0) -> int:
    v = _float(value, None)
    return default if v is None else int(v)


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _code(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text else ""


def _tokens(*values: Any) -> set[str]:
    out: set[str] = set()
    for value in values:
        text = str(value or "")
        for token in text.replace("|", ";").replace(",", ";").split(";"):
            token = token.strip().upper()
            if token:
                out.add(token)
    return out


def _first_candidate_rows() -> tuple[Path | None, list[dict[str, str]]]:
    for path in CANDIDATE_CSVS:
        rows = _read_csv(path)
        if rows:
            return path, rows
    return None, []


def _by_code(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code and code not in out:
            out[code] = row
    return out


def _add(reason: list[str], key: str, value: Any = None) -> None:
    if value in ("", None):
        reason.append(key)
    else:
        reason.append(f"{key}={value}")


def _event_axis(cand: dict[str, Any], surge: dict[str, Any]) -> tuple[float, str]:
    score = 0.0
    evidence: list[str] = []

    news_score = _float(cand.get("news_score"), None)
    if news_score is not None and news_score >= 0.55:
        score += 1.0
        _add(evidence, "news_score", round(news_score, 4))

    if _int(cand.get("news_implication_positive_rows")) > 0 or _int(cand.get("news_implication_boost_rows")) > 0:
        score += 1.0
        _add(evidence, "positive_news_implication")

    if _int(cand.get("news_implication_sector_rows")) > 0 or _int(cand.get("news_implication_macro_rows")) > 0:
        score += 0.75
        _add(evidence, "sector_or_macro_news_implication")

    sector_score = _float(cand.get("sector_score"), None)
    sector_allowed = _truthy(cand.get("sector_entry_allowed"))
    if sector_allowed and (sector_score is None or sector_score >= 0.50):
        score += 0.75
        _add(evidence, "sector_entry_allowed", "" if sector_score is None else round(sector_score, 4))

    policy_score = _float(cand.get("policy_score"), None)
    if policy_score is not None and policy_score >= 0.55:
        score += 0.5
        _add(evidence, "policy_score", round(policy_score, 4))

    macro_score = _float(cand.get("macro_score"), None)
    if macro_score is not None and macro_score >= 0.55:
        score += 0.5
        _add(evidence, "macro_score", round(macro_score, 4))

    surge_news = _float(surge.get("news_score"), None)
    if surge_news is not None and surge_news >= 0.55 and "news_score" not in "|".join(evidence):
        score += 0.5
        _add(evidence, "surge_news_score", round(surge_news, 4))

    if not evidence:
        _add(evidence, "event_evidence_missing")
    return round(score, 3), "|".join(evidence)


def _money_axis(cand: dict[str, Any], surge: dict[str, Any]) -> tuple[float, str]:
    score = 0.0
    evidence: list[str] = []

    trading_value = _float(surge.get("trading_value"), None)
    if trading_value is None or trading_value <= 0:
        trading_value = _float(cand.get("trading_value"), _float(cand.get("value"), 0.0))
    trading_value = trading_value or 0.0
    if trading_value >= 50_000_000_000:
        score += 2.0
        _add(evidence, "trading_value_ge_50b", round(trading_value, 0))
    elif trading_value >= 10_000_000_000:
        score += 1.25
        _add(evidence, "trading_value_ge_10b", round(trading_value, 0))
    elif trading_value >= 1_000_000_000:
        score += 0.5
        _add(evidence, "trading_value_ge_1b", round(trading_value, 0))

    rvol = _float(surge.get("rvol20"), None)
    if rvol is not None:
        if 1.5 <= rvol <= 5.0:
            score += 1.0
            _add(evidence, "rvol_confirmed", round(rvol, 4))
        elif rvol > 5.0:
            score += 0.5
            _add(evidence, "rvol_overheat_watch", round(rvol, 4))

    v_accel = _float(cand.get("v_accel"), None)
    if v_accel is not None:
        if v_accel >= 2.0:
            score += 1.0
            _add(evidence, "v_accel_ge_2", round(v_accel, 4))
        elif v_accel >= 1.0:
            score += 0.5
            _add(evidence, "v_accel_ge_1", round(v_accel, 4))

    flow_score = _float(cand.get("flow_score"), None)
    if flow_score is not None and flow_score >= 0.55:
        score += 0.75
        _add(evidence, "flow_score", round(flow_score, 4))

    foreign_net = _float(cand.get("foreign_net_20d"), _float(cand.get("foreign_net"), None))
    institution_net = _float(cand.get("institution_net_20d"), _float(cand.get("institution_net"), None))
    if foreign_net is not None and institution_net is not None:
        if foreign_net + institution_net > 0:
            score += 0.75
            _add(evidence, "foreign_institution_net_positive", round(foreign_net + institution_net, 0))
        if foreign_net > 0 and institution_net > 0:
            score += 0.5
            _add(evidence, "foreign_and_institution_both_positive")

    if not evidence:
        _add(evidence, "money_evidence_missing")
    return round(score, 3), "|".join(evidence)


def _pullback_axis(cand: dict[str, Any], surge: dict[str, Any]) -> tuple[float, str]:
    score = 0.0
    evidence: list[str] = []

    change = _float(surge.get("change_pct"), 0.0) or 0.0
    drawdown = _float(surge.get("intraday_high_drawdown_pct"), None)
    rebound = _float(surge.get("intraday_low_rebound_pct"), None)
    range_pos = _float(surge.get("intraday_range_position_pct"), None)
    disparity20 = _float(cand.get("disparity20"), None)
    macd_bullish = _truthy(cand.get("macd_bullish")) or _truthy(cand.get("macd_golden"))

    if drawdown is not None:
        if -0.08 <= drawdown <= -0.02:
            score += 1.0
            _add(evidence, "controlled_pullback_from_high", round(drawdown, 5))
        elif -0.02 < drawdown <= 0.0:
            score += 0.25
            _add(evidence, "shallow_pullback", round(drawdown, 5))
        elif drawdown < -0.10:
            score -= 1.0
            _add(evidence, "deep_rejection_risk", round(drawdown, 5))

    if rebound is not None and rebound >= 0.02:
        score += 0.75
        _add(evidence, "rebound_from_low", round(rebound, 5))

    if range_pos is not None and range_pos >= 0.50:
        score += 0.5
        _add(evidence, "upper_half_recovery", round(range_pos, 5))

    if disparity20 is not None and 0.98 <= disparity20 <= 1.08:
        score += 0.5
        _add(evidence, "near_20d_reference", round(disparity20, 5))

    if macd_bullish:
        score += 0.5
        _add(evidence, "macd_bullish_or_golden")

    if 0.05 <= change <= 0.16:
        score += 0.5
        _add(evidence, "not_late_chase_change_band", round(change, 5))
    elif change >= 0.24:
        score -= 0.25
        _add(evidence, "late_chase_watch", round(change, 5))

    if not evidence:
        _add(evidence, "pullback_evidence_missing")
    return round(score, 3), "|".join(evidence)


def _classify(row: dict[str, Any]) -> tuple[str, str]:
    hard_tokens = _tokens(row.get("entry_reason"), row.get("exclude_reasons"), row.get("paper_probe_block_reasons"))
    hard_hits = sorted(hard_tokens & HARD_EXCLUDE_TOKENS)
    buyable_grade = str(row.get("buyable_grade") or "")
    if hard_hits or buyable_grade == "C_NO_TOUCH":
        return "C_TRUE_EXCLUDE", "hard_or_current_no_touch=" + ("|".join(hard_hits) if hard_hits else buyable_grade)

    event_score = _float(row.get("event_score"), 0.0) or 0.0
    money_score = _float(row.get("money_score"), 0.0) or 0.0
    pullback_score = _float(row.get("pullback_score"), 0.0) or 0.0
    total = event_score + money_score + pullback_score

    if event_score >= 1.5 and money_score >= 2.0 and pullback_score >= 1.25 and total >= 5.0:
        return "A_BUYABLE_WATCH", "event_money_pullback_all_confirmed"
    if money_score >= 2.0 and (event_score >= 1.0 or pullback_score >= 1.0):
        return "B_EXPLORE_WATCH", "large_money_with_one_supporting_axis"
    if event_score >= 1.5 and pullback_score >= 1.0:
        return "B_EXPLORE_WATCH", "event_pullback_without_full_money_confirmation"
    return "C_TRUE_EXCLUDE", "expected_value_structure_not_confirmed"


def _bucket_display(bucket: str) -> str:
    if bucket == "A_BUYABLE_WATCH":
        return "HIGH_QUALITY_WATCH_NOT_BUY_APPROVAL"
    if bucket == "B_EXPLORE_WATCH":
        return "EXPLORE_WATCH_NOT_BUY_APPROVAL"
    if bucket == "C_TRUE_EXCLUDE":
        return "TRUE_EXCLUDE"
    return bucket


def build() -> dict[str, Any]:
    surge_rows = [
        row for row in _read_csv(SURGE_CSV)
        if _truthy(row.get("detected_surge_flag")) or _truthy(row.get("is_realtime_surge"))
    ]
    buyable_rows = _by_code(_read_csv(BUYABLE_CSV))
    candidate_path, candidate_rows = _first_candidate_rows()
    candidate_map = _by_code(candidate_rows)

    out_rows: list[dict[str, Any]] = []
    for surge in surge_rows:
        code = _code(surge.get("code"))
        cand = candidate_map.get(code, {})
        buy = buyable_rows.get(code, {})
        event_score, event_evidence = _event_axis(cand, surge)
        money_score, money_evidence = _money_axis(cand, surge)
        pullback_score, pullback_evidence = _pullback_axis(cand, surge)

        row: dict[str, Any] = {
            "ts": surge.get("ts", ""),
            "date": surge.get("date", cand.get("date", "")),
            "code": code,
            "name": cand.get("name", ""),
            "market": cand.get("market", ""),
            "detected_surge_type": surge.get("detected_surge_type", ""),
            "buyable_grade": buy.get("buyable_grade", ""),
            "buyable_action": buy.get("suggested_action", ""),
            "entry_decision": surge.get("entry_decision", ""),
            "entry_reason": surge.get("entry_reason", ""),
            "exclude_reasons": surge.get("exclude_reasons", ""),
            "paper_probe_block_reasons": surge.get("paper_probe_block_reasons", ""),
            "event_score": event_score,
            "event_evidence": event_evidence,
            "money_score": money_score,
            "money_evidence": money_evidence,
            "pullback_score": pullback_score,
            "pullback_evidence": pullback_evidence,
            "total_structure_score": round(event_score + money_score + pullback_score, 3),
            "change_pct": surge.get("change_pct", ""),
            "rvol20": surge.get("rvol20", ""),
            "trading_value": surge.get("trading_value", cand.get("trading_value", cand.get("value", ""))),
            "v_accel": cand.get("v_accel", ""),
            "flow_score": cand.get("flow_score", ""),
            "foreign_net_20d": cand.get("foreign_net_20d", cand.get("foreign_net", "")),
            "institution_net_20d": cand.get("institution_net_20d", cand.get("institution_net", "")),
            "news_score": cand.get("news_score", surge.get("news_score", "")),
            "news_implication_positive_rows": cand.get("news_implication_positive_rows", ""),
            "news_implication_boost_rows": cand.get("news_implication_boost_rows", ""),
            "news_implication_block_rows": cand.get("news_implication_block_rows", ""),
            "sector_score": cand.get("sector_score", ""),
            "sector_entry_allowed": cand.get("sector_entry_allowed", ""),
            "macro_score": cand.get("macro_score", ""),
            "policy_score": cand.get("policy_score", ""),
            "intraday_high_drawdown_pct": surge.get("intraday_high_drawdown_pct", ""),
            "intraday_low_rebound_pct": surge.get("intraday_low_rebound_pct", ""),
            "intraday_range_position_pct": surge.get("intraday_range_position_pct", ""),
            "disparity20": cand.get("disparity20", ""),
            "macd_bullish": cand.get("macd_bullish", ""),
            "surge_score_final": surge.get("surge_score_final", ""),
            "lob_status": surge.get("lob_status", ""),
            "orderflow_tag": surge.get("orderflow_tag", ""),
            "research_only": True,
            "policy_change": False,
            "entry_approval_changed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
            "connection_level": "SIGNAL_QUALITY_ONLY",
            "trading_connection": False,
            "signal_connection": False,
            "execution_connection": False,
            "connection_note": "Signal quality or observation only; not entry approval, order, fill, or ledger.",
        }
        bucket, bucket_reason = _classify({**surge, **buy, **row})
        row["event_money_pullback_bucket"] = bucket
        row["event_money_pullback_bucket_display"] = _bucket_display(bucket)
        row["bucket_contract"] = "watch_quality_only_not_entry_approval"
        row["bucket_reason"] = bucket_reason
        out_rows.append(row)

    out_rows.sort(
        key=lambda r: (
            {"A_BUYABLE_WATCH": 0, "B_EXPLORE_WATCH": 1, "C_TRUE_EXCLUDE": 2}.get(str(r.get("event_money_pullback_bucket")), 9),
            -float(r.get("total_structure_score") or 0.0),
            str(r.get("code") or ""),
        )
    )

    bucket_counts = Counter(str(row.get("event_money_pullback_bucket") or "") for row in out_rows)
    type_counts = Counter(str(row.get("detected_surge_type") or "") for row in out_rows)
    top_rows = [
        {
            "code": row.get("code"),
            "name": row.get("name"),
            "bucket": row.get("event_money_pullback_bucket"),
            "bucket_display": row.get("event_money_pullback_bucket_display"),
            "total_structure_score": row.get("total_structure_score"),
            "event_score": row.get("event_score"),
            "money_score": row.get("money_score"),
            "pullback_score": row.get("pullback_score"),
            "reason": row.get("bucket_reason"),
        }
        for row in out_rows[:10]
    ]
    result = {
        "generated_at": _now_ts(),
        "status": "OK",
        "scope": "surge_event_money_pullback_layer",
        "source_files": {
            "surge_realtime": str(SURGE_CSV),
            "buyable_candidate_layer": str(BUYABLE_CSV),
            "candidate_detail": str(candidate_path) if candidate_path else None,
        },
        "summary": {
            "surge_rows": len(surge_rows),
            "candidate_detail_rows": len(candidate_rows),
            "output_rows": len(out_rows),
            "bucket_counts": dict(sorted(bucket_counts.items())),
            "detected_type_counts": dict(sorted(type_counts.items())),
            "research_only": True,
            "policy_change": False,
            "entry_approval_changed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
            "connection_level": "SIGNAL_QUALITY_ONLY",
            "trading_connection": False,
            "signal_connection": False,
            "execution_connection": False,
            "connection_note": "Signal quality or observation only; not entry approval, order, fill, or ledger.",
        },
        "top_candidates": top_rows,
        "interpretation": (
            "Read-only surge layer for the user's event-beneficiary, large-money, and pullback/rebound framework. "
            "A/B/C buckets are observation labels only and do not approve orders or relax gates."
        ),
        "artifacts": {
            "csv": str(OUT_CSV),
        },
    }
    OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    fields = [
        "ts", "date", "code", "name", "market", "detected_surge_type", "buyable_grade",
        "buyable_action", "event_money_pullback_bucket", "event_money_pullback_bucket_display",
        "bucket_contract", "bucket_reason", "total_structure_score",
        "event_score", "event_evidence", "money_score", "money_evidence", "pullback_score",
        "pullback_evidence", "entry_decision", "entry_reason", "exclude_reasons",
        "paper_probe_block_reasons", "change_pct", "rvol20", "trading_value", "v_accel",
        "flow_score", "foreign_net_20d", "institution_net_20d", "news_score",
        "news_implication_positive_rows", "news_implication_boost_rows", "news_implication_block_rows",
        "sector_score", "sector_entry_allowed", "macro_score", "policy_score",
        "intraday_high_drawdown_pct", "intraday_low_rebound_pct", "intraday_range_position_pct",
        "disparity20", "macd_bullish", "surge_score_final", "lob_status", "orderflow_tag",
        "research_only", "policy_change", "entry_approval_changed", "paper_order_route",
        "broker_order_route", "trading_route", "connection_level", "trading_connection", "signal_connection", "execution_connection", "connection_note",
    ]
    _write_csv(OUT_CSV, out_rows, fields)
    return result


def main() -> int:
    result = build()
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
