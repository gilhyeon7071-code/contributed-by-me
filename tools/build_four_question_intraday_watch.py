from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SOURCE_CSV = LOG_DIR / "four_question_readonly_validation_latest.csv"
OUT_JSON = LOG_DIR / "four_question_intraday_watch_latest.json"
OUT_CSV = LOG_DIR / "four_question_intraday_watch_latest.csv"

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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _code(value: Any) -> str:
    text = str(value or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6)[-6:] if digits else ""


def _ymd(value: Any) -> str:
    text = str(value or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        text = str(value if value is not None else "").strip()
        if not text or text.lower() in {"nan", "none", "<na>"}:
            return default
        return float(text)
    except Exception:
        return default


def _bool_text(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _pct(new: float, base: float) -> float:
    return (new / base) - 1.0 if base > 0 else 0.0


def _intraday_path(asof: str) -> Path:
    return LOG_DIR / f"intraday_prices_history_{asof}.csv"


def _intraday_map(asof: str) -> dict[str, dict[str, Any]]:
    path = _intraday_path(asof)
    rows = _read_csv(path)
    if not rows:
        return {}
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if not code:
            continue
        price = _float(row.get("current_price"), None)
        if price is None or price <= 0:
            continue
        grouped.setdefault(code, []).append(row)

    out: dict[str, dict[str, Any]] = {}
    for code, items in grouped.items():
        items.sort(key=lambda r: str(r.get("ts") or ""))
        first = items[0]
        last = items[-1]
        first_price = float(_float(first.get("current_price"), 0.0) or 0.0)
        last_price = float(_float(last.get("current_price"), 0.0) or 0.0)
        price_values = [float(_float(r.get("current_price"), 0.0) or 0.0) for r in items]
        day_high_values = [float(_float(r.get("high"), _float(r.get("current_price"), 0.0)) or 0.0) for r in items]
        day_low_values = [float(_float(r.get("low"), _float(r.get("current_price"), 0.0)) or 0.0) for r in items]
        last_volume = float(_float(last.get("volume"), 0.0) or 0.0)
        last_value = float(_float(last.get("trading_value"), 0.0) or 0.0)
        if first_price <= 0 or last_price <= 0:
            continue
        out[code] = {
            "intraday_source": str(path),
            "first_ts": str(first.get("ts") or ""),
            "last_ts": str(last.get("ts") or ""),
            "first_price": first_price,
            "last_price": last_price,
            "intraday_return": round(_pct(last_price, first_price), 6),
            "intraday_high_from_first": round(_pct(max(price_values), first_price), 6) if price_values else 0.0,
            "intraday_low_from_first": round(_pct(min(price_values), first_price), 6) if price_values else 0.0,
            "day_high_from_first": round(_pct(max(day_high_values), first_price), 6) if day_high_values else 0.0,
            "day_low_from_first": round(_pct(min(day_low_values), first_price), 6) if day_low_values else 0.0,
            "intraday_last_volume": round(last_volume, 3),
            "intraday_last_trading_value": round(last_value, 3),
            "intraday_points": len(items),
        }
    return out


def _tracking_group(row: dict[str, str]) -> str:
    decision = str(row.get("four_question_decision") or "")
    bucket = str(row.get("comparison_bucket") or "")
    current_selected = _bool_text(row.get("current_selected"))
    if decision == "READONLY_CHASE_BLOCK" and current_selected:
        return "CURRENT_SELECTED_CHASE_RISK"
    if decision in WATCH_DECISIONS and bucket == "OVERLAP_CURRENT_AND_FOUR_QUESTION":
        return "WATCH_OVERLAP_CURRENT"
    if decision in WATCH_DECISIONS:
        return "WATCH_FOUR_QUESTION_ONLY"
    if current_selected:
        return "CURRENT_ONLY_NONWATCH"
    return "DATA_LIMITED_OR_NONWATCH"


def _reaction_label(markout: dict[str, Any] | None) -> tuple[str, str]:
    if not markout:
        return "WAITING_INTRADAY_PRICE", "intraday_price_history_missing_for_code_or_date"
    ret = float(markout.get("intraday_return") or 0.0)
    high = float(markout.get("intraday_high_from_first") or 0.0)
    low = float(markout.get("intraday_low_from_first") or 0.0)
    value = float(markout.get("intraday_last_trading_value") or 0.0)
    if high >= 0.05 and ret <= 0:
        return "TOP_FAIL_OR_SELLING_PRESSURE", "intraday_high_positive_but_last_return_not_positive"
    if ret >= 0.03 and value >= 10_000_000_000:
        return "FOLLOWTHROUGH_WITH_VALUE", "last_return_ge_3pct_and_trading_value_ge_10b"
    if ret >= 0.015:
        return "FOLLOWTHROUGH_LIGHT", "last_return_ge_1_5pct"
    if low > -0.03 and ret >= -0.005:
        return "PULLBACK_HOLD", "low_drawdown_limited_and_last_near_first"
    if ret <= -0.03:
        return "WEAK_DOWN", "last_return_le_minus_3pct"
    return "MIXED_OR_FLAT", "no_clear_intraday_edge"


def _role_fit_label(tracking_group: str, reaction: str) -> tuple[str, str]:
    if reaction == "WAITING_INTRADAY_PRICE":
        return "ROLE_WAITING_PRICE", "intraday_price_missing_so_role_check_waiting"
    if tracking_group.startswith("WATCH"):
        if reaction in {"FOLLOWTHROUGH_WITH_VALUE", "FOLLOWTHROUGH_LIGHT", "PULLBACK_HOLD"}:
            return "WATCH_ROLE_SUPPORTED", "watch_candidate_held_or_followed_through"
        if reaction == "WEAK_DOWN":
            return "WATCH_ROLE_FAILED_WEAK_DOWN", "watch_candidate_showed_weak_down_reaction"
        if reaction == "TOP_FAIL_OR_SELLING_PRESSURE":
            return "WATCH_ROLE_FAILED_TOP_PRESSURE", "watch_candidate_showed_top_fail_pressure"
        return "WATCH_ROLE_UNCONFIRMED", "watch_candidate_did_not_show_clear_hold_or_followthrough"
    if tracking_group == "CURRENT_SELECTED_CHASE_RISK":
        if reaction in {"FOLLOWTHROUGH_WITH_VALUE", "FOLLOWTHROUGH_LIGHT"}:
            return "CHASE_RISK_STRONG_MOVE_RETAIN_RISK_TAG", "strong_intraday_move_but_chase_risk_tag_is_not_invalidated"
        if reaction in {"WEAK_DOWN", "TOP_FAIL_OR_SELLING_PRESSURE"}:
            return "CHASE_RISK_MATERIALIZED", "chase_risk_showed_weakness_or_selling_pressure"
        if reaction == "PULLBACK_HOLD":
            return "CHASE_RISK_NOT_MATERIALIZED_YET", "risk_tag_not_triggered_intraday_but_not_removed"
        return "CHASE_RISK_UNCONFIRMED", "risk_tag_needs_later_or_finer_intraday_check"
    if tracking_group == "CURRENT_ONLY_NONWATCH":
        if reaction in {"FOLLOWTHROUGH_WITH_VALUE", "FOLLOWTHROUGH_LIGHT"}:
            return "CURRENT_ONLY_STILL_STRONG", "current_logic_candidate_showed_intraday_strength"
        if reaction in {"WEAK_DOWN", "TOP_FAIL_OR_SELLING_PRESSURE"}:
            return "CURRENT_ONLY_WEAKNESS_CONFIRMED", "current_logic_candidate_showed_weakness_or_pressure"
        if reaction == "PULLBACK_HOLD":
            return "CURRENT_ONLY_HOLD", "current_logic_candidate_held_without_strong_followthrough"
        return "CURRENT_ONLY_UNCONFIRMED", "current_logic_candidate_reaction_mixed_or_flat"
    return "DATA_LIMITED_ROLE_UNCONFIRMED", "nonwatch_or_data_limited_group_not_role_decisive"


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    evaluated = [r for r in rows if r.get("intraday_status") == "EVALUATED"]
    if not evaluated:
        return {
            "rows": 0,
            "evaluated_rows": 0,
            "avg_intraday_return": 0.0,
            "positive_rate": 0.0,
            "followthrough_rate": 0.0,
            "top_fail_rate": 0.0,
            "weak_down_rate": 0.0,
        }
    returns = [float(r.get("intraday_return") or 0.0) for r in evaluated]
    return {
        "rows": len(rows),
        "evaluated_rows": len(evaluated),
        "avg_intraday_return": round(sum(returns) / len(returns), 6),
        "positive_rate": round(sum(1 for v in returns if v > 0) / len(returns), 6),
        "followthrough_rate": round(
            sum(1 for r in evaluated if str(r.get("reaction_label") or "").startswith("FOLLOWTHROUGH")) / len(evaluated),
            6,
        ),
        "top_fail_rate": round(
            sum(1 for r in evaluated if r.get("reaction_label") == "TOP_FAIL_OR_SELLING_PRESSURE") / len(evaluated),
            6,
        ),
        "weak_down_rate": round(sum(1 for r in evaluated if r.get("reaction_label") == "WEAK_DOWN") / len(evaluated), 6),
    }


def _role_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    evaluated = [r for r in rows if r.get("intraday_status") == "EVALUATED"]
    if not evaluated:
        return {
            "rows": len(rows),
            "evaluated_rows": 0,
            "role_label_counts": dict(Counter(str(r.get("role_fit_label")) for r in rows)),
            "watch_supported_rate": 0.0,
            "watch_failed_rate": 0.0,
            "chase_risk_materialized_rate": 0.0,
            "chase_strong_move_rate": 0.0,
            "current_only_strength_rate": 0.0,
            "current_only_weakness_rate": 0.0,
        }
    total = len(evaluated)
    labels = [str(r.get("role_fit_label") or "") for r in evaluated]
    return {
        "rows": len(rows),
        "evaluated_rows": total,
        "role_label_counts": dict(Counter(str(r.get("role_fit_label")) for r in rows)),
        "watch_supported_rate": round(sum(1 for v in labels if v == "WATCH_ROLE_SUPPORTED") / total, 6),
        "watch_failed_rate": round(sum(1 for v in labels if v.startswith("WATCH_ROLE_FAILED")) / total, 6),
        "chase_risk_materialized_rate": round(sum(1 for v in labels if v == "CHASE_RISK_MATERIALIZED") / total, 6),
        "chase_strong_move_rate": round(
            sum(1 for v in labels if v == "CHASE_RISK_STRONG_MOVE_RETAIN_RISK_TAG") / total,
            6,
        ),
        "current_only_strength_rate": round(sum(1 for v in labels if v == "CURRENT_ONLY_STILL_STRONG") / total, 6),
        "current_only_weakness_rate": round(sum(1 for v in labels if v == "CURRENT_ONLY_WEAKNESS_CONFIRMED") / total, 6),
    }


def build() -> dict[str, Any]:
    source_rows = _read_csv(SOURCE_CSV)
    if not source_rows:
        payload = {
            "status": "FAIL",
            "reason": "SOURCE_MISSING_OR_EMPTY",
            "source": str(SOURCE_CSV),
            "generated_at": _now_ts(),
            "research_only": True,
            "policy_change": False,
            "entry_approval_changed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
        }
        _write_json(OUT_JSON, payload)
        return payload

    by_date: dict[str, dict[str, dict[str, Any]]] = {}
    out_rows: list[dict[str, Any]] = []
    for row in source_rows:
        asof = _ymd(row.get("asof"))
        code = _code(row.get("code"))
        if asof not in by_date:
            by_date[asof] = _intraday_map(asof)
        markout = by_date.get(asof, {}).get(code)
        reaction, reaction_reason = _reaction_label(markout)
        tracking_group = _tracking_group(row)
        role_fit, role_fit_reason = _role_fit_label(tracking_group, reaction)
        out = {
            "asof": asof,
            "code": code,
            "name": row.get("name", ""),
            "tracking_group": tracking_group,
            "four_question_decision": row.get("four_question_decision", ""),
            "comparison_bucket": row.get("comparison_bucket", ""),
            "current_selected": row.get("current_selected", ""),
            "current_sources": row.get("current_sources", ""),
            "global_event_label": row.get("global_event_label", ""),
            "global_event_feed_label": row.get("global_event_feed_label", ""),
            "beneficiary_mapping_label": row.get("beneficiary_mapping_label", ""),
            "smart_money_continuity_label": row.get("smart_money_continuity_label", ""),
            "raw_supply_history_status": row.get("raw_supply_history_status", ""),
            "foreign_consecutive_buy_days": row.get("foreign_consecutive_buy_days", ""),
            "institution_consecutive_buy_days": row.get("institution_consecutive_buy_days", ""),
            "smart_money_consecutive_buy_days": row.get("smart_money_consecutive_buy_days", ""),
            "late_buy_risk": row.get("late_buy_risk", ""),
            "price_zone_label": row.get("price_zone_label", ""),
            "daily_forward_markout_status": row.get("markout_status", ""),
            "intraday_status": "EVALUATED" if markout else "WAITING_OR_MISSING_PRICE",
            "reaction_label": reaction,
            "reaction_reason": reaction_reason,
            "role_fit_label": role_fit,
            "role_fit_reason": role_fit_reason,
            "research_only": True,
            "policy_change": False,
            "entry_approval_changed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
        }
        if markout:
            out.update(markout)
        else:
            out.update(
                {
                    "intraday_source": str(_intraday_path(asof)),
                    "first_ts": "",
                    "last_ts": "",
                    "first_price": "",
                    "last_price": "",
                    "intraday_return": "",
                    "intraday_high_from_first": "",
                    "intraday_low_from_first": "",
                    "day_high_from_first": "",
                    "day_low_from_first": "",
                    "intraday_last_volume": "",
                    "intraday_last_trading_value": "",
                    "intraday_points": 0,
                }
            )
        out_rows.append(out)

    sort_rank = {
        "WATCH_OVERLAP_CURRENT": 0,
        "WATCH_FOUR_QUESTION_ONLY": 1,
        "CURRENT_SELECTED_CHASE_RISK": 2,
        "CURRENT_ONLY_NONWATCH": 3,
        "DATA_LIMITED_OR_NONWATCH": 4,
    }
    out_rows.sort(key=lambda r: (sort_rank.get(str(r.get("tracking_group")), 9), str(r.get("code"))))

    fields = list(out_rows[0].keys()) if out_rows else ["asof", "code", "tracking_group"]
    _write_csv(OUT_CSV, out_rows, fields)

    by_group = {group: _metrics([r for r in out_rows if r.get("tracking_group") == group]) for group in sorted(set(str(r.get("tracking_group")) for r in out_rows))}
    role_by_group = {
        group: _role_metrics([r for r in out_rows if r.get("tracking_group") == group])
        for group in sorted(set(str(r.get("tracking_group")) for r in out_rows))
    }
    payload = {
        "status": "PASS",
        "reason": "ok",
        "generated_at": _now_ts(),
        "research_only": True,
        "policy_change": False,
        "entry_approval_changed": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "trading_route": False,
        "scope": "read_only_four_question_intraday_watch",
        "source_files": {
            "four_question_validation": str(SOURCE_CSV),
            "intraday_price_pattern": str(LOG_DIR / "intraday_prices_history_YYYYMMDD.csv"),
        },
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV)},
        "source_counts": {
            "source_rows": len(source_rows),
            "watch_rows": sum(1 for r in out_rows if str(r.get("tracking_group")).startswith("WATCH")),
            "current_selected_chase_risk_rows": sum(1 for r in out_rows if r.get("tracking_group") == "CURRENT_SELECTED_CHASE_RISK"),
            "current_only_nonwatch_rows": sum(1 for r in out_rows if r.get("tracking_group") == "CURRENT_ONLY_NONWATCH"),
        },
        "intraday_status_counts": dict(Counter(str(r.get("intraday_status")) for r in out_rows)),
        "tracking_group_counts": dict(Counter(str(r.get("tracking_group")) for r in out_rows)),
        "reaction_label_counts": dict(Counter(str(r.get("reaction_label")) for r in out_rows)),
        "role_fit_label_counts": dict(Counter(str(r.get("role_fit_label")) for r in out_rows)),
        "daily_forward_markout_status_counts": dict(Counter(str(r.get("daily_forward_markout_status")) for r in out_rows)),
        "metrics": _metrics(out_rows),
        "metrics_by_tracking_group": by_group,
        "role_metrics_by_tracking_group": role_by_group,
        "top_watch_rows": [r for r in out_rows if str(r.get("tracking_group")).startswith("WATCH")][:20],
        "chase_risk_rows": [r for r in out_rows if r.get("tracking_group") == "CURRENT_SELECTED_CHASE_RISK"][:20],
        "access_issues": [
            "intraday reaction is based on available intraday_prices_history_YYYYMMDD.csv snapshots only",
            "daily forward performance remains separate and may still be not evaluable until later daily bars exist",
            "this tool is read-only and does not approve orders or change entry gates",
        ],
    }
    _write_json(OUT_JSON, payload)
    return payload


def main() -> int:
    payload = build()
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
