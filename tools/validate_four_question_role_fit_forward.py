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

SOURCE_CSV = LOG_DIR / "four_question_intraday_watch_latest.csv"
OUT_JSON = LOG_DIR / "four_question_role_fit_forward_validation_latest.json"
OUT_CSV = LOG_DIR / "four_question_role_fit_forward_validation_latest.csv"


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


def _float(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value if value is not None else "").strip()
        if not text or text.lower() in {"nan", "none", "<na>"}:
            return default
        return float(text)
    except Exception:
        return default


def _pct(new: float, base: float) -> float:
    return (new / base) - 1.0 if base > 0 else 0.0


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _daily_markout_map(daily: pd.DataFrame, pairs: set[tuple[str, str]]) -> dict[tuple[str, str], dict[str, Any]]:
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
                    "forward_status": "WAITING_NO_FORWARD_BARS",
                    "forward_bars": 0,
                    "base_close": round(close, 6),
                    "next_1d_return": "",
                    "next_3d_return": "",
                    "next_5d_return": "",
                    "max_up_5d": "",
                    "max_down_5d": "",
                    "followthrough_5d": "",
                    "stop_risk_5d": "",
                }
                continue
            closes = [float(v) for v in future["close"].astype(float).tolist()]
            highs = [float(v) for v in future["high"].astype(float).tolist()] if "high" in future else closes
            lows = [float(v) for v in future["low"].astype(float).tolist()] if "low" in future else closes
            out[(code, asof)] = {
                "forward_status": "EVALUATED",
                "forward_bars": int(len(future)),
                "base_close": round(close, 6),
                "next_1d_return": round(_pct(closes[min(0, len(closes) - 1)], close), 6),
                "next_3d_return": round(_pct(closes[min(2, len(closes) - 1)], close), 6),
                "next_5d_return": round(_pct(closes[min(4, len(closes) - 1)], close), 6),
                "max_up_5d": round(_pct(max(highs), close), 6),
                "max_down_5d": round(_pct(min(lows), close), 6),
                "followthrough_5d": max(highs) > close * 1.03,
                "stop_risk_5d": min(lows) < close * 0.95,
            }
    return out


def _summarize_forward(rows: list[dict[str, Any]]) -> dict[str, Any]:
    evaluated = [row for row in rows if row.get("forward_status") == "EVALUATED"]
    if not evaluated:
        return {
            "rows": len(rows),
            "evaluated_rows": 0,
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
        "rows": len(rows),
        "evaluated_rows": len(evaluated),
        "status": "EVALUATED",
        "avg_1d": round(_avg([_float(row.get("next_1d_return")) for row in evaluated]), 6),
        "avg_3d": round(_avg([_float(row.get("next_3d_return")) for row in evaluated]), 6),
        "avg_5d": round(_avg([_float(row.get("next_5d_return")) for row in evaluated]), 6),
        "win_rate_5d": round(_avg([1.0 if _float(row.get("next_5d_return")) > 0 else 0.0 for row in evaluated]), 6),
        "followthrough_rate_5d": round(_avg([1.0 if row.get("followthrough_5d") else 0.0 for row in evaluated]), 6),
        "stop_risk_rate_5d": round(_avg([1.0 if row.get("stop_risk_5d") else 0.0 for row in evaluated]), 6),
        "avg_max_down_5d": round(_avg([_float(row.get("max_down_5d")) for row in evaluated]), 6),
    }


def _role_outcome(row: dict[str, Any]) -> tuple[str, str]:
    if row.get("forward_status") != "EVALUATED":
        return "ROLE_FORWARD_WAITING", "later_daily_bars_missing"
    role = str(row.get("role_fit_label") or "")
    ret5 = _float(row.get("next_5d_return"))
    follow = str(row.get("followthrough_5d")).lower() == "true" or row.get("followthrough_5d") is True
    stop = str(row.get("stop_risk_5d")).lower() == "true" or row.get("stop_risk_5d") is True
    if role == "WATCH_ROLE_SUPPORTED":
        if follow and not stop:
            return "WATCH_FORWARD_CONFIRMED", "watch_supported_and_followthrough_without_stop_risk"
        if stop or ret5 < 0:
            return "WATCH_FORWARD_FAILED", "watch_supported_but_forward_weak_or_stop_risk"
        return "WATCH_FORWARD_NEUTRAL", "watch_supported_but_forward_not_decisive"
    if role.startswith("WATCH_ROLE_FAILED"):
        if ret5 < 0 or stop:
            return "WATCH_FAILURE_CONFIRMED", "watch_failed_intraday_and_forward_weakness_confirmed"
        return "WATCH_FAILURE_NOT_CONFIRMED", "watch_failed_intraday_but_forward_did_not_confirm"
    if role == "CHASE_RISK_STRONG_MOVE_RETAIN_RISK_TAG":
        if stop or ret5 < 0:
            return "CHASE_STRONG_RISK_CONFIRMED", "strong_chase_group_later_weakened_or_hit_stop_risk"
        if follow:
            return "CHASE_STRONG_CONTINUED", "strong_chase_group_continued_forward"
        return "CHASE_STRONG_NEUTRAL", "strong_chase_group_forward_not_decisive"
    if role == "CHASE_RISK_MATERIALIZED":
        if ret5 < 0 or stop:
            return "CHASE_RISK_FORWARD_CONFIRMED", "risk_materialized_intraday_and_forward_weakness_confirmed"
        return "CHASE_RISK_FORWARD_NOT_CONFIRMED", "risk_materialized_intraday_but_forward_did_not_confirm"
    if role == "CURRENT_ONLY_WEAKNESS_CONFIRMED":
        if ret5 < 0 or stop:
            return "CURRENT_ONLY_WEAKNESS_FORWARD_CONFIRMED", "current_only_weakness_persisted_forward"
        return "CURRENT_ONLY_WEAKNESS_FORWARD_NOT_CONFIRMED", "current_only_weakness_did_not_persist_forward"
    if role == "CURRENT_ONLY_STILL_STRONG":
        if follow and not stop:
            return "CURRENT_ONLY_STRENGTH_FORWARD_CONFIRMED", "current_only_strength_persisted_forward"
        return "CURRENT_ONLY_STRENGTH_FORWARD_NOT_CONFIRMED", "current_only_strength_did_not_persist_forward"
    return "ROLE_FORWARD_UNCONFIRMED", "role_not_decisive_for_forward_outcome"


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

    pairs = {(_code(row.get("code")), _ymd(row.get("asof"))) for row in source_rows}
    pairs = {(code, asof) for code, asof in pairs if code and asof}
    daily = _load_daily_history()
    markouts = _daily_markout_map(daily, pairs)

    out_rows: list[dict[str, Any]] = []
    for row in source_rows:
        code = _code(row.get("code"))
        asof = _ymd(row.get("asof"))
        markout = markouts.get(
            (code, asof),
            {
                "forward_status": "MISSING_ASOF_IN_DAILY_HISTORY",
                "forward_bars": 0,
                "base_close": "",
                "next_1d_return": "",
                "next_3d_return": "",
                "next_5d_return": "",
                "max_up_5d": "",
                "max_down_5d": "",
                "followthrough_5d": "",
                "stop_risk_5d": "",
            },
        )
        out = {
            "asof": asof,
            "code": code,
            "name": row.get("name", ""),
            "tracking_group": row.get("tracking_group", ""),
            "role_fit_label": row.get("role_fit_label", ""),
            "role_fit_reason": row.get("role_fit_reason", ""),
            "four_question_decision": row.get("four_question_decision", ""),
            "comparison_bucket": row.get("comparison_bucket", ""),
            "current_selected": row.get("current_selected", ""),
            "current_sources": row.get("current_sources", ""),
            "reaction_label": row.get("reaction_label", ""),
            "intraday_status": row.get("intraday_status", ""),
            "intraday_return": row.get("intraday_return", ""),
            "late_buy_risk": row.get("late_buy_risk", ""),
            "price_zone_label": row.get("price_zone_label", ""),
            "smart_money_continuity_label": row.get("smart_money_continuity_label", ""),
            "raw_supply_history_status": row.get("raw_supply_history_status", ""),
            "global_event_label": row.get("global_event_label", ""),
            "global_event_feed_label": row.get("global_event_feed_label", ""),
            "research_only": True,
            "policy_change": False,
            "entry_approval_changed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
        }
        out.update(markout)
        outcome, outcome_reason = _role_outcome(out)
        out["role_forward_outcome"] = outcome
        out["role_forward_reason"] = outcome_reason
        out_rows.append(out)

    out_rows.sort(key=lambda r: (str(r.get("role_fit_label")), str(r.get("code"))))
    fields = list(out_rows[0].keys()) if out_rows else ["asof", "code", "role_fit_label"]
    _write_csv(OUT_CSV, out_rows, fields)

    role_labels = sorted(set(str(row.get("role_fit_label")) for row in out_rows))
    groups = sorted(set(str(row.get("tracking_group")) for row in out_rows))
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
        "scope": "read_only_four_question_role_fit_forward_validation",
        "source_files": {
            "role_fit_intraday_watch": str(SOURCE_CSV),
            "daily_archive": str(ROOT / "krx_daily_archive"),
        },
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV)},
        "source_counts": {
            "source_rows": len(source_rows),
            "daily_rows_loaded": int(len(daily)) if not daily.empty else 0,
        },
        "forward_status_counts": dict(Counter(str(row.get("forward_status")) for row in out_rows)),
        "role_fit_label_counts": dict(Counter(str(row.get("role_fit_label")) for row in out_rows)),
        "role_forward_outcome_counts": dict(Counter(str(row.get("role_forward_outcome")) for row in out_rows)),
        "summary": _summarize_forward(out_rows),
        "summary_by_role_fit": {
            label: _summarize_forward([row for row in out_rows if str(row.get("role_fit_label")) == label])
            for label in role_labels
        },
        "summary_by_tracking_group": {
            group: _summarize_forward([row for row in out_rows if str(row.get("tracking_group")) == group])
            for group in groups
        },
        "top_rows": out_rows[:30],
        "access_issues": [
            "forward validation requires later daily bars after each asof date",
            "current output may be waiting-only when the latest asof has no later daily bars yet",
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
