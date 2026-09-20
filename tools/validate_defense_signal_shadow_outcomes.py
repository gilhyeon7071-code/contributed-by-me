from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER_DIR = ROOT / "paper"

DEFENSE_CSV = LOG_DIR / "defense_signal_shadow_latest.csv"
FOUR_Q_CSV = LOG_DIR / "four_question_role_fit_forward_validation_latest.csv"
SURGE_PATH_CSV = LOG_DIR / "surge_path_validation_outcome_latest.csv"
SURGE_MARKOUT_CSV = LOG_DIR / "surge_shadow_probe_markout_summary_latest.csv"
TRADES_CSV = PAPER_DIR / "trades_calc.csv"

OUT_JSON = LOG_DIR / "defense_signal_shadow_outcome_validation_latest.json"
OUT_CSV = LOG_DIR / "defense_signal_shadow_outcome_validation_latest.csv"

KST = timezone(timedelta(hours=9))


def _now_kst() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as fh:
                return [{str(k): str(v or "") for k, v in row.items()} for row in csv.DictReader(fh)]
        except UnicodeDecodeError:
            continue
    return []


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _code(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6)[-6:] if digits else ""


def _f(value: Any, default: float | None = 0.0) -> float | None:
    try:
        text = str(value or "").strip()
        if text == "":
            return default
        return float(text)
    except Exception:
        return default


def _b(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _index_first(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code") or row.get("ticker"))
        if code and code not in out:
            out[code] = row
    return out


def _avg(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 6) if values else None


def _aggregate_surge_path(rows: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        code = _code(row.get("code"))
        if code:
            grouped[code].append(row)

    out: dict[str, dict[str, Any]] = {}
    for code, items in grouped.items():
        classes = Counter(str(r.get("outcome_class") or "") for r in items)
        close_like = [v for v in (_f(r.get("close_like_ret_pct"), None) for r in items) if v is not None]
        max_adv = [v for v in (_f(r.get("max_adverse_pct"), None) for r in items) if v is not None]
        out[code] = {
            "surge_path_rows": len(items),
            "surge_path_outcome_counts": dict(classes),
            "surge_path_positive": classes.get("PATH_TEST_POSITIVE", 0),
            "surge_path_negative": classes.get("PATH_TEST_NEGATIVE", 0),
            "surge_path_not_evaluable": classes.get("NOT_EVALUABLE", 0),
            "surge_path_mean_close_like_ret_pct": _avg(close_like),
            "surge_path_worst_adverse_pct": min(max_adv) if max_adv else None,
        }
    return out


def _aggregate_trades(rows: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        code = _code(row.get("code") or row.get("ticker"))
        if code:
            grouped[code].append(row)

    out: dict[str, dict[str, Any]] = {}
    for code, items in grouped.items():
        net_ret = [v for v in (_f(r.get("net_ret"), None) for r in items) if v is not None]
        out[code] = {
            "historical_trade_rows": len(items),
            "historical_trade_avg_net_ret": _avg(net_ret),
            "historical_trade_negative_rows": sum(1 for v in net_ret if v < 0),
            "historical_trade_positive_rows": sum(1 for v in net_ret if v > 0),
        }
    return out


def _evidence_flags(
    defense: dict[str, str],
    fourq: dict[str, str],
    surge_path: dict[str, Any],
    markout: dict[str, str],
    trades: dict[str, Any],
) -> dict[str, Any]:
    fourq_ret = _f(fourq.get("intraday_return"), None)
    markout_positive_rate = _f(markout.get("positive_markout_rate"), None)
    markout_kill_rate = _f(markout.get("kill_rate"), None)
    markout_delta = _f(markout.get("avg_markout_delta_pct_points"), None)
    trade_avg = trades.get("historical_trade_avg_net_ret")

    negative_evidence = []
    positive_evidence = []
    unavailable = []

    if fourq_ret is None:
        unavailable.append("four_question_intraday_missing")
    elif fourq_ret < -0.005:
        negative_evidence.append("four_question_intraday_negative")
    elif fourq_ret > 0.005:
        positive_evidence.append("four_question_intraday_positive")

    if surge_path:
        if int(surge_path.get("surge_path_negative") or 0) > 0:
            negative_evidence.append("surge_path_negative")
        if int(surge_path.get("surge_path_positive") or 0) > 0:
            positive_evidence.append("surge_path_positive")
        worst = surge_path.get("surge_path_worst_adverse_pct")
        if worst is not None and float(worst) <= -0.03:
            negative_evidence.append("surge_path_adverse_gt_3pct")
    else:
        unavailable.append("surge_path_missing")

    if markout:
        review_status = str(markout.get("review_status") or "")
        if "NOT_ELIGIBLE" in review_status or "KILL" in str(markout.get("latest_tracker_status") or ""):
            negative_evidence.append("surge_markout_not_eligible_or_kill")
        if markout_kill_rate is not None and markout_kill_rate >= 0.5:
            negative_evidence.append("surge_markout_kill_rate_high")
        if markout_positive_rate is not None and markout_positive_rate >= 0.6 and (markout_delta or 0.0) > 0:
            positive_evidence.append("surge_markout_positive")
    else:
        unavailable.append("surge_markout_missing")

    if trade_avg is None:
        unavailable.append("historical_trade_missing")
    elif float(trade_avg) < -0.005:
        negative_evidence.append("historical_trade_avg_negative")
    elif float(trade_avg) > 0.005:
        positive_evidence.append("historical_trade_avg_positive")

    would_block_general = _b(defense.get("would_block_general"))
    would_block_surge = _b(defense.get("would_block_surge"))
    shadow_block = would_block_general or would_block_surge
    shadow_watch = str(defense.get("defense_action_shadow") or "").upper().endswith("_WATCH")
    shadow_recheck = "RECHECK" in str(defense.get("defense_action_shadow") or "").upper()

    evaluated = bool(negative_evidence or positive_evidence)
    if not evaluated:
        label = "NOT_EVALUABLE"
    elif shadow_block and negative_evidence:
        label = "DEFENSE_SUPPORTED"
    elif shadow_block and positive_evidence and not negative_evidence:
        label = "OVERBLOCK_RISK"
    elif (not shadow_block) and negative_evidence and not (shadow_watch or shadow_recheck):
        label = "MISSED_RISK"
    elif (shadow_watch or shadow_recheck) and negative_evidence:
        label = "WATCH_OR_RECHECK_SUPPORTED"
    elif (not shadow_block) and positive_evidence:
        label = "ALLOW_SUPPORTED"
    else:
        label = "MIXED_EVIDENCE"

    return {
        "outcome_eval_status": "EVALUATED_PARTIAL" if evaluated else "NOT_EVALUABLE",
        "defense_validation_label": label,
        "negative_evidence": "|".join(sorted(set(negative_evidence))),
        "positive_evidence": "|".join(sorted(set(positive_evidence))),
        "unavailable_evidence": "|".join(sorted(set(unavailable))),
        "fourq_intraday_return": fourq_ret,
        "markout_positive_rate": markout_positive_rate,
        "markout_kill_rate": markout_kill_rate,
        "markout_avg_delta_pct_points": markout_delta,
    }


def main() -> int:
    status: dict[str, Any] = {
        "generated_at": _now_kst(),
        "mode": "read_only_defense_signal_shadow_outcome_validation",
        "score_effect": False,
        "trading_effect": False,
        "policy_effect": False,
        "entry_approval_changed": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "no_order_effect": True,
        "quality": "FAIL",
        "reason": "",
        "inputs": {
            "defense_shadow": str(DEFENSE_CSV),
            "four_question_forward": str(FOUR_Q_CSV),
            "surge_path": str(SURGE_PATH_CSV),
            "surge_markout": str(SURGE_MARKOUT_CSV),
            "trades": str(TRADES_CSV),
        },
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV)},
    }

    defense_rows = _read_csv(DEFENSE_CSV)
    if not defense_rows:
        status["reason"] = "defense_shadow_missing_or_empty"
        _write_json(OUT_JSON, status)
        return 1

    fourq_by_code = _index_first(_read_csv(FOUR_Q_CSV))
    surge_path_by_code = _aggregate_surge_path(_read_csv(SURGE_PATH_CSV))
    markout_by_code = _index_first(_read_csv(SURGE_MARKOUT_CSV))
    trades_by_code = _aggregate_trades(_read_csv(TRADES_CSV))

    out_rows: list[dict[str, Any]] = []
    for row in defense_rows:
        code = _code(row.get("code"))
        fourq = fourq_by_code.get(code, {})
        surge_path = surge_path_by_code.get(code, {})
        markout = markout_by_code.get(code, {})
        trades = trades_by_code.get(code, {})
        flags = _evidence_flags(row, fourq, surge_path, markout, trades)

        out_rows.append(
            {
                "code": code,
                "name": row.get("name", ""),
                "route_scope": row.get("route_scope", ""),
                "defense_signal_score": row.get("defense_signal_score", ""),
                "defense_action_shadow": row.get("defense_action_shadow", ""),
                "general_action_shadow": row.get("general_action_shadow", ""),
                "surge_action_shadow": row.get("surge_action_shadow", ""),
                "would_block_general": row.get("would_block_general", ""),
                "would_block_surge": row.get("would_block_surge", ""),
                "defense_reasons": row.get("defense_reasons", ""),
                "fourq_intraday_status": fourq.get("intraday_status", ""),
                "fourq_intraday_return": flags["fourq_intraday_return"],
                "fourq_forward_status": fourq.get("forward_status", ""),
                "fourq_role_forward_outcome": fourq.get("role_forward_outcome", ""),
                "surge_path_rows": surge_path.get("surge_path_rows", 0),
                "surge_path_positive": surge_path.get("surge_path_positive", 0),
                "surge_path_negative": surge_path.get("surge_path_negative", 0),
                "surge_path_not_evaluable": surge_path.get("surge_path_not_evaluable", 0),
                "surge_path_mean_close_like_ret_pct": surge_path.get("surge_path_mean_close_like_ret_pct", ""),
                "surge_path_worst_adverse_pct": surge_path.get("surge_path_worst_adverse_pct", ""),
                "markout_review_status": markout.get("review_status", ""),
                "markout_positive_rate": flags["markout_positive_rate"],
                "markout_kill_rate": flags["markout_kill_rate"],
                "markout_avg_delta_pct_points": flags["markout_avg_delta_pct_points"],
                "historical_trade_rows": trades.get("historical_trade_rows", 0),
                "historical_trade_avg_net_ret": trades.get("historical_trade_avg_net_ret", ""),
                "outcome_eval_status": flags["outcome_eval_status"],
                "defense_validation_label": flags["defense_validation_label"],
                "negative_evidence": flags["negative_evidence"],
                "positive_evidence": flags["positive_evidence"],
                "unavailable_evidence": flags["unavailable_evidence"],
                "score_effect": False,
                "trading_effect": False,
                "no_order_effect": True,
            }
        )

    out_rows.sort(
        key=lambda r: (
            str(r["defense_validation_label"]),
            -int(float(r.get("defense_signal_score") or 0)),
            str(r["code"]),
        )
    )

    fields = [
        "code",
        "name",
        "route_scope",
        "defense_signal_score",
        "defense_action_shadow",
        "general_action_shadow",
        "surge_action_shadow",
        "would_block_general",
        "would_block_surge",
        "defense_reasons",
        "fourq_intraday_status",
        "fourq_intraday_return",
        "fourq_forward_status",
        "fourq_role_forward_outcome",
        "surge_path_rows",
        "surge_path_positive",
        "surge_path_negative",
        "surge_path_not_evaluable",
        "surge_path_mean_close_like_ret_pct",
        "surge_path_worst_adverse_pct",
        "markout_review_status",
        "markout_positive_rate",
        "markout_kill_rate",
        "markout_avg_delta_pct_points",
        "historical_trade_rows",
        "historical_trade_avg_net_ret",
        "outcome_eval_status",
        "defense_validation_label",
        "negative_evidence",
        "positive_evidence",
        "unavailable_evidence",
        "score_effect",
        "trading_effect",
        "no_order_effect",
    ]
    _write_csv(OUT_CSV, out_rows, fields)

    label_counts = Counter(str(r["defense_validation_label"]) for r in out_rows)
    eval_counts = Counter(str(r["outcome_eval_status"]) for r in out_rows)
    route_label_counts: dict[str, dict[str, int]] = {}
    for row in out_rows:
        route = str(row.get("route_scope") or "UNKNOWN")
        route_label_counts.setdefault(route, {})
        label = str(row["defense_validation_label"])
        route_label_counts[route][label] = route_label_counts[route].get(label, 0) + 1

    status.update(
        {
            "quality": "PASS",
            "reason": "ok",
            "rows": len(out_rows),
            "evaluated_partial_rows": int(eval_counts.get("EVALUATED_PARTIAL", 0)),
            "not_evaluable_rows": int(eval_counts.get("NOT_EVALUABLE", 0)),
            "defense_validation_label_counts": dict(label_counts),
            "route_label_counts": route_label_counts,
            "top_supported": [r for r in out_rows if r["defense_validation_label"] == "DEFENSE_SUPPORTED"][:20],
            "top_overblock_risk": [r for r in out_rows if r["defense_validation_label"] == "OVERBLOCK_RISK"][:20],
            "top_missed_risk": [r for r in out_rows if r["defense_validation_label"] == "MISSED_RISK"][:20],
            "notes": [
                "partial validation only; four-question forward returns are still waiting where forward bars are missing",
                "surge path and markout artifacts are read-only validation sources, not trading approval",
                "historical trades are code-level reference only and are not same-signal proof",
            ],
        }
    )
    _write_json(OUT_JSON, status)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
