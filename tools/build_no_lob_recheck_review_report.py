"""Review no_lob_recheck rows after they are surfaced in the action plan.

This report is observational. It compares PROMOTE_TO_RECHECK and RECHECK_15M
outcomes for LOB-resolved rows without changing entry policy or order routing.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from collections import Counter
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

PLAN_CSV = LOG_DIR / "candidate_action_plan_latest.csv"
NO_LOB_RECHECK_CSV = LOG_DIR / "surge_no_lob_recheck_queue_latest.csv"
SURGE_CSV = LOG_DIR / "surge_realtime_latest.csv"
PARAMS_JSON = ROOT / "paper" / "surge_params.json"

OUT_JSON = LOG_DIR / "no_lob_recheck_review_report_latest.json"
OUT_CSV = LOG_DIR / "no_lob_recheck_review_report_latest.csv"

KST = dt.timezone(dt.timedelta(hours=9))


def _now() -> str:
    return dt.datetime.now(tz=KST).isoformat(timespec="seconds")


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return [{str(k): str(v) for k, v in row.items()} for row in csv.DictReader(f)]
    except Exception:
        return []


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text.zfill(6)[-6:] if text else ""


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value).strip()
        if not text:
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on", "t"}


def _by_code(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code:
            out[code] = row
    return out


def _tokens(*values: Any) -> List[str]:
    joined = "|".join(str(v or "") for v in values)
    out: List[str] = []
    for token in [
        "HIGH_REJECTION_ENTRY_BLOCK",
        "SCORE_RVOL_OVERHEAT_BLOCK",
        "ENTRY_CHANGE_BLOCK",
        "ENTRY_ATR_CAP",
        "SPREAD_BLOCK",
        "ORDERFLOW_RISK_BLOCK",
        "ORDERFLOW_PAUSE",
        "ORDER_IMBALANCE_EXTREME",
        "KRX_CAUTION",
        "KRX_WARNING",
        "KRX_RISK",
        "KRX_ADMIN",
        "NO_REFERENCE",
        "NO_REALTIME_SURGE",
        "NEWS_NEGATIVE",
        "NEWS_IMPLICATION_BLOCK",
    ]:
        if token in joined:
            out.append(token)
    return out


def _stats(vals: Iterable[float]) -> Dict[str, Any]:
    xs = [float(v) for v in vals]
    if not xs:
        return {"n": 0, "avg_return_pct": None, "median_return_pct": None, "win_rate": None}
    pos = sum(1 for x in xs if x > 0)
    return {
        "n": len(xs),
        "avg_return_pct": round(sum(xs) / len(xs), 6),
        "median_return_pct": round(float(median(xs)), 6),
        "win_rate": round(pos / len(xs), 6),
    }


def _classify(
    plan: Dict[str, str],
    queue: Dict[str, str],
    surge: Dict[str, str],
    params: Dict[str, Any],
) -> Dict[str, Any]:
    ret = _f(plan.get("return_pct_since_first_seen"))
    next_action = str(plan.get("next_action") or "")
    lob_ok = _truthy(queue.get("lob_available")) and str(queue.get("lob_status") or "").upper() in {"OK", "BID_ONLY_LIMIT"}
    spread_bps = _f(queue.get("spread_bps"), _f(surge.get("spread_bps")))
    rvol20 = _f(queue.get("rvol20"), _f(surge.get("rvol20")))
    trading_value = _f(queue.get("trading_value"), _f(surge.get("trading_value")))
    change_pct = _f(queue.get("change_pct"), _f(surge.get("change_pct")))
    high_dd = _f(surge.get("intraday_high_drawdown_pct"), 0.0)
    score_final = _f(queue.get("surge_score_final"), _f(surge.get("surge_score_final")))
    detected_type = str(queue.get("detected_surge_type") or surge.get("detected_surge_type") or "")
    blockers = _tokens(queue.get("exclude_reasons"), surge.get("exclude_reasons"), surge.get("entry_reason"))
    blockers_ex_no_lob = [x for x in blockers if x != "NO_LOB_BLOCK"]

    max_spread = _f(params.get("max_spread_bps"), 40.0)
    max_rvol20 = _f(params.get("max_rvol20"), 5.0)
    min_tv = _f(params.get("trading_value_floor"), 1_000_000_000.0)
    high_reject_block = _f(params.get("high_rejection_entry_block_pct"), 0.025)

    limit_or_breakout = detected_type in {"LIMIT_UP_NEAR", "PRICE_RANGE_BREAKOUT", "PRICE_VOL_BREAKOUT"}
    spread_ok = spread_bps <= max_spread
    rvol_ok = rvol20 <= max_rvol20
    liquidity_ok = trading_value >= min_tv
    high_rejection_ok = (not high_dd) or high_dd >= -abs(high_reject_block)
    no_remaining_hard = not blockers_ex_no_lob
    promote_ok = next_action == "PROMOTE_TO_RECHECK"
    positive_followthrough = ret > 0.0

    if (
        promote_ok
        and positive_followthrough
        and lob_ok
        and spread_ok
        and rvol_ok
        and liquidity_ok
        and no_remaining_hard
        and limit_or_breakout
    ):
        review_class = "NARROW_POLICY_REVIEW_CANDIDATE"
        action_hint = "REVIEW_ONLY_NO_ENTRY_ROUTE"
    elif promote_ok and lob_ok and spread_ok and rvol_ok and liquidity_ok and no_remaining_hard and limit_or_breakout:
        review_class = "CLEAN_BUT_NO_FOLLOWTHROUGH"
        action_hint = "KEEP_REVIEW_ONLY_UNTIL_POSITIVE_FOLLOWTHROUGH"
    elif promote_ok:
        review_class = "PROMOTE_RECHECK_WITH_BLOCKER"
        action_hint = "KEEP_REVIEW_ONLY_UNTIL_BLOCKERS_CLEAR"
    else:
        review_class = "RECHECK_15M_OBSERVE_ONLY"
        action_hint = "ACCUMULATE_FOLLOWTHROUGH_SAMPLE"

    return {
        "review_class": review_class,
        "action_hint": action_hint,
        "return_pct_since_first_seen": round(ret, 6),
        "detected_surge_type": detected_type,
        "surge_score_final": round(score_final, 6),
        "change_pct": round(change_pct, 6),
        "lob_ok": lob_ok,
        "spread_bps": round(spread_bps, 6),
        "spread_ok": spread_ok,
        "rvol20": round(rvol20, 6),
        "rvol_ok": rvol_ok,
        "trading_value": round(trading_value, 2),
        "liquidity_ok": liquidity_ok,
        "intraday_high_drawdown_pct": round(high_dd, 6),
        "high_rejection_ok": high_rejection_ok,
        "limit_or_breakout": limit_or_breakout,
        "positive_followthrough": positive_followthrough,
        "remaining_hard_ex_no_lob": "|".join(blockers_ex_no_lob),
        "no_remaining_hard": no_remaining_hard,
    }


def _build_rows() -> List[Dict[str, Any]]:
    plan_rows = [row for row in _read_csv(PLAN_CSV) if str(row.get("source") or "") == "no_lob_recheck"]
    queue_by_code = _by_code(_read_csv(NO_LOB_RECHECK_CSV))
    surge_by_code = _by_code(_read_csv(SURGE_CSV))
    params = _read_json(PARAMS_JSON)
    rows: List[Dict[str, Any]] = []
    for plan in plan_rows:
        code = _code(plan.get("code"))
        queue = queue_by_code.get(code, {})
        surge = surge_by_code.get(code, {})
        cls = _classify(plan, queue, surge, params)
        rows.append(
            {
                "code": code,
                "name": plan.get("name", ""),
                "next_action": plan.get("next_action", ""),
                "action_strength": plan.get("action_strength", ""),
                "review_bucket": plan.get("review_bucket", ""),
                "plan_reason": plan.get("reason", ""),
                "queue_ts": queue.get("ts", ""),
                "queue_entry_reason": queue.get("entry_reason", ""),
                "queue_exclude_reasons": queue.get("exclude_reasons", ""),
                "queue_probe_class": queue.get("probe_class", ""),
                "queue_recheck_action": queue.get("recheck_action", ""),
                **cls,
                "entry_approval_changed": False,
                "trading_allowed": False,
                "policy_effect": False,
                "policy_change_applied": False,
            }
        )
    return rows


def _summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    action_counts = Counter(str(row.get("next_action") or "") for row in rows)
    class_counts = Counter(str(row.get("review_class") or "") for row in rows)
    blockers = Counter()
    for row in rows:
        for token in str(row.get("remaining_hard_ex_no_lob") or "").split("|"):
            token = token.strip()
            if token:
                blockers[token] += 1
    stats_by_action = {
        action: _stats(_f(row.get("return_pct_since_first_seen")) for row in rows if row.get("next_action") == action)
        for action in sorted(action_counts)
    }
    return {
        "rows": len(rows),
        "next_action_counts": dict(sorted(action_counts.items())),
        "review_class_counts": dict(sorted(class_counts.items())),
        "remaining_hard_ex_no_lob_counts": dict(sorted(blockers.items())),
        "return_stats_by_next_action": stats_by_action,
        "policy_review_candidate_codes": [row.get("code") for row in rows if row.get("review_class") == "NARROW_POLICY_REVIEW_CANDIDATE"],
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys()) if rows else ["code"]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    rows = _build_rows()
    payload = {
        "generated_at": _now(),
        "scope": "read_only_no_lob_recheck_review",
        "policy_note": "No trading permission or policy value is changed.",
        "source_files": {
            "plan_csv": str(PLAN_CSV),
            "no_lob_recheck_csv": str(NO_LOB_RECHECK_CSV),
            "surge_csv": str(SURGE_CSV),
            "params_json": str(PARAMS_JSON),
        },
        "summary": _summary(rows),
        "rows": rows,
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"out_json": str(OUT_JSON), "out_csv": str(OUT_CSV), "summary": payload["summary"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
