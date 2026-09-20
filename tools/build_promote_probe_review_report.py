"""Build a read-only conditional review for PROMOTE_TO_RECHECK rows.

This is not an entry policy and does not open order routes. It classifies the
current promote cohort into review buckets so the surge expected-value work can
separate candidates with cleaner microstructure from candidates that still need
observation.
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
SURGE_CSV = LOG_DIR / "surge_realtime_latest.csv"
OUT_JSON = LOG_DIR / "promote_probe_review_report_latest.json"
OUT_CSV = LOG_DIR / "promote_probe_review_report_latest.csv"

KST = dt.timezone(dt.timedelta(hours=9))

# Review-only thresholds. These are intentionally not production policy values.
MIN_RETURN_SINCE_FIRST_SEEN = 0.5
MAX_SPREAD_BPS = 35.0
MAX_HIGH_DRAWDOWN_ABS = 0.08
MAX_RVOL20 = 8.0
MIN_TRADING_VALUE = 1_000_000_000.0


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


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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


def _code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text.zfill(6)[-6:] if text else ""


def _split_reasons(text: str) -> List[str]:
    return [part.strip() for part in str(text or "").replace(";", "|").split("|") if part.strip()]


def _surge_by_code() -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in _read_csv(SURGE_CSV):
        code = _code(row.get("code"))
        if code:
            out[code] = row
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


def _review_row(row: Dict[str, str], surge: Dict[str, str]) -> Dict[str, Any]:
    code = _code(row.get("code"))
    ret = _f(row.get("return_pct_since_first_seen"), 0.0)
    reason = str(row.get("reason") or row.get("action_reason") or "")
    reasons = _split_reasons(reason)

    spread_bps = _f(surge.get("spread_bps"), 0.0)
    rvol20 = _f(surge.get("rvol20"), 0.0)
    high_drawdown = _f(surge.get("intraday_high_drawdown_pct"), 0.0)
    trading_value = _f(surge.get("trading_value"), _f(row.get("current_trading_value"), 0.0))
    lob_status = str(surge.get("lob_status") or "")
    orderflow_tag = str(surge.get("orderflow_tag") or "")
    surge_score_final = _f(surge.get("surge_score_final"), _f(surge.get("surge_score"), 0.0))
    entry_reason = str(surge.get("entry_reason") or "")

    has_surge_row = bool(surge)
    lob_ok = lob_status in {"OK", "LOB_OK", "AVAILABLE"} or _truthy(surge.get("lob_available"))
    orderflow_ok = orderflow_tag in {"OK", "LOW_RISK", "NORMAL", ""}
    spread_ok = has_surge_row and spread_bps <= MAX_SPREAD_BPS
    high_rejection_ok = (not high_drawdown) or high_drawdown >= -MAX_HIGH_DRAWDOWN_ABS
    rvol_ok = (not rvol20) or rvol20 <= MAX_RVOL20
    liquidity_ok = trading_value >= MIN_TRADING_VALUE
    momentum_ok = ret >= MIN_RETURN_SINCE_FIRST_SEEN
    no_current_hard = not any(
        token in "|".join(reasons + [entry_reason])
        for token in [
            "NO_LOB_BLOCK",
            "HIGH_REJECTION_ENTRY_BLOCK",
            "ENTRY_CHANGE_BLOCK",
            "ENTRY_ATR_CAP",
            "RVOL",
            "SPREAD",
        ]
    )

    passes = [
        momentum_ok,
        has_surge_row,
        lob_ok,
        orderflow_ok,
        spread_ok,
        high_rejection_ok,
        rvol_ok,
        liquidity_ok,
        no_current_hard,
    ]
    failed = []
    labels = [
        "momentum_ok",
        "has_surge_row",
        "lob_ok",
        "orderflow_ok",
        "spread_ok",
        "high_rejection_ok",
        "rvol_ok",
        "liquidity_ok",
        "no_current_hard",
    ]
    for label, ok in zip(labels, passes):
        if not ok:
            failed.append(label)

    if all(passes):
        review_class = "PROBE_REVIEW_CANDIDATE"
    elif momentum_ok and has_surge_row and liquidity_ok and not any(x in failed for x in ["high_rejection_ok", "rvol_ok"]):
        review_class = "MICROSTRUCTURE_WAIT"
    elif momentum_ok:
        review_class = "MOMENTUM_OBSERVE_ONLY"
    else:
        review_class = "REJECT_FOR_NOW"

    return {
        "review_class": review_class,
        "code": code,
        "name": row.get("name", ""),
        "source": row.get("source", ""),
        "review_bucket": row.get("review_bucket", ""),
        "action_reason": row.get("action_reason", ""),
        "reason": reason,
        "return_pct_since_first_seen": round(ret, 6),
        "current_price_available": _truthy(row.get("current_price_available")),
        "has_surge_row": has_surge_row,
        "surge_score_final": round(surge_score_final, 6),
        "entry_reason": entry_reason,
        "lob_status": lob_status,
        "lob_ok": lob_ok,
        "orderflow_tag": orderflow_tag,
        "orderflow_ok": orderflow_ok,
        "spread_bps": round(spread_bps, 6),
        "spread_ok": spread_ok,
        "intraday_high_drawdown_pct": round(high_drawdown, 6),
        "high_rejection_ok": high_rejection_ok,
        "rvol20": round(rvol20, 6),
        "rvol_ok": rvol_ok,
        "trading_value": round(trading_value, 2),
        "liquidity_ok": liquidity_ok,
        "momentum_ok": momentum_ok,
        "no_current_hard": no_current_hard,
        "failed_checks": ",".join(failed),
        "trading_allowed": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }


def _summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    class_counts = Counter(str(r.get("review_class") or "") for r in rows)
    bucket_counts = Counter(str(r.get("review_bucket") or "") for r in rows)
    source_counts = Counter(str(r.get("source") or "") for r in rows)
    stats_by_class: Dict[str, Dict[str, Any]] = {}
    for cls in sorted(class_counts):
        vals = [_f(r.get("return_pct_since_first_seen"), 0.0) for r in rows if r.get("review_class") == cls]
        stats_by_class[cls] = _stats(vals)
    return {
        "rows": len(rows),
        "class_counts": dict(sorted(class_counts.items())),
        "review_bucket_counts": dict(sorted(bucket_counts.items())),
        "source_counts": dict(sorted(source_counts.items())),
        "return_stats_by_class": stats_by_class,
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "review_class",
        "code",
        "name",
        "source",
        "review_bucket",
        "action_reason",
        "return_pct_since_first_seen",
        "has_surge_row",
        "surge_score_final",
        "entry_reason",
        "lob_status",
        "lob_ok",
        "orderflow_tag",
        "orderflow_ok",
        "spread_bps",
        "spread_ok",
        "intraday_high_drawdown_pct",
        "high_rejection_ok",
        "rvol20",
        "rvol_ok",
        "trading_value",
        "liquidity_ok",
        "momentum_ok",
        "no_current_hard",
        "failed_checks",
        "reason",
        "trading_allowed",
        "policy_effect",
        "policy_change_applied",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    surge_map = _surge_by_code()
    by_code: Dict[str, Dict[str, Any]] = {}
    source_rank = {
        "score_rvol_conditional_recheck": 0,
        "surge_realtime": 1,
        "action_recheck_due": 2,
        "market_rising": 3,
        "daily_candidate": 4,
    }
    class_rank = {
        "PROBE_REVIEW_CANDIDATE": 0,
        "MICROSTRUCTURE_WAIT": 1,
        "MOMENTUM_OBSERVE_ONLY": 2,
        "REJECT_FOR_NOW": 3,
    }
    for row in _read_csv(PLAN_CSV):
        if str(row.get("next_action") or "") != "PROMOTE_TO_RECHECK":
            continue
        code = _code(row.get("code"))
        if not code:
            continue
        reviewed = _review_row(row, surge_map.get(code, {}))
        old = by_code.get(code)
        new_key = (
            class_rank.get(str(reviewed.get("review_class")), 9),
            source_rank.get(str(reviewed.get("source")), 9),
            -_f(reviewed.get("return_pct_since_first_seen"), 0.0),
        )
        old_key = (
            class_rank.get(str(old.get("review_class")), 9),
            source_rank.get(str(old.get("source")), 9),
            -_f(old.get("return_pct_since_first_seen"), 0.0),
        ) if old else None
        if old is None or new_key < old_key:
            by_code[code] = reviewed
    rows = list(by_code.values())
    rows.sort(
        key=lambda r: (
            {"PROBE_REVIEW_CANDIDATE": 0, "MICROSTRUCTURE_WAIT": 1, "MOMENTUM_OBSERVE_ONLY": 2}.get(
                str(r.get("review_class")), 9
            ),
            -_f(r.get("return_pct_since_first_seen"), 0.0),
            str(r.get("code") or ""),
        )
    )
    payload = {
        "generated_at": _now(),
        "schema_version": "promote_probe_review_report_v1",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "review_thresholds": {
            "min_return_since_first_seen": MIN_RETURN_SINCE_FIRST_SEEN,
            "max_spread_bps": MAX_SPREAD_BPS,
            "max_high_drawdown_abs": MAX_HIGH_DRAWDOWN_ABS,
            "max_rvol20": MAX_RVOL20,
            "min_trading_value": MIN_TRADING_VALUE,
        },
        "summary": _summarize(rows),
        "artifacts": {
            "candidate_action_plan": str(PLAN_CSV),
            "surge_realtime": str(SURGE_CSV),
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
        },
        "rows": rows,
        "interpretation": {
            "probe_review_candidate": "passes all review-only checks; still not an entry approval",
            "microstructure_wait": "momentum/liquidity look acceptable but LOB/spread/current hard checks are not clean",
            "momentum_observe_only": "move exists but at least one key risk check is not clean",
            "reject_for_now": "insufficient current move or missing evidence for probe review",
        },
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"status": "OK", "rows": len(rows), "summary": payload["summary"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
