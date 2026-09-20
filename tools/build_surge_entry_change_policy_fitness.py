from __future__ import annotations

import csv
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PARAMS_JSON = ROOT / "paper" / "surge_params.json"
EXCEPTION_JSON = LOG_DIR / "surge_entry_change_exception_candidates_latest.json"
DECOMP_JSON = LOG_DIR / "surge_entry_cap_decomposition_latest.json"
OUT_JSON = LOG_DIR / "surge_entry_change_policy_fitness_latest.json"
OUT_CSV = LOG_DIR / "surge_entry_change_policy_fitness_latest.csv"
KST = timezone(timedelta(hours=9))
HARD_BLOCKERS = {
    "ENTRY_ATR_CAP", "HIGH_REJECTION_ENTRY_BLOCK", "RVOL_OVERHEAT_BLOCK",
    "SCORE_RVOL_OVERHEAT_BLOCK", "NO_LOB_BLOCK", "SPREAD_BLOCK",
    "ORDERFLOW_RISK_BLOCK", "ORDERFLOW_PAUSE", "NEWS_NEGATIVE", "NEWS_IMPLICATION_BLOCK",
    "KRX_ADMIN", "KRX_WARNING", "KRX_RISK", "KRX_CAUTION", "TRADING_VALUE_FLOOR",
}
FIELDS = [
    "ts", "date", "code", "change_pct", "surge_score_final", "rvol20",
    "intraday_high_drawdown_pct", "lob_available", "lob_status", "spread_bps",
    "entry_change_exception_candidate", "remaining_hard_blockers", "policy_class",
    "interpretation", "policy_change", "entry_approval_changed", "research_only",
]


def _now() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in FIELDS})


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def _reasons(text: Any) -> List[str]:
    keys: List[str] = []
    for part in str(text or "").split("|"):
        token = part.strip()
        if token:
            key = token.split(":", 1)[0].strip().upper()
            if key and key not in keys:
                keys.append(key)
    return keys


def build() -> Dict[str, Any]:
    params = _load_json(PARAMS_JSON)
    decomp = _load_json(DECOMP_JSON)
    exc = _load_json(EXCEPTION_JSON)
    rows: List[Dict[str, Any]] = []
    for r in exc.get("rows", []):
        keys = _reasons(r.get("exclude_reasons"))
        non_entry_hard = [k for k in keys if k in HARD_BLOCKERS and k != "ENTRY_CHANGE_BLOCK"]
        change = _f(r.get("change_pct"))
        rvol = _f(r.get("rvol20"))
        drawdown = _f(r.get("intraday_high_drawdown_pct"))
        spread = _f(r.get("spread_bps"))
        score = _f(r.get("surge_score_final"))
        atr_cap = "ENTRY_ATR_CAP" in non_entry_hard
        score_rvol = score >= _f(params.get("overheat_score_min"), 90.0) and rvol >= _f(params.get("overheat_rvol20_min"), 3.0)
        clean_16_20 = (
            0.16 < change <= 0.20 and not non_entry_hard and bool(r.get("lob_available"))
            and spread <= 20.0 and drawdown > -abs(_f(params.get("high_rejection_entry_block_pct"), 0.025))
            and rvol <= _f(params.get("max_rvol20"), 5.0) and not score_rvol
        )
        if clean_16_20:
            policy_class = "CLEAN_16_20_EXCEPTION_REVIEW"
        elif change > 0.20 and atr_cap:
            policy_class = "ABOVE_20_ENTRY_AND_ATR_WITH_OTHER_HARD_BLOCKERS"
        elif 0.16 < change <= 0.20:
            policy_class = "ENTRY_16_20_WITH_OTHER_HARD_BLOCKERS"
        else:
            policy_class = "ENTRY_CHANGE_WITH_OTHER_HARD_BLOCKERS"
        rows.append({
            "ts": r.get("ts"),
            "date": r.get("date"),
            "code": str(r.get("code") or "").zfill(6),
            "change_pct": round(change, 6),
            "surge_score_final": round(score, 6),
            "rvol20": round(rvol, 6),
            "intraday_high_drawdown_pct": round(drawdown, 6),
            "lob_available": bool(r.get("lob_available")),
            "lob_status": r.get("lob_status"),
            "spread_bps": round(spread, 6),
            "entry_change_exception_candidate": bool(r.get("entry_change_exception_candidate")),
            "remaining_hard_blockers": "|".join(non_entry_hard),
            "policy_class": policy_class,
            "interpretation": "clean_sample_needs_forward_markout" if clean_16_20 else "16pct_relaxation_not_supported_current_sample",
            "policy_change": False,
            "entry_approval_changed": False,
            "research_only": True,
        })
    class_counts: Dict[str, int] = {}
    for row in rows:
        class_counts[str(row.get("policy_class"))] = class_counts.get(str(row.get("policy_class")), 0) + 1
    summary = {
        "review_rows": len(rows),
        "candidate_rows": int(exc.get("candidate_rows") or 0),
        "possible_over_suppression_rows": class_counts.get("CLEAN_16_20_EXCEPTION_REVIEW", 0),
        "class_counts": [{"policy_class": k, "count": v} for k, v in sorted(class_counts.items())],
        "entry_cap_decomposition": decomp.get("summary", {}),
        "decision": "NO_RELAXATION_SUPPORT_CURRENT_SAMPLE" if class_counts.get("CLEAN_16_20_EXCEPTION_REVIEW", 0) == 0 else "WATCH_CLEAN_16_20_MARKOUTS",
        "reason": "All current ENTRY_CHANGE_BLOCK rows still have other hard blockers; clean 16-20pct continuation sample is absent." if class_counts.get("CLEAN_16_20_EXCEPTION_REVIEW", 0) == 0 else "Clean 16-20pct rows require repeated forward markout evidence before any policy change.",
    }
    payload = {
        "ts": _now(),
        "status": "OK",
        "scope": "surge_entry_change_policy_fitness",
        "source_exception_json": str(EXCEPTION_JSON),
        "source_decomposition_json": str(DECOMP_JSON),
        "source_params_json": str(PARAMS_JSON),
        "thresholds": {
            "max_entry_change_pct": params.get("max_entry_change_pct"),
            "limit_near_pct": params.get("limit_near_pct"),
            "atr_entry_cap_enabled": params.get("atr_entry_cap_enabled"),
            "entry_change_pct_multiplier": params.get("entry_change_pct_multiplier"),
            "atr_entry_cap_multiplier": params.get("atr_entry_cap_multiplier"),
            "high_rejection_entry_block_pct": params.get("high_rejection_entry_block_pct"),
            "max_rvol20": params.get("max_rvol20"),
            "overheat_score_min": params.get("overheat_score_min"),
            "overheat_rvol20_min": params.get("overheat_rvol20_min"),
        },
        "summary": summary,
        "risk_contract": {
            "policy_change": False, "entry_approval_changed": False, "entry_signal": False,
            "live_order_allowed": False, "paper_order_route": False, "broker_order_route": False,
            "trading_route": False, "research_only": True, "must_not_dispatch": True,
        },
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    stamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    (LOG_DIR / f"surge_entry_change_policy_fitness_{stamp}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(LOG_DIR / f"surge_entry_change_policy_fitness_{stamp}.csv", rows)
    return payload


def main() -> int:
    payload = build()
    print(json.dumps({"status": payload["status"], "summary": payload["summary"], "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
