"""build_shadow_promotion_report.py
Shadow-promotion observation layer for NEWS_ONLY and RECHECK_MISSED_MOVE_CANDIDATE entries.

POLICY: READ-ONLY. No score, gate, order, fill, or trade changes.

For each candidate of these types, simulates the following counterfactual:
  "If execution_pool=True and sector_entry_allowed=True were injected, would this
   candidate have passed through the remaining gate stack — and why / why not?"

This is NOT a policy change recommendation. It is a calibration log used to detect
whether the gating at the execution_pool / sector_entry_allowed layer is masking
candidates that would anyway be blocked deeper in the stack (in which case the outer
gate is redundant) or passing candidates that the outer gate is correctly stopping
(in which case the outer gate is necessary).

Output:
  2_Logs/shadow_promotion_report_latest.json
  2_Logs/shadow_promotion_report_latest.csv
  2_Logs/shadow_promotion_report_history.csv  (append-only)
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CONFIG_PATH = ROOT / "paper" / "paper_engine_config.json"
FILLS_CSV = ROOT / "paper" / "fills.csv"

OUT_JSON = LOG_DIR / "shadow_promotion_report_latest.json"
OUT_CSV = LOG_DIR / "shadow_promotion_report_latest.csv"
HISTORY_CSV = LOG_DIR / "shadow_promotion_report_history.csv"

# Shadow check thresholds (mirrors paper_engine positive_entry_criteria defaults)
DEFAULT_MORNING_STRENGTH_MIN = 0.80
DEFAULT_MIN_FINAL_SCORE = 0.0


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _now_kst() -> str:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).isoformat(timespec="seconds")


def _today_ymd() -> str:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).strftime("%Y%m%d")


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _read_csv(path: Path, limit: Optional[int] = None) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    rows: List[Dict[str, str]] = []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                rows.append({str(k): str(v) for k, v in row.items()})
                if limit is not None and len(rows) >= int(limit):
                    break
    except Exception:
        return []
    return rows


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value).strip()
        if not text or text.lower() in {"nan", "none", ""}:
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _clean_code(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    return s.zfill(6)[-6:]


# ---------------------------------------------------------------------------
# shadow simulation core
# ---------------------------------------------------------------------------

def _simulate_promotion(
    code: str,
    name: str,
    final_score: float,
    sector_strength: Optional[float],
    candidate_source: str,
    review_bucket: str,
    return_pct_since_first_seen: float,
    current_change_pct: float,
    *,
    today_buys: set,
    p1_entry_ready: int,
    p1_max_new: int,
    p1_max_new_surge: int,
    p1_max_new_zero_reason: str,
    market_regime: str,
    morning_strength_min: float,
    surge_exclusions: Dict[str, str],  # code -> exclude_reasons
) -> Dict[str, Any]:
    """Return shadow check result. Does NOT modify any state."""
    passed: List[str] = []
    blocked: List[str] = []

    # --- 1. Score check ---
    if final_score > DEFAULT_MIN_FINAL_SCORE:
        passed.append(f"score_ok:{final_score:.5f}")
    else:
        blocked.append(f"score_zero_or_missing:{final_score:.5f}")

    # --- 2. Same-code-day buy check ---
    if code in today_buys:
        blocked.append("same_code_day_already_buy")
    else:
        passed.append("same_code_day_clear")

    # --- 3. Market regime ---
    regime_upper = str(market_regime or "").strip().upper()
    if regime_upper in {"CRASH", "CRASH_BLOCK", "BEAR_EXTREME"}:
        blocked.append(f"regime_block:{regime_upper}")
    else:
        passed.append(f"regime_ok:{regime_upper}")

    # --- 4. P1 gate (entry slots) ---
    if p1_entry_ready > 0 and p1_max_new > 0:
        passed.append(f"p1_gate_open:entry_ready={p1_entry_ready},max_new={p1_max_new}")
    else:
        reason = p1_max_new_zero_reason or "gate_closed"
        blocked.append(f"p1_gate_closed:{reason}:entry_ready={p1_entry_ready},max_new={p1_max_new}")

    # --- 5. Surge-specific gate ---
    is_surge_source = "surge" in str(candidate_source).lower()
    if is_surge_source:
        if p1_max_new_surge > 0:
            passed.append(f"surge_slot_ok:max_new_surge={p1_max_new_surge}")
        else:
            blocked.append(f"surge_slot_blocked:max_new_surge={p1_max_new_surge}")

    # --- 6. Surge policy exclusion check ---
    surge_excl = surge_exclusions.get(code, "")
    if surge_excl:
        blocked.append(f"surge_policy_excluded:{surge_excl}")
    else:
        passed.append("surge_policy_clear")

    # --- 7. Sector strength check (soft — only when data available) ---
    if sector_strength is not None:
        if sector_strength >= morning_strength_min:
            passed.append(f"sector_strength_ok:{sector_strength:.4f}>={morning_strength_min:.4f}")
        else:
            blocked.append(f"sector_strength_weak:{sector_strength:.4f}<{morning_strength_min:.4f}")
    else:
        # No sector data for NEWS_ONLY — gate assumed unknown
        blocked.append("sector_strength_unknown:no_sector_data")

    shadow_pass = len(blocked) == 0
    observation = (
        "shadow_pass_all_checks" if shadow_pass
        else "shadow_blocked_at:" + ";".join(blocked[:3])
    )

    return {
        "generated_at": _now_kst(),
        "code": code,
        "name": name,
        "candidate_source": candidate_source,
        "review_bucket": review_bucket,
        "final_score": round(final_score, 6),
        "sector_strength": round(sector_strength, 4) if sector_strength is not None else None,
        "current_change_pct": round(current_change_pct, 4),
        "return_pct_since_first_seen": round(return_pct_since_first_seen, 4),
        "shadow_pass": shadow_pass,
        "shadow_passed_checks": passed,
        "shadow_blocked_reasons": blocked,
        "shadow_observation": observation,
        "trading_effect": False,
        "policy_effect": False,
    }


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> int:
    generated_at = _now_kst()
    today_ymd = _today_ymd()

    cfg = _read_json(CONFIG_PATH)
    intraday_pol = ((cfg.get("p1_entry_policy") or {}).get("intraday") or {}) if isinstance(cfg, dict) else {}
    morning_strength_min = float(
        intraday_pol.get("morning_sector_strength_min", DEFAULT_MORNING_STRENGTH_MIN) or DEFAULT_MORNING_STRENGTH_MIN
    )

    pending = _read_json(LOG_DIR / "pending_entry_status_latest.json")
    p1 = _read_json(LOG_DIR / "p1_entry_gate_status_latest.json")
    p1_entry_ready = int(pending.get("entry_ready") or 0)
    p1_max_new = int(pending.get("max_new") or 0)
    p1_max_new_surge = int(pending.get("max_new_surge") or 0)
    p1_max_new_zero_reason = str(pending.get("max_new_zero_reason") or "")
    market_regime = str(pending.get("market_regime") or p1.get("market_regime") or "NORMAL")

    # Today's BUY fills → same-code-day check
    today_buys: set = set()
    for row in _read_csv(FILLS_CSV):
        if str(row.get("date") or "")[:8] == today_ymd and str(row.get("side") or "").upper() == "BUY":
            c = _clean_code(row.get("code") or row.get("stck_shrn_iscd") or "")
            if c:
                today_buys.add(c)

    # Surge exclusions → code -> exclude_reasons
    surge_exclusions: Dict[str, str] = {}
    for row in _read_csv(LOG_DIR / "surge_realtime_latest.csv"):
        if _truthy(row.get("excluded_by_policy")):
            c = _clean_code(row.get("code") or "")
            if c:
                surge_exclusions[c] = str(row.get("exclude_reasons") or "")

    # ---- Source 1: NEWS_ONLY candidates from final_score CSV ----
    news_only_candidates: List[Dict[str, Any]] = []
    for row in _read_csv(LOG_DIR / "candidates_latest_data.with_final_score.csv"):
        if str(row.get("candidate_origin_hybrid") or "").strip().upper() != "NEWS_ONLY":
            continue
        code = _clean_code(row.get("code") or "")
        if not code:
            continue
        ss_raw = row.get("sector_strength")
        sector_strength: Optional[float] = None
        try:
            if ss_raw and str(ss_raw).strip() not in {"", "nan", "None"}:
                sector_strength = float(ss_raw)
        except Exception:
            pass
        news_only_candidates.append({
            "code": code,
            "name": str(row.get("name") or ""),
            "final_score": _f(row.get("final_score"), _f(row.get("final_score_base"), 0.0)),
            "sector_strength": sector_strength,
            "current_change_pct": _f(row.get("day_ret_pct"), _f(row.get("ret1_pct"), 0.0)),
            "return_pct_since_first_seen": 0.0,
            "candidate_source": "news_only_final_score",
            "review_bucket": "NEWS_ONLY",
        })

    # ---- Source 2: RECHECK_MISSED_MOVE_CANDIDATE from review JSON ----
    review_doc = _read_json(LOG_DIR / "candidate_action_review_latest.json")

    # 이름 사전: review 전체 행(버킷 무관) + surge CSV에서 비어있지 않은 이름 수집
    _name_lookup: Dict[str, str] = {}
    for row in (review_doc.get("rows") or []):
        if not isinstance(row, dict):
            continue
        c = _clean_code(row.get("code") or "")
        n = str(row.get("name") or "").strip()
        if c and n and not _name_lookup.get(c):
            _name_lookup[c] = n
    for row in _read_csv(LOG_DIR / "surge_realtime_latest.csv"):
        c = _clean_code(row.get("code") or "")
        n = str(row.get("name") or row.get("hts_kor_isnm") or "").strip()
        if c and n and not _name_lookup.get(c):
            _name_lookup[c] = n
    for row in _read_csv(LOG_DIR / "market_rising_latest.csv"):
        c = _clean_code(row.get("code") or row.get("stck_shrn_iscd") or "")
        n = str(row.get("name") or row.get("hts_kor_isnm") or "").strip()
        if c and n and not _name_lookup.get(c):
            _name_lookup[c] = n

    recheck_candidates: List[Dict[str, Any]] = []
    for row in (review_doc.get("rows") or []):
        if not isinstance(row, dict):
            continue
        bucket = str(row.get("review_bucket") or "")
        if bucket not in {"RECHECK_MISSED_MOVE_CANDIDATE", "WATCH_MISSED_MOVE_CANDIDATE", "BLOCKED_MOVED_REVIEW"}:
            continue
        code = _clean_code(row.get("code") or "")
        if not code:
            continue
        resolved_name = str(row.get("name") or "").strip() or _name_lookup.get(code, "")
        recheck_candidates.append({
            "code": code,
            "name": resolved_name,
            "final_score": _f(row.get("final_score"), _f(row.get("queue_priority"), 0.0)),
            "sector_strength": None,
            "current_change_pct": _f(row.get("current_change_pct"), 0.0),
            "return_pct_since_first_seen": _f(row.get("return_pct_since_first_seen"), 0.0),
            "candidate_source": str(row.get("source") or "action_review"),
            "review_bucket": bucket,
        })

    # Dedup: merge sources by code; prefer recheck over news_only for priority
    seen_codes: Dict[str, Dict[str, Any]] = {}
    for c in news_only_candidates:
        seen_codes[c["code"]] = c
    for c in recheck_candidates:
        # recheck overrides news_only if same code (richer observation data)
        existing = seen_codes.get(c["code"])
        if existing is None or existing["review_bucket"] == "NEWS_ONLY":
            # Merge: keep review_bucket from recheck, final_score from news_only if available
            if existing and existing["final_score"] > 0 and c["final_score"] <= 0:
                c["final_score"] = existing["final_score"]
            if existing and existing["sector_strength"] is not None and c["sector_strength"] is None:
                c["sector_strength"] = existing["sector_strength"]
            seen_codes[c["code"]] = c
        else:
            # Both are recheck-type: pick higher priority
            if c["return_pct_since_first_seen"] > existing["return_pct_since_first_seen"]:
                seen_codes[c["code"]] = c

    candidates = sorted(seen_codes.values(), key=lambda x: -x.get("return_pct_since_first_seen", 0.0))

    # ---- Run shadow simulation ----
    results: List[Dict[str, Any]] = []
    for cand in candidates:
        result = _simulate_promotion(
            code=cand["code"],
            name=cand["name"],
            final_score=cand["final_score"],
            sector_strength=cand["sector_strength"],
            candidate_source=cand["candidate_source"],
            review_bucket=cand["review_bucket"],
            return_pct_since_first_seen=cand["return_pct_since_first_seen"],
            current_change_pct=cand["current_change_pct"],
            today_buys=today_buys,
            p1_entry_ready=p1_entry_ready,
            p1_max_new=p1_max_new,
            p1_max_new_surge=p1_max_new_surge,
            p1_max_new_zero_reason=p1_max_new_zero_reason,
            market_regime=market_regime,
            morning_strength_min=morning_strength_min,
            surge_exclusions=surge_exclusions,
        )
        results.append(result)

    # ---- Summary ----
    pass_count = sum(1 for r in results if r["shadow_pass"])
    block_count = len(results) - pass_count

    # Tabulate which check gates block most often
    block_freq: Dict[str, int] = {}
    for r in results:
        for b in r["shadow_blocked_reasons"]:
            key = b.split(":")[0]
            block_freq[key] = block_freq.get(key, 0) + 1

    payload = {
        "generated_at": generated_at,
        "today_ymd": today_ymd,
        "schema_version": "shadow_promotion_report_v1",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "purpose": (
            "Counterfactual: if execution_pool=True and sector_entry_allowed=True were injected, "
            "would remaining gate stack pass or block each candidate?"
        ),
        "summary": {
            "total_candidates": len(results),
            "news_only_inputs": len(news_only_candidates),
            "recheck_inputs": len(recheck_candidates),
            "shadow_pass": pass_count,
            "shadow_block": block_count,
            "block_frequency": dict(sorted(block_freq.items(), key=lambda x: -x[1])),
        },
        "gate_context": {
            "p1_entry_ready": p1_entry_ready,
            "p1_max_new": p1_max_new,
            "p1_max_new_surge": p1_max_new_surge,
            "p1_max_new_zero_reason": p1_max_new_zero_reason,
            "market_regime": market_regime,
            "morning_strength_min": morning_strength_min,
            "today_buy_codes": sorted(today_buys),
            "surge_excluded_codes": sorted(surge_exclusions.keys()),
        },
        "artifacts": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
            "history": str(HISTORY_CSV),
        },
        "rows": results,
    }

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # Write latest CSV
    fields = [
        "generated_at", "code", "name", "candidate_source", "review_bucket",
        "final_score", "sector_strength", "current_change_pct", "return_pct_since_first_seen",
        "shadow_pass", "shadow_observation",
        "shadow_passed_checks", "shadow_blocked_reasons",
    ]
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in results:
            row_flat = dict(row)
            row_flat["shadow_passed_checks"] = "|".join(row.get("shadow_passed_checks") or [])
            row_flat["shadow_blocked_reasons"] = "|".join(row.get("shadow_blocked_reasons") or [])
            writer.writerow({k: row_flat.get(k, "") for k in fields})

    # Append to history
    history_is_new = not HISTORY_CSV.exists()
    with HISTORY_CSV.open("a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if history_is_new:
            writer.writeheader()
        for row in results:
            row_flat = dict(row)
            row_flat["shadow_passed_checks"] = "|".join(row.get("shadow_passed_checks") or [])
            row_flat["shadow_blocked_reasons"] = "|".join(row.get("shadow_blocked_reasons") or [])
            writer.writerow({k: row_flat.get(k, "") for k in fields})

    print(json.dumps({
        "status": "OK",
        "total": len(results),
        "shadow_pass": pass_count,
        "shadow_block": block_count,
        "block_frequency": payload["summary"]["block_frequency"],
        "out_json": str(OUT_JSON),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
