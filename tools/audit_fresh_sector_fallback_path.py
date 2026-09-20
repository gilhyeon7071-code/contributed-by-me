"""Audit whether fresh sector-allowed fallback behaved as an intended path.

Read-only. This script does not change candidates, gates, risk, orders, fills,
ledger, or policy values. It checks the current config, latest candidate rows,
runtime log evidence, and fills for the 2026-05-28 fresh sector fallback path.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER_DIR = ROOT / "paper"

CONFIG_JSON = PAPER_DIR / "paper_engine_config.json"
CANDIDATES_CSV = LOG_DIR / "candidates_latest_data.with_sector_score.csv"
FILLS_CSV = PAPER_DIR / "fills.csv"
RUN_LOG = LOG_DIR / "run_paper_daily_last.txt"
PLANS_MD = ROOT / ".agent" / "PLANS.md"

OUT_JSON = LOG_DIR / "fresh_sector_fallback_path_audit_latest.json"
OUT_CSV = LOG_DIR / "fresh_sector_fallback_path_audit_latest.csv"

TARGET_CODES = ["476060", "058970"]


def _now_kst() -> str:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).isoformat(timespec="seconds")


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [{str(k): str(v) for k, v in row.items()} for row in csv.DictReader(f)]


def _code(value: Any) -> str:
    raw = "".join(ch for ch in str(value or "") if ch.isdigit())
    return raw.zfill(6)[-6:] if raw else ""


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value or "").strip()
        if not text:
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _truthy(value: Any) -> bool:
    return str(value or "").strip().upper() in {"TRUE", "1", "Y", "YES", "T"}


def _latest_buy_fills(rows: Iterable[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code not in TARGET_CODES or str(row.get("side") or "").upper() != "BUY":
            continue
        out[code] = row
    return out


def _log_evidence(text: str) -> Dict[str, Any]:
    lines = text.splitlines()
    hits = [line for line in lines if "fresh_sector_allowed_fallback" in line]
    selected = [line for line in lines if "picked=058970" in line or "picked=476060" in line]
    fill_lines = [line for line in lines if "PAPER_BUY_476060_20260528" in line or "PAPER_BUY_058970_20260528" in line]
    return {
        "fallback_log_lines": hits,
        "selection_log_lines": selected,
        "fill_log_lines": fill_lines,
        "fallback_added_count": sum(
            int(m.group(1))
            for line in hits
            for m in [re.search(r"added=(\d+)", line)]
            if m
        ),
    }


def _candidate_row(row: Dict[str, str], fallback_cfg: Dict[str, Any], union_min: float) -> Dict[str, Any]:
    code = _code(row.get("code"))
    score = _f(row.get("final_score") or row.get("score"))
    strength = _f(row.get("sector_strength"))
    action = str(row.get("sector_action") or "").strip().upper()
    origin = str(row.get("candidate_origin") or "").strip().upper()
    sector_allowed = _truthy(row.get("sector_entry_allowed"))
    execution_pool = _truthy(row.get("execution_pool"))
    original_union_ok = (
        origin == "SECTOR_PREFILTER_UNION"
        and action == "BUY"
        and sector_allowed
        and strength >= union_min
    )
    original_path_ok = bool(score > 0 and (execution_pool or original_union_ok) and sector_allowed)

    allowed_actions_raw = fallback_cfg.get("allowed_sector_actions", ["BUY", "WAIT"])
    if not isinstance(allowed_actions_raw, list):
        allowed_actions_raw = ["BUY", "WAIT"]
    allowed_actions = {str(x).strip().upper() for x in allowed_actions_raw if str(x).strip()}
    fallback_origin = str(fallback_cfg.get("candidate_origin", "SECTOR_PREFILTER_UNION") or "").strip().upper()
    fallback_score_min = _f(fallback_cfg.get("min_final_score"), 0.10)
    fallback_strength_min = _f(fallback_cfg.get("min_sector_strength"), 0.40)
    fallback_path_ok = (
        sector_allowed
        and action in allowed_actions
        and (not fallback_origin or origin == fallback_origin)
        and score >= fallback_score_min
        and strength >= fallback_strength_min
    )

    if fallback_path_ok and not original_path_ok:
        verdict = "FALLBACK_ONLY_PATH"
    elif original_path_ok:
        verdict = "ORIGINAL_POSITIVE_PATH"
    else:
        verdict = "NOT_ELIGIBLE"

    return {
        "code": code,
        "name": row.get("name", ""),
        "final_score": score,
        "sector_entry_allowed": sector_allowed,
        "sector_strength": strength,
        "sector_action": action,
        "candidate_origin": origin,
        "execution_pool": execution_pool,
        "original_path_ok": original_path_ok,
        "fallback_path_ok": fallback_path_ok,
        "verdict": verdict,
    }


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "code",
        "name",
        "final_score",
        "sector_entry_allowed",
        "sector_strength",
        "sector_action",
        "candidate_origin",
        "execution_pool",
        "original_path_ok",
        "fallback_path_ok",
        "verdict",
        "buy_fill_present",
        "buy_order_id",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    cfg = _read_json(CONFIG_JSON)
    pol = cfg.get("positive_entry_criteria") if isinstance(cfg.get("positive_entry_criteria"), dict) else {}
    fallback_cfg = pol.get("fresh_sector_allowed_fallback") if isinstance(pol.get("fresh_sector_allowed_fallback"), dict) else {}
    union_min = _f(pol.get("union_entry_strength_min"), 0.65)
    if union_min <= 0:
        union_min = 0.65

    candidates = _read_csv(CANDIDATES_CSV)
    fills = _latest_buy_fills(_read_csv(FILLS_CSV))
    log_text = RUN_LOG.read_text(encoding="utf-8", errors="replace") if RUN_LOG.exists() else ""
    plans_text = PLANS_MD.read_text(encoding="utf-8", errors="replace") if PLANS_MD.exists() else ""

    target_rows: List[Dict[str, Any]] = []
    for row in candidates:
        if _code(row.get("code")) not in TARGET_CODES:
            continue
        item = _candidate_row(row, fallback_cfg, union_min)
        fill = fills.get(item["code"], {})
        item["buy_fill_present"] = bool(fill)
        item["buy_order_id"] = fill.get("order_id", "")
        target_rows.append(item)

    fallback_only_buys = [
        r for r in target_rows
        if r.get("verdict") == "FALLBACK_ONLY_PATH" and r.get("buy_fill_present")
    ]
    log = _log_evidence(log_text)
    documented = "2026-05-28 Fresh Sector-Allowed Fallback - RootA" in plans_text
    config_enabled = bool(fallback_cfg.get("enabled", False))

    if config_enabled and documented and len(fallback_only_buys) >= 1 and log["fallback_added_count"] >= 1:
        verdict = "INTENDED_POLICY_PATH_WITH_REVIEW_NEEDED"
    elif len(fallback_only_buys) >= 1:
        verdict = "POSSIBLE_UNDOCUMENTED_BYPASS"
    else:
        verdict = "NO_FALLBACK_BUY_EVIDENCE"

    payload = {
        "generated_at": _now_kst(),
        "schema_version": "fresh_sector_fallback_path_audit_v1",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "generated_from": {
            "config": str(CONFIG_JSON),
            "candidates": str(CANDIDATES_CSV),
            "fills": str(FILLS_CSV),
            "run_log": str(RUN_LOG),
            "plans": str(PLANS_MD),
        },
        "summary": {
            "verdict": verdict,
            "config_enabled": config_enabled,
            "documented_in_plans": documented,
            "target_codes": TARGET_CODES,
            "target_candidate_rows": len(target_rows),
            "fallback_only_buy_rows": len(fallback_only_buys),
            "fallback_added_log_count": log["fallback_added_count"],
            "classification": "fallback_policy_path_not_baseline_sample" if "FALLBACK" in verdict or "POLICY_PATH" in verdict else "no_fallback_evidence",
        },
        "config": {
            "fresh_sector_allowed_fallback": fallback_cfg,
            "original_union_entry_strength_min_effective": union_min,
        },
        "log_evidence": log,
        "rows": target_rows,
        "artifacts": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
        },
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, target_rows)
    print(json.dumps({"status": "OK", **payload["summary"], "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
