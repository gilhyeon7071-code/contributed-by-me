from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
OUT_JSON = LOG_DIR / "buy_entry_quality_fallback_disable_validation_20260612_1417.json"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text.zfill(6)[-6:] if text else ""


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value or "").strip()
        return float(text) if text else float(default)
    except Exception:
        return float(default)


def _truthy(value: Any) -> bool:
    return str(value or "").strip().upper() in {"TRUE", "1", "Y", "YES", "T"}


def main() -> int:
    cfg = _read_json(ROOT / "paper" / "paper_engine_config.json")
    pol = cfg.get("positive_entry_criteria", {}) if isinstance(cfg.get("positive_entry_criteria"), dict) else {}
    fallback = pol.get("fresh_sector_allowed_fallback", {}) if isinstance(pol.get("fresh_sector_allowed_fallback"), dict) else {}
    timing = _read_json(LOG_DIR / "buy_sell_timing_check_20260610_20260612_20260612_1412.json")
    timing_by_code = {_code(row.get("code")): row for row in timing.get("rows", []) if isinstance(row, dict)}

    target_codes = {"087010", "328380", "058970", "320000", "403870", "037440"}
    rows: list[dict[str, Any]] = []
    with (LOG_DIR / "candidates_latest_data.with_final_score.csv").open("r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            code = _code(row.get("code"))
            if code not in target_codes:
                continue
            final_score = _f(row.get("final_score") or row.get("score"))
            strength = _f(row.get("sector_strength"))
            action = str(row.get("sector_action") or "").strip().upper()
            origin = str(row.get("candidate_origin") or "").strip().upper()
            sector_allowed = _truthy(row.get("sector_entry_allowed"))
            execution_pool = _truthy(row.get("execution_pool"))
            union_min = _f(cfg.get("union_entry_strength_min"), 0.65)
            original_path_ok = bool(
                final_score > _f(pol.get("min_score"))
                and sector_allowed
                and (
                    execution_pool
                    or (
                        origin == "SECTOR_PREFILTER_UNION"
                        and action == "BUY"
                        and strength >= union_min
                    )
                )
            )
            actions_raw = fallback.get("allowed_sector_actions", ["BUY", "WAIT"])
            if not isinstance(actions_raw, list):
                actions_raw = ["BUY", "WAIT"]
            allowed_actions = {str(x).strip().upper() for x in actions_raw if str(x).strip()}
            fallback_origin = str(fallback.get("candidate_origin", "SECTOR_PREFILTER_UNION") or "").strip().upper()
            fallback_would_match = bool(
                sector_allowed
                and origin == fallback_origin
                and action in allowed_actions
                and strength >= _f(fallback.get("min_sector_strength"), 0.4)
                and final_score >= _f(fallback.get("min_final_score"), 0.1)
            )
            timing_row = timing_by_code.get(code, {})
            rows.append(
                {
                    "code": code,
                    "name": row.get("name", ""),
                    "final_score": final_score,
                    "sector_strength": strength,
                    "sector_action": action,
                    "candidate_origin": origin,
                    "execution_pool": execution_pool,
                    "sector_entry_allowed": sector_allowed,
                    "original_path_ok": original_path_ok,
                    "fallback_enabled_now": bool(fallback.get("enabled", False)),
                    "fallback_would_have_matched_if_enabled": fallback_would_match,
                    "post_fix_entry_eligible": bool(original_path_ok or (bool(fallback.get("enabled", False)) and fallback_would_match)),
                    "buy_verdict": timing_row.get("buy_verdict", ""),
                    "outcome": timing_row.get("outcome", ""),
                    "gross_pnl_krw": timing_row.get("gross_pnl_krw"),
                    "open_max_close_markout_pct": timing_row.get("open_max_close_markout_pct"),
                }
            )

    payload = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "scope": "buy_entry_quality_fresh_sector_fallback_disable_validation",
        "policy_change_applied": True,
        "orders_modified": False,
        "fills_modified": False,
        "config_change": "positive_entry_criteria.fresh_sector_allowed_fallback.enabled=false",
        "evidence": {
            "config": str(ROOT / "paper" / "paper_engine_config.json"),
            "lock": str(ROOT / "paper" / "paper_engine_config.lock.json"),
            "change_log": str(LOG_DIR / "paper_engine_config.change_20260612_141623.json"),
            "candidates": str(LOG_DIR / "candidates_latest_data.with_final_score.csv"),
            "timing": str(LOG_DIR / "buy_sell_timing_check_20260610_20260612_20260612_1412.json"),
        },
        "summary": {
            "fallback_enabled_now": bool(fallback.get("enabled", False)),
            "post_fix_eligible_target_codes": [r["code"] for r in rows if r["post_fix_entry_eligible"]],
            "blocked_by_disable_codes": [
                r["code"]
                for r in rows
                if not r["original_path_ok"] and r["fallback_would_have_matched_if_enabled"] and not r["post_fix_entry_eligible"]
            ],
            "still_eligible_original_path_codes": [r["code"] for r in rows if r["original_path_ok"]],
        },
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": "OK", "out_json": str(OUT_JSON), "summary": payload["summary"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
