from __future__ import annotations

import csv
import json
import math
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER_DIR = ROOT / "paper"

CANDIDATE_PATH = LOG_DIR / "candidates_latest_data.with_final_score.csv"
PENDING_PATH = LOG_DIR / "pending_entry_status_latest.json"
CONFIG_PATH = PAPER_DIR / "paper_engine_config.json"
FILLS_PATH = PAPER_DIR / "fills.csv"
STATE_PATH = PAPER_DIR / "paper_state.json"

OUT_JSON = LOG_DIR / "entry_fallback_stage_whatif_latest.json"
OUT_CSV = LOG_DIR / "entry_fallback_stage_whatif_latest.csv"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    with path.open("r", newline="") as f:
        return list(csv.DictReader(f))


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except UnicodeDecodeError:
            continue
        except Exception:
            return {}
    return {}


def _write_csv(path: Path, rows: List[Dict[str, Any]], fields: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _ymd(value: Any) -> str:
    digits = re.sub(r"[^0-9]", "", str(value or ""))
    return digits[:8] if len(digits) >= 8 else ""


def _float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except Exception:
        return default


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _latest_buy_ymd(fills: List[Dict[str, str]]) -> str:
    buys = [_ymd(r.get("datetime")) for r in fills if str(r.get("side") or "").strip().upper() == "BUY"]
    buys = [x for x in buys if len(x) == 8]
    if buys:
        return max(buys)
    all_dates = [_ymd(r.get("datetime")) for r in fills]
    all_dates = [x for x in all_dates if len(x) == 8]
    return max(all_dates) if all_dates else ""


def _open_codes(state: Dict[str, Any]) -> set[str]:
    rows = state.get("open_positions")
    if not isinstance(rows, list):
        return set()
    out = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = str(row.get("code") or "").zfill(6)
        if code.strip("0"):
            out.add(code)
    return out


def _buy_codes_on_ymd(fills: List[Dict[str, str]], ymd: str) -> set[str]:
    out = set()
    for row in fills:
        if str(row.get("side") or "").strip().upper() != "BUY":
            continue
        if _ymd(row.get("datetime")) != ymd:
            continue
        code = str(row.get("code") or "").zfill(6)
        if code.strip("0"):
            out.add(code)
    return out


def _date_age_days(signal_ymd: str, ref_ymd: str) -> int | None:
    if len(signal_ymd) != 8 or len(ref_ymd) != 8:
        return None
    try:
        a = datetime.strptime(signal_ymd, "%Y%m%d")
        b = datetime.strptime(ref_ymd, "%Y%m%d")
        return (b - a).days
    except Exception:
        return None


def _positive_like(row: Dict[str, str]) -> tuple[bool, str]:
    score = _float(row.get("final_score") or row.get("score"), 0.0)
    execution_pool = _truthy(row.get("execution_pool"))
    sector_allowed = _truthy(row.get("sector_entry_allowed")) if "sector_entry_allowed" in row else True
    origin = str(row.get("candidate_origin") or "").strip().upper()
    sector_action = str(row.get("sector_action") or "").strip().upper()
    sector_strength = _float(row.get("sector_strength"), 0.0)
    sector_union = origin == "SECTOR_PREFILTER_UNION" and sector_action == "BUY" and sector_allowed and sector_strength >= 0.65
    ok = score > 0 and (execution_pool or sector_union) and sector_allowed
    reason = []
    if score <= 0:
        reason.append("score_not_positive")
    if not (execution_pool or sector_union):
        reason.append("execution_pool_false")
    if not sector_allowed:
        reason.append("sector_entry_not_allowed")
    if ok:
        reason.append("positive_like_ok")
    return ok, ",".join(reason)


def main() -> int:
    cfg = _read_json(CONFIG_PATH)
    candidates = _read_csv(CANDIDATE_PATH)
    pending = _read_json(PENDING_PATH)
    fills = _read_csv(FILLS_PATH)
    state = _read_json(STATE_PATH)

    d_ref = str(pending.get("d_ref") or pending.get("as_of_ymd") or "")[:8]
    if len(d_ref) != 8:
        d_ref = _latest_buy_ymd(fills)

    mop = cfg.get("market_ops_policy") if isinstance(cfg.get("market_ops_policy"), dict) else {}
    fallback = mop.get("entry_fallback_policy") if isinstance(mop.get("entry_fallback_policy"), dict) else {}
    current_max_stage = int(fallback.get("max_stage", 0) or 0)
    signal_valid_days = int(fallback.get("signal_valid_days", 1) or 1)
    strict_same_day_only = bool(mop.get("strict_same_day_only", False))

    open_codes = _open_codes(state)
    buy_today_codes = _buy_codes_on_ymd(fills, d_ref)

    rows: List[Dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for row in candidates:
        code = str(row.get("code") or "").zfill(6)
        signal_ymd = _ymd(row.get("date_yyyymmdd") or row.get("date"))
        age_days = _date_age_days(signal_ymd, d_ref)
        positive_ok, positive_reason = _positive_like(row)
        score = _float(row.get("final_score") or row.get("score"), 0.0)
        already_open = code in open_codes
        already_bought = code in buy_today_codes
        stage1_candidate = bool(positive_ok and not already_open and not already_bought)
        expiry_ok = bool(age_days is not None and age_days <= signal_valid_days)
        stage1_current_policy_allowed = bool(stage1_candidate and expiry_ok and (not strict_same_day_only) and current_max_stage >= 1)
        stage2_current_policy_allowed = bool(stage1_candidate and expiry_ok and (not strict_same_day_only) and current_max_stage >= 2)
        shadow_state = "SHADOW_STAGE1_2_CANDIDATE" if stage1_candidate else "NOT_STAGE_FALLBACK_CANDIDATE"
        candidate_blockers: List[str] = []
        if not positive_ok:
            candidate_blockers.append(positive_reason or "positive_like_fail")
        if already_open:
            candidate_blockers.append("already_open")
        if already_bought:
            candidate_blockers.append("already_bought_on_d_ref")
        if not stage1_candidate:
            block_reason = ",".join(candidate_blockers) or "not_candidate"
        elif not expiry_ok:
            block_reason = f"signal_expired(age_days={age_days},valid_days={signal_valid_days})"
        elif strict_same_day_only:
            block_reason = "strict_same_day_only"
        elif current_max_stage < 1:
            block_reason = f"max_stage={current_max_stage}"
        else:
            block_reason = "would_be_allowed_under_current_stage_policy"
        counts[shadow_state] += 1
        if stage1_current_policy_allowed:
            counts["current_policy_stage1_allowed"] += 1
        if stage2_current_policy_allowed:
            counts["current_policy_stage2_allowed"] += 1
        if stage1_candidate and not expiry_ok:
            counts["expired_stage_candidate"] += 1
        rows.append(
            {
                "generated_at": "",
                "d_ref": d_ref,
                "code": code,
                "name": row.get("name", ""),
                "candidate_date": signal_ymd,
                "age_days": "" if age_days is None else age_days,
                "final_score": score,
                "horizon_label": row.get("horizon_label", ""),
                "execution_pool": row.get("execution_pool", ""),
                "sector_entry_allowed": row.get("sector_entry_allowed", ""),
                "sector_strength": row.get("sector_strength", ""),
                "positive_like_ok": positive_ok,
                "positive_like_reason": positive_reason,
                "already_open": already_open,
                "already_bought_on_d_ref": already_bought,
                "shadow_state": shadow_state,
                "whatif_stage1_current_policy_allowed": stage1_current_policy_allowed,
                "whatif_stage2_current_policy_allowed": stage2_current_policy_allowed,
                "whatif_block_reason": block_reason,
                "trading_effect": "false",
                "policy_effect": "false",
                "policy_change_applied": "false",
            }
        )

    generated_at = _now_ts()
    for row in rows:
        row["generated_at"] = generated_at

    out = {
        "generated_at": generated_at,
        "status": "PASS",
        "d_ref": d_ref,
        "source_files": {
            "candidates": str(CANDIDATE_PATH),
            "pending": str(PENDING_PATH),
            "config": str(CONFIG_PATH),
            "fills": str(FILLS_PATH),
            "state": str(STATE_PATH),
        },
        "current_policy": {
            "entry_fallback_policy.enabled": bool(fallback.get("enabled", False)),
            "entry_fallback_policy.max_stage": current_max_stage,
            "entry_fallback_policy.signal_valid_days": signal_valid_days,
            "strict_same_day_only": strict_same_day_only,
        },
        "candidate_rows": len(candidates),
        "whatif_rows": len(rows),
        "counts": dict(counts),
        "current_policy_stage1_allowed": int(counts.get("current_policy_stage1_allowed", 0)),
        "current_policy_stage2_allowed": int(counts.get("current_policy_stage2_allowed", 0)),
        "interpretation": (
            "stage 1/2 fallback remains shadow-only; current policy does not allow stage 1/2 when "
            "strict_same_day_only is true or max_stage is below the requested stage"
        ),
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    fields = [
        "generated_at",
        "d_ref",
        "code",
        "name",
        "candidate_date",
        "age_days",
        "final_score",
        "horizon_label",
        "execution_pool",
        "sector_entry_allowed",
        "sector_strength",
        "positive_like_ok",
        "positive_like_reason",
        "already_open",
        "already_bought_on_d_ref",
        "shadow_state",
        "whatif_stage1_current_policy_allowed",
        "whatif_stage2_current_policy_allowed",
        "whatif_block_reason",
        "trading_effect",
        "policy_effect",
        "policy_change_applied",
    ]
    _write_csv(OUT_CSV, rows, fields)
    print(
        f"[FINAL] entry fallback stage what-if -> {OUT_JSON} "
        f"candidates={len(candidates)} stage1_allowed={out['current_policy_stage1_allowed']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
