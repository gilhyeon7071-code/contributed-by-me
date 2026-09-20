from __future__ import annotations

import csv
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
IMPACT_CSV = LOGS / "news_topic_candidate_impact_latest.csv"
FINAL_CANDIDATES_CSV = LOGS / "candidates_latest_data.with_final_score.csv"
SCHEMA_JSON = LOGS / "news_judgment_l3_schema_definition_latest.json"
OUT_CSV = LOGS / "news_judgment_l3_layer_latest.csv"
OUT_JSON = LOGS / "news_judgment_l3_layer_status_latest.json"
OVERRIDES_JSONL = ROOT / "news_trading" / "data" / "news_l3_manual_overrides.jsonl"
KST = timezone(timedelta(hours=9))


def _now() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as fp:
                return [dict(row) for row in csv.DictReader(fp)]
        except UnicodeDecodeError:
            continue
    with path.open("r", newline="") as fp:
        return [dict(row) for row in csv.DictReader(fp)]


def _norm_code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    if not text:
        return ""
    return text.zfill(6) if len(text) <= 6 else text[-6:]


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _load_overrides() -> Dict[str, Dict[str, str]]:
    """news_l3_manual_overrides.jsonl → {(code, topic_key): override_dict}"""
    if not OVERRIDES_JSONL.exists():
        return {}
    result: Dict[str, Dict[str, str]] = {}
    for line in OVERRIDES_JSONL.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        code = _norm_code(entry.get("code", ""))
        topic_key = str(entry.get("topic_key") or "").strip()
        if not code or not topic_key:
            continue
        key = f"{code}|{topic_key}"
        # 같은 키가 여러 줄이면 마지막 줄이 우선 (최신 override 적용)
        result[key] = entry
    return result


def _execution_effect(candidate_effect: str, allowed: bool) -> str:
    if not allowed:
        return ""
    effect = str(candidate_effect or "").strip()
    if effect in {"block_review", "avoid_chase"}:
        return "block_order"
    if effect == "reduce_size_context":
        return "reduce_size"
    return ""


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "judgment_id",
        "generated_at",
        "code",
        "name",
        "topic_key",
        "topic_label",
        "candidate_effect",
        "l3_execution_effect",
        "confirm_level",
        "avg_confidence",
        "active_for_l3",
        "source_trading_effect",
        "execution_allowed",
        "execution_denied_reason",
        "reviewed_execution_allowed",
        "reviewer",
        "reviewed_at",
        "override_reason",
        "promoted_from_scope",
        "promotion_reason",
        "consumer",
        "processing_state",
        "input_trading_effect",
        "auto_trading_effect",
        "direct_candidate_allowed",
        "in_candidate_universe",
        "candidate_actions",
        "representative_titles",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def main() -> int:
    schema = _read_json(SCHEMA_JSON)
    rule = dict(schema.get("automatic_l3_rule") or {})
    allowed_effects = set(rule.get("candidate_effect_in") or ["block_review", "avoid_chase", "reduce_size_context"])
    min_confirm = int(rule.get("min_confirm_level") or 2)
    min_conf = float(rule.get("min_confidence") or 0.60)

    # 자동 source_trading_effect=True 조건 (수동 override와 독립)
    # boost/positive_context는 제외 — 차단·축소 방향만 자동 허용
    auto_te_rule = dict(schema.get("auto_trading_effect_rule") or {})
    auto_te_effects = set(auto_te_rule.get("candidate_effect_in") or ["block_review", "reduce_size_context"])
    auto_te_min_confirm = int(auto_te_rule.get("min_confirm_level") or 3)
    auto_te_min_conf = float(auto_te_rule.get("min_confidence") or 0.65)

    overrides = _load_overrides()
    impact_rows = _read_csv(IMPACT_CSV)
    candidate_rows = _read_csv(FINAL_CANDIDATES_CSV)
    candidate_codes = {_norm_code(row.get("code")) for row in candidate_rows if _norm_code(row.get("code"))}

    now = _now()
    out_rows: List[Dict[str, Any]] = []
    denied_counts: Dict[str, int] = {}
    effect_counts: Dict[str, int] = {}
    allowed_count = 0
    override_applied_count = 0
    for idx, row in enumerate(impact_rows, start=1):
        code = _norm_code(row.get("code"))
        topic_key = str(row.get("topic_key") or "").strip()
        effect = str(row.get("candidate_effect") or "").strip()
        confirm = _to_int(row.get("confirm_level"), 0)
        conf = _to_float(row.get("avg_confidence"), 0.0)
        input_trading_effect = _truthy(row.get("trading_effect"))
        direct_candidate_allowed = _truthy(row.get("direct_candidate_allowed"))
        in_candidate = code in candidate_codes

        # 자동 trading_effect 판정: 차단·축소 방향이고 조건 충족 시 True
        auto_trading_effect = (
            effect in auto_te_effects
            and confirm >= auto_te_min_confirm
            and conf >= auto_te_min_conf
            and in_candidate
        )
        effective_trading_effect = input_trading_effect or auto_trading_effect

        # 수동 override 우선 적용 (자동 생성 impact CSV의 빈 값을 덮어씀)
        override = overrides.get(f"{code}|{topic_key}", {})
        if override:
            override_applied_count += 1
        reviewed_allowed = _truthy(override.get("reviewed_execution_allowed") or row.get("reviewed_execution_allowed"))
        reviewer = str(override.get("reviewer") or row.get("reviewer") or "").strip()
        reviewed_at = str(override.get("reviewed_at") or row.get("reviewed_at") or "").strip()
        override_reason = str(override.get("override_reason") or row.get("override_reason") or "").strip()
        active_for_l3 = True

        denied: List[str] = []
        if effect not in allowed_effects:
            denied.append("candidate_effect_not_l3")
        if confirm < min_confirm:
            denied.append("confirm_level_below_min")
        if conf < min_conf:
            denied.append("avg_confidence_below_min")
        if not active_for_l3:
            denied.append("inactive_for_l3")
        if not in_candidate:
            denied.append("not_in_candidate_universe")
        if not effective_trading_effect:
            denied.append("source_trading_effect_false")
        if reviewed_allowed and (not reviewer or not reviewed_at or not override_reason):
            denied.append("review_override_audit_missing")

        auto_allowed = not denied
        execution_allowed = bool(auto_allowed or (reviewed_allowed and "review_override_audit_missing" not in denied))
        if execution_allowed:
            allowed_count += 1
        for reason in denied:
            denied_counts[reason] = denied_counts.get(reason, 0) + 1
        if effect:
            effect_counts[effect] = effect_counts.get(effect, 0) + 1

        out_rows.append(
            {
                "judgment_id": f"L3_{str(row.get('run_id') or '').strip() or 'run'}_{idx:04d}_{code}",
                "generated_at": now,
                "code": code,
                "name": str(row.get("name") or "").strip(),
                "topic_key": str(row.get("topic_key") or "").strip(),
                "topic_label": str(row.get("topic_label") or "").strip(),
                "candidate_effect": effect,
                "l3_execution_effect": _execution_effect(effect, execution_allowed),
                "confirm_level": str(confirm),
                "avg_confidence": f"{conf:.6f}",
                "active_for_l3": str(active_for_l3),
                "source_trading_effect": str(effective_trading_effect),
                "execution_allowed": str(execution_allowed),
                "execution_denied_reason": "|".join(denied),
                "reviewed_execution_allowed": str(reviewed_allowed),
                "reviewer": reviewer,
                "reviewed_at": reviewed_at,
                "override_reason": override_reason,
                "promoted_from_scope": str(row.get("promoted_from_scope") or "").strip(),
                "promotion_reason": str(row.get("promotion_reason") or "").strip(),
                "consumer": "candidate",
                "processing_state": "normalized" if not execution_allowed else "promoted",
                "input_trading_effect": str(input_trading_effect),
                "auto_trading_effect": str(auto_trading_effect),
                "direct_candidate_allowed": str(direct_candidate_allowed),
                "in_candidate_universe": str(in_candidate),
                "candidate_actions": str(row.get("candidate_actions") or "").strip(),
                "representative_titles": str(row.get("representative_titles") or "").strip()[:250],
            }
        )

    _write_csv(OUT_CSV, out_rows)
    payload = {
        "generated_at": now,
        "status": "PASS",
        "reason": "ok_no_l3_allowed" if allowed_count == 0 else "ok",
        "input": str(IMPACT_CSV),
        "output": str(OUT_CSV),
        "rows_in": len(impact_rows),
        "rows_out": len(out_rows),
        "execution_allowed_rows": int(allowed_count),
        "manual_override_applied": int(override_applied_count),
        "manual_overrides_loaded": len(overrides),
        "auto_trading_effect_rows": sum(1 for r in out_rows if r.get("auto_trading_effect") == "True"),
        "l3_execution_effect_counts": {
            key: sum(1 for row in out_rows if row.get("l3_execution_effect") == key)
            for key in ["block_order", "reduce_size"]
        },
        "candidate_effect_counts": dict(sorted(effect_counts.items())),
        "denied_reason_counts": dict(sorted(denied_counts.items())),
        "policy": {
            "min_confirm_level": min_confirm,
            "min_confidence": min_conf,
            "source_trading_effect_required": True,
            "review_override_requires_audit_fields": True,
            "auto_trading_effect_rule": {
                "candidate_effect_in": sorted(auto_te_effects),
                "min_confirm_level": auto_te_min_confirm,
                "min_confidence": auto_te_min_conf,
                "in_candidate_universe_required": True,
            },
        },
        "trading_effect": False,
        "live_broker_effect": False,
        "gate_effect": False,
        "score_effect": False,
        "risk_lock_effect": False,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": payload["status"], "reason": payload["reason"], "execution_allowed_rows": allowed_count, "output": str(OUT_CSV)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
