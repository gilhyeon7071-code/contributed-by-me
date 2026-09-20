from __future__ import annotations

import csv
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
MANUAL_JSONL = ROOT / "news_trading" / "data" / "manual_news_implications.jsonl"
MANUAL_CSV = LOGS / "manual_news_implications_latest.csv"
TOPIC_CSV = LOGS / "news_topic_judgment_latest.csv"
IMPACT_CSV = LOGS / "news_topic_candidate_impact_latest.csv"
FINAL_CANDIDATES_CSV = LOGS / "candidates_latest_data.with_final_score.csv"
SCHEMA_JSON = LOGS / "news_judgment_l3_schema_definition_latest.json"
OUT_JSON = LOGS / "news_judgment_l3_schema_gap_audit_latest.json"
OUT_CSV = LOGS / "news_judgment_l3_schema_gap_audit_latest.csv"

KST = timezone(timedelta(hours=9))


def _now() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    for line_no, line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), start=1):
        text = line.strip()
        if not text:
            continue
        try:
            row = json.loads(text)
        except Exception as exc:
            rows.append({"_line_no": line_no, "_read_error": type(exc).__name__})
            continue
        if isinstance(row, dict):
            row["_line_no"] = line_no
            rows.append(row)
    return rows


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


def _fieldnames(rows: Iterable[Dict[str, Any]]) -> List[str]:
    keys = set()
    for row in rows:
        keys.update(str(k) for k in row.keys() if not str(k).startswith("_"))
    return sorted(keys)


def _norm_code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text.zfill(6) if len(text) <= 6 and text else text[-6:]


def _split_codes(value: Any) -> List[str]:
    if isinstance(value, list):
        parts = value
    else:
        text = str(value or "").replace("|", ",").replace(";", ",")
        parts = [x.strip() for x in text.split(",")]
    out: List[str] = []
    for item in parts:
        code = _norm_code(item)
        if len(code) == 6:
            out.append(code)
    return sorted(set(out))


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return default


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _layer_gap(layer: str, rows: List[Dict[str, Any]], required: List[str]) -> Dict[str, Any]:
    fields = _fieldnames(rows)
    present = set(fields)
    missing = [field for field in required if field not in present]
    return {
        "layer": layer,
        "rows": len(rows),
        "fields": fields,
        "missing_required_fields": missing,
        "missing_required_count": len(missing),
        "schema_ready": len(missing) == 0,
    }


def _audit_manual_rows(rows: List[Dict[str, Any]], required: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    l3_effect_actions = {"block", "reduce_size"}
    for row in rows:
        row_keys = set(str(k) for k in row.keys())
        missing = [field for field in required if field not in row_keys]
        codes = _split_codes(row.get("related_codes"))
        action = str(row.get("action") or "").strip().lower()
        scope = str(row.get("scope") or "").strip().lower()
        possible_l3_intent = bool(action in l3_effect_actions or "CONFIRM_LEVEL" in str(row.get("implication") or ""))
        denied = []
        if scope != "stock":
            denied.append("scope_not_stock_schema_uses_company_or_sector")
        if "candidate_effect" not in row_keys:
            denied.append("candidate_effect_missing")
        if "confirm_level" not in row_keys:
            denied.append("confirm_level_missing")
        if "source_trading_effect" not in row_keys:
            denied.append("source_trading_effect_missing")
        if "execution_allowed" not in row_keys:
            denied.append("execution_allowed_missing")
        if "reviewed_execution_allowed" not in row_keys:
            denied.append("reviewed_execution_allowed_missing")
        if not codes:
            denied.append("related_codes_missing")
        out.append(
            {
                "layer": "manual_jsonl",
                "row_id": f"manual:{row.get('_line_no', '')}",
                "code": "|".join(codes),
                "scope": scope,
                "action": action,
                "candidate_effect": str(row.get("candidate_effect") or ""),
                "confirm_level": str(row.get("confirm_level") or ""),
                "confidence": str(row.get("confidence") or ""),
                "possible_l3_intent": str(bool(possible_l3_intent)),
                "l3_schema_ready": str(False),
                "missing_required_count": str(len(missing)),
                "denied_reasons": "|".join(denied),
                "source_title": str(row.get("source_title") or "")[:180],
            }
        )
    return out


def _audit_impact_rows(rows: List[Dict[str, Any]], candidate_codes: set[str], rule: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    allowed_effects = set(rule.get("candidate_effect_in") or [])
    min_confirm = int(rule.get("min_confirm_level") or 2)
    min_conf = float(rule.get("min_confidence") or 0.6)
    for idx, row in enumerate(rows, start=1):
        code = _norm_code(row.get("code"))
        effect = str(row.get("candidate_effect") or "").strip()
        confirm = _to_int(row.get("confirm_level"), 0)
        confidence = _to_float(row.get("avg_confidence"), 0.0)
        trading_effect = _truthy(row.get("trading_effect"))
        direct_allowed = _truthy(row.get("direct_candidate_allowed"))
        in_candidate = code in candidate_codes
        denied = []
        if effect not in allowed_effects:
            denied.append("candidate_effect_not_l3")
        if confirm < min_confirm:
            denied.append("confirm_level_below_min")
        if confidence < min_conf:
            denied.append("avg_confidence_below_min")
        if not in_candidate:
            denied.append("not_in_candidate_universe")
        if not trading_effect:
            denied.append("source_trading_effect_false")
        execution_allowed = not denied
        out.append(
            {
                "layer": "candidate_impact",
                "row_id": f"impact:{idx}",
                "code": code,
                "scope": "stock",
                "action": str(row.get("candidate_actions") or ""),
                "candidate_effect": effect,
                "confirm_level": str(confirm),
                "confidence": f"{confidence:.6f}",
                "possible_l3_intent": str(effect in allowed_effects),
                "l3_schema_ready": str(False),
                "missing_required_count": "",
                "denied_reasons": "|".join(denied) if denied else "",
                "source_title": str(row.get("representative_titles") or "")[:180],
                "in_candidate_universe": str(in_candidate),
                "direct_candidate_allowed": str(direct_allowed),
                "source_trading_effect": str(trading_effect),
                "execution_allowed_by_defined_rule": str(execution_allowed),
            }
        )
    return out


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: List[str] = []
    for preferred in [
        "layer",
        "row_id",
        "code",
        "scope",
        "action",
        "candidate_effect",
        "confirm_level",
        "confidence",
        "possible_l3_intent",
        "l3_schema_ready",
        "execution_allowed_by_defined_rule",
        "denied_reasons",
        "source_title",
    ]:
        if preferred not in fields:
            fields.append(preferred)
    for row in rows:
        for key in row.keys():
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def main() -> int:
    schema = _read_json(SCHEMA_JSON)
    required = [str(x) for x in schema.get("required_l3_fields") or []]
    rule = dict(schema.get("automatic_l3_rule") or {})

    manual_jsonl = _read_jsonl(MANUAL_JSONL)
    manual_csv = _read_csv(MANUAL_CSV)
    topic_rows = _read_csv(TOPIC_CSV)
    impact_rows = _read_csv(IMPACT_CSV)
    final_rows = _read_csv(FINAL_CANDIDATES_CSV)
    candidate_codes = {_norm_code(row.get("code")) for row in final_rows if _norm_code(row.get("code"))}

    raw_source_gaps = [
        _layer_gap("manual_jsonl_raw_source", manual_jsonl, required),
    ]
    layer_gaps = [
        _layer_gap("manual_csv", manual_csv, required),
        _layer_gap("topic_judgment", topic_rows, required),
        _layer_gap("candidate_impact", impact_rows, required),
    ]
    row_audit = _audit_manual_rows(manual_csv, required) + _audit_impact_rows(impact_rows, candidate_codes, rule)
    possible_l3 = [row for row in row_audit if str(row.get("possible_l3_intent")).lower() == "true"]
    allowed_by_rule = [
        row for row in row_audit
        if str(row.get("execution_allowed_by_defined_rule")).lower() == "true"
    ]
    denied_counts: Dict[str, int] = {}
    for row in row_audit:
        for reason in str(row.get("denied_reasons") or "").split("|"):
            reason = reason.strip()
            if reason:
                denied_counts[reason] = denied_counts.get(reason, 0) + 1

    schema_ready_layers = sum(1 for item in layer_gaps if item.get("schema_ready"))
    status = "PASS" if schema_ready_layers == len(layer_gaps) else "FAIL"
    reason = "ok" if status == "PASS" else "normalized_layers_missing_l3_fields"

    payload = {
        "generated_at": _now(),
        "status": status,
        "reason": reason,
        "schema_definition": str(SCHEMA_JSON),
        "inputs": {
            "manual_jsonl": str(MANUAL_JSONL),
            "manual_csv": str(MANUAL_CSV),
            "topic_judgment": str(TOPIC_CSV),
            "candidate_impact": str(IMPACT_CSV),
            "final_candidates": str(FINAL_CANDIDATES_CSV),
        },
        "summary": {
            "manual_jsonl_rows": len(manual_jsonl),
            "manual_csv_rows": len(manual_csv),
            "topic_judgment_rows": len(topic_rows),
            "candidate_impact_rows": len(impact_rows),
            "final_candidate_rows": len(final_rows),
            "possible_l3_intent_rows": len(possible_l3),
            "execution_allowed_by_defined_rule_rows": len(allowed_by_rule),
            "schema_ready_layers": schema_ready_layers,
            "schema_ready_layer_total": len(layer_gaps),
        },
        "raw_source_gaps": raw_source_gaps,
        "layer_gaps": layer_gaps,
        "denied_reason_counts": dict(sorted(denied_counts.items())),
        "current_block_order_experiment_note": (
            "Topic and candidate-impact layers can now carry L3 schema fields, but automatic execution remains blocked "
            "because source_trading_effect/execution_allowed are false unless a reviewed L3 permission exists."
        ),
        "outputs": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
        },
        "trading_effect": False,
        "live_broker_effect": False,
        "gate_effect": False,
        "score_effect": False,
        "risk_lock_effect": False,
    }

    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, row_audit)
    print(json.dumps({"status": payload["status"], "summary": payload["summary"], "outputs": payload["outputs"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
