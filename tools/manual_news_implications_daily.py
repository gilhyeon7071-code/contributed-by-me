from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "news_trading" / "data"
LOGS = ROOT / "2_Logs"
IN_PATH = DATA / "manual_news_implications.jsonl"
OUT_CSV = LOGS / "manual_news_implications_latest.csv"
STATUS_LATEST = LOGS / "manual_news_implications_status_latest.json"
KST = timezone(timedelta(hours=9))

ALLOWED_SCOPES = {"market", "sector", "company", "macro"}
ALLOWED_DIRECTIONS = {"positive", "negative", "neutral"}
ALLOWED_ACTIONS = {"boost", "watch", "reduce_size", "penalize", "block", "ignore"}
ALLOWED_HORIZONS = {"intraday", "short", "medium"}
FIELDS = [
    "judgment_id",
    "asof_ymd",
    "source_url",
    "source_title",
    "scope",
    "target",
    "sector_tag",
    "related_codes",
    "time_axis",
    "horizon",
    "direction",
    "action",
    "candidate_effect",
    "strength",
    "confidence",
    "confirm_level",
    "implication",
    "active_for_l3",
    "source_trading_effect",
    "execution_allowed",
    "execution_denied_reason",
    "reviewed_execution_allowed",
    "consumer",
    "processing_state",
    "applies_to_existing_only",
    "direct_candidate_allowed",
    "reviewer",
    "reviewed_at",
]


def _now_kst() -> datetime:
    return datetime.now(KST)


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _stable_id(*parts: Any) -> str:
    text = "|".join(_as_text(part) for part in parts)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    text = _as_text(value).lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off"}:
        return False
    return bool(default)


def _norm_codes(value: Any) -> str:
    if isinstance(value, list):
        raw = value
    else:
        raw = [x for x in _as_text(value).replace(",", "|").split("|") if x.strip()]
    out: List[str] = []
    for item in raw:
        code = "".join(ch for ch in _as_text(item) if ch.isdigit())
        if code:
            out.append(code.zfill(6)[-6:])
    return "|".join(dict.fromkeys(out))


def _candidate_effect(action: str, direction: str, implication: str) -> str:
    text = implication.lower()
    if action == "block":
        return "block_review"
    if action == "reduce_size":
        return "reduce_size_context"
    if action == "penalize":
        return "avoid_chase"
    if "추격" in implication or "fomo" in text or "과열" in implication or "급락" in implication:
        return "avoid_chase"
    if action == "boost" or direction == "positive":
        return "positive_context"
    if action == "watch":
        return "watch_only"
    return "none"


def _confirm_level(row: Dict[str, Any], implication: str) -> int:
    explicit = row.get("confirm_level")
    try:
        parsed = int(float(str(explicit).strip()))
        if parsed in {1, 2, 3}:
            return parsed
    except Exception:
        pass
    text = implication.lower()
    if "confirm_level 2→3" in text or "confirm_level=3" in text or "실제 개최" in implication or "공시" in implication:
        return 3
    if "confirm_level=2" in text or "정책 방향" in implication or "pass" in text:
        return 2
    return 1


def _target(scope: str, sector_tag: str, related_codes: str) -> str:
    if scope == "company" and related_codes:
        return related_codes.split("|")[0]
    if sector_tag:
        return sector_tag
    return scope or "unknown"


def _execution_denied_reason(
    *,
    scope: str,
    effect: str,
    confirm_level: int,
    confidence: float,
    source_trading_effect: bool,
) -> str:
    reasons: List[str] = []
    if scope != "company":
        reasons.append("scope_not_stock")
    if effect not in {"block_review", "avoid_chase", "reduce_size_context"}:
        reasons.append("candidate_effect_not_l3")
    if confirm_level < 2:
        reasons.append("confirm_level_below_min")
    if confidence < 0.60:
        reasons.append("confidence_below_min")
    if not source_trading_effect:
        reasons.append("source_trading_effect_false")
    return "|".join(reasons) if reasons else "execution_allowed_false"


def _read_jsonl(path: Path) -> Tuple[List[Dict[str, Any]], List[str]]:
    errors: List[str] = []
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows, ["input_missing"]
    for line_no, line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), start=1):
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        try:
            obj = json.loads(text)
        except Exception as exc:
            errors.append(f"line_{line_no}:json_error:{type(exc).__name__}")
            continue
        if not isinstance(obj, dict):
            errors.append(f"line_{line_no}:not_object")
            continue
        obj["_line_no"] = line_no
        rows.append(obj)
    return rows, errors


def _validate_row(row: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    errors: List[str] = []
    asof_ymd = "".join(ch for ch in _as_text(row.get("asof_ymd")) if ch.isdigit())[:8]
    if len(asof_ymd) != 8:
        errors.append("asof_ymd_invalid")

    source_url = _as_text(row.get("source_url") or row.get("url"))
    if not source_url:
        errors.append("source_url_missing")
    source_title = _as_text(row.get("source_title") or row.get("title"))

    scope = _as_text(row.get("scope")).lower()
    direction = _as_text(row.get("direction")).lower()
    action = _as_text(row.get("action")).lower()
    horizon = _as_text(row.get("horizon") or "short").lower()
    if scope not in ALLOWED_SCOPES:
        errors.append("scope_invalid")
    if direction not in ALLOWED_DIRECTIONS:
        errors.append("direction_invalid")
    if action not in ALLOWED_ACTIONS:
        errors.append("action_invalid")
    if horizon not in ALLOWED_HORIZONS:
        errors.append("horizon_invalid")

    strength = max(0.0, min(1.0, _as_float(row.get("strength"), 0.0)))
    confidence = max(0.0, min(1.0, _as_float(row.get("confidence"), 0.0)))
    applies_to_existing_only = _as_bool(row.get("applies_to_existing_only"), True)
    direct_candidate_allowed = _as_bool(row.get("direct_candidate_allowed"), False)
    if direct_candidate_allowed:
        errors.append("direct_candidate_allowed_forbidden")

    implication = _as_text(row.get("implication") or row.get("reason"))
    effect = _candidate_effect(action, direction, implication)
    confirm_level = _confirm_level(row, implication)
    related_codes = _norm_codes(row.get("related_codes") or row.get("codes"))
    sector_tag = _as_text(row.get("sector_tag"))
    source_trading_effect = _as_bool(row.get("source_trading_effect"), False)
    reviewed_execution_allowed = _as_bool(row.get("reviewed_execution_allowed"), False)
    execution_allowed = False
    target = _target(scope, sector_tag, related_codes)
    clean = {
        "judgment_id": _as_text(row.get("judgment_id")) or f"manual:{_stable_id(asof_ymd, source_url, source_title, scope, related_codes)}",
        "asof_ymd": asof_ymd,
        "source_url": source_url,
        "source_title": source_title,
        "scope": scope,
        "target": target,
        "sector_tag": sector_tag,
        "related_codes": related_codes,
        "time_axis": _as_text(row.get("time_axis") or "current"),
        "horizon": horizon,
        "direction": direction,
        "action": action,
        "candidate_effect": effect,
        "strength": f"{strength:.6f}",
        "confidence": f"{confidence:.6f}",
        "confirm_level": str(confirm_level),
        "implication": implication,
        "active_for_l3": str(_as_bool(row.get("active_for_l3"), True)),
        "source_trading_effect": str(source_trading_effect),
        "execution_allowed": str(execution_allowed),
        "execution_denied_reason": _execution_denied_reason(
            scope=scope,
            effect=effect,
            confirm_level=confirm_level,
            confidence=confidence,
            source_trading_effect=source_trading_effect,
        ),
        "reviewed_execution_allowed": str(reviewed_execution_allowed),
        "consumer": _as_text(row.get("consumer") or ("candidate|report" if scope == "company" else "sector|report")),
        "processing_state": _as_text(row.get("processing_state") or "normalized"),
        "applies_to_existing_only": str(bool(applies_to_existing_only)),
        "direct_candidate_allowed": "False",
        "reviewer": _as_text(row.get("reviewer") or "manual"),
        "reviewed_at": _as_text(row.get("reviewed_at") or _now_kst().isoformat(timespec="seconds")),
    }
    if clean["scope"] in {"market", "sector", "macro"} and not clean["sector_tag"] and clean["scope"] == "sector":
        errors.append("sector_tag_missing")
    if clean["scope"] == "company" and not clean["related_codes"]:
        errors.append("company_related_codes_missing")
    if reviewed_execution_allowed and not clean["reviewer"]:
        errors.append("reviewed_execution_allowed_without_reviewer")
    return clean, errors


def _write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in FIELDS})


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate manual news implications without adding news-only trading candidates.")
    parser.add_argument("--input", default=str(IN_PATH))
    parser.add_argument("--output", default=str(OUT_CSV))
    args = parser.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)
    raw_rows, read_errors = _read_jsonl(in_path)
    valid_rows: List[Dict[str, Any]] = []
    row_errors: List[str] = []
    for row in raw_rows:
        clean, errors = _validate_row(row)
        if errors:
            row_errors.append(f"line_{row.get('_line_no', '?')}:" + ",".join(errors))
            continue
        valid_rows.append(clean)

    _write_csv(out_path, valid_rows)
    status = {
        "generated_at": _now_kst().isoformat(timespec="seconds"),
        "input": str(in_path),
        "output": str(out_path),
        "rows_raw": int(len(raw_rows)),
        "rows_valid": int(len(valid_rows)),
        "rows_invalid": int(len(row_errors)),
        "read_errors": read_errors,
        "row_errors": row_errors,
        "direct_candidate_policy": "forbidden",
        "candidate_append": False,
        "quality": "PASS" if not read_errors and not row_errors else "FAIL",
    }
    text = json.dumps(status, ensure_ascii=False, indent=2)
    STATUS_LATEST.write_text(text, encoding="utf-8")
    dated = LOGS / f"manual_news_implications_status_{_now_kst().strftime('%Y%m%d_%H%M%S')}.json"
    dated.write_text(text, encoding="utf-8")
    print(text)
    return 0 if status["quality"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
