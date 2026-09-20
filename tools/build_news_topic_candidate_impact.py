from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
TOPIC_CSV = LOGS / "news_topic_judgment_latest.csv"
ARTICLE_CSV = LOGS / "auto_news_implications_latest.csv"
OUT_CSV = LOGS / "news_topic_candidate_impact_latest.csv"
STATUS_LATEST = LOGS / "news_topic_candidate_impact_status_latest.json"
NAME_SOURCES = [
    LOGS / "candidates_latest_data.with_final_score.csv",
    LOGS / "candidates_latest_data.with_news_score.csv",
    ROOT / "_cache" / "dart_corp_code_map.csv",
]
KST = timezone(timedelta(hours=9))

FIELDS = [
    "judgment_id",
    "run_id",
    "generated_at",
    "asof_ymd",
    "source_url",
    "source_title",
    "scope",
    "target",
    "related_codes",
    "time_axis",
    "horizon",
    "code",
    "name",
    "topic_key",
    "topic_label",
    "topic_state",
    "direction",
    "action",
    "risk_state",
    "confirm_level",
    "candidate_effect",
    "effect_reason",
    "topic_article_count",
    "candidate_article_count",
    "candidate_actions",
    "avg_strength",
    "strength",
    "avg_confidence",
    "confidence",
    "representative_titles",
    "representative_evidence",
    "applies_to_existing_only",
    "direct_candidate_allowed",
    "active_for_l3",
    "source_trading_effect",
    "execution_allowed",
    "execution_denied_reason",
    "reviewed_execution_allowed",
    "consumer",
    "processing_state",
    "trading_effect",
]

TOPIC_RULES = [
    ("semiconductor_fomo", ("반도체", "삼전", "닉스", "삼성전자", "SK하이닉스", "HBM", "메모리", "FOMO", "쏠림")),
    ("power_infra_demand", ("전력", "전력망", "전력 인프라", "전력설비", "전력수요", "ESS", "데이터센터")),
    ("robotics_policy", ("로봇", "K-로보틱스", "휴머노이드", "피지컬 AI", "정책 보고회")),
    ("earnings_season_sell_on", ("실적", "잠정실적", "어닝", "발표", "차익실현", "sell-on", "Sell-on")),
    ("foreign_sell_korea", ("외국인", "셀 코리아", "순매도", "순매수", "환율", "원화", "금리")),
    ("ai_datacenter_infra", ("AI", "데이터센터", "인공지능", "반도체 생산시설", "냉각", "전력안전")),
    ("market_overheat_volatility", ("급등", "급락", "사이드카", "서킷브레이커", "추격매수", "과열", "쏠림")),
    ("bio_momentum", ("바이오", "알테오젠", "리가켐", "삼성바이오", "제약")),
    ("construction_nuclear_order", ("대우건설", "원전", "수주", "PF 보증", "공사계약", "목표가 하향", "수주 지연")),
    ("mlcc_component_cycle", ("MLCC", "삼성전기", "부품", "기판", "대덕전자", "LG이노텍", "업황 회복")),
    ("tender_offer_governance", ("공개매수", "지분", "책임 경영", "지배구조", "동일제강", "에스폼")),
    ("brokerage_market_beta", ("증권주", "미래에셋증권", "키움증권", "삼성증권", "거래대금", "코스피")),
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


def _norm_code(value: Any) -> str:
    text = re.sub(r"[^0-9]", "", _as_text(value))
    return text.zfill(6)[-6:] if text else ""


def _split_codes(value: Any) -> List[str]:
    out: List[str] = []
    for part in re.split(r"[,|; ]+", _as_text(value)):
        code = _norm_code(part)
        if code:
            out.append(code)
    return list(dict.fromkeys(out))


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _load_name_map(paths: List[Path]) -> Tuple[Dict[str, str], Dict[str, Any]]:
    out: Dict[str, str] = {}
    meta_sources: List[Dict[str, Any]] = []
    for path in paths:
        rows = _read_csv(path)
        usable = 0
        for row in rows:
            code = _norm_code(row.get("code"))
            name = _as_text(row.get("name") or row.get("corp_name"))
            if code and name and code not in out:
                out[code] = name
                usable += 1
        meta_sources.append({"path": str(path), "rows": int(len(rows)), "usable": int(usable)})
    return out, {"sources": meta_sources, "codes": int(len(out))}


def _write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in FIELDS})


def _write_status(status: Dict[str, Any]) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    text = json.dumps(status, ensure_ascii=False, indent=2)
    STATUS_LATEST.write_text(text, encoding="utf-8")
    stamp = _now_kst().strftime("%Y%m%d_%H%M%S")
    (LOGS / f"news_topic_candidate_impact_status_{stamp}.json").write_text(text, encoding="utf-8")


def _article_topic_key(row: Dict[str, str]) -> str:
    blob = " ".join([_as_text(row.get("source_title")), _as_text(row.get("sector_tag")), _as_text(row.get("implication"))]).lower()
    for topic_key, terms in TOPIC_RULES:
        if any(term.lower() in blob for term in terms):
            return topic_key
    scope = _as_text(row.get("scope")) or "unknown"
    sector = _as_text(row.get("sector_tag")) or scope
    safe = re.sub(r"[^0-9A-Za-z가-힣_]+", "_", sector).strip("_").lower()
    return f"other_{safe or 'news'}"


def _representative(rows: List[Dict[str, str]], field: str, limit: int = 3) -> str:
    out: List[str] = []
    for row in rows:
        value = _as_text(row.get(field))
        if value and value not in out:
            out.append(value)
        if len(out) >= limit:
            break
    return " | ".join(out)


def _action_summary(rows: List[Dict[str, str]]) -> str:
    counts: Dict[str, int] = {}
    for row in rows:
        action = _as_text(row.get("action")).lower()
        if action:
            counts[action] = counts.get(action, 0) + 1
    return "|".join(f"{key}:{counts[key]}" for key in sorted(counts))


def _dominant_action(rows: List[Dict[str, str]]) -> str:
    actions = {_as_text(row.get("action")).lower() for row in rows}
    for action in ("block", "reduce_size", "penalize", "boost", "watch", "ignore"):
        if action in actions:
            return action
    return "watch"


def _denied_reason(effect: str, confirm_level: Any, confidence: float) -> str:
    reasons = ["source_trading_effect_false"]
    if effect not in {"block_review", "avoid_chase", "reduce_size_context"}:
        reasons.append("candidate_effect_not_l3")
    try:
        confirm = int(float(str(confirm_level).strip()))
    except Exception:
        confirm = 0
    if confirm < 2:
        reasons.append("confirm_level_below_min")
    if confidence < 0.60:
        reasons.append("avg_confidence_below_min")
    return "|".join(dict.fromkeys(reasons))


def _row_effect(topic: Dict[str, str], rows: List[Dict[str, str]]) -> Tuple[str, str]:
    topic_effect = _as_text(topic.get("candidate_effect")) or "watch_only"
    topic_reason = _as_text(topic.get("effect_reason"))
    actions = {_as_text(row.get("action")).lower() for row in rows}
    if "block" in actions:
        return "block_review", "candidate has block article; fail-closed review only"
    if "reduce_size" in actions:
        return "reduce_size_context", "candidate has reduce_size article; existing-position context only"
    if topic_effect == "avoid_chase":
        return "avoid_chase", topic_reason or "topic risk is high"
    if "boost" in actions and topic_effect == "positive_context":
        return "positive_context", "candidate has boost article under positive topic; direct append forbidden"
    if "watch" in actions:
        return "watch_only", "candidate has watch article; confirmation required"
    return topic_effect, topic_reason or "topic-level analysis context"


def _build_rows(
    topic_rows: List[Dict[str, str]],
    article_rows: List[Dict[str, str]],
    run_id: str,
    generated_at: str,
    name_map: Dict[str, str],
) -> Tuple[List[Dict[str, Any]], int]:
    topic_by_key = {_as_text(row.get("topic_key")): row for row in topic_rows}
    grouped: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
    for row in article_rows:
        topic_key = _article_topic_key(row)
        for code in _split_codes(row.get("related_codes")):
            grouped.setdefault((topic_key, code), []).append(row)

    out: List[Dict[str, Any]] = []
    rejected_unknown_code_rows = 0
    for (topic_key, code), rows in sorted(grouped.items()):
        topic = topic_by_key.get(topic_key, {})
        if not topic:
            continue
        name = name_map.get(code, "")
        if not name:
            rejected_unknown_code_rows += 1
            continue
        strengths = [_as_float(row.get("strength")) for row in rows]
        confidences = [_as_float(row.get("confidence")) for row in rows]
        effect, reason = _row_effect(topic, rows)
        avg_strength = (sum(strengths) / len(strengths)) if strengths else 0.0
        avg_confidence = (sum(confidences) / len(confidences)) if confidences else 0.0
        confirm_level = _as_text(topic.get("confirm_level"))
        source_title = _representative(rows, "source_title")
        source_url = _representative(rows, "source_url") or f"auto_news_candidate:{topic_key}:{code}"
        horizon = _as_text(topic.get("horizon") or topic.get("time_horizon")) or "short"
        action = _dominant_action(rows)
        out.append(
            {
                "judgment_id": f"impact:{_stable_id(run_id, topic_key, code)}",
                "run_id": run_id,
                "generated_at": generated_at,
                "asof_ymd": run_id[:8],
                "source_url": source_url,
                "source_title": source_title,
                "scope": "stock",
                "target": code,
                "related_codes": code,
                "time_axis": "current",
                "horizon": horizon,
                "code": code,
                "name": name,
                "topic_key": topic_key,
                "topic_label": _as_text(topic.get("topic_label")),
                "topic_state": _as_text(topic.get("topic_state")),
                "direction": _as_text(topic.get("direction")),
                "action": action,
                "risk_state": _as_text(topic.get("risk_state")),
                "confirm_level": confirm_level,
                "candidate_effect": effect,
                "effect_reason": reason,
                "topic_article_count": _as_text(topic.get("article_count")),
                "candidate_article_count": int(len(rows)),
                "candidate_actions": _action_summary(rows),
                "avg_strength": f"{avg_strength:.6f}",
                "strength": f"{avg_strength:.6f}",
                "avg_confidence": f"{avg_confidence:.6f}",
                "confidence": f"{avg_confidence:.6f}",
                "representative_titles": source_title,
                "representative_evidence": _representative(rows, "implication", limit=2)[:900],
                "applies_to_existing_only": "True",
                "direct_candidate_allowed": "False",
                "active_for_l3": "True",
                "source_trading_effect": "False",
                "execution_allowed": "False",
                "execution_denied_reason": _denied_reason(effect, confirm_level, avg_confidence),
                "reviewed_execution_allowed": "False",
                "consumer": "candidate|report",
                "processing_state": "normalized",
                "trading_effect": "False",
            }
        )
    return out, rejected_unknown_code_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Build read-only topic-to-candidate news impact layer.")
    parser.add_argument("--topics", default=str(TOPIC_CSV))
    parser.add_argument("--articles", default=str(ARTICLE_CSV))
    parser.add_argument("--output", default=str(OUT_CSV))
    parser.add_argument("--name-source", action="append", default=[])
    args = parser.parse_args()

    topics_path = Path(args.topics)
    articles_path = Path(args.articles)
    out_path = Path(args.output)
    generated = _now_kst()
    generated_at = generated.isoformat(timespec="seconds")
    run_id = generated.strftime("%Y%m%d_%H%M%S")
    topic_rows = _read_csv(topics_path)
    article_rows = _read_csv(articles_path)
    name_source_paths = [Path(p) for p in args.name_source] if args.name_source else NAME_SOURCES
    name_map, name_meta = _load_name_map(name_source_paths)
    status: Dict[str, Any] = {
        "generated_at": generated_at,
        "run_id": run_id,
        "topics_input": str(topics_path),
        "articles_input": str(articles_path),
        "output": str(out_path),
        "topic_rows": int(len(topic_rows)),
        "article_rows": int(len(article_rows)),
        "name_map": name_meta,
        "direct_candidate_policy": "forbidden",
        "candidate_append": False,
        "trading_effect": False,
        "quality": "FAIL",
    }
    if not topic_rows:
        status["reason"] = "topic_input_missing_or_empty"
        _write_status(status)
        print(json.dumps(status, ensure_ascii=False, indent=2))
        return 1
    if not article_rows:
        status["reason"] = "article_input_missing_or_empty"
        _write_status(status)
        print(json.dumps(status, ensure_ascii=False, indent=2))
        return 1

    out_rows, rejected_unknown_code_rows = _build_rows(topic_rows, article_rows, run_id, generated_at, name_map)
    _write_csv(out_path, out_rows)
    effect_counts: Dict[str, int] = {}
    for row in out_rows:
        effect = str(row.get("candidate_effect") or "")
        effect_counts[effect] = effect_counts.get(effect, 0) + 1
    status.update(
        {
            "rows_output": int(len(out_rows)),
            "rows_with_name": int(sum(1 for row in out_rows if _as_text(row.get("name")))),
            "rejected_unknown_code_rows": int(rejected_unknown_code_rows),
            "effect_counts": dict(sorted(effect_counts.items())),
            "quality": "PASS",
            "reason": "ok",
        }
    )
    _write_status(status)
    print(json.dumps(status, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
