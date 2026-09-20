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
IN_CSV = LOGS / "auto_news_implications_latest.csv"
OUT_LATEST = LOGS / "news_topic_judgment_latest.csv"
OUT_HISTORY = LOGS / "news_topic_judgment_history.csv"
STATUS_LATEST = LOGS / "news_topic_judgment_status_latest.json"
KST = timezone(timedelta(hours=9))

LATEST_FIELDS = [
    "judgment_id",
    "run_id",
    "generated_at",
    "asof_ymd",
    "source_url",
    "source_title",
    "scope",
    "target",
    "topic_key",
    "topic_label",
    "topic_state",
    "direction",
    "action",
    "risk_state",
    "confirm_level",
    "time_axis",
    "horizon",
    "time_horizon",
    "article_count",
    "positive_count",
    "negative_count",
    "watch_count",
    "boost_count",
    "reduce_size_count",
    "penalize_count",
    "block_count",
    "avg_strength",
    "strength",
    "avg_confidence",
    "confidence",
    "related_codes",
    "representative_titles",
    "representative_evidence",
    "candidate_effect",
    "effect_reason",
    "active_for_l3",
    "source_trading_effect",
    "execution_allowed",
    "execution_denied_reason",
    "reviewed_execution_allowed",
    "consumer",
    "processing_state",
    "trading_effect",
]
HISTORY_FIELDS = LATEST_FIELDS

TOPIC_RULES = [
    (
        "semiconductor_fomo",
        "반도체 쏠림/FOMO",
        ("반도체", "삼전", "닉스", "삼성전자", "SK하이닉스", "HBM", "메모리", "FOMO", "쏠림"),
    ),
    (
        "power_infra_demand",
        "전력 인프라 수요",
        ("전력", "전력망", "전력 인프라", "전력설비", "전력수요", "ESS", "데이터센터"),
    ),
    (
        "robotics_policy",
        "로봇/AI 정책",
        ("로봇", "K-로보틱스", "휴머노이드", "피지컬 AI", "정책 보고회"),
    ),
    (
        "earnings_season_sell_on",
        "실적 시즌/Sell-on",
        ("실적", "잠정실적", "어닝", "발표", "차익실현", "sell-on", "Sell-on"),
    ),
    (
        "foreign_sell_korea",
        "외국인 수급/셀코리아",
        ("외국인", "셀 코리아", "순매도", "순매수", "환율", "원화", "금리"),
    ),
    (
        "ai_datacenter_infra",
        "AI/데이터센터 인프라",
        ("AI", "데이터센터", "인공지능", "반도체 생산시설", "냉각", "전력안전"),
    ),
    (
        "market_overheat_volatility",
        "시장 과열/변동성",
        ("급등", "급락", "사이드카", "서킷브레이커", "추격매수", "과열", "쏠림"),
    ),
    (
        "bio_momentum",
        "바이오 모멘텀",
        ("바이오", "알테오젠", "리가켐", "삼성바이오", "제약"),
    ),
    (
        "construction_nuclear_order",
        "건설/원전 수주",
        ("대우건설", "원전", "수주", "PF 보증", "공사계약", "목표가 하향", "수주 지연"),
    ),
    (
        "mlcc_component_cycle",
        "MLCC/부품 사이클",
        ("MLCC", "삼성전기", "부품", "기판", "대덕전자", "LG이노텍", "업황 회복"),
    ),
    (
        "tender_offer_governance",
        "공개매수/지배구조",
        ("공개매수", "지분", "책임 경영", "지배구조", "동일제강", "에스폼"),
    ),
    (
        "brokerage_market_beta",
        "증권/시장 베타",
        ("증권주", "미래에셋증권", "키움증권", "삼성증권", "거래대금", "코스피"),
    ),
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


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Iterable[Dict[str, Any]], fields: List[str], *, append: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    mode = "a" if append else "w"
    with path.open(mode, encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        if not append or not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _write_status(status: Dict[str, Any]) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    text = json.dumps(status, ensure_ascii=False, indent=2)
    STATUS_LATEST.write_text(text, encoding="utf-8")
    stamp = _now_kst().strftime("%Y%m%d_%H%M%S")
    (LOGS / f"news_topic_judgment_status_{stamp}.json").write_text(text, encoding="utf-8")


def _text_blob(row: Dict[str, str]) -> str:
    return " ".join(
        [
            _as_text(row.get("source_title")),
            _as_text(row.get("sector_tag")),
            _as_text(row.get("implication")),
        ]
    )


def _topic_for(row: Dict[str, str]) -> Tuple[str, str]:
    blob = _text_blob(row).lower()
    for key, label, terms in TOPIC_RULES:
        if any(term.lower() in blob for term in terms):
            return key, label
    scope = _as_text(row.get("scope")) or "unknown"
    sector = _as_text(row.get("sector_tag")) or scope
    safe = re.sub(r"[^0-9A-Za-z가-힣_]+", "_", sector).strip("_").lower()
    return f"other_{safe or 'news'}", f"기타/{sector}"


def _direction(rows: List[Dict[str, str]]) -> str:
    pos = sum(1 for r in rows if _as_text(r.get("direction")).lower() == "positive")
    neg = sum(1 for r in rows if _as_text(r.get("direction")).lower() == "negative")
    neu = sum(1 for r in rows if _as_text(r.get("direction")).lower() == "neutral")
    if pos and neg:
        return "mixed"
    if pos > max(neg, neu):
        return "positive"
    if neg > max(pos, neu):
        return "negative"
    return "neutral"


def _risk_state(rows: List[Dict[str, str]], blob: str) -> str:
    actions = [_as_text(r.get("action")).lower() for r in rows]
    if "block" in actions or actions.count("reduce_size") >= 2:
        return "high"
    risk_terms = ("급락", "쏠림", "추격매수", "과열", "사이드카", "서킷브레이커", "순매도", "환율")
    if any(term in blob for term in risk_terms) or "reduce_size" in actions or "penalize" in actions:
        return "medium"
    return "low"


def _confirm_level(rows: List[Dict[str, str]], blob: str) -> int:
    if any(term in blob for term in ("실제 개최", "공개매수", "계약", "수주", "공급", "공시")):
        return 3
    if any(term in blob for term in ("정책", "전망", "기대", "부각", "강세")):
        return 2
    return 1


def _topic_state(rows: List[Dict[str, str]], direction: str, risk: str, confirm_level: int, blob: str) -> str:
    if risk == "high":
        return "overheated" if any(term in blob for term in ("쏠림", "급등", "추격매수", "사이드카")) else "weakening"
    if direction == "mixed":
        return "mixed_watch"
    if confirm_level >= 3 and direction == "positive":
        return "confirmed"
    if len(rows) >= 2 and direction == "positive":
        return "strengthening"
    if len(rows) >= 2 and direction == "negative":
        return "weakening"
    return "emerging"


def _candidate_effect(topic_state: str, direction: str, risk: str, actions: List[str]) -> Tuple[str, str]:
    if "block" in actions:
        return "block_review", "block action exists; keep fail-closed review only"
    if risk == "high":
        return "avoid_chase", "high risk or overheated topic; no direct candidate append"
    if "reduce_size" in actions:
        return "reduce_size_context", "reduce_size action exists for existing candidate context"
    if direction == "positive" and topic_state in {"confirmed", "strengthening"}:
        return "positive_context", "positive topic context; direct candidate append remains forbidden"
    if direction == "mixed":
        return "watch_only", "positive and negative drivers conflict"
    return "watch_only", "analysis-only topic context"


def _horizon(rows: List[Dict[str, str]]) -> str:
    # 긴급도 우선: intraday > short > medium > long
    # long은 단기 이슈가 없을 때만 올라옴
    values = [_as_text(r.get("horizon")).lower() for r in rows]
    for wanted in ("intraday", "short", "medium", "long"):
        if wanted in values:
            return wanted
    return "short"


def _representative(rows: List[Dict[str, str]], field: str, limit: int = 3) -> str:
    out: List[str] = []
    for row in rows:
        value = _as_text(row.get(field))
        if value and value not in out:
            out.append(value)
        if len(out) >= limit:
            break
    return " | ".join(out)


def _dominant_action(actions: List[str]) -> str:
    for action in ("block", "reduce_size", "penalize", "boost", "watch", "ignore"):
        if action in actions:
            return action
    return "watch"


def _aggregate(topic_key: str, topic_label: str, rows: List[Dict[str, str]], run_id: str, generated_at: str) -> Dict[str, Any]:
    blob = _text_blob({"source_title": " ".join(_as_text(r.get("source_title")) for r in rows), "implication": " ".join(_as_text(r.get("implication")) for r in rows)})
    blob_l = blob.lower()
    direction = _direction(rows)
    risk = _risk_state(rows, blob)
    confirm = _confirm_level(rows, blob)
    state = _topic_state(rows, direction, risk, confirm, blob)
    actions = [_as_text(r.get("action")).lower() for r in rows]
    effect, reason = _candidate_effect(state, direction, risk, actions)
    codes = sorted({code for row in rows for code in [_norm_code(row.get("related_codes"))] if code})
    strengths = [_as_float(r.get("strength")) for r in rows]
    confidences = [_as_float(r.get("confidence")) for r in rows]
    avg_strength = (sum(strengths) / len(strengths)) if strengths else 0.0
    avg_confidence = (sum(confidences) / len(confidences)) if confidences else 0.0
    source_title = _representative(rows, "source_title")
    source_url = _representative(rows, "source_url") or f"auto_news_topic:{topic_key}"
    horizon = _horizon(rows)
    action = _dominant_action(actions)
    return {
        "judgment_id": f"topic:{_stable_id(run_id, topic_key, topic_label)}",
        "run_id": run_id,
        "generated_at": generated_at,
        "asof_ymd": run_id[:8],
        "source_url": source_url,
        "source_title": source_title,
        "scope": "sector",
        "target": topic_label,
        "topic_key": topic_key,
        "topic_label": topic_label,
        "topic_state": state,
        "direction": direction,
        "action": action,
        "risk_state": risk,
        "confirm_level": int(confirm),
        "time_axis": "current",
        "horizon": horizon,
        "time_horizon": horizon,
        "article_count": int(len(rows)),
        "positive_count": sum(1 for r in rows if _as_text(r.get("direction")).lower() == "positive"),
        "negative_count": sum(1 for r in rows if _as_text(r.get("direction")).lower() == "negative"),
        "watch_count": actions.count("watch"),
        "boost_count": actions.count("boost"),
        "reduce_size_count": actions.count("reduce_size"),
        "penalize_count": actions.count("penalize"),
        "block_count": actions.count("block"),
        "avg_strength": f"{avg_strength:.6f}",
        "strength": f"{avg_strength:.6f}",
        "avg_confidence": f"{avg_confidence:.6f}",
        "confidence": f"{avg_confidence:.6f}",
        "related_codes": "|".join(codes),
        "representative_titles": source_title,
        "representative_evidence": _representative(rows, "implication", limit=2)[:900],
        "candidate_effect": effect,
        "effect_reason": reason,
        "active_for_l3": "True",
        "source_trading_effect": "False",
        "execution_allowed": "False",
        "execution_denied_reason": "scope_not_stock|source_trading_effect_false",
        "reviewed_execution_allowed": "False",
        "consumer": "sector|report",
        "processing_state": "normalized",
        "trading_effect": "False",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build read-only topic-level news judgment layer.")
    parser.add_argument("--input", default=str(IN_CSV))
    parser.add_argument("--latest-output", default=str(OUT_LATEST))
    parser.add_argument("--history-output", default=str(OUT_HISTORY))
    args = parser.parse_args()

    in_path = Path(args.input)
    latest_path = Path(args.latest_output)
    history_path = Path(args.history_output)
    generated = _now_kst()
    generated_at = generated.isoformat(timespec="seconds")
    run_id = generated.strftime("%Y%m%d_%H%M%S")
    rows = _read_csv(in_path)
    status: Dict[str, Any] = {
        "generated_at": generated_at,
        "run_id": run_id,
        "input": str(in_path),
        "latest_output": str(latest_path),
        "history_output": str(history_path),
        "rows_input": int(len(rows)),
        "trading_effect": False,
        "quality": "FAIL",
    }
    if not rows:
        status["reason"] = "input_missing_or_empty"
        _write_status(status)
        print(json.dumps(status, ensure_ascii=False, indent=2))
        return 1

    grouped: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
    for row in rows:
        key, label = _topic_for(row)
        grouped.setdefault((key, label), []).append(row)

    out_rows = [
        _aggregate(key, label, topic_rows, run_id, generated_at)
        for (key, label), topic_rows in sorted(grouped.items())
    ]
    _write_csv(latest_path, out_rows, LATEST_FIELDS, append=False)
    _write_csv(history_path, out_rows, HISTORY_FIELDS, append=True)

    state_counts: Dict[str, int] = {}
    effect_counts: Dict[str, int] = {}
    for row in out_rows:
        state_counts[str(row["topic_state"])] = state_counts.get(str(row["topic_state"]), 0) + 1
        effect_counts[str(row["candidate_effect"])] = effect_counts.get(str(row["candidate_effect"]), 0) + 1
    status.update(
        {
            "topics": int(len(out_rows)),
            "state_counts": dict(sorted(state_counts.items())),
            "candidate_effect_counts": dict(sorted(effect_counts.items())),
            "quality": "PASS",
            "reason": "ok",
        }
    )
    _write_status(status)
    print(json.dumps(status, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
