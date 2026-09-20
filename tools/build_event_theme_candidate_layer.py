from __future__ import annotations

import csv
import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CACHE_DIR = ROOT / "_cache"

CANDIDATES_CSV = LOG_DIR / "candidates_latest_data.with_final_score.csv"
NEWS_CANDIDATES_CSV = LOG_DIR / "news_candidates_latest.csv"
MARKET_RISING_CSV = LOG_DIR / "market_rising_latest.csv"
PULLBACK_SCREENER_CSV = LOG_DIR / "pullback_methodology_first_screener_latest.csv"
SURGE_EVENT_SCREENER_CSV = LOG_DIR / "surge_event_money_pullback_screener_latest.csv"
GLOBAL_EVENT_FEED_CSV = LOG_DIR / "global_event_feed_latest.csv"
NEWS_SCORE_STATUS_JSON = LOG_DIR / "news_score_status_latest.json"
NEWS_IMPLICATION_STATUS_JSON = LOG_DIR / "news_implication_status_latest.json"

OUT_JSON = LOG_DIR / "event_theme_candidate_layer_latest.json"
OUT_CSV = LOG_DIR / "event_theme_candidate_layer_latest.csv"


THEME_KEYWORDS = {
    "TECH_INNOVATION": ["AI", "GPT", "로봇", "반도체", "데이터센터", "양자", "스테이블코인", "비전프로", "피지컬"],
    "GLOBAL_CONFLICT": ["전쟁", "분쟁", "이스라엘", "하마스", "러시아", "우크라이나", "이란", "중동"],
    "POLICY_REGULATION": ["정책", "규제", "IRA", "관세", "원전", "재건", "대마", "정부", "법안"],
    "COMMODITY_SUPPLY": ["리튬", "희토류", "구리", "곡물", "팜유", "천연가스", "원자재", "공급망"],
    "BIGTECH_LINK": ["삼성", "애플", "테슬라", "스페이스X", "엔비디아", "구글", "마이크로소프트"],
    "CONTENT_CULTURE": ["K푸드", "K화장품", "오징어게임", "한강", "콘텐츠", "화장품", "미디어"],
    "POLITICAL_PERSON": ["대선", "후보", "정치", "트럼프", "한동훈", "계엄"],
    "ENERGY_SPACE": ["우주", "태양광", "전력", "에너지", "원전", "전선", "유리기판"],
}

THEME_SECTOR_KEYWORDS = {
    "TECH_INNOVATION": ["반도체", "소프트웨어", "전자", "기계", "로봇", "통신", "장비", "AI", "데이터", "유리기판"],
    "GLOBAL_CONFLICT": ["방산", "항공", "해운", "에너지", "화학", "기계", "건설", "운송"],
    "POLICY_REGULATION": ["전력", "전기", "에너지", "건설", "바이오", "화학", "자동차", "태양광", "원전"],
    "COMMODITY_SUPPLY": ["금속", "광물", "화학", "에너지", "음식료", "농업", "비철", "철강"],
    "BIGTECH_LINK": ["전자", "반도체", "소프트웨어", "통신", "장비", "로봇", "자동차"],
    "CONTENT_CULTURE": ["음식료", "화장품", "미디어", "게임", "엔터", "콘텐츠", "유통"],
    "POLITICAL_PERSON": ["건설", "교육", "금융", "방송", "미디어"],
    "ENERGY_SPACE": ["전력", "전기", "에너지", "태양광", "원전", "전선", "우주", "항공", "기계"],
}

GLOBAL_EVENT_THEMES = {"GLOBAL_CONFLICT", "POLICY_REGULATION", "COMMODITY_SUPPLY", "BIGTECH_LINK", "ENERGY_SPACE"}


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    return []


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except UnicodeDecodeError:
            continue
    return {}


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _code(value: Any) -> str:
    text = str(value or "").strip()
    digits = re.sub(r"[^0-9]", "", text)
    return digits.zfill(6)[-6:] if digits else ""


def _float(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value if value is not None else "").strip()
        if not text or text.lower() in {"nan", "none", "<na>"}:
            return default
        return float(text)
    except Exception:
        return default


def _bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"true", "1", "yes", "y"}


def _first(*values: Any) -> str:
    for value in values:
        text = str(value if value is not None else "").strip()
        if text and text.lower() not in {"nan", "none", "<na>"}:
            return text
    return ""


def _index_by_code(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code and code not in out:
            out[code] = row
    return out


def _supply_history_by_code() -> tuple[dict[str, list[dict[str, Any]]], list[str]]:
    out: dict[str, list[dict[str, Any]]] = {}
    files = sorted(CACHE_DIR.glob("pykrx_supply_????????.csv"))
    used_files: list[str] = []
    for path in files:
        ymd = path.stem[-8:]
        used_files.append(path.name)
        for row in _read_csv(path):
            code = _code(row.get("code"))
            if not code:
                continue
            out.setdefault(code, []).append(
                {
                    "ymd": ymd,
                    "foreign_net": _float(row.get("foreign_net"), 0.0),
                    "institution_net": _float(row.get("institution_net"), 0.0),
                    "personal_net": _float(row.get("personal_net"), 0.0),
                }
            )
    for rows in out.values():
        rows.sort(key=lambda r: str(r["ymd"]))
    return out, used_files


def _consecutive_positive(rows: list[dict[str, Any]], key: str) -> int:
    count = 0
    for row in reversed(rows):
        if _float(row.get(key), 0.0) > 0:
            count += 1
            continue
        break
    return count


def _supply_continuity_from_history(code: str, history: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    rows = history.get(code, [])
    if not rows:
        return {
            "raw_supply_history_status": "MISSING",
            "raw_supply_history_days": 0,
            "foreign_consecutive_buy_days": 0,
            "institution_consecutive_buy_days": 0,
            "smart_money_consecutive_buy_days": 0,
            "raw_supply_latest_ymd": "",
        }
    latest = rows[-1]
    augmented = [
        {
            **row,
            "smart_money_net": _float(row.get("foreign_net"), 0.0) + _float(row.get("institution_net"), 0.0),
        }
        for row in rows
    ]
    return {
        "raw_supply_history_status": "AVAILABLE",
        "raw_supply_history_days": len(rows),
        "foreign_consecutive_buy_days": _consecutive_positive(augmented, "foreign_net"),
        "institution_consecutive_buy_days": _consecutive_positive(augmented, "institution_net"),
        "smart_money_consecutive_buy_days": _consecutive_positive(augmented, "smart_money_net"),
        "raw_supply_latest_ymd": latest.get("ymd", ""),
    }


def _merge_rows(*rows: dict[str, str]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for row in rows:
        for key, value in row.items():
            if key not in merged or merged[key] in ("", None):
                merged[key] = value
            elif key in {
                "news_score",
                "news_article_count",
                "news_implication_positive_rows",
                "news_implication_sector_rows",
                "news_implication_macro_rows",
                "news_implication_block_rows",
                "foreign_net_20d",
                "institution_net_20d",
                "personal_net_20d",
                "foreign_net",
                "institution_net",
                "personal_net",
                "flow_score",
                "final_score",
                "value",
                "trading_value",
                "ret1_pct",
                "change_pct",
                "v_accel",
                "high_52w_gap",
                "global_event_feed_score",
                "global_event_feed_item_count",
                "global_event_feed_fresh_items",
            }:
                merged[key] = value
    return merged


def _theme_from_text(text: str) -> tuple[str, str]:
    hits: list[str] = []
    upper_text = text.upper()
    for theme, keywords in THEME_KEYWORDS.items():
        for keyword in keywords:
            if keyword.upper() in upper_text:
                hits.append(theme)
                break
    if not hits:
        return "UNCLASSIFIED", ""
    return hits[0], "|".join(hits)


def _theme_from_row(row: dict[str, Any]) -> tuple[str, str]:
    text = " ".join(
        [
            str(row.get("name") or ""),
            str(row.get("candidate_origin_hybrid") or ""),
            str(row.get("krx_sector") or ""),
            str(row.get("sector_reason") or ""),
            str(row.get("news_reason") or ""),
            str(row.get("news_implication_top_scopes") or ""),
            str(row.get("news_implication_top_actions") or ""),
            str(row.get("watch_badge") or ""),
        ]
    )
    theme, hits = _theme_from_text(text)
    if theme != "UNCLASSIFIED":
        return theme, hits

    origin = str(row.get("candidate_origin_hybrid") or row.get("candidate_origin") or "").upper()
    if "TECH" in origin:
        return "TECH_INNOVATION", "origin=TECH"
    if "NEWS" in origin:
        return "NEWS_UNCLASSIFIED", "origin=NEWS"
    return "UNCLASSIFIED", ""


def _row_text(row: dict[str, Any]) -> str:
    return " ".join(
        str(row.get(key) or "")
        for key in [
            "name",
            "candidate_origin_hybrid",
            "krx_sector",
            "sector_reason",
            "sector_policy_reason",
            "news_reason",
            "news_source_reason",
            "news_entity_top_tags",
            "news_implication_top_scopes",
            "news_implication_top_actions",
            "watch_badge",
        ]
    )


def _global_event_axis(row: dict[str, Any], theme: str) -> tuple[str, float, str, str]:
    evidence: list[str] = []
    score = 0.0
    news_score = _float(row.get("news_score"), 0.0)
    news_articles = _float(row.get("news_article_count"), 0.0)
    macro_rows = _float(row.get("news_implication_macro_rows"), 0.0)
    market_rows = _float(row.get("news_implication_market_rows"), 0.0)
    sector_rows = _float(row.get("news_implication_sector_rows"), 0.0)
    policy_score = _float(row.get("policy_score"), 0.0)
    macro_score = _float(row.get("macro_score"), 0.0)
    feed_label = str(row.get("global_event_feed_label") or "")
    feed_score = _float(row.get("global_event_feed_score"), 0.0)

    if feed_label in {"GLOBAL_EVENT_FEED_MATCH", "THEME_NEWS_FEED_MATCH"}:
        score += min(35.0, feed_score * 0.45)
        evidence.append(f"global_event_feed={feed_label}")
    if theme in GLOBAL_EVENT_THEMES:
        score += 30.0
        evidence.append(f"global_theme={theme}")
    elif theme not in {"UNCLASSIFIED", "NEWS_UNCLASSIFIED"}:
        score += 15.0
        evidence.append(f"theme={theme}")
    if news_score >= 0.55:
        score += 20.0
        evidence.append(f"news_score={news_score:.3f}")
    if news_articles >= 3:
        score += 15.0
        evidence.append(f"repeat_news={news_articles:.0f}")
    elif news_articles >= 1:
        score += 7.5
        evidence.append(f"single_news={news_articles:.0f}")
    if macro_rows > 0 or market_rows > 0:
        score += 15.0
        evidence.append("macro_or_market_implication")
    if sector_rows > 0:
        score += 10.0
        evidence.append("sector_implication")
    if policy_score >= 0.5 or macro_score >= 0.5:
        score += 10.0
        evidence.append("policy_or_macro_score")

    score = min(score, 100.0)
    source_status = "GLOBAL_EVENT_FEED_PLUS_NEWS_THEME_POLICY_PROXY" if feed_label else "PROXY_FROM_NEWS_THEME_POLICY"
    if score >= 60:
        return "GLOBAL_OR_POLICY_EVENT_PROXY", round(score, 3), "|".join(evidence), source_status
    if score >= 35:
        return "THEME_EVENT_PROXY", round(score, 3), "|".join(evidence), source_status
    return "EVENT_EVIDENCE_WEAK", round(score, 3), "|".join(evidence) or "no_event_evidence", source_status


def _beneficiary_mapping(row: dict[str, Any], theme: str) -> tuple[str, float, str]:
    evidence: list[str] = []
    score = 0.0
    text = _row_text(row)
    sector = str(row.get("krx_sector") or "")
    directness = _float(row.get("news_implication_directness_score"), 0.0)
    company_rows = _float(row.get("news_implication_company_rows"), 0.0)
    sector_rows = _float(row.get("news_implication_sector_rows"), 0.0)
    sector_allowed = _bool(row.get("sector_entry_allowed"))
    sector_score = _float(row.get("sector_score"), 0.0)
    news_score = _float(row.get("news_score"), 0.0)

    if company_rows > 0 or directness >= 0.65:
        score += 40.0
        evidence.append("company_direct_implication")
    elif directness >= 0.35:
        score += 20.0
        evidence.append("company_directness_partial")
    keywords = THEME_SECTOR_KEYWORDS.get(theme, [])
    if keywords and any(keyword.upper() in text.upper() or keyword.upper() in sector.upper() for keyword in keywords):
        score += 25.0
        evidence.append("theme_sector_keyword_fit")
    if sector_rows > 0 or sector_allowed or sector_score >= 0.2:
        score += 20.0
        evidence.append("sector_link")
    if news_score >= 0.55:
        score += 10.0
        evidence.append("news_score_support")
    if theme in {"UNCLASSIFIED", "NEWS_UNCLASSIFIED"}:
        score -= 10.0
        evidence.append("theme_unclassified_penalty")

    score = max(0.0, min(score, 100.0))
    if score >= 65.0:
        return "DIRECT_OR_STRONG_PROXY", round(score, 3), "|".join(evidence)
    if score >= 40.0:
        return "INDIRECT_SECTOR_FIT", round(score, 3), "|".join(evidence)
    if score >= 20.0:
        return "ASSOCIATIVE_THEME_FIT", round(score, 3), "|".join(evidence)
    return "UNCLEAR", round(score, 3), "|".join(evidence) or "no_mapping_evidence"


def _beneficiary_grade(row: dict[str, Any]) -> tuple[str, str, float]:
    directness = _float(row.get("news_implication_directness_score"), 0.0)
    sector_rows = _float(row.get("news_implication_sector_rows"), 0.0)
    macro_rows = _float(row.get("news_implication_macro_rows"), 0.0)
    company_rows = _float(row.get("news_implication_company_rows"), 0.0)
    news_score = _float(row.get("news_score"), 0.0)
    sector_allowed = _bool(row.get("sector_entry_allowed"))
    sector_score = _float(row.get("sector_score"), 0.0)

    evidence: list[str] = []
    score = 0.0
    if company_rows > 0 or directness >= 0.65:
        score += 35.0
        evidence.append("company_or_direct_implication")
    if sector_rows > 0 or sector_allowed or sector_score >= 0.2:
        score += 25.0
        evidence.append("sector_link")
    if macro_rows > 0:
        score += 15.0
        evidence.append("macro_theme_link")
    if news_score >= 0.55:
        score += 15.0
        evidence.append("news_score")
    if not evidence:
        return "UNCLEAR", "no_direct_or_sector_evidence", score
    if score >= 55.0:
        return "DIRECT_OR_STRONG", "|".join(evidence), score
    if score >= 30.0:
        return "INDIRECT", "|".join(evidence), score
    return "ASSOCIATIVE", "|".join(evidence), score


def _combine_beneficiary(
    base_grade: str,
    base_score: float,
    base_evidence: str,
    mapping_label: str,
    mapping_score: float,
    mapping_evidence: str,
) -> tuple[str, float, str]:
    score = max(base_score, mapping_score)
    evidence = "|".join(part for part in [base_evidence, mapping_evidence] if part)
    if base_grade == "DIRECT_OR_STRONG" or mapping_label == "DIRECT_OR_STRONG_PROXY":
        return "DIRECT_OR_STRONG", round(score, 3), evidence
    if base_grade == "INDIRECT" or mapping_label == "INDIRECT_SECTOR_FIT":
        return "INDIRECT", round(score, 3), evidence
    if base_grade == "ASSOCIATIVE" or mapping_label == "ASSOCIATIVE_THEME_FIT":
        return "ASSOCIATIVE", round(score, 3), evidence
    return "UNCLEAR", round(score, 3), evidence or "no_direct_or_sector_evidence"


def _event_score(row: dict[str, Any], theme: str, beneficiary_score: float) -> tuple[float, str]:
    evidence: list[str] = []
    score = 0.0

    if theme not in {"UNCLASSIFIED", "NEWS_UNCLASSIFIED"}:
        score += 15.0
        evidence.append(f"theme={theme}")
    if theme in {"GLOBAL_CONFLICT", "POLICY_REGULATION", "COMMODITY_SUPPLY", "ENERGY_SPACE"}:
        score += 10.0
        evidence.append("global_or_policy_money_direction")

    news_score = _float(row.get("news_score"), 0.0)
    news_articles = _float(row.get("news_article_count"), 0.0)
    if news_score >= 0.55:
        score += 15.0
        evidence.append(f"news_score={news_score:.3f}")
    if news_articles >= 2:
        score += 10.0
        evidence.append(f"news_repeat={news_articles:.0f}")

    trading_value = max(_float(row.get("trading_value"), 0.0), _float(row.get("value"), 0.0))
    if trading_value >= 50_000_000_000:
        score += 15.0
        evidence.append("trading_value_ge_50b")
    elif trading_value >= 10_000_000_000:
        score += 8.0
        evidence.append("trading_value_ge_10b")

    foreign_net = _float(row.get("foreign_net_20d"), 0.0)
    institution_net = _float(row.get("institution_net_20d"), 0.0)
    flow_score = _float(row.get("flow_score"), 0.0)
    if foreign_net + institution_net > 0 or flow_score >= 0.55:
        score += 15.0
        evidence.append("smart_money_or_flow")

    score += min(15.0, beneficiary_score * 0.25)
    if beneficiary_score > 0:
        evidence.append(f"beneficiary_score={beneficiary_score:.1f}")

    ret = max(_float(row.get("ret1_pct"), 0.0), _float(row.get("change_pct"), 0.0))
    high_gap = _float(row.get("high_52w_gap"), 1.0)
    if 0 <= ret <= 15 and (high_gap <= 0.25 or high_gap == 1.0):
        score += 10.0
        evidence.append("price_position_not_extreme")

    return round(min(score, 100.0), 3), "|".join(evidence)


def _prepriced_risk(row: dict[str, Any]) -> tuple[str, float, str]:
    evidence: list[str] = []
    risk = 0.0
    ret = max(_float(row.get("ret1_pct"), 0.0), _float(row.get("change_pct"), 0.0))
    if ret >= 20.0:
        risk += 25.0
        evidence.append(f"ret_ge_20={ret:.2f}")
    elif ret >= 10.0:
        risk += 12.0
        evidence.append(f"ret_ge_10={ret:.2f}")

    v_accel = _float(row.get("v_accel"), 0.0)
    rvol = _float(row.get("rvol20"), 0.0)
    if v_accel >= 3.0 or rvol >= 3.0:
        risk += 20.0
        evidence.append("volume_spike")

    high_gap = _float(row.get("high_52w_gap"), 1.0)
    if 0 <= high_gap <= 0.05:
        risk += 20.0
        evidence.append("near_52w_high")

    high_drawdown = abs(_float(row.get("intraday_high_drawdown_pct"), 0.0))
    if high_drawdown >= 0.05:
        risk += 20.0
        evidence.append(f"high_rejection={high_drawdown:.4f}")

    news_score = _float(row.get("news_score"), 0.0)
    if news_score >= 0.55 and ret >= 10.0:
        risk += 15.0
        evidence.append("news_after_price_move")

    if _float(row.get("news_implication_block_rows"), 0.0) > 0:
        risk += 30.0
        evidence.append("news_implication_block")

    if risk >= 55.0:
        return "HIGH", round(min(risk, 100.0), 3), "|".join(evidence)
    if risk >= 30.0:
        return "MEDIUM", round(risk, 3), "|".join(evidence)
    return "LOW", round(risk, 3), "|".join(evidence) or "no_major_prepriced_risk"


def _smart_money_quality(row: dict[str, Any]) -> tuple[str, float, str]:
    evidence: list[str] = []
    score = 0.0

    foreign_20d = _float(row.get("foreign_net_20d"), 0.0)
    institution_20d = _float(row.get("institution_net_20d"), 0.0)
    personal_20d = _float(row.get("personal_net_20d"), 0.0)
    foreign_recent = _float(row.get("foreign_net"), 0.0)
    institution_recent = _float(row.get("institution_net"), 0.0)
    trading_value = max(_float(row.get("trading_value"), 0.0), _float(row.get("value"), 0.0))
    flow_score = _float(row.get("flow_score"), 0.0)
    smart_20d = foreign_20d + institution_20d
    smart_recent = foreign_recent + institution_recent

    if smart_20d > 0:
        score += 30.0
        evidence.append("smart_20d_positive")
    if foreign_20d > 0 and institution_20d > 0:
        score += 20.0
        evidence.append("foreign_and_institution_20d_positive")
    elif foreign_20d > 0 or institution_20d > 0:
        score += 10.0
        evidence.append("one_smart_side_20d_positive")
    if smart_recent > 0:
        score += 15.0
        evidence.append("smart_recent_positive")
    if flow_score >= 0.55:
        score += 20.0
        evidence.append(f"flow_score={flow_score:.3f}")
    if trading_value >= 50_000_000_000:
        score += 10.0
        evidence.append("trading_value_ge_50b")
    elif trading_value >= 10_000_000_000:
        score += 5.0
        evidence.append("trading_value_ge_10b")
    if personal_20d > 0 and smart_20d <= 0:
        score -= 15.0
        evidence.append("personal_led_without_smart_20d")

    score = max(0.0, min(score, 100.0))
    if score >= 65.0:
        return "STRONG", round(score, 3), "|".join(evidence)
    if score >= 40.0:
        return "SUPPORTED", round(score, 3), "|".join(evidence)
    if score > 0.0:
        return "WEAK", round(score, 3), "|".join(evidence)
    return "ABSENT", round(score, 3), "no_smart_money_evidence"


def _smart_money_continuity_proxy(row: dict[str, Any]) -> tuple[str, float, str, str]:
    evidence: list[str] = []
    score = 0.0
    foreign_days = int(_float(row.get("foreign_consecutive_buy_days"), 0.0))
    institution_days = int(_float(row.get("institution_consecutive_buy_days"), 0.0))
    smart_days = int(_float(row.get("smart_money_consecutive_buy_days"), 0.0))
    foreign_20d = _float(row.get("foreign_net_20d"), 0.0)
    institution_20d = _float(row.get("institution_net_20d"), 0.0)
    foreign_recent = _float(row.get("foreign_net"), 0.0)
    institution_recent = _float(row.get("institution_net"), 0.0)
    personal_20d = _float(row.get("personal_net_20d"), 0.0)
    smart_20d = foreign_20d + institution_20d
    smart_recent = foreign_recent + institution_recent

    if smart_days >= 3:
        score += 45.0
        evidence.append(f"smart_consecutive_days={smart_days}")
    elif smart_days >= 1:
        score += 20.0
        evidence.append(f"smart_consecutive_days={smart_days}")
    if foreign_days >= 3:
        score += 20.0
        evidence.append(f"foreign_consecutive_days={foreign_days}")
    elif foreign_days >= 1:
        score += 8.0
        evidence.append(f"foreign_consecutive_days={foreign_days}")
    if institution_days >= 3:
        score += 20.0
        evidence.append(f"institution_consecutive_days={institution_days}")
    elif institution_days >= 1:
        score += 8.0
        evidence.append(f"institution_consecutive_days={institution_days}")
    if foreign_20d > 0 and institution_20d > 0:
        score += 20.0
        evidence.append("foreign_and_institution_20d_positive")
    elif foreign_20d > 0 or institution_20d > 0:
        score += 10.0
        evidence.append("one_side_20d_positive")
    if smart_20d > 0 and smart_recent > 0:
        score += 20.0
        evidence.append("20d_and_recent_same_positive")
    elif smart_20d > 0:
        score += 8.0
        evidence.append("20d_positive_recent_unconfirmed")
    if personal_20d > 0 and smart_20d <= 0:
        score -= 20.0
        evidence.append("personal_led_without_smart_20d")

    score = max(0.0, min(score, 100.0))
    source = (
        "RAW_PYKRX_SUPPLY_CACHE_CONSECUTIVE_DAYS"
        if str(row.get("raw_supply_history_status") or "") == "AVAILABLE"
        else "NET_20D_AND_RECENT_PROXY_NOT_RAW_CONSECUTIVE_DAYS"
    )
    if score >= 60.0:
        return "CONTINUITY_RAW_OR_PROXY_STRONG", round(score, 3), "|".join(evidence), source
    if score >= 30.0:
        return "CONTINUITY_RAW_OR_PROXY_PARTIAL", round(score, 3), "|".join(evidence), source
    if score > 0.0:
        return "CONTINUITY_RAW_OR_PROXY_WEAK", round(score, 3), "|".join(evidence), source
    return "CONTINUITY_NOT_CONFIRMED", round(score, 3), "|".join(evidence) or "no_continuity_proxy", source


def _late_buy_risk(row: dict[str, Any], prepriced_label: str, prepriced_score: float) -> tuple[str, float, str]:
    evidence: list[str] = []
    risk = prepriced_score * 0.45
    ret = max(_float(row.get("ret1_pct"), 0.0), _float(row.get("change_pct"), 0.0))
    rvol = max(_float(row.get("rvol20"), 0.0), _float(row.get("v_accel"), 0.0))
    high_gap = _float(row.get("high_52w_gap"), 1.0)
    personal_20d = _float(row.get("personal_net_20d"), 0.0)
    smart_20d = _float(row.get("foreign_net_20d"), 0.0) + _float(row.get("institution_net_20d"), 0.0)

    if prepriced_label == "HIGH":
        risk += 25.0
        evidence.append("prepriced_high")
    elif prepriced_label == "MEDIUM":
        risk += 10.0
        evidence.append("prepriced_medium")
    if ret >= 15.0:
        risk += 20.0
        evidence.append(f"short_return_hot={ret:.2f}")
    elif ret >= 8.0:
        risk += 10.0
        evidence.append(f"short_return_warm={ret:.2f}")
    if rvol >= 3.0:
        risk += 15.0
        evidence.append("volume_hot")
    if 0 <= high_gap <= 0.08:
        risk += 15.0
        evidence.append("near_52w_high")
    if personal_20d > 0 and smart_20d <= 0:
        risk += 15.0
        evidence.append("personal_led_without_smart_money")

    risk = max(0.0, min(risk, 100.0))
    if risk >= 55.0:
        return "HIGH", round(risk, 3), "|".join(evidence)
    if risk >= 30.0:
        return "MEDIUM", round(risk, 3), "|".join(evidence)
    return "LOW", round(risk, 3), "|".join(evidence) or "no_late_buy_risk"


def _price_zone(row: dict[str, Any], prepriced_label: str) -> tuple[str, float, str]:
    evidence: list[str] = []
    score = 0.0
    high_gap = _float(row.get("high_52w_gap"), 1.0)
    intraday_high_drawdown = abs(_float(row.get("intraday_high_drawdown_pct"), 0.0))
    first_pullback = str(row.get("first_pullback_label") or "")
    rr_label = str(row.get("rr_detail_label") or "")
    volume_label = str(row.get("pullback_volume_label") or "")
    methodology_score = _float(row.get("methodology_score"), 0.0)
    range_pos = _float(row.get("intraday_range_position_pct"), -1.0)

    if prepriced_label == "HIGH":
        score -= 35.0
        evidence.append("news_or_price_top_risk")
    if 0 <= high_gap <= 0.05:
        score -= 25.0
        evidence.append("near_52w_high")
    if intraday_high_drawdown >= 0.05:
        score -= 20.0
        evidence.append(f"high_rejection={intraday_high_drawdown:.4f}")
    if "FIRST_PULLBACK" in first_pullback or "HEALTHY" in first_pullback:
        score += 25.0
        evidence.append(f"first_pullback={first_pullback}")
    if "RR_OK" in rr_label or "RR_GOOD" in rr_label:
        score += 20.0
        evidence.append(f"rr={rr_label}")
    if "NEGATIVE_VOLUME_EXPANSION_RISK" not in volume_label and volume_label:
        score += 10.0
        evidence.append(f"volume={volume_label}")
    if methodology_score >= 5.0:
        score += 15.0
        evidence.append(f"methodology_score={methodology_score:.2f}")
    if range_pos >= 0.6:
        score += 10.0
        evidence.append(f"upper_range={range_pos:.3f}")

    if score >= 35.0:
        return "PULLBACK_OR_TURNING_WATCH", round(score, 3), "|".join(evidence)
    if score <= -35.0:
        return "TOP_RISK", round(score, 3), "|".join(evidence)
    return "NEUTRAL_OR_UNCONFIRMED", round(score, 3), "|".join(evidence) or "price_zone_unconfirmed"


def _four_question_decision(
    event_decision: str,
    smart_money_label: str,
    late_risk_label: str,
    prepriced_label: str,
    price_zone_label: str,
    beneficiary_grade: str,
) -> tuple[str, str]:
    if late_risk_label == "HIGH" or prepriced_label == "HIGH" or price_zone_label == "TOP_RISK":
        return "READONLY_CHASE_BLOCK", "late_buy_or_news_top_or_price_top_risk"
    if (
        event_decision == "EVENT_THEME_WATCH"
        and smart_money_label in {"STRONG", "SUPPORTED"}
        and beneficiary_grade in {"DIRECT_OR_STRONG", "INDIRECT"}
        and price_zone_label != "TOP_RISK"
    ):
        return "READONLY_PRIORITY_WATCH", "event_beneficiary_smart_money_supported"
    if smart_money_label in {"STRONG", "SUPPORTED"} and price_zone_label == "PULLBACK_OR_TURNING_WATCH":
        return "READONLY_MONEY_PULLBACK_WATCH", "smart_money_and_price_zone_supported"
    if event_decision in {"EVENT_THEME_WATCH", "EVENT_THEME_EXPLORE"} or smart_money_label in {"STRONG", "SUPPORTED"}:
        return "READONLY_EXPLORE", "one_or_more_axes_supported_but_not_complete"
    return "READONLY_DATA_LIMITED", "four_question_evidence_weak"


def _leader_laggard(row: dict[str, Any]) -> tuple[str, str]:
    current_pool = str(row.get("current_pool_sources") or row.get("source_tags") or "")
    ret = max(_float(row.get("ret1_pct"), 0.0), _float(row.get("change_pct"), 0.0))
    value = max(_float(row.get("trading_value"), 0.0), _float(row.get("value"), 0.0))
    if "market_rising" in current_pool or ret >= 20.0 or value >= 100_000_000_000:
        return "LEADER_OR_ACTIVE", "rising_or_large_value"
    if value >= 10_000_000_000 or ret >= 5.0:
        return "FOLLOWER_ACTIVE", "moderate_value_or_return"
    return "UNCONFIRMED", "no_leader_evidence"


def _decision(event_score: float, beneficiary_grade: str, risk_label: str) -> tuple[str, str]:
    if risk_label == "HIGH":
        return "WATCH_BLOCK_CHASE", "prepriced_or_high_risk"
    if event_score >= 60 and beneficiary_grade in {"DIRECT_OR_STRONG", "INDIRECT"}:
        return "EVENT_THEME_WATCH", "event_and_beneficiary_supported"
    if event_score >= 40:
        return "EVENT_THEME_EXPLORE", "needs_more_mapping_or_market_confirmation"
    return "DATA_LIMITED", "event_theme_evidence_weak"


def build() -> dict[str, Any]:
    candidates = _read_csv(CANDIDATES_CSV)
    news = _read_csv(NEWS_CANDIDATES_CSV)
    rising = _read_csv(MARKET_RISING_CSV)
    pullback = _read_csv(PULLBACK_SCREENER_CSV)
    surge = _read_csv(SURGE_EVENT_SCREENER_CSV)
    global_feed = _read_csv(GLOBAL_EVENT_FEED_CSV)
    supply_history, supply_history_files = _supply_history_by_code()

    candidate_by_code = _index_by_code(candidates)
    news_by_code = _index_by_code(news)
    rising_by_code = _index_by_code(rising)
    pullback_by_code = _index_by_code(pullback)
    surge_by_code = _index_by_code(surge)
    global_feed_by_code = _index_by_code(global_feed)
    codes = sorted(set().union(candidate_by_code, news_by_code, rising_by_code, pullback_by_code, surge_by_code, global_feed_by_code))

    out_rows: list[dict[str, Any]] = []
    for code in codes:
        row = _merge_rows(
            candidate_by_code.get(code, {}),
            news_by_code.get(code, {}),
            rising_by_code.get(code, {}),
            surge_by_code.get(code, {}),
            pullback_by_code.get(code, {}),
            global_feed_by_code.get(code, {}),
        )
        row.update(_supply_continuity_from_history(code, supply_history))
        row["code"] = code
        row["name"] = _first(row.get("name"), candidate_by_code.get(code, {}).get("name"), rising_by_code.get(code, {}).get("name"))
        theme, theme_hits = _theme_from_row(row)
        base_beneficiary_grade, base_beneficiary_evidence, base_beneficiary_score = _beneficiary_grade(row)
        mapping_label, mapping_score, mapping_evidence = _beneficiary_mapping(row, theme)
        beneficiary_grade, beneficiary_score, beneficiary_evidence = _combine_beneficiary(
            base_beneficiary_grade,
            base_beneficiary_score,
            base_beneficiary_evidence,
            mapping_label,
            mapping_score,
            mapping_evidence,
        )
        global_event_label, global_event_score, global_event_evidence, global_event_source = _global_event_axis(row, theme)
        score, event_evidence = _event_score(row, theme, beneficiary_score)
        prepriced_label, prepriced_score, prepriced_evidence = _prepriced_risk(row)
        smart_money_label, smart_money_score, smart_money_evidence = _smart_money_quality(row)
        continuity_label, continuity_score, continuity_evidence, continuity_source = _smart_money_continuity_proxy(row)
        late_risk_label, late_risk_score, late_risk_evidence = _late_buy_risk(row, prepriced_label, prepriced_score)
        price_zone_label, price_zone_score, price_zone_evidence = _price_zone(row, prepriced_label)
        leader_label, leader_evidence = _leader_laggard(row)
        decision, decision_reason = _decision(score, beneficiary_grade, prepriced_label)
        four_question_decision, four_question_reason = _four_question_decision(
            decision,
            smart_money_label,
            late_risk_label,
            prepriced_label,
            price_zone_label,
            beneficiary_grade,
        )
        out_rows.append(
            {
                "asof": _first(row.get("date"), row.get("as_of_ymd"), row.get("date_yyyymmdd")),
                "code": code,
                "name": row.get("name", ""),
                "theme_type": theme,
                "theme_hits": theme_hits,
                "global_event_label": global_event_label,
                "global_event_score": global_event_score,
                "global_event_evidence": global_event_evidence,
                "global_event_source": global_event_source,
                "global_event_feed_label": row.get("global_event_feed_label", ""),
                "global_event_feed_score": _float(row.get("global_event_feed_score"), 0.0),
                "global_event_feed_themes": row.get("global_event_feed_themes", ""),
                "global_event_feed_terms": row.get("global_event_feed_terms", ""),
                "event_score": score,
                "event_evidence": event_evidence,
                "beneficiary_grade": beneficiary_grade,
                "beneficiary_score": round(beneficiary_score, 3),
                "beneficiary_evidence": beneficiary_evidence,
                "base_beneficiary_grade": base_beneficiary_grade,
                "beneficiary_mapping_label": mapping_label,
                "beneficiary_mapping_score": mapping_score,
                "beneficiary_mapping_evidence": mapping_evidence,
                "leader_laggard_label": leader_label,
                "leader_laggard_evidence": leader_evidence,
                "news_prepriced_risk": prepriced_label,
                "news_prepriced_risk_score": prepriced_score,
                "news_prepriced_risk_evidence": prepriced_evidence,
                "late_buy_risk": late_risk_label,
                "late_buy_risk_score": late_risk_score,
                "late_buy_risk_evidence": late_risk_evidence,
                "smart_money_quality": smart_money_label,
                "smart_money_score": smart_money_score,
                "smart_money_evidence": smart_money_evidence,
                "smart_money_continuity_label": continuity_label,
                "smart_money_continuity_score": continuity_score,
                "smart_money_continuity_evidence": continuity_evidence,
                "smart_money_continuity_source": continuity_source,
                "raw_supply_history_status": row.get("raw_supply_history_status", ""),
                "raw_supply_history_days": row.get("raw_supply_history_days", 0),
                "raw_supply_latest_ymd": row.get("raw_supply_latest_ymd", ""),
                "foreign_consecutive_buy_days": row.get("foreign_consecutive_buy_days", 0),
                "institution_consecutive_buy_days": row.get("institution_consecutive_buy_days", 0),
                "smart_money_consecutive_buy_days": row.get("smart_money_consecutive_buy_days", 0),
                "price_zone_label": price_zone_label,
                "price_zone_score": price_zone_score,
                "price_zone_evidence": price_zone_evidence,
                "four_question_decision": four_question_decision,
                "four_question_reason": four_question_reason,
                "event_theme_decision": decision,
                "decision_reason": decision_reason,
                "news_score": _float(row.get("news_score"), 0.0),
                "news_article_count": _float(row.get("news_article_count"), 0.0),
                "trading_value": max(_float(row.get("trading_value"), 0.0), _float(row.get("value"), 0.0)),
                "flow_score": _float(row.get("flow_score"), 0.0),
                "foreign_net_20d": _float(row.get("foreign_net_20d"), 0.0),
                "institution_net_20d": _float(row.get("institution_net_20d"), 0.0),
                "ret_pct": max(_float(row.get("ret1_pct"), 0.0), _float(row.get("change_pct"), 0.0)),
                "candidate_sources": "|".join(
                    name
                    for name, exists in [
                        ("candidate", code in candidate_by_code),
                        ("news", code in news_by_code),
                        ("market_rising", code in rising_by_code),
                        ("surge_event", code in surge_by_code),
                        ("pullback_methodology", code in pullback_by_code),
                        ("global_event_feed", code in global_feed_by_code),
                    ]
                    if exists
                ),
                "research_only": True,
                "policy_change": False,
                "entry_approval_changed": False,
                "paper_order_route": False,
                "broker_order_route": False,
                "trading_route": False,
            }
        )

    theme_universe_counts = Counter(row["theme_type"] for row in out_rows if row["theme_type"] not in {"", "UNCLASSIFIED", "NEWS_UNCLASSIFIED"})
    for row in out_rows:
        theme_count = int(theme_universe_counts.get(row["theme_type"], 0))
        row["theme_universe_count"] = theme_count
        if theme_count == 0:
            row["theme_scarcity_label"] = "THEME_UNCLASSIFIED"
        elif theme_count <= 3:
            row["theme_scarcity_label"] = "SCARCE_RELATED_UNIVERSE_PROXY"
        elif theme_count <= 10:
            row["theme_scarcity_label"] = "MODERATE_RELATED_UNIVERSE_PROXY"
        else:
            row["theme_scarcity_label"] = "BROAD_RELATED_UNIVERSE_PROXY"

    out_rows.sort(key=lambda r: (-float(r["event_score"]), r["news_prepriced_risk"], r["code"]))
    fields = list(out_rows[0].keys()) if out_rows else [
        "asof",
        "code",
        "event_theme_decision",
    ]
    _write_csv(OUT_CSV, out_rows, fields)

    news_score_status = _read_json(NEWS_SCORE_STATUS_JSON)
    news_implication_status = _read_json(NEWS_IMPLICATION_STATUS_JSON)
    summary = {
        "generated_at": _now_ts(),
        "research_only": True,
        "policy_change": False,
        "entry_approval_changed": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "trading_route": False,
        "scope": "read_only_event_theme_candidate_layer",
        "source_files": {
            "candidates": str(CANDIDATES_CSV),
            "news_candidates": str(NEWS_CANDIDATES_CSV),
            "market_rising": str(MARKET_RISING_CSV),
            "pullback_methodology": str(PULLBACK_SCREENER_CSV),
            "surge_event": str(SURGE_EVENT_SCREENER_CSV),
            "global_event_feed": str(GLOBAL_EVENT_FEED_CSV),
            "news_score_status": str(NEWS_SCORE_STATUS_JSON),
            "news_implication_status": str(NEWS_IMPLICATION_STATUS_JSON),
            "supply_history_cache": str(CACHE_DIR / "pykrx_supply_YYYYMMDD.csv"),
        },
        "source_counts": {
            "candidates": len(candidates),
            "news_candidates": len(news),
            "market_rising": len(rising),
            "pullback_methodology": len(pullback),
            "surge_event": len(surge),
            "global_event_feed": len(global_feed),
            "supply_history_files": len(supply_history_files),
            "supply_history_codes": len(supply_history),
            "unique_codes": len(codes),
        },
        "news_score_quality": news_score_status.get("quality", ""),
        "news_score_asof": news_score_status.get("asof_ymd", ""),
        "news_implication_quality": news_implication_status.get("quality", ""),
        "news_implication_asof": news_implication_status.get("asof_ymd", ""),
        "decision_counts": dict(Counter(row["event_theme_decision"] for row in out_rows)),
        "four_question_decision_counts": dict(Counter(row["four_question_decision"] for row in out_rows)),
        "global_event_label_counts": dict(Counter(row["global_event_label"] for row in out_rows)),
        "theme_counts": dict(Counter(row["theme_type"] for row in out_rows)),
        "theme_scarcity_counts": dict(Counter(row["theme_scarcity_label"] for row in out_rows)),
        "beneficiary_counts": dict(Counter(row["beneficiary_grade"] for row in out_rows)),
        "beneficiary_mapping_counts": dict(Counter(row["beneficiary_mapping_label"] for row in out_rows)),
        "prepriced_risk_counts": dict(Counter(row["news_prepriced_risk"] for row in out_rows)),
        "late_buy_risk_counts": dict(Counter(row["late_buy_risk"] for row in out_rows)),
        "smart_money_quality_counts": dict(Counter(row["smart_money_quality"] for row in out_rows)),
        "smart_money_continuity_counts": dict(Counter(row["smart_money_continuity_label"] for row in out_rows)),
        "raw_supply_history_status_counts": dict(Counter(row["raw_supply_history_status"] for row in out_rows)),
        "price_zone_counts": dict(Counter(row["price_zone_label"] for row in out_rows)),
        "implementation_coverage": {
            "global_event_collection": "IMPLEMENTED_WITH_GOOGLE_RSS_FEED_ARTIFACT_AND_NEWS_THEME_POLICY_PROXY",
            "theme_classification": "IMPLEMENTED",
            "domestic_beneficiary_mapping": "IMPLEMENTED_AS_DIRECTNESS_AND_THEME_SECTOR_PROXY",
            "trading_value_reaction": "IMPLEMENTED",
            "foreign_institution_flow": "IMPLEMENTED_AS_NET_20D_AND_RECENT_PROXY",
            "foreign_institution_consecutive_days": "IMPLEMENTED_FROM_RAW_PYKRX_SUPPLY_CACHE_WHEN_AVAILABLE",
            "price_position_and_chase_block": "IMPLEMENTED",
            "leader_laggard": "IMPLEMENTED_AS_ACTIVITY_PROXY",
            "paper_or_live_trading": "NOT_IMPLEMENTED_READ_ONLY_ONLY",
            "forward_performance_validation": "WAITING_FOR_FORWARD_BARS",
        },
        "top_event_theme": out_rows[:30],
        "access_issues": [
            "theme classification is keyword/rule based and does not prove real direct beneficiary status",
            "global event collection uses latest available Google RSS probe artifact and does not fetch network in this tool",
            "foreign/institution continuity uses local pykrx supply cache only for codes and dates present in cache",
            "bottom/top logic is a risk-zone label, not exact bottom/top prediction",
            "news implication status is older than latest candidate asof when asof values differ",
        ],
    }
    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    return summary


def main() -> int:
    summary = build()
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
