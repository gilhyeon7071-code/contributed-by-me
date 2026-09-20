from __future__ import annotations

import argparse
import json
import os
import time
import urllib.parse
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
KST = timezone(timedelta(hours=9))

OPENAI_KEY_FILES = [
    ROOT / ".secrets" / "openai_api_key.txt",
    ROOT / "_secrets" / "openai_api_key.txt",
    ROOT / "_cache" / "openai_api_key.txt",
]


def _now_kst() -> datetime:
    return datetime.now(KST)


def _api_key() -> str:
    key = str(os.getenv("OPENAI_API_KEY", "")).strip()
    if key:
        return key
    for path in OPENAI_KEY_FILES:
        try:
            if path.exists():
                key = path.read_text(encoding="utf-8").replace("\ufeff", "").strip()
                if key:
                    return key
        except Exception:
            continue
    return ""


def _call_openai(prompt: str, sys_prompt: str, timeout_sec: int) -> str:
    key = _api_key()
    if not key:
        return ""
    
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {key}",
    }
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.0,
        "max_tokens": 300,
        "response_format": {"type": "json_object"}
    }
    
    req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"), headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data["choices"][0]["message"]["content"]
    except Exception as e:
        return ""


def _as_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    text = str(value).strip()
    if not text:
        return []
    return [part.strip() for part in text.replace(";", ",").split(",") if part.strip()]


def _global_market_mode(macro_scope: str) -> str:
    scope = str(macro_scope or "").upper()
    if scope in {"GLOBAL_RISK_OFF", "REGIONAL_RISK", "RISK_OFF"}:
        return "RISK_OFF"
    if scope in {"GLOBAL_RISK_ON", "REGIONAL_RISK_ON", "RISK_ON", "BULL"}:
        return "RISK_ON"
    return "NEUTRAL"


def _macro_label(macro_scope: str) -> str:
    scope = str(macro_scope or "").upper()
    labels = {
        "GLOBAL_RISK_OFF": "글로벌 위험회피",
        "REGIONAL_RISK": "아시아권 위험 부담",
        "RISK_OFF": "위험회피",
        "GLOBAL_RISK_ON": "글로벌 위험선호",
        "REGIONAL_RISK_ON": "아시아권 위험선호",
        "RISK_ON": "위험선호",
        "BULL": "위험선호",
        "NEUTRAL": "중립",
    }
    return labels.get(scope, "중립")


def _sector_labels(sectors: List[str]) -> List[str]:
    labels = {
        "TECH_INNOVATION": "기술주",
        "SEMICONDUCTOR": "반도체",
        "DEFENSE": "방산",
        "ALL": "증시 전반",
    }
    out: List[str] = []
    for sector in sectors:
        key = str(sector or "").upper()
        out.append(labels.get(key, str(sector).strip()))
    return [item for item in dict.fromkeys(out) if item]


def _evidence_labels(evidence: str) -> List[str]:
    text = str(evidence or "").lower()
    labels: List[str] = []
    if "capital outflow" in text or "dump" in text or "foreign" in text:
        labels.append("외국인 자금 이탈")
    if "ai" in text or "tech" in text or "semiconductor" in text:
        labels.append("기술주 수급 부담")
    if "profit" in text or "crowded" in text:
        labels.append("차익실현 압력")
    return list(dict.fromkeys(labels))


def _build_display_summary(result: Dict[str, Any]) -> str:
    macro_scope = str(result.get("macro_scope") or "NEUTRAL").upper()
    mode = _global_market_mode(macro_scope)
    evidence = str(result.get("evidence") or "").strip()
    sectors = _sector_labels(_as_list(result.get("sector_scope")))
    sector_text = ", ".join(sectors) if sectors else "증시 전반"
    evidence_text = ", ".join(_evidence_labels(evidence)) or "뚜렷한 방향성 제한"
    macro_text = _macro_label(macro_scope)

    if mode == "RISK_OFF":
        return f"해외 증시는 {macro_text} 흐름입니다. {sector_text}에 부담이 관찰되며, 국내 증시에는 경계 요인으로 봅니다. 핵심 신호는 {evidence_text}입니다."
    if mode == "RISK_ON":
        return f"해외 증시는 {macro_text} 흐름입니다. {sector_text} 중심으로 우호적 신호가 관찰되며, 국내 증시에는 긍정 요인으로 봅니다. 핵심 신호는 {evidence_text}입니다."
    return f"해외 증시는 뚜렷한 방향성이 제한된 중립 흐름입니다. 확인 범위는 {sector_text}이며, 핵심 신호는 {evidence_text}입니다."


def _with_display_contract(result: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(result, dict):
        result = {}
    out = dict(result)
    out["market_mode"] = str(out.get("market_mode") or _global_market_mode(str(out.get("macro_scope") or "NEUTRAL"))).upper()
    out["summary"] = _build_display_summary(out)
    mode_keyword = {"RISK_OFF": "위험회피", "RISK_ON": "위험선호"}.get(out["market_mode"], "중립")
    keywords = [mode_keyword]
    keywords.extend(_sector_labels(_as_list(out.get("sector_scope"))))
    out["top_keywords"] = [kw for kw in keywords if kw]
    return out

def run_shadow_llm(test_prompt: bool = False) -> Dict[str, Any]:
    now = _now_kst()
    status = {
        "generated_at": now.isoformat(timespec="seconds"),
        "run_mode": "test_prompt" if test_prompt else "daily",
        "success": False,
        "results": [],
        "errors": [],
        "gate_effect": False,
        "score_effect": False,
        "trading_effect": False,
        "policy_effect": False,
        "no_order_effect": False,
        "execution_allowed": False
    }

    if test_prompt:
        feed_data = [
            {"name": "foreigners dump asia stocks", "evidence": "Foreign investors sold Asian equities at the fastest pace in 16 years due to crowded AI trades."}
        ]
    else:
        feed_path = LOGS / "global_event_feed_latest.json"
        if feed_path.exists():
            try:
                feed = json.loads(feed_path.read_text(encoding="utf-8-sig"))
                feed_data = feed.get("top_rows", [])[:5]
            except Exception as e:
                feed_data = []
                status["errors"].append(str(e))
        else:
            feed_data = []

    if not feed_data:
        status["success"] = True
        status["reason"] = "no_data"
        return status

    sys_prompt = """You are a financial risk analyst evaluating global macro news headlines for the Korean stock market.
Focus on identifying "Foreign capital outflow (외국인 이탈)", "Sector rotation (섹터 로테이션)", or "AI profit-taking (차익실현)".
Output valid JSON containing:
- "macro_scope": (str) GLOBAL_RISK_OFF, REGIONAL_RISK, NEUTRAL
- "sector_scope": (str) List affected sectors (e.g., SEMICONDUCTOR, DEFENSE, ALL)
- "evidence": (str) Brief reason
- "confidence": (float) 0.0 to 1.0
- "market_mode": (str) RISK_OFF, RISK_ON, NEUTRAL
- "summary": (str) Korean one-sentence dashboard summary
- "top_keywords": (list[str]) Short dashboard keywords
"""
    
    text_to_eval = "\n".join(f"- {row.get('name')} | {row.get('global_event_feed_themes', '')} | {row.get('global_event_feed_terms', '')}" for row in feed_data)
    prompt = f"Evaluate the following recent global macro feeds:\n{text_to_eval}"

    llm_out = _call_openai(prompt, sys_prompt, 30)
    
    try:
        parsed = json.loads(llm_out) if llm_out else {}
        parsed = _with_display_contract(parsed)
        status["results"].append(parsed)
        status["success"] = True
    except Exception as e:
        status["errors"].append(f"JSON parsing error: {e}")

    out_path = LOGS / "news_llm_global_shadow_latest.json"
    with out_path.open("w", encoding="utf-8-sig") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)

    return status


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--test-prompt", action="store_true", help="Run with a hardcoded test prompt")
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    summary = run_shadow_llm(test_prompt=args.test_prompt)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
