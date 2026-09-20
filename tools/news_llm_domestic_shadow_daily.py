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
import csv


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
        "max_tokens": 400,
        "response_format": {"type": "json_object"}
    }
    
    req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"), headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data["choices"][0]["message"]["content"]
    except Exception as e:
        return ""


def run_shadow_domestic_llm(test_prompt: bool = False) -> Dict[str, Any]:
    now = _now_kst()
    status = {
        "generated_at": now.isoformat(timespec="seconds"),
        "run_mode": "test_prompt" if test_prompt else "daily",
        "scope": "candidate_news_only",
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

    feed_data = []
    if test_prompt:
        feed_data = [
            {"name": "삼성전자", "news_score": "0.85", "news_entity_top_tags": "AI:5|반도체:3", "news_implication_top_actions": "boost:3"},
            {"name": "현대차", "news_score": "0.60", "news_entity_top_tags": "전기차:2|수출:2", "news_implication_top_actions": "boost:1"},
            {"name": "LG에너지솔루션", "news_score": "-0.50", "news_entity_top_tags": "배터리:3|화재:1", "news_implication_top_actions": "penalize:2"}
        ]
    else:
        feed_path = LOGS / "candidates_latest_data.with_news_score.csv"
        if feed_path.exists():
            try:
                with feed_path.open("r", encoding="utf-8-sig", newline="") as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                    # Filter rows with significant news scores
                    scored_rows = []
                    for row in rows:
                        score_str = row.get("news_score", "0")
                        try:
                            score = float(score_str)
                            if abs(score) > 0.1:
                                scored_rows.append(row)
                        except ValueError:
                            pass
                    
                    # Sort by absolute news score descending
                    scored_rows.sort(key=lambda x: abs(float(x.get("news_score", "0"))), reverse=True)
                    
                    # Take top 15 for summary
                    for row in scored_rows[:15]:
                        feed_data.append({
                            "name": row.get("name", ""),
                            "news_score": row.get("news_score", ""),
                            "news_entity_top_tags": row.get("news_entity_top_tags", ""),
                            "news_implication_top_actions": row.get("news_implication_top_actions", "")
                        })
            except Exception as e:
                status["errors"].append(str(e))

    if not feed_data:
        status["success"] = True
        status["reason"] = "no_data"
        return status

    sys_prompt = """You are a financial analyst summarizing candidate-stock news signals in Korea. The input is limited to individual candidate news scores and tags; it is not KOSPI/KOSDAQ index data, market breadth, sector return data, or investor-flow data.
Output valid JSON containing:
- "market_status": (str) BULL, BEAR, or NEUTRAL for candidate-news sentiment only
- "top_keywords": (list of str) Up to 5 keywords that appear in the supplied tags
- "summary": (str) One Korean sentence beginning exactly with "뉴스 후보군 기준:". Do not claim the overall domestic market is rising, falling, mixed, or sustained. Do not claim sector-wide performance unless the supplied tags explicitly support it.
"""
    
    text_to_eval = "\n".join(f"- {row.get('name')} | Score: {row.get('news_score')} | Tags: {row.get('news_entity_top_tags')} | Implication: {row.get('news_implication_top_actions')}" for row in feed_data)
    prompt = f"Evaluate the following top domestic news signals:\n{text_to_eval}"

    llm_out = _call_openai(prompt, sys_prompt, 30)
    
    try:
        parsed = json.loads(llm_out) if llm_out else {}
        if isinstance(parsed, dict):
            parsed["scope"] = "candidate_news_only"
            parsed["input_rows"] = len(feed_data)
            parsed["candidate_news_summary"] = str(parsed.get("summary") or "").strip()
            parsed["summary"] = "뉴스 후보군 기준: 개별 후보 뉴스 신호 관찰 결과이며 국내 지수·업종 성과 판단에는 사용하지 않습니다."
        status["results"].append(parsed)
        status["success"] = True
    except Exception as e:
        status["errors"].append(f"JSON parsing error: {e}")

    out_path = LOGS / "news_llm_domestic_shadow_latest.json"
    with out_path.open("w", encoding="utf-8-sig") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)

    return status


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--test-prompt", action="store_true", help="Run with a hardcoded test prompt")
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    summary = run_shadow_domestic_llm(test_prompt=args.test_prompt)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())


