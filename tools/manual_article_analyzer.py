from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple


ROOT = Path("E:/1_Data")
LOG_DIR = ROOT / "2_Logs"

POSITIVE_TERMS = {
    "계약": 1.0,
    "공급": 0.8,
    "수주": 1.2,
    "흑자": 1.0,
    "실적": 0.6,
    "증가": 0.5,
    "성장": 0.7,
    "승인": 0.8,
    "허가": 0.8,
    "특허": 0.7,
    "투자": 0.5,
    "인수": 0.5,
    "합병": 0.5,
    "상향": 0.7,
    "돌파": 0.5,
    "최대": 0.4,
    "호조": 0.8,
}

NEGATIVE_TERMS = {
    "적자": -1.2,
    "감소": -0.6,
    "하락": -0.5,
    "급락": -1.0,
    "손실": -0.9,
    "소송": -1.1,
    "제재": -1.0,
    "압수수색": -1.4,
    "횡령": -1.5,
    "배임": -1.5,
    "상장폐지": -1.8,
    "거래정지": -1.7,
    "감사의견": -1.2,
    "유상증자": -0.8,
    "전환사채": -0.6,
    "CB": -0.6,
    "불성실": -1.0,
    "경고": -0.7,
    "리콜": -1.0,
}

RISK_TERMS = {
    "상장폐지": "listing_risk",
    "거래정지": "trading_halt",
    "횡령": "governance_risk",
    "배임": "governance_risk",
    "감사의견": "audit_risk",
    "불성실": "disclosure_risk",
    "유상증자": "dilution_risk",
    "전환사채": "dilution_risk",
    "CB": "dilution_risk",
    "소송": "legal_risk",
    "제재": "regulatory_risk",
    "압수수색": "investigation_risk",
}

EVENT_TERMS = {
    "계약": "contract",
    "공급": "supply",
    "수주": "order_win",
    "실적": "earnings",
    "승인": "approval",
    "허가": "approval",
    "특허": "patent",
    "투자": "investment",
    "인수": "mna",
    "합병": "mna",
    "유상증자": "financing",
    "전환사채": "financing",
    "CB": "financing",
}


def _read_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8-sig", errors="replace")


def _read_json(path: str) -> Dict[str, object]:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _normalize_code(code: str) -> str:
    digits = re.sub(r"\D", "", str(code or ""))
    return digits.zfill(6)[-6:] if digits else ""


def _count_terms(text: str, terms: Dict[str, float]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for term, weight in terms.items():
        count = len(re.findall(re.escape(term), text, flags=re.IGNORECASE))
        if count:
            rows.append({"term": term, "count": count, "weight": weight, "score": round(float(count) * float(weight), 6)})
    return sorted(rows, key=lambda r: abs(float(r["score"])), reverse=True)


def _collect_labels(text: str, labels: Dict[str, str]) -> List[str]:
    out = []
    for term, label in labels.items():
        if re.search(re.escape(term), text, flags=re.IGNORECASE):
            out.append(label)
    return sorted(set(out))


def _score(text: str) -> Tuple[float, List[Dict[str, object]], List[Dict[str, object]]]:
    pos = _count_terms(text, POSITIVE_TERMS)
    neg = _count_terms(text, NEGATIVE_TERMS)
    raw = sum(float(r["score"]) for r in pos) + sum(float(r["score"]) for r in neg)
    normalized = max(-1.0, min(1.0, raw / 5.0))
    return round(normalized, 6), pos, neg


def _sentiment_label(score: float) -> str:
    if score >= 0.25:
        return "positive"
    if score <= -0.25:
        return "negative"
    return "neutral"


def _decision_hint(score: float, risk_labels: List[str]) -> str:
    hard_risks = {"listing_risk", "trading_halt", "governance_risk", "audit_risk", "investigation_risk"}
    if hard_risks.intersection(risk_labels):
        return "manual_review_high_risk"
    if score <= -0.25:
        return "manual_review_negative"
    if score >= 0.25:
        return "manual_review_positive"
    return "manual_review_neutral"


def analyze_article(code: str, title: str, body: str, url: str = "", source: str = "manual") -> Dict[str, object]:
    text = f"{title}\n{body}".strip()
    score, positive_hits, negative_hits = _score(text)
    risk_labels = _collect_labels(text, RISK_TERMS)
    event_labels = _collect_labels(text, EVENT_TERMS)
    return {
        "generated_at_local": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": "manual_article_review",
        "policy": "observe_only_no_gate_no_score_write",
        "code": _normalize_code(code),
        "source": source,
        "url": url,
        "title": title,
        "body_length": len(body),
        "sentiment_score": score,
        "sentiment_label": _sentiment_label(score),
        "event_labels": event_labels,
        "risk_labels": risk_labels,
        "decision_hint": _decision_hint(score, risk_labels),
        "positive_hits": positive_hits,
        "negative_hits": negative_hits,
        "notes": [
            "This report is for manual review only.",
            "It does not update trading gates, candidate scores, news_score, or ledger data.",
        ],
    }


def _write_report(report: Dict[str, object], out_json: str = "") -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = Path(out_json) if out_json else LOG_DIR / f"manual_article_analysis_{stamp}.json"
    latest = LOG_DIR / "manual_article_analysis_latest.json"
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    path.write_text(payload, encoding="utf-8")
    latest.write_text(payload, encoding="utf-8")
    return path


def main() -> int:
    ap = argparse.ArgumentParser(description="Manual article analyzer for observe-only news review")
    ap.add_argument("--code", default="")
    ap.add_argument("--title", default="")
    ap.add_argument("--body", default="")
    ap.add_argument("--body-file", default="")
    ap.add_argument("--title-file", default="")
    ap.add_argument("--input-json", default="")
    ap.add_argument("--url", default="")
    ap.add_argument("--source", default="manual")
    ap.add_argument("--out-json", default="")
    args = ap.parse_args()

    code = args.code
    title = args.title
    body = args.body
    url = args.url
    source = args.source
    if args.input_json:
        payload = _read_json(args.input_json)
        code = str(payload.get("code") or code)
        title = str(payload.get("title") or title)
        body = str(payload.get("body") or body)
        url = str(payload.get("url") or url)
        source = str(payload.get("source") or source)
    if args.title_file:
        title = _read_text(args.title_file).strip()
    if args.body_file:
        body = _read_text(args.body_file)
    if not title and not body:
        raise SystemExit("[FAILED] title or body is required")

    report = analyze_article(code=code, title=title, body=body, url=url, source=source)
    out = _write_report(report, args.out_json)
    print(f"[MANUAL_ARTICLE] report={out}")
    print(
        "[MANUAL_ARTICLE] code={code} sentiment={sentiment_label} score={sentiment_score} hint={decision_hint}".format(
            **report
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
