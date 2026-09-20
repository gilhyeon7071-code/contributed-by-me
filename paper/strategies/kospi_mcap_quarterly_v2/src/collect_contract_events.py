"""H6 공급계약 — 계약 규모가 회사에 비해 클 때. 사전 등록한 조건(시총 대비 10% 이상)으로만 잰다.

    python -m paper.strategies.kospi_mcap_quarterly_v2.src.collect_contract_events --events-dir <폴더> --universe <input csv>

왜 이 후보인가 (2026-09-18 검정력 계산)
  공급계약 공시는 208건이 50일에 흩어져 있어요. 날짜 기준 최소 탐지 1.4%p 라
  **큰 계약만 골라야** 지금 표본으로 보일 가능성이 있어요.

어디서
  공시 목록은 이미 쌓은 DART 원본. 계약금액은 **공시 원문**에서 뽑아요
  `https://opendart.fss.or.kr/api/document.xml?rcept_no=...` → ZIP 안 XML → 태그 제거 후 본문 파싱
  (2026-09-18 확인: "계약금액(원) 24,200,000,000 최근매출액(원) ... 매출액대비(%) 5.32")

기록
  `contract_details.jsonl` — 원문에서 뽑은 수치(append-only, 접수번호로 중복 차단)
  `h6_events.jsonl`        — 사건 목록(파생). 시총 대비 비율 포함
"""
from __future__ import annotations

import argparse
import io
import json
import re
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

ROOT = Path(__file__).resolve().parents[4]
DEFAULT_KEY_FILE = ROOT / "_cache" / "dart_api_key.txt"
DOC_URL = "https://opendart.fss.or.kr/api/document.xml"
TITLE = "단일판매"


def fetch_document(key: str, rcept_no: str, session: requests.Session) -> Dict[str, Any]:
    try:
        r = session.get(DOC_URL, params={"crtfc_key": key, "rcept_no": rcept_no}, timeout=30)
    except Exception as e:
        return {"status": "FETCH_ERROR", "error": f"{type(e).__name__}:{e}"}
    if r.status_code >= 400:
        return {"status": f"HTTP_{r.status_code}"}
    try:
        z = zipfile.ZipFile(io.BytesIO(r.content))
        txt = z.read(z.namelist()[0]).decode("utf-8", "replace")
    except Exception:
        return {"status": "NOT_ZIP", "body": r.text[:200]}
    plain = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", txt))
    return {"status": "OK", "text": plain}


def parse_contract(text: str) -> Dict[str, Any]:
    """계약금액·최근매출액·매출액대비. 못 찾으면 None — 0 으로 채우지 않는다."""
    def num(pat: str) -> Optional[float]:
        m = re.search(pat, text)
        if not m:
            return None
        s = m.group(1).replace(",", "").strip()
        try:
            return float(s)
        except ValueError:
            return None
    return {"contract_amount": num(r"계약금액\(원\)\s*([\d,]+)"),
            "recent_sales": num(r"최근매출액\(원\)\s*([\d,]+)"),
            "sales_ratio_pct": num(r"매출액대비\(%\)\s*([\d.,]+)"),
            "contract_kind": (re.search(r"판매ㆍ공급계약 구분\s*(\S+)", text) or [None, None])[1]
            if re.search(r"판매ㆍ공급계약 구분\s*(\S+)", text) else None}


def run(key: str, events_dir: Path, now: datetime, session: Optional[requests.Session] = None,
        limit: Optional[int] = None) -> Dict[str, Any]:
    events_dir = Path(events_dir)
    raw = [json.loads(x) for x in (events_dir / "dart_disclosures.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    targets = [r for r in raw if TITLE in str(r.get("report_nm") or "") and "정정" not in str(r.get("report_nm") or "")
               and r.get("stock_code")]
    out_path = events_dir / "contract_details.jsonl"
    done = set()
    if out_path.exists():
        for line in out_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                done.add(json.loads(line)["rcept_no"])
    todo = [r for r in targets if r["rcept_no"] not in done]
    if limit:
        todo = todo[:limit]
    s = session or requests.Session()
    res = {"run_at": now.isoformat(timespec="seconds"), "targets": len(targets), "todo": len(todo),
           "ok": 0, "failed": [], "status": "OK"}
    rows = []
    for r in todo:
        got = fetch_document(key, r["rcept_no"], s)
        if got["status"] != "OK":
            res["failed"].append({"rcept_no": r["rcept_no"], "status": got["status"]})
            continue
        parsed = parse_contract(got["text"])
        rows.append({"rcept_no": r["rcept_no"], "rcept_dt": r["rcept_dt"], "stock_code": r["stock_code"],
                     "corp_name": r["corp_name"], **parsed, "observed_at": now.isoformat(timespec="seconds")})
        res["ok"] += 1
        time.sleep(0.05)
    if rows:
        with out_path.open("a", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    if res["failed"]:
        res["status"] = "WARN"
    return res


def build_events(events_dir: Path, caps: Dict[str, float], now: datetime, min_ratio: float) -> Dict[str, Any]:
    events_dir = Path(events_dir)
    det = [json.loads(x) for x in (events_dir / "contract_details.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    out, no_amt, no_cap = [], 0, 0
    for d in det:
        amt = d.get("contract_amount")
        cap = caps.get(d["stock_code"])
        if not amt:
            no_amt += 1
            continue
        if not cap:
            no_cap += 1
            continue
        ratio = amt / cap
        out.append({"event_id": f"{d['rcept_no']}|CONTRACT", "rcept_no": d["rcept_no"], "rcept_dt": d["rcept_dt"],
                    "stock_code": d["stock_code"], "corp_name": d.get("corp_name"), "event_type": "CONTRACT_BIG"
                    if ratio >= min_ratio else "CONTRACT_SMALL", "direction": "INFO", "is_amendment": False,
                    "contract_amount": amt, "cap_ratio": ratio, "sales_ratio_pct": d.get("sales_ratio_pct"),
                    "rules_version": "h6", "observed_at": now.isoformat(timespec="seconds")})
    path = events_dir / "h6_events.jsonl"
    with path.open("w", encoding="utf-8") as fh:
        for r in out:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    big = [r for r in out if r["event_type"] == "CONTRACT_BIG"]
    return {"events": len(out), "big": len(big), "big_days": len({r["rcept_dt"] for r in big}),
            "big_codes": len({r["stock_code"] for r in big}), "no_amount": no_amt, "no_cap": no_cap,
            "min_ratio": min_ratio}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events-dir", required=True, type=Path)
    ap.add_argument("--universe", required=True, type=Path)
    ap.add_argument("--min-ratio", type=float, default=0.10, help="사전 등록 조건: 계약금액 ÷ 시총")
    ap.add_argument("--key-file", type=Path, default=DEFAULT_KEY_FILE)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--build-only", action="store_true")
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    import pandas as pd
    uni = pd.read_csv(args.universe, dtype={"code": str})
    uni = uni[uni["market"] == "KOSPI"]
    caps = {str(c): float(m) for c, m in zip(uni["code"], uni["market_cap"]) if m and m > 0}
    from paper.strategies.kospi_mcap_quarterly_v2.src.collect_dart_events import resolve_key
    now = datetime.now()
    res = {}
    if not args.build_only:
        res = run(resolve_key(args.key_file), args.events_dir, now, limit=args.limit or None)
    built = build_events(args.events_dir, caps, now, args.min_ratio)
    print(json.dumps({**res, "failed": (res.get("failed") or [])[:3], "built": built}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
