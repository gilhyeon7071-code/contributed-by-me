"""H1 수집기 2 — 공시 제목만으로는 압력 크기를 모른다. 주요사항보고서 본문 수치(수량·금액·기간)를 받아 쌓는다.

    python -m paper.strategies.kospi_mcap_quarterly_v2.src.collect_event_details --events-dir <폴더> [--limit 200]

무엇을 / 어디서 / 언제 / 지연
  무엇   자기주식 취득(직접)·취득 신탁계약·처분·유상증자 결정의 **계획 수량·금액·기간**
  어디서 https://opendart.fss.or.kr/api/<사건별 API>.json  (2026-09-18 실물 확인)
           자기주식취득결정            tsstkAqDecsn        aqpln_stk_ostk 계획 주식수 / aqpln_prc_ostk 계획 금액 / aqexpd_bgd~edd 취득 예정기간
           자기주식취득신탁계약체결결정 tsstkAqTrctrCnsDecsn ctr_prc 계약금액 / ctr_pd_bgd~edd 계약기간
           자기주식처분결정            tsstkDpDecsn
           유상증자결정                piicDecsn
  언제   제목 수집(collect_dart_events) 뒤 언제든. 접수 당일이면 본문이 아직 없을 수 있어 **다음 날 한 번 더** 돌린다
  지연   접수 직후 조회되는 편이지만 보장 없음 → 못 받은 건은 NOT_FOUND 로 남기고 다시 시도한다

기록: `event_details.jsonl` (날짜 없는 append-only, event_id 로 중복 차단).
찾지 못한 건도 남긴다 — "안 찾아봤다" 와 "찾았는데 없다" 를 구분하려고.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

ROOT = Path(__file__).resolve().parents[4]
DEFAULT_KEY_FILE = ROOT / "_cache" / "dart_api_key.txt"
BASE = "https://opendart.fss.or.kr/api/{api}.json"

# 사건 종류 → (API, 수량 필드, 금액 필드, 기간 시작, 기간 끝)
DETAIL_API = {
    "BUYBACK_DIRECT": ("tsstkAqDecsn", "aqpln_stk_ostk", "aqpln_prc_ostk", "aqexpd_bgd", "aqexpd_edd"),
    "BUYBACK_TRUST": ("tsstkAqTrctrCnsDecsn", None, "ctr_prc", "ctr_pd_bgd", "ctr_pd_edd"),
    "BUYBACK_DISPOSAL": ("tsstkDpDecsn", "dppln_stk_ostk", "dpstk_prc_ostk", "dpprd_bgd", "dpprd_edd"),
    "RIGHTS_OFFERING": ("piicDecsn", "nstk_ostk_cnt", "fdpp_fclt", "sbd", "pymd"),
}


def _num(v: Any) -> Optional[int]:
    s = str(v or "").replace(",", "").strip()
    if not s or s in {"-", "0"} and str(v).strip() == "-":
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _ymd(v: Any) -> Optional[str]:
    s = str(v or "")
    m = re.search(r"(\d{4})\D+(\d{1,2})\D+(\d{1,2})", s)
    if m:
        return f"{m.group(1)}{int(m.group(2)):02d}{int(m.group(3)):02d}"
    m = re.fullmatch(r"\s*(\d{8})\s*", s)
    return m.group(1) if m else None


def fetch_detail(key: str, api: str, corp_code: str, ymd: str, session: requests.Session) -> Dict[str, Any]:
    try:
        r = session.get(BASE.format(api=api), params={"crtfc_key": key, "corp_code": corp_code,
                                                      "bgn_de": ymd, "end_de": ymd}, timeout=30)
        j = r.json()
    except Exception as e:
        return {"status": "FETCH_ERROR", "error": f"{type(e).__name__}:{e}"}
    st = str(j.get("status"))
    if st == "013":
        return {"status": "NOT_FOUND", "rows": []}
    if st != "000":
        return {"status": "API_ERROR", "error": f"{st}:{j.get('message')}"}
    return {"status": "OK", "rows": j.get("list") or []}


def normalize(event: Dict[str, Any], row: Dict[str, Any]) -> Dict[str, Any]:
    api, qty_f, amt_f, bgd_f, edd_f = DETAIL_API[event["event_type"]]
    return {"event_id": event["event_id"], "rcept_no": event["rcept_no"], "event_type": event["event_type"],
            "stock_code": event["stock_code"], "corp_name": event["corp_name"], "rcept_dt": event["rcept_dt"],
            "planned_qty": _num(row.get(qty_f)) if qty_f else None,
            "planned_amount": _num(row.get(amt_f)) if amt_f else None,
            "period_start": _ymd(row.get(bgd_f)), "period_end": _ymd(row.get(edd_f)),
            "method": row.get("aq_mth") or row.get("dp_mth") or None,
            "purpose": row.get("aq_pp") or None, "source_api": api, "raw": row}


def run(key: str, events_dir: Path, corp_by_rcept: Dict[str, str], now: datetime, limit: Optional[int] = None,
        session: Optional[requests.Session] = None) -> Dict[str, Any]:
    events_dir = Path(events_dir)
    ev_path = events_dir / "h1_events.jsonl"
    out_path = events_dir / "event_details.jsonl"
    events = [json.loads(x) for x in ev_path.read_text(encoding="utf-8").splitlines() if x.strip()]
    done = set()
    if out_path.exists():
        for line in out_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                d = json.loads(line)
                if d.get("status") in (None, "OK"):
                    done.add(d["event_id"])
    todo = [e for e in events if e["event_type"] in DETAIL_API and e["event_id"] not in done]
    if limit:
        todo = todo[:limit]
    s = session or requests.Session()
    res = {"run_at": now.isoformat(timespec="seconds"), "candidates": len(todo), "ok": 0, "not_found": 0,
           "error": 0, "status": "OK", "reasons": []}
    rows_out: List[Dict[str, Any]] = []
    for e in todo:
        api = DETAIL_API[e["event_type"]][0]
        corp = corp_by_rcept.get(e["rcept_no"])
        if not corp:
            res["error"] += 1
            res["reasons"].append(f"NO_CORP_CODE:{e['rcept_no']}")
            continue
        got = fetch_detail(key, api, corp, e["rcept_dt"], s)
        if got["status"] == "OK":
            match = [r for r in got["rows"] if str(r.get("rcept_no")) == e["rcept_no"]]
            if match:
                rows_out.append({**normalize(e, match[0]), "status": "OK",
                                 "observed_at": now.isoformat(timespec="seconds")})
                res["ok"] += 1
            else:  # 같은 날 다른 건만 옴 — 접수번호로만 귀속한다
                rows_out.append({"event_id": e["event_id"], "rcept_no": e["rcept_no"], "status": "NOT_MATCHED",
                                 "event_type": e["event_type"], "observed_at": now.isoformat(timespec="seconds")})
                res["not_found"] += 1
        elif got["status"] == "NOT_FOUND":
            rows_out.append({"event_id": e["event_id"], "rcept_no": e["rcept_no"], "status": "NOT_FOUND",
                             "event_type": e["event_type"], "observed_at": now.isoformat(timespec="seconds")})
            res["not_found"] += 1
        else:
            res["error"] += 1
            res["reasons"].append(f"{got['status']}:{e['event_id']}:{got.get('error', '')}")
            if len(res["reasons"]) >= 5:
                res["status"] = "STOP"
                break
        time.sleep(0.12)
    if rows_out:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("a", encoding="utf-8") as fh:
            for r in rows_out:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    if res["error"]:
        res["status"] = "STOP" if res["status"] == "STOP" else "WARN"
    with (events_dir / "collect_log.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"tool": "collect_event_details", **res}, ensure_ascii=False) + "\n")
    return res


def load_corp_map(events_dir: Path) -> Dict[str, str]:
    p = Path(events_dir) / "dart_disclosures.jsonl"
    out = {}
    for line in p.read_text(encoding="utf-8").splitlines():
        if line.strip():
            d = json.loads(line)
            out[d["rcept_no"]] = d["corp_code"]
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events-dir", required=True, type=Path)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--key-file", type=Path, default=DEFAULT_KEY_FILE)
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    from paper.strategies.kospi_mcap_quarterly_v2.src.collect_dart_events import resolve_key
    res = run(resolve_key(args.key_file), args.events_dir, load_corp_map(args.events_dir), datetime.now(),
              limit=args.limit or None)
    print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0 if res["status"] == "OK" else 3


if __name__ == "__main__":
    raise SystemExit(main())
