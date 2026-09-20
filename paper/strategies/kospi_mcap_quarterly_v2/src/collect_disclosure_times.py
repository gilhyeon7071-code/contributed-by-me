"""H1 수집기 5 — 공시가 **몇 시에** 났는지. 장중이면 그날 살 수 있고, 장후면 다음 날이 되어야 한다.

    python -m paper.strategies.kospi_mcap_quarterly_v2.src.collect_disclosure_times --events-dir <폴더>

무엇을 / 어디서
  KIND(kind.krx.co.kr) 당일 공시 목록 — 시각·회사·제목·접수번호를 한 화면에 준다.
  DART 목록 API 는 시각을 안 준다(2026-09-18 확인). DART 뷰어 페이지에도 시각 표시가 없다.

  **KIND 는 거래소 접수분만 싣는다.** 우리 사건의 상당수는 금감원 접수(주요사항보고서)라 접수번호가 목록에 없다
  (2026-09-18 실측: 자기주식 62건 중 접수번호로 맞은 것 17건). 그래서 두 단계로 맞춘다.
    match_method = RCEPT_NO   접수번호 일치 — 확실
                 = NAME_TITLE 같은 날·같은 회사·제목 핵심어 일치 — **보조**(시각 용도로만, 금액 귀속엔 쓰지 않음)
  두 방법 다 실패하면 그 사건은 시각 미상으로 남는다(추정하지 않는다).

왜 필요한가
  지금 우리 측정은 t0 = 접수일 **다음 거래일**이에요. 장중 공시였다면 하루 늦게 들어간 셈이라
  먹을 수 있는 구간을 놓쳤을 수 있어요. 반대로 장후 공시면 다음 날이 맞아요.

기록
  `kind_disclosures.jsonl`  — **그날 목록 전부**(시각·회사·제목·접수번호). 우리 사건과 무관한 것도 저장한다.
                              시장조치(투자경고·단기과열·거래정지)처럼 다른 가설의 재료가 여기 들어 있고,
                              버리면 다시 못 구한다. 날짜 없는 append-only, KIND 접수번호로 중복 차단
  `disclosure_times.jsonl`  — 우리 사건에 붙인 시각(파생). 위 원본에서 다시 만들 수 있다
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

KIND_URL = "https://kind.krx.co.kr/disclosure/todaydisclosure.do"
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://kind.krx.co.kr/"}
ROW_RE = re.compile(
    r'<td class="first txc">(\d{2}:\d{2})</td>.*?title=\'([^\']*)\'.*?openDisclsViewer\(\'(\d{14})\',\s*\'\'\).*?title=\'([^\']*)\'',
    re.S)
TITLE_KEYS = {
    "BUYBACK_DIRECT": ["자기주식취득결정"],
    "BUYBACK_TRUST": ["자기주식취득신탁계약체결"],
    "BUYBACK_DISPOSAL": ["자기주식처분결정"],
    "RIGHTS_OFFERING": ["유상증자결정"],
    "CB_BW": ["전환사채", "신주인수권부사채"],
}
PAGE_SIZE = 100
MAX_PAGES = 20


def parse_rows(html: str) -> List[Dict[str, str]]:
    """시각 + 회사 + 접수번호 + 제목. 화면 구조가 바뀌면 0건이 나오므로 호출 쪽에서 0건을 실패로 다룬다."""
    return [{"time": t, "corp_name": corp.strip(), "rcept_no": no, "title": title.strip()}
            for t, corp, no, title in ROW_RE.findall(html)]


def fetch_day(ymd: str, session: Optional[requests.Session] = None, market: str = "1") -> Dict[str, Any]:
    s = session or requests.Session()
    sel = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}"
    rows: List[Dict[str, str]] = []
    lost = 0
    for page in range(1, MAX_PAGES + 1):
        try:
            r = s.post(KIND_URL, headers=HEADERS, timeout=30, data={
                "method": "searchTodayDisclosureSub", "currentPageSize": str(PAGE_SIZE), "pageIndex": str(page),
                "orderMode": "0", "orderStat": "D", "forward": "todaydisclosure_sub", "chose": "S",
                "todayFlag": "N", "selDate": sel, "marketType": market})
        except Exception as e:
            return {"status": "FETCH_ERROR", "error": f"{type(e).__name__}:{e}", "rows": rows}
        if r.status_code >= 400:
            return {"status": f"HTTP_{r.status_code}", "rows": rows}
        raw = r.text.count("openDisclsViewer(")   # 페이지 넘김은 **원본 행 수**로 — 파서가 놓쳐도 멈추지 않게
        got = parse_rows(r.text)
        lost += raw - len(got)
        rows.extend(got)
        if raw < PAGE_SIZE:
            break
        time.sleep(0.2)
    return {"status": "OK" if rows else "NO_ROWS", "rows": rows, "ymd": ymd, "parse_lost": lost}


def _row(e: Dict[str, Any], row: Dict[str, str], method: str, now: datetime) -> Dict[str, Any]:
    return {"rcept_no": e["rcept_no"], "rcept_dt": e["rcept_dt"], "event_id": e["event_id"],
            "event_type": e["event_type"], "stock_code": e["stock_code"], "corp_name": e.get("corp_name"),
            "time": row["time"], "session": classify_time(row["time"]), "match_method": method,
            "kind_rcept_no": row["rcept_no"], "kind_title": row.get("title"),
            "observed_at": now.isoformat(timespec="seconds")}


def classify_time(hhmm: str) -> str:
    """장중(09:00~15:30) / 장전 / 장후. 접수 시각 기준이라 실제 공시 노출과 몇 분 차이는 있을 수 있어요."""
    if hhmm < "09:00":
        return "BEFORE_OPEN"
    if hhmm <= "15:30":
        return "INTRADAY"
    return "AFTER_CLOSE"


def run(events_dir: Path, now: datetime, session: Optional[requests.Session] = None,
        limit_days: Optional[int] = None, extra_days: Optional[set] = None) -> Dict[str, Any]:
    events_dir = Path(events_dir)
    out_path = events_dir / "disclosure_times.jsonl"
    events = [json.loads(x) for x in (events_dir / "h1_events.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    want = {e["rcept_no"]: e for e in events}
    have = set()
    if out_path.exists():
        for line in out_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                have.add(json.loads(line)["rcept_no"])
    raw_path = events_dir / "kind_disclosures.jsonl"
    raw_have = set()
    raw_days = set()
    if raw_path.exists():
        for line in raw_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                r = json.loads(line)
                raw_have.add(r["rcept_no"])
                raw_days.add(r["ymd"])
    # 시각이 아직 없는 사건의 날 + 원본을 아직 안 받은 날 둘 다 받는다
    need_time = {e["rcept_dt"] for e in events if e["rcept_no"] not in have}
    need_raw = {e["rcept_dt"] for e in events} - raw_days      # 원본을 아직 안 받은 날
    days = sorted(need_time | need_raw | (extra_days or set()))
    if limit_days:
        days = days[:limit_days]
    res = {"run_at": now.isoformat(timespec="seconds"), "days": len(days), "matched": 0, "by_method": {},
           "day_fail": [], "status": "OK"}
    s = session or requests.Session()
    new: List[Dict[str, Any]] = []
    for ymd in days:
        day = fetch_day(ymd, s)
        if day["status"] != "OK":
            res["day_fail"].append({"ymd": ymd, "status": day["status"]})
            continue
        res["parse_lost"] = res.get("parse_lost", 0) + int(day.get("parse_lost", 0))
        raw_new = [{**row, "ymd": ymd, "session": classify_time(row["time"]),
                    "observed_at": now.isoformat(timespec="seconds")}
                   for row in day["rows"] if row["rcept_no"] not in raw_have]
        for row in raw_new:
            raw_have.add(row["rcept_no"])
        if raw_new:
            with raw_path.open("a", encoding="utf-8") as fh:
                for row in raw_new:
                    fh.write(json.dumps(row, ensure_ascii=False) + chr(10))
        res["raw_new"] = res.get("raw_new", 0) + len(raw_new)
        day_events = [e for e in events if e["rcept_dt"] == ymd and e["rcept_no"] not in have]
        for row in day["rows"]:
            rc = row["rcept_no"]
            if rc in want and rc not in have:
                have.add(rc)
                e = want[rc]
                new.append(_row(e, row, "RCEPT_NO", now))
        # 보조: 접수번호가 목록에 없는 사건은 같은 날·같은 회사·제목 핵심어로 시각만 가져온다
        for e in day_events:
            if e["rcept_no"] in have:
                continue
            keys = TITLE_KEYS.get(e["event_type"])
            if not keys:
                continue
            cand = [r for r in day["rows"]
                    if r["corp_name"] == (e.get("corp_name") or "")
                    and any(k in r["title"].replace(" ", "") for k in keys)]
            if len(cand) == 1:      # 두 건 이상이면 고르지 않는다
                have.add(e["rcept_no"])
                new.append(_row(e, cand[0], "NAME_TITLE", now))
        time.sleep(0.15)
    if new:
        with out_path.open("a", encoding="utf-8") as fh:
            for r in new:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    res["matched"] = len(new)
    for r in new:
        res["by_method"][r["match_method"]] = res["by_method"].get(r["match_method"], 0) + 1
    if res["day_fail"]:
        res["status"] = "WARN"
    with (events_dir / "collect_log.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"tool": "collect_disclosure_times", **res}, ensure_ascii=False) + "\n")
    return res


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events-dir", required=True, type=Path)
    ap.add_argument("--limit-days", type=int, default=0)
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    res = run(args.events_dir, datetime.now(), limit_days=args.limit_days or None)
    print(json.dumps({**res, "day_fail": res["day_fail"][:5]}, ensure_ascii=False, indent=2))
    return 0 if res["status"] == "OK" else 1


if __name__ == "__main__":
    raise SystemExit(main())
