"""H3 지분공시 사건 — 내부자·대주주가 자기 돈으로 샀나 팔았나. 수집·분류만, 매매 없음.

    python -m paper.strategies.kospi_mcap_quarterly_v2.src.collect_ownership_events --events-dir <폴더>

가설 H3
  임원·주요주주는 회사 사정을 먼저 안다. 최대주주 매수는 지분율·경영권이 목적이라 **가격을 덜 따진다**
  (= H1 의 "가격과 무관한 매수" 와 같은 성격). 그 매수가 이후 수익률과 관계있는가.

  **주의 — 이건 사후 보고예요.** 실제 거래는 보고일보다 며칠 앞서 있어요. 우리가 아는 시점은 **보고 접수일**이고,
  그 사이에 이미 가격에 반영됐을 수 있어요. 그래서 t0 는 접수일 다음 거래일로 잡고, 그게 남아 있는지를 봐요.

어디서 (2026-09-18 실물 확인)
  임원·주요주주  https://opendart.fss.or.kr/api/elestock.json   corp_code 로 조회
                 sp_stock_lmp_irds_cnt 증감 수량 / sp_stock_lmp_cnt 보유 / isu_exctv_ofcps 직위 / isu_main_shrholdr 주요주주 여부
  대량보유(5%)   https://opendart.fss.or.kr/api/majorstock.json  corp_code 로 조회
                 stkqy_irds 증감 수량 / stkrt_irds 지분율 증감 / repror 보고자 / report_resn 사유
  둘 다 기간 인자가 없어 **회사별로 한 번 받아** 접수번호로 우리 사건과 맞춘다.

기록
  `ownership_details.jsonl` — API 원본(회사별 전체). append-only, 접수번호로 중복 차단
  `h3_events.jsonl`        — 분류 결과(파생). INSIDER_BUY/SELL, MAJOR_BUY/SELL
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

ROOT = Path(__file__).resolve().parents[4]
DEFAULT_KEY_FILE = ROOT / "_cache" / "dart_api_key.txt"
API = "https://opendart.fss.or.kr/api/{name}.json"
INSIDER_TITLE = "임원ㆍ주요주주특정증권등소유상황보고서"
MAJOR_TITLE = "주식등의대량보유상황보고서"


def _num(v: Any) -> Optional[int]:
    s = str(v or "").replace(",", "").strip()
    if not s or s == "-":
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def fetch_corp(key: str, api: str, corp_code: str, session: requests.Session) -> Dict[str, Any]:
    try:
        r = session.get(API.format(name=api), params={"crtfc_key": key, "corp_code": corp_code}, timeout=30)
        j = r.json()
    except Exception as e:
        return {"status": "FETCH_ERROR", "error": f"{type(e).__name__}:{e}", "rows": []}
    st = str(j.get("status"))
    if st == "013":
        return {"status": "NOT_FOUND", "rows": []}
    if st != "000":
        return {"status": "API_ERROR", "error": f"{st}:{j.get('message')}", "rows": []}
    return {"status": "OK", "rows": j.get("list") or []}


def classify_row(api: str, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """증감 수량의 부호로 매수·매도. 0 이나 값 없음은 사건이 아니다(모르면 만들지 않는다)."""
    if api == "elestock":
        d = _num(row.get("sp_stock_lmp_irds_cnt"))
        who = "MAIN_SHAREHOLDER" if str(row.get("isu_main_shrholdr") or "").strip() not in ("", "-") else "EXECUTIVE"
        base = {"qty_change": d, "holder": row.get("repror"), "who": who,
                "position": row.get("isu_exctv_ofcps"), "held_after": _num(row.get("sp_stock_lmp_cnt"))}
        prefix = "INSIDER"
    else:
        d = _num(row.get("stkqy_irds"))
        base = {"qty_change": d, "holder": row.get("repror"), "who": str(row.get("report_tp") or ""),
                "reason": row.get("report_resn"), "rate_change": row.get("stkrt_irds"),
                "held_after": _num(row.get("stkqy"))}
        prefix = "MAJOR"
    if not d:
        return None
    return {**base, "event_type": f"{prefix}_{'BUY' if d > 0 else 'SELL'}",
            "direction": "BUY_PRESSURE" if d > 0 else "SELL_PRESSURE"}


def run(key: str, events_dir: Path, now: datetime, session: Optional[requests.Session] = None,
        limit_corps: Optional[int] = None) -> Dict[str, Any]:
    events_dir = Path(events_dir)
    raw = [json.loads(x) for x in (events_dir / "dart_disclosures.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    targets = {"elestock": {}, "majorstock": {}}
    for r in raw:
        nm = str(r.get("report_nm") or "")
        api = "elestock" if INSIDER_TITLE in nm else ("majorstock" if MAJOR_TITLE in nm else None)
        if api and r.get("stock_code"):
            targets[api].setdefault(r["corp_code"], []).append(r)

    det_path = events_dir / "ownership_details.jsonl"
    seen = set()
    if det_path.exists():
        for line in det_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                d = json.loads(line)
                seen.add((d["api"], d["rcept_no"]))
    s = session or requests.Session()
    res = {"run_at": now.isoformat(timespec="seconds"), "corps": 0, "new_rows": 0, "failed": [], "status": "OK"}
    new_det: List[Dict[str, Any]] = []
    done_corps = {(d.split("|")[0], d.split("|")[1]) for d in []}
    for api, by_corp in targets.items():
        corps = sorted(by_corp)
        if limit_corps:
            corps = corps[:limit_corps]
        for corp in corps:
            if all((api, r["rcept_no"]) in seen for r in by_corp[corp]):
                continue                      # 이 회사 건은 이미 다 받아 뒀다
            res["corps"] += 1
            got = fetch_corp(key, api, corp, s)
            if got["status"] not in ("OK", "NOT_FOUND"):
                res["failed"].append({"api": api, "corp": corp, "status": got["status"]})
                continue
            for row in got["rows"]:
                rc = str(row.get("rcept_no"))
                if (api, rc) in seen:
                    continue
                seen.add((api, rc))
                new_det.append({"api": api, "rcept_no": rc, "corp_code": corp, "raw": row,
                                "observed_at": now.isoformat(timespec="seconds")})
            time.sleep(0.05)
    if new_det:
        with det_path.open("a", encoding="utf-8") as fh:
            for d in new_det:
                fh.write(json.dumps(d, ensure_ascii=False) + "\n")
    res["new_rows"] = len(new_det)
    if res["failed"]:
        res["status"] = "WARN"
    return res


def build_events(events_dir: Path, now: datetime) -> Dict[str, Any]:
    """원본에서 사건 목록을 다시 만든다(파생 파일). 우리 수집 구간의 접수번호만 남긴다."""
    events_dir = Path(events_dir)
    raw = {json.loads(x)["rcept_no"]: json.loads(x)
           for x in (events_dir / "dart_disclosures.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()}
    det = [json.loads(x) for x in (events_dir / "ownership_details.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    out, skipped = [], {"no_change": 0, "not_in_window": 0}
    for d in det:
        base = raw.get(d["rcept_no"])
        if not base:
            skipped["not_in_window"] += 1     # 우리 수집 구간 밖(API 는 과거분도 준다)
            continue
        hit = classify_row(d["api"], d["raw"])
        if not hit:
            skipped["no_change"] += 1
            continue
        out.append({"event_id": f"{d['rcept_no']}|{hit['event_type']}", "rcept_no": d["rcept_no"],
                    "rcept_dt": base["rcept_dt"], "stock_code": base.get("stock_code", ""),
                    "corp_name": base.get("corp_name"), "report_nm": base.get("report_nm"),
                    "is_amendment": "정정" in str(base.get("report_nm") or ""),
                    "rules_version": "h3", "observed_at": now.isoformat(timespec="seconds"), **hit})
    path = events_dir / "h3_events.jsonl"
    with path.open("w", encoding="utf-8") as fh:
        for r in out:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    import collections
    return {"events": len(out), "skipped": skipped,
            "by_type": dict(collections.Counter(r["event_type"] for r in out))}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events-dir", required=True, type=Path)
    ap.add_argument("--key-file", type=Path, default=DEFAULT_KEY_FILE)
    ap.add_argument("--limit-corps", type=int, default=0)
    ap.add_argument("--build-only", action="store_true")
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    from paper.strategies.kospi_mcap_quarterly_v2.src.collect_dart_events import resolve_key
    now = datetime.now()
    res = {}
    if not args.build_only:
        res = run(resolve_key(args.key_file), args.events_dir, now, limit_corps=args.limit_corps or None)
    built = build_events(args.events_dir, now)
    print(json.dumps({**res, "failed": (res.get("failed") or [])[:3], "built": built}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
