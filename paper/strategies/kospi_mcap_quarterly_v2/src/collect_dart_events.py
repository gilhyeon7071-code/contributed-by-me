"""H1 수집기 1 — 전자공시(DART) 유가증권 공시를 매일 쌓는다. 판단·매매 없음, 수집만.

    python -m paper.strategies.kospi_mcap_quarterly_v2.src.collect_dart_events --out-dir <폴더>
    ... --start 20260901 --end 20260918          (과거 구간 채우기)

무엇을 / 어디서 / 언제 / 지연
  무엇   유가증권시장(corp_cls=Y) 당일 접수 공시 전체 — 제목·접수번호·종목코드·접수일
  어디서 https://opendart.fss.or.kr/api/list.json (키: _cache/dart_api_key.txt 또는 DART_API_KEY)
  언제   장 마감 뒤 1회 권장. 당일치는 접수가 계속 들어오므로 **다음 날 한 번 더** 받아 보완한다
  지연   접수 즉시 목록에 뜬다(공시 원문 조회는 별도)

두 가지를 나눠 쌓는다
  1) 원본  dart_disclosures.jsonl   — 필터 없이 전부. **append-only, 지우지 않는다**
  2) 분류  h1_events.jsonl          — 규칙(config/h1_event_rules_*.json)으로 고른 H1 후보 사건.
                                      원본에서 만들어지는 **파생 파일**이라 규칙이 바뀌면 `--rebuild` 로 다시 만든다
접수번호(rcept_no)로 중복을 막는다(다시 돌려도 같은 결과).

이 수집은 매매 행동을 바꾸지 않는다. 사건이 실제로 가격을 미는지는 아직 모른다 —
그걸 재려면 사건 뒤 가격이 필요하고, 그건 별도 단계다.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

ROOT = Path(__file__).resolve().parents[4]
STRATEGY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RULES = STRATEGY_ROOT / "config" / "h1_event_rules_v1.json"
DEFAULT_KEY_FILE = ROOT / "_cache" / "dart_api_key.txt"
LIST_URL = "https://opendart.fss.or.kr/api/list.json"
PAGE_COUNT = 100
MAX_PAGES = 50


def resolve_key(key_file: Path) -> str:
    key = os.getenv("DART_API_KEY", "").strip()
    if key:
        return key
    if Path(key_file).exists():
        return Path(key_file).read_text(encoding="utf-8").strip()
    raise SystemExit("DART_API_KEY 도 _cache/dart_api_key.txt 도 없어요")


def fetch_day(key: str, ymd: str, market: str, session: Optional[requests.Session] = None) -> Dict[str, Any]:
    """하루치 공시 전체(페이지 넘겨가며). 상태를 같이 돌려준다 — 실패를 0건으로 읽지 않게."""
    s = session or requests.Session()
    rows: List[Dict[str, Any]] = []
    page, total, status = 1, None, "OK"
    while page <= MAX_PAGES:
        try:
            r = s.get(LIST_URL, params={"crtfc_key": key, "bgn_de": ymd, "end_de": ymd, "corp_cls": market,
                                        "page_no": page, "page_count": PAGE_COUNT}, timeout=30)
            j = r.json()
        except Exception as e:
            return {"status": "FETCH_ERROR", "error": f"{type(e).__name__}:{e}", "ymd": ymd, "rows": rows}
        st = str(j.get("status"))
        if st == "013":  # 조회 결과 없음
            return {"status": "OK", "ymd": ymd, "rows": [], "total": 0, "pages": page}
        if st != "000":
            return {"status": "API_ERROR", "error": f"{st}:{j.get('message')}", "ymd": ymd, "rows": rows}
        total = int(j.get("total_count") or 0)
        rows.extend(j.get("list") or [])
        if page >= int(j.get("total_page") or 1):
            break
        page += 1
        time.sleep(0.2)
    if total is not None and len(rows) != total:
        status = "INCOMPLETE"  # 모르면 모른다고 적는다
    return {"status": status, "ymd": ymd, "rows": rows, "total": total, "pages": page}


def classify(report_nm: str, rules: List[Dict[str, Any]], cfg: Optional[Dict[str, Any]] = None) -> List[Dict[str, str]]:
    """제목으로 사건 종류를 고른다. 여러 개에 걸리면 전부 남긴다(고르지 않는다).

    cfg 의 exclude_any(예: 종속회사·자회사)에 걸리면 사건이 아니다 — 그 상장사 주식의 강제 매매가 아니라서.
    amendment_markers(정정 공시)는 새 사건이 아니라 표시만 남긴다(is_amendment)."""
    cfg = cfg or {}
    name = str(report_nm or "").replace(" ", "")
    for bad in cfg.get("exclude_any", []):
        if bad.replace(" ", "") in name:
            return []
    amend = any(m.replace(" ", "") in name for m in cfg.get("amendment_markers", []))
    out = []
    for rule in rules:
        if any(k.replace(" ", "") in name for k in rule["any"]) and \
           not any(k.replace(" ", "") in name for k in rule.get("none", [])):
            out.append({"event_type": rule["event_type"], "direction": rule["direction"],
                        "is_amendment": amend})
    return out


def _load_ids(path: Path, key: str) -> set:
    if not Path(path).exists():
        return set()
    ids = set()
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.strip():
            try:
                ids.add(json.loads(line)[key])
            except Exception:
                continue
    return ids


def _append(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("a", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    return len(rows)


def collect(key: str, days: List[str], out_dir: Path, rules_cfg: Dict[str, Any], now: datetime) -> Dict[str, Any]:
    raw_path = Path(out_dir) / "dart_disclosures.jsonl"
    ev_path = Path(out_dir) / "h1_events.jsonl"
    seen_raw = _load_ids(raw_path, "rcept_no")
    seen_ev = _load_ids(ev_path, "event_id")
    rules = rules_cfg["rules"]
    res = {"run_at": now.isoformat(timespec="seconds"), "days": [], "new_raw": 0, "new_events": 0,
           "status": "OK", "reasons": []}
    session = requests.Session()
    for ymd in days:
        day = fetch_day(key, ymd, rules_cfg.get("market", "Y"), session)
        if day["status"] not in ("OK",):
            res["status"] = "STOP"
            res["reasons"].append(f"{ymd}:{day['status']}:{day.get('error', '')}")
        raw_new = []
        ev_new = []
        for r in day["rows"]:
            rc = str(r.get("rcept_no"))
            if rc in seen_raw:
                continue
            seen_raw.add(rc)
            raw_new.append({"rcept_no": rc, "rcept_dt": r.get("rcept_dt"), "corp_code": r.get("corp_code"),
                            "corp_name": r.get("corp_name"), "stock_code": (r.get("stock_code") or "").strip(),
                            "corp_cls": r.get("corp_cls"), "report_nm": r.get("report_nm"),
                            "flr_nm": r.get("flr_nm"), "rm": r.get("rm"),
                            "observed_at": now.isoformat(timespec="seconds")})
            for hit in classify(r.get("report_nm"), rules, rules_cfg):
                eid = f"{rc}|{hit['event_type']}"
                if eid in seen_ev:
                    continue
                seen_ev.add(eid)
                ev_new.append({"event_id": eid, "rcept_no": rc, "rcept_dt": r.get("rcept_dt"),
                               "stock_code": (r.get("stock_code") or "").strip(), "corp_name": r.get("corp_name"),
                               "report_nm": r.get("report_nm"), "event_type": hit["event_type"],
                               "direction": hit["direction"], "is_amendment": hit.get("is_amendment", False),
                               "rules_version": rules_cfg["version"],
                               "observed_at": now.isoformat(timespec="seconds")})
        res["new_raw"] += _append(raw_path, raw_new)
        res["new_events"] += _append(ev_path, ev_new)
        res["days"].append({"ymd": ymd, "status": day["status"], "rows": len(day["rows"]),
                            "total": day.get("total"), "new_raw": len(raw_new), "new_events": len(ev_new)})
    _append(Path(out_dir) / "collect_log.jsonl", [res])
    return res


def rebuild(out_dir: Path, rules_cfg: Dict[str, Any], now: datetime) -> Dict[str, Any]:
    """원본에서 분류 파일을 다시 만든다. 규칙을 고쳤을 때. 옛 분류 파일은 백업으로 옮긴다."""
    out_dir = Path(out_dir)
    raw_path = out_dir / "dart_disclosures.jsonl"
    ev_path = out_dir / "h1_events.jsonl"
    rows = [json.loads(x) for x in raw_path.read_text(encoding="utf-8").splitlines() if x.strip()]
    if ev_path.exists():
        ev_path.replace(out_dir / f"h1_events_before_{rules_cfg['version']}_{now:%Y%m%d_%H%M%S}.jsonl")
    seen = set()
    new = []
    for r in rows:
        for hit in classify(r.get("report_nm"), rules_cfg["rules"], rules_cfg):
            eid = f"{r['rcept_no']}|{hit['event_type']}"
            if eid in seen:
                continue
            seen.add(eid)
            new.append({"event_id": eid, "rcept_no": r["rcept_no"], "rcept_dt": r.get("rcept_dt"),
                        "stock_code": (r.get("stock_code") or "").strip(), "corp_name": r.get("corp_name"),
                        "report_nm": r.get("report_nm"), "event_type": hit["event_type"],
                        "direction": hit["direction"], "is_amendment": hit.get("is_amendment", False),
                        "rules_version": rules_cfg["version"],
                        "observed_at": now.isoformat(timespec="seconds")})
    _append(ev_path, new)
    res = {"mode": "rebuild", "raw_rows": len(rows), "events": len(new), "rules_version": rules_cfg["version"],
           "status": "OK", "reasons": []}
    _append(out_dir / "collect_log.jsonl", [{"run_at": now.isoformat(timespec="seconds"), **res}])
    return res


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--rebuild", action="store_true", help="원본에서 분류 파일을 다시 만든다(규칙 변경 시)")
    ap.add_argument("--start", default="")
    ap.add_argument("--end", default="")
    ap.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    ap.add_argument("--key-file", type=Path, default=DEFAULT_KEY_FILE)
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    now = datetime.now()
    start = args.start or now.strftime("%Y%m%d")
    end = args.end or start
    days = [d.strftime("%Y%m%d") for d in
            (date.fromordinal(o) for o in range(date(int(start[:4]), int(start[4:6]), int(start[6:])).toordinal(),
                                                date(int(end[:4]), int(end[4:6]), int(end[6:])).toordinal() + 1))]
    rules_cfg = json.loads(args.rules.read_text(encoding="utf-8"))
    if args.rebuild:
        res = rebuild(args.out_dir, rules_cfg, now)
    else:
        res = collect(resolve_key(args.key_file), days, args.out_dir, rules_cfg, now)
    print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0 if res["status"] == "OK" else 3


if __name__ == "__main__":
    raise SystemExit(main())
