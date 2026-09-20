# -*- coding: utf-8 -*-
"""지수 관측이 **얼마나 자주 끊기는지** 센다.

[2026-09-13] 왜 있나

  2026-09-12 에 진입 게이트를 "관측 못 하면 막는다"(fail-closed)로 바꿨다.
  근거는 사용자 판단 *"그 기간에는 매매가 이루어지지 않으니까"* 인데,
  **그 말은 실제 발동 구간(사이드카·CB)에 대한 것이었다.**
  내 가드는 **WS 가 끊긴 경우**에도 걸린다 - 그때 시장은 정상이다.

  둘을 우리가 구별할 방법이 지금 없다. 그래서 선택은 빈도에 달려 있다.
  하루 한두 번이면 보수적으로 둘 만하고, 수시로 걸리면 그 가드는
  **매매를 사실상 정지**시키는 것이라 다시 봐야 한다.

  그런데 탐지기 상태 파일은 매 실행 덮여서 이력이 없었다.
  `market_observation_ledger.jsonl` 에 한 줄씩 쌓고, 이 도구가 센다.

**사용자에게 자동으로 보이게 한다.** 사람이 물어봐야 알 수 있으면
이 프로젝트에서 반복된 실패(쌓이는데 아무도 안 읽는다)를 그대로 반복한다.
그래서 밤 20:50 `run_invariant_watch.bat` 이 매일 이 요약을 찍는다.

    python tools/market_observation_report.py
    python tools/market_observation_report.py --days 7 --json
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import io
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
LEDGER = LOG_DIR / "market_observation_ledger.jsonl"
OUT_NAME = "market_observation_report_latest.json"

# 장중만 센다. 장 밖의 실패는 정상이고(틱이 없다) 섞으면 빈도가 무의미해진다
SESSION_START, SESSION_END = 900, 1530


def _is_trading_day(ymd: str) -> bool:
    try:
        d = dt.datetime.strptime(ymd, "%Y%m%d").date()
    except Exception:
        return False
    if d.weekday() >= 5:
        return False
    try:
        obj = json.loads((ROOT / "holidays.json").read_text(encoding="utf-8-sig"))
        vals = obj.get("holidays") if isinstance(obj, dict) else obj
        hs = {"".join(c for c in str(v) if c.isdigit())[:8] for v in (vals or [])}
    except Exception:
        hs = set()
    return ymd not in hs


def _rows(days: int) -> List[Dict[str, Any]]:
    if not LEDGER.is_file():
        return []
    cutoff = (dt.date.today() - dt.timedelta(days=days)).strftime("%Y%m%d")
    out = []
    for line in io.open(str(LEDGER), encoding="utf-8", errors="replace"):
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except Exception:
            continue
        if str(r.get("ymd") or "") >= cutoff:
            out.append(r)
    return out


def _hhmm(ts: str) -> int:
    try:
        return int(ts[11:13]) * 100 + int(ts[14:16])
    except Exception:
        return -1


def report(days: int = 7) -> Dict[str, Any]:
    rows = _rows(days)
    per_day: Dict[str, Dict[str, Any]] = collections.defaultdict(
        lambda: {"total": 0, "fail": 0, "reasons": collections.Counter(), "fail_hhmm": []})
    for r in rows:
        ymd = str(r.get("ymd") or "")
        if not _is_trading_day(ymd):
            continue
        h = _hhmm(str(r.get("ts") or ""))
        if not (SESSION_START <= h < SESSION_END):
            continue
        d = per_day[ymd]
        d["total"] += 1
        if not r.get("valid"):
            d["fail"] += 1
            d["fail_hhmm"].append(h)
            for code, why in (r.get("rejected") or {}).items():
                d["reasons"][str(why).split(":")[0]] += 1

    days_out = []
    tot = fail = 0
    for ymd in sorted(per_day):
        d = per_day[ymd]
        tot += d["total"]
        fail += d["fail"]
        days_out.append({
            "ymd": ymd,
            "cycles": d["total"],
            "fail": d["fail"],
            "fail_pct": round(100.0 * d["fail"] / d["total"], 1) if d["total"] else None,
            "reasons": dict(d["reasons"].most_common(3)),
            "first_fail_hhmm": min(d["fail_hhmm"]) if d["fail_hhmm"] else None,
            "last_fail_hhmm": max(d["fail_hhmm"]) if d["fail_hhmm"] else None,
        })
    return {
        "generated_at": dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "window_days": days,
        "session": "%04d-%04d" % (SESSION_START, SESSION_END),
        "trading_days_seen": len(days_out),
        "cycles": tot,
        "fail": fail,
        "fail_pct": round(100.0 * fail / tot, 1) if tot else None,
        "days": days_out,
        "ledger": str(LEDGER),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--out-dir", default=str(LOG_DIR))
    args = ap.parse_args()

    rep = report(args.days)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / OUT_NAME).write_text(json.dumps(rep, ensure_ascii=False, indent=2),
                                    encoding="utf-8")
    if args.json:
        print(json.dumps(rep, ensure_ascii=False, indent=2))
        return 0

    print("=" * 74)
    print(" 지수 관측 실패율 (장중 %s, 최근 %d일)" % (rep["session"], rep["window_days"]))
    print("=" * 74)
    if not rep["days"]:
        print("  아직 거래일 표본이 없다. (원장 %s)" % ("있음" if LEDGER.is_file() else "**없음**"))
        print("  -> 2026-09-13 신설. 첫 거래일은 2026-09-14")
        return 0
    for d in rep["days"]:
        span = ("%04d~%04d" % (d["first_fail_hhmm"], d["last_fail_hhmm"])
                if d["first_fail_hhmm"] is not None else "-")
        print("  %s  사이클 %-4d 실패 %-4d (%4s%%)  구간 %-10s %s"
              % (d["ymd"], d["cycles"], d["fail"],
                 d["fail_pct"] if d["fail_pct"] is not None else "-", span,
                 json.dumps(d["reasons"], ensure_ascii=False) if d["reasons"] else ""))
    print("-" * 74)
    print("  합계 사이클 %d / 실패 %d (%s%%)  거래일 %d"
          % (rep["cycles"], rep["fail"],
             rep["fail_pct"] if rep["fail_pct"] is not None else "-",
             rep["trading_days_seen"]))
    print()
    print("  * 이 숫자가 진입 게이트 fail-closed 의 **비용**이다.")
    print("    실패 1건 = 그 사이클 신규 진입 차단. 그때 시장은 정상일 수 있다")
    print("    (실제 CB 중이면 차단이 옳다. 둘을 구별할 방법이 지금 없다)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
