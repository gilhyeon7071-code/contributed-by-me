# -*- coding: utf-8 -*-
"""RD_20260901_topn 1단계 - 거래일 가드

왜 필요한가 (2026-09-03 검증에서 발견)
  장중 배치가 Windows `Daily` 트리거라 **주말·공휴일에도 10분마다 돈다.**
  모의계좌 발주는 디스패처의 세션 가드가 면제되므로(`session guard bypassed: mock apply`)
  holidays.json 이 있어도 그 경로로는 안 걸린다. 그러면 비거래일에
    - 주문 파일과 decision 사이드카가 생기고
    - reconcile 이 **held_days 를 +1 한다**
  HOLD_DAYS=13 은 **거래일** 기준이므로 이건 보유 기간을 망가뜨린다.

종료 코드
  0  거래일이다. 계속 진행
  3  거래일이 아니다. 이 저장소 규약상 "대기, 실패 아님"
     (run_tool_with_alert.bat 이 [WAIT] 로 처리한다)

읽기 전용. 아무것도 쓰지 않는다.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HOLIDAYS = ROOT / "holidays.json"


def load_holidays() -> set:
    try:
        with HOLIDAYS.open("r", encoding="utf-8") as f:
            j = json.load(f)
        return {str(x).strip() for x in (j.get("holidays") or []) if str(x).strip()}
    except Exception:
        return set()


def _parse_hhmm(s: str):
    """'0900' 또는 '09:00' -> (9, 0). 형식이 틀리면 None."""
    t = str(s or "").strip().replace(":", "")
    if len(t) != 4 or not t.isdigit():
        return None
    h, m = int(t[:2]), int(t[2:])
    if not (0 <= h <= 23 and 0 <= m <= 59):
        return None
    return h, m


def check_market_hours(now_hhmm: str, window: str) -> tuple:
    """장 시간 안인지 본다. 반환 (통과여부, 설명).

    [2026-09-04] 왜 필요한가
      모든 작업이 StartWhenAvailable=True 라, 정전 후 로그인하면 **놓친 실행이 즉시 따라온다.**
      장중 배치가 예컨대 월요일 08:00 에 따라오면 거래일 가드는 통과하고(월요일이니까),
      디스패처의 세션 가드는 mock 이라 면제된다("session guard bypassed: mock apply").
      -> 장 시작 전에 주문이 나갈 수 있다. 지금은 폭락 가드가 막고 있을 뿐이다.
      시각은 **장중 배치에서만** 검사한다. 저녁 배치(22:10)가 같은 가드를 쓰기 때문이다.
    """
    w = str(window or "").strip()
    if "-" not in w:
        return True, "창 형식 오류(%s) - 시각 검사를 건너뛴다" % w
    a, b = w.split("-", 1)
    lo, hi, now = _parse_hhmm(a), _parse_hhmm(b), _parse_hhmm(now_hhmm)
    if lo is None or hi is None or now is None:
        return True, "시각 파싱 실패(창=%s now=%s) - 검사를 건너뛴다" % (w, now_hhmm)
    lo_m, hi_m, now_m = lo[0] * 60 + lo[1], hi[0] * 60 + hi[1], now[0] * 60 + now[1]
    if lo_m <= now_m <= hi_m:
        return True, "%02d:%02d 는 장 시간(%s) 안이다" % (now[0], now[1], w)
    return False, "%02d:%02d 는 장 시간(%s) 밖이다" % (now[0], now[1], w)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="", help="YYYYMMDD. 비우면 오늘")
    ap.add_argument("--require-market-hours", action="store_true",
                    help="장 시간(기본 0900-1530) 밖이면 rc=3. 장중 배치만 켠다")
    args = ap.parse_args()

    import os
    # 시험용 override. 배치가 이 변수를 세우지 않으므로 운영에는 영향이 없다
    d = (args.date.strip() or os.environ.get("TOPN_GUARD_DATE", "").strip()
         or dt.date.today().strftime("%Y%m%d"))
    try:
        day = dt.datetime.strptime(d, "%Y%m%d").date()
    except Exception:
        print("[STOP] 날짜 형식 오류: %s" % d)
        return 2

    if day.weekday() >= 5:
        print("[WAIT] %s 는 주말(%s)이다. 거래일이 아니다"
              % (d, ["월", "화", "수", "목", "금", "토", "일"][day.weekday()]))
        return 3

    hol = load_holidays()
    if not hol:
        # 휴일표를 못 읽으면 **막지 않는다**. 주말 검사는 이미 통과했고,
        # 여기서 fail-closed 로 막으면 파일 하나 때문에 매매가 통째로 선다.
        # 다만 조용히 넘어가지는 않는다.
        print("[WARN] holidays.json 을 읽지 못했다. 주말 검사만 적용한다: %s" % HOLIDAYS)
        return 0

    if d in hol:
        print("[WAIT] %s 는 휴장일이다 (holidays.json)" % d)
        return 3

    # [2026-09-04] 장 시간 검사. 장중 배치가 --require-market-hours 로만 켠다.
    #   저녁 배치(22:10)는 이 플래그를 주지 않으므로 영향이 없다.
    if args.require_market_hours:
        if str(os.environ.get("TOPN_GUARD_IGNORE_HOURS", "")).strip() == "1":
            print("[WARN] TOPN_GUARD_IGNORE_HOURS=1 - 시각 검사를 건너뛴다(수동 실행용)")
        else:
            window = str(os.environ.get("TOPN_MARKET_HOURS", "") or "0900-1530").strip()
            now_hhmm = (str(os.environ.get("TOPN_GUARD_NOW", "") or "").strip()
                        or dt.datetime.now().strftime("%H%M"))
            ok, why = check_market_hours(now_hhmm, window)
            if not ok:
                print("[WAIT] %s. 밀린 실행이 장외에 따라붙는 것을 막는다" % why)
                return 3
            print("[OK] %s" % why)

    print("[OK] %s 는 거래일이다" % d)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
