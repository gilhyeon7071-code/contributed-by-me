# -*- coding: utf-8 -*-
"""지금이 run_paper_daily.bat 의 **저녁 실행**인가.

[2026-09-12] PLANS (368).

run_paper_daily.bat 은 같은 파일이 하루 두 번 돈다.
    08:30  STOC_FullAuto -> full_auto.bat -> run_daily_auto_sync.ps1:255
    21:30  VIBE_Paper_Daily -> run_paper_daily_hidden.vbs
무거운 연구 검증([6.96/9])을 저녁 실행에서만 돌리려면 이 구분이 필요하다.

왜 별도 스크립트인가 - bat 안에서 시각을 판정하면 두 함정을 밟는다
  1) `%TIME%` 은 로캘을 탄다. 한국어 Windows 는 "오후 4:23:45" 형식이고
     cp949 콘솔에서 오전/오후는 둘 다 4바이트라 깨지면 구별되지 않는다
     (2026-09-10 에 그것으로 하루 계획을 틀렸다)
  2) 인라인 `python -c` 에 `>=` 를 쓰면 cmd 가 `>` 를 리다이렉트로 먹는다

종료 코드
  0   저녁 실행 (기본 경계 18시 이후)
  1   그 외 (아침 실행)

    python tools/is_evening_pass.py
    python tools/is_evening_pass.py --from-hour 18
    python tools/is_evening_pass.py --at 2130     # 시험용 고정 시각
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-hour", type=int, default=18,
                    help="이 시(hour) 이상이면 저녁 실행. 기본 18")
    ap.add_argument("--at", default="", help="HHMM 고정 시각 (시험용)")
    args = ap.parse_args()

    if args.at.strip():
        s = args.at.strip().zfill(4)
        hour = int(s[:2])
        src = "fixed"
    else:
        hour = dt.datetime.now().hour
        src = "now"

    evening = hour >= int(args.from_hour)
    print("[PASS_KIND] hour=%02d from_hour=%02d source=%s -> %s"
          % (hour, args.from_hour, src, "EVENING" if evening else "MORNING"), flush=True)
    return 0 if evening else 1


if __name__ == "__main__":
    sys.exit(main())
