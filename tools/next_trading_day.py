# -*- coding: utf-8 -*-
"""다음 거래일 하나를 표준출력에 찍는다. 배치에서 exec_date 를 잡는 용도.

[2026-09-08] run_topn_evening.bat 의 사전 빌드가 "내일이 언제인가"를 알아야 해서 만들었다.
  같은 일을 bat 안의 python -c 한 줄로 하려 했으나, for /f usebackq 안에서 따옴표가
  두 번 열리면 cmd 가 명령을 잘라먹는다(실측: 'python.exe" -c "import' is not recognized).
  스크립트 파일로 빼면 그 문제가 사라지고, 재사용도 된다.

사용   python tools/next_trading_day.py [YYYYMMDD]
       인자가 없으면 오늘 기준. 판정 불가면 아무것도 찍지 않고 rc=1 (호출부가 fail-closed 하게)
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from holiday_manager import HolidayManager  # noqa: E402


def main() -> int:
    base = sys.argv[1].strip() if len(sys.argv) > 1 else datetime.now().strftime("%Y%m%d")
    nxt = HolidayManager().next_trading_day(base)
    if not nxt:
        print("", end="")
        return 1
    print(nxt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
