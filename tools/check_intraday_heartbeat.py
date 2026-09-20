# -*- coding: utf-8 -*-
"""장중 루프 하트비트를 읽고 이상하면 알린다.

2026-08-25 신규. (113) 에서 확인한 것 - **하트비트는 살아 있는데 읽는 곳이 없다.**
`run_intraday_paper.bat:39` 가 `LOCK_HEARTBEAT` 를 잡고
`intraday_paper_loop.py:788 _write_lock_heartbeat()` 가 매 사이클 쓴다.
```json
{"ts":"...","pid":9792,"cycle":26,"steps_ok":24,"steps_total":24,"next_run_at":"-"}
```
그런데 이 파일을 참조하는 코드가 tools/ · 루트 · ps1 어디에도 없었다.
워치독(`intraday_loop_watchdog.ps1`)은 **프로세스가 살아 있는지만** 본다 -
프로세스는 멀쩡한데 스텝이 매 사이클 실패하는 상태를 아무도 못 본다.
실제로 2026-08-25 에 `pre_entry_lob_refresh` 가 매 사이클 타임아웃하고 있었고
그 사실은 내가 손으로 파일을 열어보고서야 드러났다.

## 무엇을 보나

```
정지    ts 가 --max-age-min 보다 오래됨   -> 루프가 사이클을 못 돌리고 있다
실패    steps_ok < steps_total            -> 어떤 스텝이 죽는다(라벨까지 같이 보낸다)
```

장 시간 밖에서는 정지를 알리지 않는다 - 루프가 안 도는 게 정상이다.
알림 실패가 종료 코드를 바꾸면 안 되므로 예외를 삼키고, 판정 결과만 rc 로 낸다.

    rc=0 정상 또는 단계실패(warning 알림만) / rc=1 루프 정지 / rc=2 하트비트 파일 없음
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path

ROOT = Path(r"E:\1_Data")
HB = ROOT / "2_Logs" / "run_intraday_paper.lock" / "heartbeat.json"
STATUS = ROOT / "2_Logs" / "intraday_loop_status_latest.json"


def _alert(text: str, level: str = "warning") -> None:
    try:
        sys.path.insert(0, str(ROOT / "tools"))
        from notify_channels import send_alert  # type: ignore
        send_alert(text, level=level, cooldown_sec=3600.0)
    except Exception:
        pass


def _failing_labels() -> list[str]:
    try:
        d = json.loads(STATUS.read_text(encoding="utf-8-sig"))
        return [str(s.get("label") or "?") for s in (d.get("steps") or [])
                if isinstance(s, dict) and not s.get("ok")]
    except Exception:
        return []


def main() -> int:
    ap = argparse.ArgumentParser(description="장중 루프 하트비트 점검")
    ap.add_argument("--max-age-min", type=float, default=15.0)
    ap.add_argument("--market-open", default="09:00")
    ap.add_argument("--market-close", default="15:40")
    ap.add_argument("--quiet", action="store_true", help="정상일 때 아무것도 출력하지 않는다")
    args = ap.parse_args()

    now = dt.datetime.now()
    hh, mm = (int(x) for x in args.market_open.split(":"))
    open_t = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    hh, mm = (int(x) for x in args.market_close.split(":"))
    close_t = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    in_market = (open_t <= now <= close_t) and now.weekday() < 5

    if not HB.exists():
        if in_market:
            print("[HB] 하트비트 파일이 없다: %s" % HB)
            _alert("[장중루프] 하트비트 파일이 없다 - 루프가 기동조차 안 했을 수 있다\n%s" % HB,
                   level="error")
            return 2
        if not args.quiet:
            print("[HB] 장 시간 밖. 하트비트 없음은 정상.")
        return 0

    d = json.loads(HB.read_text(encoding="utf-8-sig"))
    ts_raw = str(d.get("ts") or "")
    try:
        ts = dt.datetime.fromisoformat(ts_raw)
        if ts.tzinfo is not None:
            ts = ts.replace(tzinfo=None)
    except Exception:
        ts = dt.datetime.fromtimestamp(HB.stat().st_mtime)
    age_min = (now - ts).total_seconds() / 60.0
    ok_n, tot_n = int(d.get("steps_ok") or 0), int(d.get("steps_total") or 0)
    cycle = d.get("cycle")

    bad = []
    if in_market and age_min > args.max_age_min:
        bad.append("정지: 하트비트가 %.1f분 낡았다(한도 %.0f분). 마지막 %s, cycle=%s"
                   % (age_min, args.max_age_min, ts_raw, cycle))
    if tot_n and ok_n < tot_n:
        labels = _failing_labels()
        bad.append("스텝실패: %d/%d (cycle=%s) %s"
                   % (ok_n, tot_n, cycle, ", ".join(labels[:6]) if labels else ""))

    if not bad:
        if not args.quiet:
            print("[HB] 정상  cycle=%s  steps %d/%d  age=%.1f분%s"
                  % (cycle, ok_n, tot_n, age_min, "" if in_market else "  (장 시간 밖)"))
        return 0

    for b in bad:
        print("[HB][이상] %s" % b)
    halted = any(x.startswith("정지") for x in bad)
    _alert("[장중루프 하트비트]\n" + "\n".join(bad),
           level="error" if halted else "warning")
    # [2026-08-28] 단계실패만으로는 rc=1 을 내지 않는다.
    #   여기서 warning 으로 낮춘 등급을 바깥 run_tool_with_alert.bat 이 되살렸다 -
    #   그 껍데기는 rc!=0 을 보고 task_fail_alert.py 로 **error** 알림을 또 만든다.
    #   그래서 만성 단계 타임아웃(08-26 116건 / 08-27 106건)이 하루 7건의
    #   "예약작업 실패" 알림이 되어 진짜 정지 알림을 덮고 있었다.
    #   정지(하트비트 낡음)만 rc=1. 단계실패는 warning 알림 + rc=0 으로 둔다.
    return 1 if halted else 0


if __name__ == "__main__":
    sys.exit(main())
