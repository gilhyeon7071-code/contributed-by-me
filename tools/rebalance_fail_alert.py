# -*- coding: utf-8 -*-
"""리밸런싱 일일 운영 실패를 사람에게 알린다.

2026-08-25. `run_rebalance_daily.bat` 이 실패해도 아무도 모르면
장부가 조용히 며칠 멈춘다 - [[project_1data_alert_delivery_outage]] 가
정확히 그 형태였다(3개월 18일간 45건 미전송).

`notify_channels.py` 는 라이브러리라 CLI 가 없다. 이 얇은 껍데기가 그 역할을 한다.
알림 실패가 배치 종료 코드를 바꿔서는 안 되므로 **모든 예외를 삼키고 항상 0** 을 반환한다.

    python tools/rebalance_fail_alert.py <phase> <rc> <logfile>
"""
import os
import sys


def main() -> int:
    try:
        phase = sys.argv[1] if len(sys.argv) > 1 else "?"
        rc = sys.argv[2] if len(sys.argv) > 2 else "?"
        log = sys.argv[3] if len(sys.argv) > 3 else ""
        tail = ""
        if log and os.path.exists(log):
            with open(log, "r", encoding="utf-8", errors="replace") as fh:
                tail = "".join(fh.readlines()[-25:])

        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        tools = os.path.join(root, "tools")
        if tools not in sys.path:
            sys.path.insert(0, tools)
        from notify_channels import send_alert  # type: ignore

        send_alert(
            "[리밸런싱] %s 국면 실패 rc=%s\n\n%s" % (phase, rc, tail),
            level="error",
            cooldown_sec=3600.0,
            extra={"phase": phase, "rc": rc, "log": log},
        )
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
