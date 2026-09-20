# -*- coding: utf-8 -*-
"""루프 재시작 상한 도달을 사람에게 알린다.

2026-08-24: run_intraday_paper.bat 의 RESTART_COUNT 에 상한이 없었다.
파이썬이 임포트 단계에서 죽으면 부모가 15초마다 무한 재시작하고,
외부 워치독(tools/intraday_loop_watchdog.ps1)은 pidsBefore 에 부모가 잡혀
영원히 alive_skip 이라 아무도 못 잡는다.
intraday_paper_loop.py 의 [LOOP CRASH] 훅은 run_cycle 실패용이라
기동 자체가 안 되는 경우에는 돌지 않는다.

상한을 두어 부모가 물러나게 하고(워치독이 이어받는다), 그 사실을 알린다.
알림 실패가 종료를 막아서는 안 되므로 모든 예외를 삼키고 항상 0 을 반환한다.
"""
import sys, os

def main() -> int:
    try:
        count = sys.argv[1] if len(sys.argv) > 1 else "?"
        exitcode = sys.argv[2] if len(sys.argv) > 2 else "?"
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        tools = os.path.join(root, "tools")
        if tools not in sys.path:
            sys.path.insert(0, tools)
        from notify_channels import send_alert  # type: ignore
        send_alert(
            "[LOOP_RESTART_CAP] 루프 재시작 상한 도달 - 사람이 확인해야 한다 "
            "restarts=%s last_exitcode=%s" % (count, exitcode),
            level="error",
            cooldown_sec=1800.0,
            extra={
                "restart_count": count,
                "last_exitcode": exitcode,
                "log": os.path.join(root, "2_Logs", "run_intraday_paper_last.txt"),
                "note": "부모가 물러났으므로 외부 워치독이 재시작을 이어받는다",
            },
        )
        print("[RESTART_CAP] alert sent restarts=%s exitcode=%s" % (count, exitcode))
    except Exception as e:
        try:
            print("[RESTART_CAP] alert failed (ignored): %s" % e)
        except Exception:
            pass
    return 0

if __name__ == "__main__":
    sys.exit(main())
