# -*- coding: utf-8 -*-
"""예약작업 실패를 사람에게 알린다. 도구 무관 범용.

2026-08-26 신규. 감사에서 드러났다 - **새로 만든 예약작업 5개가 실패해도 조용했다.**
```
VIBE_Spread_Passrate_1100      없음
VIBE_Index_Daily_Fetch         없음
VIBE_Status_Digest_Daily       없음   <- 이게 죽으면 사용자는 아무것도 못 본다
VIBE_Intraday_Heartbeat_Check  없음
VIBE_Cleanup_OneShot           없음
```
실제로 오늘 11:00 첫 자동 스프레드 측정이 조용히 죽었고,
내가 우연히 로그를 열어보고서야 알았다.

`notify_channels.py` 에는 CLI 가 없다(라이브러리다). 이 껍데기가 그 역할을 한다.
`rebalance_fail_alert.py` 와 같은 구조이나 라벨을 인자로 받아 어떤 작업에도 쓴다.

알림 실패가 배치 종료 코드를 바꿔서는 안 되므로 **모든 예외를 삼키고 항상 0** 을 반환한다.

    python tools/task_fail_alert.py <라벨> <rc> <로그파일>
"""
import os
import sys


def main() -> int:
    try:
        label = sys.argv[1] if len(sys.argv) > 1 else "?"
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
            "[예약작업 실패] %s  rc=%s\n\n%s" % (label, rc, tail),
            level="error",
            cooldown_sec=3600.0,
            # [2026-09-08] 억제 키를 (작업, rc) 로 고정한다.
            #   본문에 로그 25줄이 들어가는데 거기엔 시각·주문번호·파일명이 있어
            #   매 회차 텍스트가 달라졌고, 그래서 cooldown_sec=3600 이 한 번도 안 걸렸다.
            #   실측 2026-09-08: 같은 topn_dispatch rc=2 가 28건 발송 -> 텔레그램 429.
            #   같은 실패가 채널을 포화시키면 새 경보가 못 나간다.
            #   파일 채널은 억제와 무관하게 매번 기록하므로 국소 기록은 잃지 않는다.
            dedup_key="task_fail:%s:rc=%s" % (label, rc),
            extra={"task": label, "rc": rc, "log": log},
        )
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
