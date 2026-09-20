# -*- coding: utf-8 -*-
"""진입 전 backtest_validation_latest.json 신선도 검사.

[2026-09-11] PLANS (358)(359).
run_paper_daily.bat 의 [6.96/9](백테스트 검증)를 [7/9](진입) **뒤로** 옮기면서,
검증 도구가 인프라 실패로 죽었을 때 진입을 막던 2차 차단이 사라졌다.

  rc=2 (NO_GO, 전략 게이트 실패)  -> 원래도 통과시키는 설계다. 여기서 보지 않는다
  rc=60~67 (파이썬/스크립트/산출물 없음, 산출물 stale) -> 인프라 실패. 이것이 막던 것

엔진 내부 가드(backtest_validation_guard)는 stale_max_age_days=3 을 갖지만
caution_affects_entry=false 라 낡은 산출물을 **막지 않는다**. 그 구멍을 같은 임계값으로 메운다.

파일 나이 검사라 비용이 없다 - 84분을 되돌리지 않고 의도만 복원한다.

종료 코드
  0   신선함 (통과)
  65  산출물 없음
  67  임계값 초과 (기본 3일)

    python tools/precheck_btval_freshness.py
    python tools/precheck_btval_freshness.py --max-age-days 3
    python tools/precheck_btval_freshness.py --path <다른 파일>   # 시험용
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PATH = ROOT / "2_Logs" / "backtest_validation_latest.json"

RC_OK = 0
RC_MISSING = 65
RC_STALE = 67


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", default=str(DEFAULT_PATH))
    ap.add_argument("--max-age-days", type=float, default=3.0)
    args = ap.parse_args()

    p = Path(args.path)
    if not p.is_file():
        print("[BTVAL_FRESH] MISSING path=%s" % p, flush=True)
        return RC_MISSING

    age_days = (time.time() - p.stat().st_mtime) / 86400.0
    verdict = "STALE" if age_days > args.max_age_days else "OK"
    print(
        "[BTVAL_FRESH] %s age_days=%.3f max=%.1f path=%s"
        % (verdict, age_days, args.max_age_days, p),
        flush=True,
    )
    return RC_STALE if verdict == "STALE" else RC_OK


if __name__ == "__main__":
    sys.exit(main())
