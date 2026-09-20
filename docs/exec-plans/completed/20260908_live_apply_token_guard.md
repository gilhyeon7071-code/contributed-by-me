# 20260908_live_apply_token_guard

> **사후 작성 (retroactive).** 코딩 전에 쓰지 못했다. -> PLANS 2026-09-08 (248)(249)

## Goal
실계좌(prod) 발주가 **환경변수 하나로** 나가는 상태를 없앤다. 관례가 아니라 계약으로 만든다.

## 배경 (실측)
```
run_paper_daily.bat     APPLY/APPLY_SYNC 에 BROKER_CONFIRM=LIVE_APPLY 요구      잠금 있음
run_daily.bat           APPLY/APPLY_SYNC 에 토큰 없음. BROKER_MODE 기본 APPLY_SYNC
                        -> BROKER_MOCK=false 하나로 실화폐 발주                  잠금 없음
run_intraday_paper.bat  KIS_MOCK=0 이면 LOOP_DISPATCH_APPLY=0 (관례, 환경변수로 뒤집힘)
```
안전장치가 경로마다 갈려 있었다. 실계좌 발주 이력은 전 기간 6건뿐이고 전부 수동 카나리아였다
(2026-07-29 035720 / 2026-08-25 462860·001510). 자동 경로에서 실화폐가 나간 적은 없다.
**그 상태를 계약으로 굳힌다.**

## In-Scope
- `tools/kis_order_dispatch_from_exec.py` — 유일한 길목
- `run_paper_daily.bat` — 이미 토큰 검사가 있는 두 지점에 배선

## Out-of-Scope
- `run_daily.bat` — **일부러 배선하지 않는다.** 토큰을 심으면 구멍을 다시 만드는 셈이다. fail-closed 유지
- `tools/kis_canary_run.py` — `--confirm CANARY` 는 "카나리아 크기" 지 "실화폐" 가 아니다. 의도를 분리한다
- 모의계좌 경로 전부

## 변경
```
tools/kis_order_dispatch_from_exec.py  e99658db5fc2070c... -> cf0a72aee500c15b...
  + --confirm-live (또는 KIS_LIVE_CONFIRM). prod + --apply 인데 토큰 없으면 rc=2
  + PRECHECK_LIMIT_UNREACHABLE 분리 (같은 커밋 범위, 별도 목적)
run_paper_daily.bat  82b1313c897501d2... -> 1057e2a26f056611...
  + --confirm-live %BROKER_CONFIRM%  (토큰 검사 뒤 두 지점)
백업 backup/20260908_lob_status_split/20260908_125132/
     backup/20260908_live_confirm/20260908_125814/
```

## 검증 (AGENTS.md §6)
```
① 기능        PASS  prod 무토큰 rc=2 / prod 토큰 rc=0 / mock 무영향 (3건 실행)
② 정합성      PASS  eligible_rows=0 파일로 시험해 실제 주문은 나가지 않음을 확인
③ 운영 반영   PASS  호출부 전수 확인 (240) — 예약 경로 중 prod+apply 조합 없음
④ 정책        PASS  topn_dispatch 의 --confirm 방식과 통일
⑤ FAIL-CLOSED PASS  토큰 없으면 발주하지 않고 무엇을 줘야 하는지 알린다
⑥ 회귀        PASS  pytest tests/test_kis_order_dispatch_unknown_pending.py 4 passed
```

## 부작용 (의도된 것)
실계좌 카나리아는 이제 토큰을 함께 줘야 한다.
```
python tools\kis_canary_run.py --mock false --apply --confirm CANARY
  -> [STOP] live(prod) apply requires an explicit token
  해결: set KIS_LIVE_CONFIRM=LIVE_APPLY
```
조용히 실패하지 않고 필요한 것을 알려주며 멈춘다.
