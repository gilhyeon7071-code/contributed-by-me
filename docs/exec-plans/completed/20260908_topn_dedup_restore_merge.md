# 20260908_topn_dedup_restore_merge

> **사후 작성 (retroactive).** 이 건은 사고 대응이라 조사->수리가 먼저 갔다.
> 그래도 코딩 전에 계획을 남겼어야 한다. -> PLANS 2026-09-08 (245)(248)(249)

## Goal
장중 반복 시도 구조에서 **같은 주문이 다시 나가는 경로**를 닫는다.

## 배경 (사고)
```
003230  12:41:54 12주 / 13:05:41 12주 / 13:15:27 3주  = 27주 체결 (의도 12주)
계좌 합계 100,090,780원. 한 슬롯에 예산(16,666,666)의 2.09배
```
원인 두 겹.
```
(가) 방아쇠  12:57:54 에 (237C) 시험을 **생산 경로 그대로** 돌려
             paper/orders_20260908_broker_submit_mock.csv 를 헤더만 있는 파일로 만들었다
             -> feedback_check_where_the_tool_writes 에 이미 적힌 실수의 반복
(나) 덫      topn_dispatch 의 복원 조건이 `ours.exists() and not p.exists()` 였다
             그 자리에 파일이 하나라도 있으면 복원을 건너뛰고 done_keys 가 빈다
             -> dedup 이 조용히 죽는다. 로그에서 12:55 이후 [DEDUP] 줄이 사라진다
```
**(나)는 나와 무관하게도 터진다.** v41.1 이 그 경로에 정상적으로 쓰는 날이면 동일하다.

## In-Scope
- `tools/topn_dispatch.py` 1-b 복원 블록

## Out-of-Scope
- `kis_order_dispatch_from_exec._load_done_keys` — dedup 판정 자체는 정상이었다
- 초과 보유분 처분 — 별도 건 (PLANS (246), 예약작업 VIBE_TopN_DupFix_Once)

## 변경
```
tools/topn_dispatch.py  bf55c0e27abf768c... -> d2b65d48009990ab...
  csv 산출물: 우리 이력과 그 자리 파일을 union(drop_duplicates) 해서 되돌린다
  json 산출물: 기존대로 "없을 때만 복원"
  병합 실패는 [DEDUP][WARN] ... **중복 발주 위험** 으로 크게 남긴다
백업 backup/20260908_dedup_restore/20260908_153609/
```

## 검증 (AGENTS.md §6)
```
① 기능        PASS  주입 4건 (파일 없음 / 헤더만=사고 재현 / v41.1 행 공존 / 이미 병합)
② 정합성      PASS  남의 이력을 지우지 않는다 (case3 두 종목 모두 잔존)
③ 운영 반영   PEND  **2026-09-09 09:05** [DEDUP] ... (+N행 병합) 로그 출현 확인
④ 정책        PASS  2026-09-03 의 dedup 의도를 강화. 존재 여부 분기 -> union
⑤ FAIL-CLOSED PASS  병합 실패를 삼키지 않는다
⑥ 회귀        PASS  json 경로 동작 불변
```

## 재발 방지
디스패처 시험은 `--orders-path` / 격리 출력 경로를 쓴다. 생산 경로로 돌리지 않는다.
오늘 다른 시험(reconcile, build_orders)은 `--out-dir` 로 격리했는데 **이 하나를 빠뜨렸다.**
