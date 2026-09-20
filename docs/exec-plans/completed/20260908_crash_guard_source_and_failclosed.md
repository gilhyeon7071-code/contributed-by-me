# 20260908_crash_guard_source_and_failclosed

> **사후 작성 (retroactive).** AGENTS.md §9 는 복잡한 작업의 ExecPlan 을 **코딩 전에** 쓰라고
> 한다. 이 건은 그러지 못했다. 실행이 끝난 뒤 사용자 지적으로 규율을 대조하다 누락을 발견해
> 기록을 복구한 것이다. 순서를 지킨 척하지 않는다. -> PLANS 2026-09-08 (248)(249)

## Goal
폭락 가드(매매 정지 스위치)가 **무엇으로 판정했는지 산출물에서 읽히게** 하고,
판정할 소스가 하나도 없을 때 조용히 통과하지 않게 한다.

## 배경 (실측)
```
보관된 p0 산출물 38개(2026-08-10 이후) 전부에 reasons=[error_fetch_empty]
재현: pykrx get_index_ohlcv_by_date(..., '1001'/'2001') -> KeyError '종가'
즉 주 소스가 30일 넘게 죽어 있었고, 판정은 매번 폴백이 했다
안 드러난 이유: 폴백이 조용히 성공했고, 실패 사유가 발동 사유와 같은 reasons 에 섞였다
2026-08-31~09-04 에 실제로 매매를 막은 문턱은 폴백 경로의 max_dd 0.35 였다
```

## In-Scope
- `p0_daily_check.py` 판정 소스 순서와 표기
- 무판정 시 동작

## Out-of-Scope
- 문턱값(0.25/0.10, 0.35) 변경 — 손대지 않았다
- `utils/crash_index_blend.py` — 읽기만 한다 (동결 라운드 기준선)
- kill_switch / market_regime / 주문 경로

## 결정 (사용자 승인 2026-09-08)
```
① 구성일치 혼합지수(index_blend)를 주 소스로, pykrx 를 1차 폴백으로 내린다
② 세 소스가 다 실패하면 fail-closed (triggered=True)
```
①의 근거 5개: 이미 판정 중 / 매일 16:05 수집 실지수 + 신선도 fail-closed 기존 배선 /
스타일 일치(KOSPI 46.4·KOSDAQ 53.6) / **전략 패널과 독립** / pykrx 는 외부 파싱 의존이고 깨져 있다.
②는 새 정책이 아니라 같은 함수의 신선도 분기 선례("열지 않는다", 2026-09-04)를 빈 자리에 채운 것.

## 변경
```
p0_daily_check.py  4e20ee0bb849b0d2... -> dd8e689acda98fc9...
  + limits.effective_path / effective_trigger_pct / effective_release_pct
  + metrics.primary_source / primary_source_ok / primary_source_status
  + metrics.fallback1_* / verdict_source / verdict_role
  + 무판정 fail-closed (정상 경로 + 예외 경로 양쪽)
  + status 라벨이 역할을 따라가게 (ok_primary_* / ok_fallback_*)
백업 backup/20260908_crash_limits_label/20260908_130443/
     backup/20260908_crash_source_order/20260908_132022/
```

## 검증 (AGENTS.md §6)
```
① 기능        PASS  주입 3건 (전 소스 실패 / 혼합지수 정상 / 혼합 실패+pykrx OK)
② 정합성      PASS  cur_dd -0.2006 불변, data_date_max 20260907
③ 운영 반영   PASS  13:25 생산 실행 primary=index_blend / verdict_source=primary / reasons=[]
④ 정책        PASS  사용자 승인 후 집행. 문턱·mode 불변
⑤ FAIL-CLOSED PASS  세 소스 실패 -> triggered=True + no_source_verdict(fail_closed)
⑥ 회귀        PASS  risk_off False / kill_switch 불변 / pykrx 성공 경로 보존
```

## 남긴 것
- pykrx 자체는 여전히 깨져 있다. 1차 폴백 자리가 죽은 상태다 -> 결함 목록
- `reasons` 로 필터하던 소비자가 있다면 `error_fetch_empty` 가 더는 안 나온다
