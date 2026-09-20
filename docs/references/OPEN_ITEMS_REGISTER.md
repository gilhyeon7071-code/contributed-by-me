# 미결 작업 대장 (Open Items Register)

작성 2026-09-12. 근거 대화: *"급하지 않다 나중에 하자는 일이 수십 개 이상이야"*,
*"187건은 목록화가 되어있어?"* -> **안 돼 있었다.**
*"처리하면 체크해서 갯수에서 빼고 또 나오면 목록에 포함시키고"*

## 왜 있나

기존 대장 셋(OBJECTIVE_LEDGER / CONCLUSION_REGISTER / BROKEN_WINDOW_REGISTER)은
전부 **인용하기 전에 보는 것**이다. **"해야 할 일" 을 담는 곳이 없어서** PLANS 산문에 흩어졌다.
2026-08-31 이후 12일치에서 보류 표현 187건, 중복 제거 후 문장 94건이 나왔는데
**실제 항목은 그보다 훨씬 적다** - 같은 5건(A1~A5)이 네 번 반복된 식이다.

## 규칙

```
개수는 **손으로 쓰지 않는다.** 아래 표에서 계산한다:  python tools/open_items.py
미루려면 세 가지를 적는다 (차단사유 / 트리거 / 안 하면 나빠지는 것) -> 스킬 defer-honestly
차단 사유가 없으면 미룬 게 아니라 **안 한 것**이다
닫을 때 행을 지우지 않는다. 상태만 DONE 으로 바꾸고 날짜·근거를 남긴다
새로 나오면 다음 번호로 추가한다 (A=결정대기 B=차단 C=즉시가능 D=큰조사 E=완료)
```

상태: `DECISION`(사용자 결정) / `BLOCKED`(차단사유 명시) / `OPEN`(지금 가능) / `DONE`

---

| ID | 상태 | 항목 | 차단사유·트리거 | 근거 |
|---|---|---|---|---|
| A1 | DECISION | 낙폭 단계 C-1 을 고칠 것인가 | 사용자 결정. 고치면 0.25/0.35/0.40/0.45 로 느슨해짐. 매매 동작 변경 | PLANS 154 |
| A2 | DECISION | sector_concentration_limit | 사용자 결정 | PLANS 160/161/164/165 |
| A3 | DECISION | R1 기본 동작 (a)/(b)/(c) | 사용자 결정 | GATE_JUDGMENT_FORM / PLANS 151 |
| A4 | DECISION | 폭락 지수 선택 | 사용자 결정 | PLANS 165 |
| A5 | DECISION | 폭락 문턱 | 사용자 결정 | PLANS 165 |
| A6 | DECISION | 목적함수 확정 (UNDECIDED) | 사용자 결정. (가)수동도구/(나)지수매수/(다)수급1년/(라)조합 | OBJECTIVE_LEDGER / PLANS 147 |
| B1 | BLOCKED | run_intraday_paper.bat 환경변수 주석 | 차단: 루프 bat 실행 중 / 트리거: 루프 정지. 스크립트 준비됨 | PLANS 362/366 |
| B2 | BLOCKED | 사용자 폴더 스킬 사본 11개 정리 | 차단: 세션 루트가 System32 / 트리거: start_claude.bat 전환 후 | SKILL_CANDIDATES |
| B3 | BLOCKED | 09-14 애프터마켓 판정 3건 | 차단: 거래일 필요 / 트리거: 09-14, 09-15 (예약 완료) | PLANS 367/369 |
| B4 | BLOCKED | 발주 시험 PENDING 2건 | 차단: 거래일 필요 / 트리거: 09-14 | PLANS 362 |
| C1 | DONE | Account_Snapshot_Daily RC 0x41306 | DONE 2026-09-12. 실측: 09-11 16:20:02~12 (10초) RC 0x0 정상. 09-10 종료는 단발. 원 우려(drift monitor)는 해소 | PLANS 333 |
| C2 | DONE | 수급 원장이 숫자를 문자열로 저장 | DONE 2026-09-12. 실측 43,934행 중 비숫자 8행(0.02%), 소비자 없음. 데이터 불변, 읽기 계약을 round exceptions.md 예외2로 기록 | PLANS 369 |
| C3 | DONE | AGENTS.md 의 ## 23 번호 중복 | DONE 2026-09-12. '## 23. 지표 인용 규약' -> '## 25.' 로 재번호. 상호참조 없음. 백업 AGENTS.md.bak_*_before_sec_renumber | PLANS 366 |
| C4 | DONE | 폭락 프록시 자료 노후 | DONE 2026-09-12. 실측: source=index_blend, blend_date_max=20260911(최신). (163)(164) 의 '4월 자료' 는 같은 날 O5-8 에서 이미 (나)안으로 해결됨. crash_risk_off triggered=false | PLANS 163/164 |
| C5 | DONE | crash_risk_off DEAD 4개 | DONE 2026-09-12. 4키 삭제 완료. 어떤 .py 도 안 읽음을 루트 108개 포함 실측. 잠금도구에 --unset 신설(6경우 시험) | PLANS 163/165 |
| C6 | DONE | DEAD 설정 나머지 18개 | DONE 2026-09-12. 18개 값 전수 확인. 위험한 것은 crash_risk_off 4개뿐(처분 완료). engine_log_dir 제거. 나머지는 값이 합리적이라 revival risk 낮음 - 기록으로 닫음 | PLANS 163/165 |
| C7 | DONE | final_score_news_reflected | DONE 2026-09-12. 실측: 09-11 08:50 preopen readiness status=READY. (313) 상류 수리로 해소됨. (333) 기록이 그 이전 | PLANS 333 |
| C8 | DONE | 게이트 구현 2벌·설정 키 2벌 단일출처화 | DONE 2026-09-12. 유령 항목. 2026-08-20 (68)A 에서 이미 철회됨: 두 구현이 같은 키를 읽고 state.py:2594 가 utils 로 위임. 호출경로 둘, 구현 하나 | PLANS 151 |
| C9 | DONE | 08-20 변경 9건 E2E | DONE 2026-09-12. E2E 완료. 9건 중 8건 PASS, 1번은 결함 발견+수리(최소수량 바닥이 축소되지 않은 1주 주문을 차단). 재검증 도구 tools/verify_20260820_changes.py 신설. 1b E2E 관측은 09-14 | PLANS 151 |
| D1 | OPEN | 파라미터 개별 근거 확인 32+40개 | 큰 조사. 착수 전 범위 확정 | PLANS 160/161 |
| D2 | OPEN | 기본값 없는 설정의 함수 내 폴백 조사 261개 | 큰 조사 | PLANS 161 |
| D3 | OPEN | leaf 미검사 573개 | 큰 조사 | PLANS 160 |
| D4 | OPEN | 캘리브 재생성 | 착수 전 확인 2건(스냅샷 보존 / 비용 컬럼 처리) | PLANS 178 |
| D5 | OPEN | RD_20260831_flow_h10 측정 미시작 | 동결만 됨. 판정 2027-08 | PLANS 147 |
| D6 | DONE | 화면 후속 3건 | DONE 2026-09-12. 1번 topn_cycle_status.py 신설+배선(시험 11건, 진행중 회차 누락 결함 1건 수리). 3번은 원인이 둘(생산자 미배선 + 화면 통과) - 배치 [13.1/14] 신설, 빌더가 STALE 판정, UI 전달. tsc 0, 151 passed. 2번은 결정으로 분리 | PLANS 207 |
| E1 | DONE | 청산 책임 선언 (2026-09-11) | broker_ledger_reconcile 에 ownership 층 | PLANS 348 |
| E2 | DONE | 브로커 자본 생산자+소비 연결 (09-11~12) | broker_account_basis / p0_daily_check | PLANS 350/363 |
| E3 | DONE | 청산 충돌 규칙 6개 (09-11) | EXIT_OWNERSHIP_RULES | PLANS 351 |
| E4 | DONE | 진입 스위치·발주 스위치·노출 상한 (09-11) | exit_only=False / DISPATCH_APPLY=1 / 0.01 | PLANS 355/362 |
| E5 | DONE | advisory rc=1 수리 (09-11) | 산출물 오염 직전이었다 | PLANS 356 |
| E6 | DONE | 위험 청산 선택기 + 왕복 손익 (09-12) | ddm_liquidation_preview / entry_size 확장 | PLANS 364 |
| E7 | DONE | [6.96/9] 원인 규명 + 저녁 분리 (09-12) | 게이트 3개 실패 / is_evening_pass | PLANS 365/368 |
| E8 | DONE | 애프터마켓 관측 예약 3건 (09-12) | T1/T2/T3_FLOW | PLANS 367/369 |
| E9 | DONE | 검증 스크립트 거짓 PASS 수리 (09-12) | 휴장일 가드 + 점유분 검사 | PLANS 368 |
| C10 | DONE | Account_Snapshot 16:20 이 09-14 부터 애프터마켓 장중 — 평가액이 확정 전일 수 있다 | DONE 2026-09-12. Account_Snapshot 16:20->20:20. 같은날 소비자는 21:30 배치의 build_drift_monitor 뿐이라 안전, 오히려 확정값을 받는다 | PLANS 370 / AFTERMARKET 관측 |
| C11 | DONE | RD_20260831_flow_h10 이 09-10 부터 동결 위반 (load_merged_panel.py 변경, 예외3). 그 로더를 실제로 타는지 확인 필요 | DONE 2026-09-12. 조사 완료: 기준선 3파일 중 load_merged_panel.py 가 의도적 포함이라 영향 실재. 단 측정 산출물 0건이라 오염된 결과 없음. 재동결 여부는 결정 항목으로 분리 | round exceptions.md 예외3 / PLANS 306 |
| C12 | DONE | round_preflight --check 를 정기 실행하는 장치가 없다 | DONE 2026-09-12. tools/round_freeze_watch.py 신설 + VIBE_Round_Freeze_Watch_1850 매일 예약. 상태가 바뀐 날만 경보(지속 FAIL 로는 안 울림) | round exceptions.md 예외3 |
| C13 | DONE | crash_risk_off 문턱 이원화 확인: max_dd -0.366 > 0.35 인데 cur_dd -0.201 로 판정. 두 값이 크게 갈리는 구조가 의도인지 근거 확인 | DONE 2026-09-12. 근거 있음: 09-07 cur_dd+히스테리시스 전환, 문턱 0.25/0.10 은 11.6년 2838일 실측(연 5.9일)에서 유도. 산출물에 effective_path=cur_dd_hysteresis 가 이미 명시돼 있다(09-08 추가). 내가 limits 를 필터링해 보다 놓쳤다 | p0_daily_check O5-8 / PLANS 2026-09-07 |
| C14 | DONE | 설정이 제어권을 허위 선언: intraday_residual_overnight_guard 의 apply_to_surge/apply_to_non_surge/trigger_on_same_day_loss 를 엔진은 안 읽는다(scope 만 읽음). 검사기는 True 를 요구한다 | DONE 2026-09-12. 엔진이 apply_to_surge/apply_to_non_surge 를 읽게 함(기본 True, 동작 불변). trigger_on_same_day_loss 는 유일 트리거라 fail-closed 경고. 실효값을 산출물에 노출. 시험 8건(행동 5) 135 passed. 검사기 PASS 유지 | exit.py:2725 / validate_...activation.py:36 / PLANS 371 |
| C15 | DONE | config 파일에서 키를 지워도 DEFAULT_CONFIG 가 다시 공급한다 — 죽은 설정 처분은 코드까지 같이 봐야 한다 | DONE 2026-09-12. 잠금도구가 --unset 후 실효설정을 실측해 경고한다(시험: power_hour_start_hhmm 으로 WARN 확인 후 복구). 오늘 처분한 5키는 실효설정에서도 사라짐을 재확인 | paper_engine/config.py DEFAULT_CONFIG |
| C16 | DONE | round_freeze_watch 가 RD_20260831_index_gap 을 안 본다 (frozen.json 없음) — 동결 안 된 라운드인지 확인 | DONE 2026-09-12. RD_20260831_index_gap 은 DRAFT->CLOSED(동결 미실행) 정상 상태. 감시기에 비동결 라운드 표시 추가 | PLANS 371 |
| A7 | DECISION | RD_20260831_flow_h10 재동결 여부 (교정된 load_merged_panel 위에서 다시 동결할 것인가) | 측정 시작 전에 정해야 한다. 예외 1 과 같은 종류의 결정 | round exceptions.md 예외3 보강 |
| C17 | DONE | VIBE_Decision_Sources_1650 이 그날 시장자료를 수집하는지 미확인 — 수집이면 애프터마켓 장중이다 | DONE 2026-09-12. 실측: build_market_master 에 fetch 호출 없음, refresh_market_cap_from_panel 은 저장된 clean parquet 를 읽는다. 라이브 수집 아님 -> 16:50 유지 | AFTERMARKET 관측 문서 개정 / PLANS 372 |
| C18 | DONE | close_auction.block_v_accel_min=1 의 근거 확인 | DONE 2026-09-12. 답: 근거 문제가 아니라 죽은 가지. _normal_close_auction_decision 은 use_intraday_realtime_entry 의 else 에만 있고 운영은 항상 realtime 이다. 종가매매 게이트 전체가 미도달 -> 08-20 변경 9번(max_day_range_pct 0.30)도 효과 없었음 | PLANS 373 |
| C19 | DONE | final_score_base 가중 0.75/0.25 가 코드 기본값이고 선언이 없다 | DONE 2026-09-12. stable_params_v41_1.json 에 w_tech_score=0.75 / w_fundamental_score=0.25 선언. 기본값과 동일해 동작 변화 없음. sha16 4ce1902870a80395 -> 78334bb072eb02ad. 백업 stable_params_v41_1.json.bak_20260912_192456. 123 passed | PLANS 373 |
| C20 | DONE | entry.py 수량 축소기 14개가 전부 곱셈이고 줄이기만 한다 - 곱의 측정 | DONE 2026-09-12. 곱을 쟀다. 사슬을 초기 산정 전으로 옮기고 budget_cap/split/adaptive 계측 추가(산식 불변, 127 passed). entry_size_floor_audit.py --chain 으로 소비. 실측 073010 698->10 x0.014, 단계 중앙배수 곱 0.0095. 재조정은 결정 항목으로 분리 | PLANS 373 |
| A8 | DECISION | 종가매매 경로를 살릴 것인가 지울 것인가 (close_auction 게이트 전체가 죽은 가지) | 사용자 결정. 08-20 변경 9번의 효과가 여기 달려 있다 | PLANS 374 |
| C21 | DONE | _offhours_news_session 이 16:00~20:00 을 evening(장외)으로 라벨 - 09-14 부터 장중 | DONE 2026-09-12. KRX_AFTERMARKET_* 상수 신설, 16:00~20:00 을 거래일에 한해 intraday 로 라벨. 개설일 전 소급 없음. 시험 5건, 140 passed | PLANS 374 |
| A9 | DECISION | 수량 축소기 재조정 여부 - 곱이 0.0095 (의도 수량의 1%) | 사용자 결정. 위험 노출을 늘리는 방향이다. 판단 재료는 09-14 이후 전체 사슬(budget_cap 포함) | PLANS 375 |
| C22 | OPEN | 뉴스 세션 라벨 불일치 - 배치는 intraday/afterhours/daily, 루프는 evening/night/dawn 을 보낸다 | 차단 없음. 동작은 안 깨지나 배치 분기 셋이 죽어 있다 | PLANS 377 |
| A10 | DECISION | 하네스 종목 목록을 투자 탭에도 놓을지 | 사용자 결정. 현황판(실험) vs 투자(실매매) 분리는 의도에 맞다. 평소 어느 탭을 보는지에 달렸다 | PLANS 378 |
| C23 | OPEN | 측정층이 생산의 진입 시점을 안 따라왔다 - 옵티마이저 same_close / 백테스트 next_open / 생산 intraday_realtime | 차단 없음. 백테스트 PF·게이트·HPO 숫자가 생산이 안 쓰는 진입 규칙 위의 값이다 | PLANS 11617/11762, report_backtest_v41_1.py:1095 |
| B5 | BLOCKED | frontend-design 플러그인 설치 (슬래시 명령이라 사용자만 실행 가능) | 차단: /plugin 은 내가 실행할 수 없다 / 트리거: 사용자가 /plugin install frontend-design@claude-plugins-official | PLANS 379 |
| C24 | DONE | 화면 단정문 37건 중 미처리 33건 - 표 안 '데이터가 없습니다' 류가 같은 형태 | DONE 2026-09-12. 33건 전부 처리. emptyStateLabel/mapToSidecarMeta 로 한 곳에 모음. 감사 중 3건 추가 발견(사이드카 status 무시 / 마지막실행 시각 날조 / 빌드 8일째 깨짐) 전부 수리. check:claims 21건, build 성공, 157 passed | PLANS 380 |
| A11 | DECISION | SIDECAR 이벤트를 아무도 생산하지 않는다 - 탐지기가 만드는 타입은 CIRCUIT_BREAKER/BOOST 뿐 | 사용자 결정. 진짜 사이드카는 코스피200 선물 ±5% 1분 지속이라 선물 데이터가 필요하다 | PLANS 382 |
| A12 | DECISION | 시장 이벤트 임계값이 제도와 다르다 - CB 1단계는 -8%인데 우리는 현물 -5% | 사용자 결정. 제도값을 쓸지 자체 프록시로 둘지. 프록시는 더 일찍 건다 | PLANS 382 |
| C25 | OPEN | CB 재개 직후 10분 단일가 구간 미처리 - 지정가 즉시체결 가정이 깨진다 | 차단 없음. 단일가/single_price/call_auction 참조 0건. 장 시작 단일가도 같은 문제 | PLANS 383, MARKET_HALT_RULES.md |
| C26 | OPEN | expectancyStatus 가 표본 10건 미만을 'active' 로 표시 - 판단불가를 가동중으로 보여준다 | 차단 없음. 현재 표본 넷 다 10건 이상이라 지금은 안 터진다 | PLANS 384 |
| C27 | OPEN | surge_detector_realtime.py 배치 실패 - 09-12 08:30 rc=1, 급등 원천이 STALE_INTRADAY_DATE 인 원인 | 차단 없음. 화면이 원천 상태를 말하게 한 덕에 연결됐다 | PLANS 385 |
| C28 | OPEN | topn 전진 선택 평가 - 표본 부족(신호일 9/5). 부호 안정까지 관측 지속 | 차단 없음. 저녁 배치에 배선됨. h=10 은 만기 미도달 | PLANS 386 |
| C29 | OPEN | 배치가 메모리로 죽으면 원인이 안 남는다 - 계측 0건 | 차단 없음. 지금은 여유 5.1GB/15.7GB 로 실제 문제 아님. 트리거: 배치 rc≠0 인데 stderr 에 사유가 없을 때 | 2026-09-13 low-memory 알림, 패널 로딩 9.4M행 |
| A13 | DECISION | 수집 배치 시각 이동(16:30->20:30) 승인 여부 - O5 불가침 경계를 읽지 않고 넘었다 | 사용자 결정. 피해는 0(결손 없음)이나 절차 위반. 되돌리면 09-14부터 장중 수집이 된다 | OBJECTIVE_LEDGER, PLANS 372/387 |
| A14 | DECISION | 이벤트 게이트 fail-closed 를 관측 실패에도 적용할지 - 빈도 실측 후 판단 | 사용자 결정. 측정 장치 2026-09-13 신설, 매일 밤 [5/6] 에 출력. 판단 재료는 09-14 이후 1주 | PLANS 389 |
| A15 | DONE | 루프 재기동 - (362) 승인 발주가 집행되지 않는다 | DONE 2026-09-13. 사용자 승인으로 10:18:25 재기동(pid 29080, 09:10 판 intraday_paper_loop.py 적재). 재기동 중 고아 락 결함 발견·수리(stale_lock_check.py, BROKEN_WINDOW E16). 워치독은 실측상 복구 못 했다 | PLANS 362/391 |
| A16 | DONE | 거시 특징이 마지막 체결일(08-24)에서 잘려 20일 낡은 값으로 오늘 진입을 결정한다 | DONE 2026-09-13. 사용자 승인. as_of 를 달력으로. 생산 재생성 실측: as_of 20260913, stale 0, VIXCLS/HY 20260910, global exposure 0.8->0.5. BROKEN_WINDOW E15 | build_rate_series_external.py:1496,1790-1792 / macro_feature_external_latest.json as_of_ymd=20260824 vs generated_for_ymd=20260913 |

---

## 처리 카드 (항목별 5단계)

근거 대화 2026-09-13: *"문제 도출되면 목록화 하기로한거아니었어?
목록화후 각대상별 검증 원인분석 판단 원인해결 보고"*

**표는 목록일 뿐이다.** 무엇을 확인했고 원인이 무엇이며 왜 그렇게 판단했는지는
그동안 PLANS 산문에만 있었다. 인용하려면 날짜를 찾아 헤매야 했다.
여기에 **항목별로 한 자리에** 모은다.

형식 — 한 항목당 다섯 줄. 비어 있으면 아직 그 단계를 안 한 것이다.
```
### <ID> <제목>
- 검증: 무엇을 재서 문제가 실재함을 확인했나 (수치·경로)
- 원인: 왜 그렇게 됐나 (증상이 아니라 기전)
- 판단: 고칠 것인가 / 둘 것인가 / 사용자 결정인가. 그 이유
- 조치: 실제로 무엇을 바꿨나 (파일·설정·해시)
- 보고: 무엇을 봐야 검증되나 + 그게 언제 오나
```

    python tools/open_items.py --card C9            # 카드 보기
    python tools/open_items.py --cards              # 카드 없는 항목 찾기
    python tools/open_items.py --card C9 --verify "..." --cause "..." --judge "..." --fix "..." --report "..."

### A13 수집 배치 시각 이동(16:30->20:30) 승인 여부 - O5 불가침 경계를 읽지 않고 넘었다
- 검증: VIBE_Investor_Flow_Daily 가 **당일 자료**를 받는 것을 파켓으로 확인. 09-02~09-11 전부 수집일==최대date. 09-12 수집 747종목 성공747/실패0, span 20260716~20260911 43,934행 결손0
- 원인: 2026-09-14 KRX 애프터마켓(16:00~20:00) 개설으로 16:30 이 장중이 된다. 게다가 fetch_investor_flow 는 already_today() 로 같은 날 재수집을 건너뛰어(--no-resume 없음) 부분치가 그대로 굳는다
- 판단: **유지 권고.** 되돌리면 09-14부터 라운드 원자료 정의가 갈린다 - 그것이 문서가 이 배치를 보호하려던 피해다. 문서의 16:30 은 애프터마켓 이전 숫자다. 단 절차는 틀렸다(O5 불가침 경계를 읽지 않고 변경) -> 사용자 결정
- 조치: 2026-09-12 19시경 16:30 -> 20:30. OBJECTIVE_LEDGER 에 경계 침범 기록(2026-09-13). boundary_watch.py 로 재발 탐지
- 보고: 사용자 승인 또는 되돌림 결정. 승인 시 boundary_watch --accept 로 기준선 갱신

### A14 이벤트 게이트 fail-closed 를 관측 실패에도 적용할지 - 빈도 실측 후 판단
- 검증: 진입 게이트 fail-closed 가 관측 실패에도 걸리는 것을 코드로 확인(_market_event_observation_block). 빈도는 **아직 못 쟀다** - 탐지기 상태 파일이 매 실행 덮여 이력이 없었다
- 원인: 사용자 승인 근거 '그 기간에는 매매가 이루어지지 않으니까' 는 **실제 발동 구간**(사이드카·CB)에 대한 것이었다. 나는 그것을 **WS 끊김**까지 덮는 것으로 넓게 읽었다. 그때 시장은 정상이다
- 판단: **사용자 결정 - 빈도에 달렸다.** 하루 한두 번이면 보수적으로 둘 만하고, 수시면 매매를 사실상 정지시킨다. ①실제CB와 ②WS끊김을 우리가 구별할 방법이 지금 없다. 비용은 비대칭(②는 하루 진입 손실, ①은 폭락 중 매수)
- 조치: 측정 장치 신설(2026-09-13): market_observation_ledger.jsonl 에 성공·실패 모두 기록, market_observation_report.py 가 장중·거래일만 집계, 야간 감시 [5/6] 에 배선. **게이트 자체는 그대로 뒀다**
- 보고: run_invariant_watch.bat 이 매일 20:50 에 실패율을 찍는다. 판단 재료는 09-14 이후 1주. 예상은 5% 미만이고 장 시작 직후에 몰릴 것 - 다르면 그 차이가 발견

### C25 CB 재개 직후 10분 단일가 구간 미처리 - 지정가 즉시체결 가정이 깨진다
- 검증: paper_engine/ 와 kis_order_dispatch_from_exec.py 에 단일가/single_price/call_auction 참조 **0건**(close_auction 은 종가 단일가라 별개)
- 원인: CB 재개 직후 10분은 단일가 매매다(사용자 제공 조문). 우리 집행은 지정가 즉시체결·호가 기반 LOB 판정을 가정한다 - 그 동안 성립하지 않는다. 장 시작 단일가(08:30~09:00)도 같은 문제
- 판단: 지금 고치지 않는다. CB 는 드물고, 장 시작 단일가는 우리가 09:00 이후에만 진입하므로 현재 노출이 작다. 다만 **가정이 깨지는 구간이 있다는 사실**을 기록한다
- 조치: docs/references/MARKET_HALT_RULES.md 에 제도와 우리 구현을 표로 대조. 조치 없음
- 보고: CB 가 실제로 발동하는 날. 그때 우리 주문이 어떻게 처리됐는지 원장으로 확인

### C29 배치가 메모리로 죽으면 원인이 안 남는다 - 계측 0건
- 검증: **판단을 뒤집는다.** 2026-09-13 하루에 저메모리로 배경 작업이 **두 번** 죽었고, 두 번째는 run_paper_daily 배치를 중간에 끊었다(pid 13708, 락만 남음). 죽은 뒤 여유 8.7GB 로 회복 - 즉 그 순간에만 몰린다
- 원인: 배치가 패널 9.4M행을 로딩하는 구간에 대화형 앱(ChatGPT 3개 1.27GB + codex 687MB + claude 499MB)이 겹친다. 죽으면 rc 만 남고 사유가 안 남는다
- 판단: **앞선 판단(지금 고치지 않는다) 철회.** 실제로 배치를 끊었고, 더 나쁜 것은 **고아 락이 루프를 2시간 막는다**는 것이었다. 메모리 계측보다 **락 처리**가 먼저다 - 그쪽이 ①을 막는다
- 조치: 루프의 _daily_batch_lock_state 가 run.info 의 pid 를 보게 함(죽었으면 무시, 판정불가면 fail-closed). 시험 5건. 고아 락 수동 제거. **메모리 계측 자체는 아직 안 넣었다**
- 보고: 다음 저메모리 킬 때 배치가 죽어도 루프가 안 멈추는지. 그리고 그때 사유가 로그에 남는지 - 안 남으면 그때 계측을 넣는다

### A16 거시 특징이 마지막 체결일(08-24)에서 잘려 20일 낡은 값으로 오늘 진입을 결정한다
- 검증: macro_feature_external_latest.json as_of_ymd=20260824 인데 generated_for_ymd=20260913. 같은 실행이 쓴 rate_series_external_all_latest.csv 는 20260911 까지 있다 - 데이터는 있고 요약만 잘렸다
- 원인: build_rate_series_external.py:1496 as_of_ymd=_derive_d_from_fills() -> 1790-1792 에서 feature_df 를 그 날짜로 절단. 마지막 체결일이 08-24 라 거시 전체가 20일 전으로 잘린다. 2026-08-21 에 판정 기준일만 분리하고 절단은 '별도 판단 대상' 으로 남긴 자리다
- 판단: 절단 제거는 **완화가 아니다**(내 첫 기술은 틀렸다). 실측: stale 10->0 이라 macro_critical_bad 2->0 으로 REDUCE 사유 하나는 사라지지만, global exposure_multiplier 는 0.8->0.5 로 오히려 줄어든다(VIX 15.85->17.84, US10Y 4.70->4.95). 방향이 섞여 있고 09-14 가 O6 판정일이라 시점이 걸린다 - 사용자 결정
- 조치: 
- 보고: 반증 실행은 tools/ 안 임시복사본 + LOGS 만 임시경로로 돌렸다(SSOT 미변경, 임시파일 제거 완료). 첫 두 번의 반증은 무효였다: ROOT 가 옮겨가 api_key_missing 으로 전 계열 None 이 나왔다
