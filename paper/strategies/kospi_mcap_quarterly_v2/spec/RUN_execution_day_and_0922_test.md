# 집행일 실행기 · 09-22 1건 시험 절차

- 만든 날: 2026-09-17
- 코드: `src/run_execution_day.py`, `src/kis_adapter.py`, `src/check_account.py`
- 시험: `tests/test_kospi_mcap_quarterly_v2_runner.py` (21건, 가짜 한투로 전 경로)
- 09-17 개정: 전략 자본 6,000만원, `--cancel-test`, `liquidate`, `post-exec-check` 추가(사용자 승인)

## 09-22(화) 장중 시험 — 명령 하나

```powershell
cd E:\1_Data
$env:PYTHONIOENCODING='utf-8'
python -m paper.strategies.kospi_mcap_quarterly_v2.src.run_execution_day test-one --code 005930 --out-dir E:\1_Data\paper\strategies\kospi_mcap_quarterly_v2\data\test_runs\20260922 --cancel-test
```

- 시각: **10:00 이후 권장**(09:05~15:20 창 안, 09:19:30~09:21:30 카나리아 구간 밖). 창 밖이면 한 건도 안 보내고 `STOP` 으로 끝난다
- 비용: 삼성전자 1주 매수·매도 왕복, 약 25만원 노출 몇 분, 비용 약 600원
- 결과: `test_one_20260922_HHMMSS.json` + 날짜 없는 `test_one_log.jsonl`. rc 0 = OK, 3 = STOP
- 전날 리허설(발주 없음, 장 밖 가능): 같은 명령에 `--no-submit` — **09-17 17:05 실측 통과**(호가 254,500/254,000, 매수가능 조회 정상)

### 도는 순서

1. 창·휴장 확인 → 잔고·호가·매수가능 조회(시작값)
2. 매수 1주 지정가 = 매도호가1 올림. **보내기 직전 호가를 다시 받아** 가격 갱신
3. 3초마다 체결 조회, 최대 60초. 모르는 상태면 조회로 풀고 **재전송하지 않는다**
4. 60초 안에 안 끝나면 즉시 취소 → 한 번 더 조회
5. 원장·잔고·매수가능 조회(매수 뒤)
6. 체결된 수량만큼 매도, 지정가 = 매수호가1 내림 → 3~4 반복
7. 원장·잔고·매수가능 조회(매도 뒤) → 사고 판정 → 보고

### 이 시험으로 확인하는 가정 (`checks`)

| 키 | 확인하는 것 | 기대 |
|---|---|---|
| `order_no_returned` | 주문 응답에 주문번호가 온다 | true |
| `ccld_row_matched` | 체결 조회 행이 주문번호로 귀속된다 | true |
| `balance_prpr_present` | 잔고 행에 현재가 `prpr` 가 있다 | true |
| `buy_fee_pct_inferred` | D+2 예수금 감소 − 체결금액 = 수수료 | 0.015% 근처면 설정 유지, 다르면 설정 수정 |
| `sell_fee_tax_pct_inferred` | 매도 수수료+세금 | 0.215% 근처 |
| `same_day_reuse_rise` | 매도 뒤 재사용가능금액이 오른다 | > 0 이면 재구성일 같은 날 매수 가능 |
| `broker_holding_back_to_start` | 계좌 보유가 시작과 같아진다 | true |
| `cancel_exercised` | 취소가 쓰였으면 그 응답 | 대개 비어 있음(즉시 체결) |

`--cancel-test` (09-17 사용자 승인): 왕복 뒤 매수호가1 × 0.9 내림 가격으로 1주 매수 → 10초 → 취소 → 조회.
추가 확인 키: `cancel_response_ok`, `cancel_final_state`(CANCELLED_UNFILLED), `cancel_row_after`(원 주문 행 필드 원문),
`cancel_confirmed_by_query`(true). 취소가 새 주문번호 행으로 따로 찍히는지는 보고서 `same_code_rows_after` 로 본다.
비용 0원(체결 안 됨). 조회에 취소가 안 보이면 `CANCEL_NOT_CONFIRMED` 사고로 STOP — 그 자체가 확인 결과다.

## 재구성일 (10-01) 명령

```
rebalance --phase SELL --target <target_portfolio_20260930.csv> --summary <target_20260930_summary.json> --state-dir <상태폴더>
sync      --state-dir <상태폴더>      (매도 체결 확인)
rebalance --phase BUY  ... 같은 인자
sync      --state-dir <상태폴더>
cancel    --state-dir <상태폴더>      (15:20 이후)
ledger    --state-dir <상태폴더>
post-exec-check --state-dir <상태폴더> --summary <target_20260930_summary.json>   (비중 판정, 집행 직후 한 번만)
```

**(09-19 추가) post-exec-check 가 OK 면 현재 목표 등록 — 이게 없으면 다음 날부터의 일일 운용이 "활성 목표 없음" 으로 아무것도 하지 않아요:**
```
python -m paper.strategies.kospi_mcap_quarterly_v2.src.daily_ops set-current --state-dir <상태폴더> --target <target_portfolio_20260930.csv> --summary <target_20260930_summary.json>
```
그 뒤는 예약 작업 3개가 매일 돌아요(exec plan 4-2): `VIBE_V2_Daily_Evening_2020` / `VIBE_V2_Daily_Morning_1000` / `VIBE_V2_Daily_Afternoon_1525` → `run_v2_daily_ops.bat`. 자동 발주는 `config/daily_ops_v1.json` `auto_submit=true` 일 때만(사용자 승인).

한도(−25%) 래치가 켜진 **다음 거래일부터**: `liquidate --state-dir <상태폴더> [--no-submit]` — 원장 보유 전량을 보내기 직전 매수호가1 내림 지정가로. 하루 한 번(주문 ID 에 날짜), 자동 재개 없음, 래치 파일 유지. 전날 주문이 살아 있으면 멈춤.

- 모든 단계에 `--no-submit` 리허설이 있다(09-28 go/no-go 전에 돌린다)
- 매수 가능액 = min(전략 원장 경제적 현금, 한투 `nrcvb_buy_amt`). 모자라면 수량을 줄이지 않고 `BUY_CASH_SHORT` 로 멈춤(C9)
- 전략 원장 보유 > 계좌 보유면 멈춤. 계좌에만 있는 보유는 계좌 점검에서 `WARN`
- 50종목 호가를 차례로 받으면 앞 종목이 5초를 넘는다 → 계획은 수량만 정하고 **신선도·지정가는 보내기 직전 한 건씩**
- `test-one` 은 전략 상태 폴더(`data/state`)를 거부한다

## 계좌 점검 (읽기 전용)

```
python -m paper.strategies.kospi_mcap_quarterly_v2.src.check_account --out-dir <폴더> [--state-dir <상태폴더>]
```
- 판정: `nrcvb_buy_amt` ≥ 전략 자본(09-17 부터 6,000만원)
- 09-17 17:05 기준선: 보유 0, `dnca_tot_amt` 3,342,301, D+2 104,505,086, `nrcvb_buy_amt` 104,418,011, `ruse_psbl_amt` 101,075,710 → OK
- 09-21(월) 장 마감 뒤 다시 — topn 매도(09-17 목) 대금의 결제일이 D+2 = 09-21(월, `HolidayManager` 실측). `dnca_tot_amt` 가 약 1억 440만원으로 올라오는지
