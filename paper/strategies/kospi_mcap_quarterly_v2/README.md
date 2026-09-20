# 분기 시총 (`KOSPI_MCAP_QUARTERLY_V2`)

- 만든 날: 2026-09-17
- 한 줄: KOSPI 보통주 시가총액 상위 50종목을 분기마다 재구성하고, KOSPI200 이 MA200 아래면 노출 50%
- 첫 재구성: 선정일 2026-09-30 / 집행일 2026-10-01 (가부 판정 09-28)
- 전략 자본: 6,000만원 (KIS 모의계좌에서 운영, 09-17 3,000만원에서 개정 — 노출 50% 정수화 통과용)

## 문서

| 무엇 | 어디 |
|---|---|
| 목적(1층) | `docs/references/NEW_LOGIC_L1_PURPOSE_20260916.md` |
| 뼈대·게이트 | `docs/references/TRADING_LOGIC_SKELETON_20260916.md` |
| 용어 | `docs/references/NEW_LOGIC_GLOSSARY.md` |
| 실행계획 | `docs/exec-plans/active/20260916_new_logic_first_rebalance_20261001.md` |
| 칸 명세 | `spec/` |
| 확인된 사실 | `spec/confirmed_facts.md` |

V1(`kospi_mcap_quarterly_v1`)과 폴더·원장·산출물을 섞지 않아요.
