# Dashboard UI Preflight Checklist (SSOT)

Last updated: 2026-03-17  
Scope: `E:\vibe\buffett\dashboard.py`, `E:\vibe\buffett\dashboard_stock_v2.py`

## 1) 목적
- 대시보드 수정 전에 반드시 확인해야 할 항목을 고정한다.
- P0 -> P1 -> P2 순서를 강제한다.
- 수정 후 복구 반복과 역할 혼합 재발을 막는다.

## 2) 사전 확인
- [ ] 이번 수정 대상 화면이 `투자 / 운영 / 전략` 중 어디인지 명확한가
- [ ] 이번 수정이 P0 / P1 / P2 중 어디에 해당하는지 정했는가
- [ ] 관련 SSOT를 확인했는가
  - `dashboard_safe_change_protocol.md`
  - `dashboard_ui_operating_rules_ssot_latest.md`
  - `ui_numeric_display_rules.md`
  - `ui_typography_hierarchy_rules.md`

## 3) P0 체크
- [ ] 개발 UI (`Deploy`, `Rerun`, `Always rerun`) 노출이 없는가
- [ ] `keyboard_double_arrow_right` 같은 아이콘 문자열 노출이 없는가
- [ ] 내부키 (`dashboard_state.*`, `observer_state.*`, `next_step=*`)가 기본 화면에 안 보이는가
- [ ] 숫자 깨짐 (`?1.00억`, `?0`)이 없는가
- [ ] 투자/운영/전략 역할이 섞이지 않았는가

## 4) P1 체크
- [ ] 상단 정보가 `현재 화면 / 기준일 / 검증 상태 / 다음 조치 / 알림` 순서로 읽히는가
- [ ] `문제/원인/조치` 보드가 사용자 문장으로 보이는가
- [ ] 메뉴보다 KPI가 먼저 보이는가
- [ ] 한글/영문/내부명 혼합이 없는가

## 5) P2 체크
- [ ] 자산곡선/낙폭에 해석 문장이 있는가
- [ ] 데이터 신선도 카드가 있는가
- [ ] stale/lag 상태가 사용자 문장으로 보이는가

## 6) 데이터/이름 체크
- [ ] 종목명 한글이 깨지지 않았는가
- [ ] 섹터명이 깨지지 않았는가
- [ ] 이름 복구 소스가 정상인가
  - `E:\1_Data\2_Logs\candidates_latest_data.csv`
  - `E:\1_Data\_cache\dart_corp_code_map.csv`
  - `E:\1_Data\2_Logs\code_name_cache_latest.json`

## 7) 수정 절차 체크
- [ ] 백업 생성
- [ ] 소규모 패치 적용
- [ ] `py_compile` 통과
- [ ] 대상 화면 스모크 체크
- [ ] 필요 시 대시보드 재시작
- [ ] `dashboard_state_latest.json` 핵심 상태 확인
  - `overall`
  - `alerts_count`
  - `orders_contract_ok`
  - `lvb_recent_ok`

## 8) 완료 판정
- [ ] 화면 기준으로 요청 사항이 반영되었는가
- [ ] 규칙 위반 항목이 남지 않았는가
- [ ] 추가 개선사항은 별도 목록으로 분리했는가
