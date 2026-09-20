## 2026-04-24 검증
- 백업:
  - `E:\1_Data\backup\20260424_price_integrity_adj_anomaly\20260424_1132_price_integrity_verify`
- 수정:
  - 없음
- 기능 검증:
  - `tests/test_price_integrity.py` 내 테스트 함수 4개 직접 호출 PASS
  - `pytest`는 현재 Python 경로에 없어 직접 호출로 대체
- 정합성 검증:
  - `E:\1_Data\2_Logs\price_integrity_status_latest.json` 파싱 통과
  - `E:\1_Data\2_Logs\krx_price_integrity_status_latest.json` 파싱 통과
- 운영 반영 검증:
  - paper status=PASS
  - paper date_max=20260423
  - paper fetched_days=20260423
  - paper rows_written=175759
  - KRX status=PASS
  - KRX range=20260423->20260423
  - KRX fetched_days=20260423
- 정책 검증:
  - 주문/엔진/Gate 정책 변경 없음
  - 가격 무결성 차단/보정 helper와 update path만 확인
- FAIL-CLOSED 검증:
  - invalid OHLC row 차단 테스트 PASS
  - spike candidate 감지 테스트 PASS
  - adjusted window repair 판단 테스트 PASS
- 회귀 검증:
  - paper blocked_rows=0
  - paper spike_candidates=0
  - paper repair_applied_rows=0
  - KRX blocked_rows=3
  - KRX spike_candidates=0
  - KRX repair_backfill_rows=0
  - KRX tolerated_zero_ohlc_blocks=3

## 목표
- `prices_update_paper_incremental.py`와 `krx_update_clean_incremental.py`에 수정주가 보정 후보 처리와 가격 이상값 차단을 최소 범위로 추가한다.
- 현재 배치 기준 `paper/prices/ohlcv_paper.parquet`가 비정상 봉 또는 최근 기업행위 급변을 그대로 삼키지 않도록 정렬한다.

## 비목표
- 주문/엔진/게이트 정책 변경
- 과거 전체 히스토리 일괄 재작성
- 백테스트 성과 개선 확정

## 작업 범위
- `E:\1_Data\prices_update_paper_incremental.py`
- `E:\1_Data\krx_update_clean_incremental.py`
- `E:\1_Data\tools\price_integrity.py` 신규
- `E:\1_Data\tests\test_price_integrity.py` 신규

## 현재 동작
- `prices_update_paper_incremental.py`는 pykrx 일별 시세를 그대로 `ohlcv_paper.parquet`에 합친다.
- `krx_update_clean_incremental.py`도 일별 clean parquet를 생성하지만 가격 이상값 차단/기업행위 보정 레이어는 없다.
- 최신 운영 경로는 `run_paper_daily.bat`에서 [1/9]와 [6.26/9] 단계로 가격 적재기를 재실행한다.

## 위험 요소
- 과도한 이상값 탐지로 정상 급등락을 누락할 수 있다.
- 기업행위 보정 로직이 잘못 적용되면 최근 2거래일 가격이 왜곡될 수 있다.
- FAIL-CLOSED 원칙상 검증 실패 시 배치가 중단될 수 있다.

## 구현 단계
1. 공통 가격 무결성 헬퍼 추가
2. 구조 오류/제로값/OHLC 오류 탐지 추가
3. 최근 2거래일 급변 종목에 한해 adjusted 시계열 비교 후 최근 윈도우 교체
4. 상태 JSON 기록 추가
5. 단위 테스트와 공식 배치 검증

## 검증 계획
1. 기능 검증: corporate-action repair 후보와 anomaly filtering helper 테스트
2. 정합성 검증: 최신 price parquet와 clean parquet 상태 JSON 확인
3. 운영 반영 검증: `run_paper_daily.bat`
4. 정책 검증: gate/entry 의미 불변 확인
5. FAIL-CLOSED 검증: 구조 오류 행이 차단되거나 배치가 중단되는지 로그 확인
6. 회귀 검증: 기존 배치 완료와 핵심 산출물 생성 여부 확인

## 롤백 계획
- 백업 파일로 `prices_update_paper_incremental.py`, `krx_update_clean_incremental.py`, `PLANS.md` 복원
- 신규 파일은 삭제 후 배치 재실행으로 상태 확인
