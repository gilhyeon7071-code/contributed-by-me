## 목표
- `대시보드 진입차단`의 직접 원인인 `krx_clean_universe_degraded(ncode < MIN_UNI)`를 제거한다.
- 범위는 `same-day degraded clean parquet` 생성 차단과 `p0_daily_check.py`의 KRX universe 판정 정렬에 한정한다.

## 비목표
- kill_switch, DAILY_LOSS, 뉴스, surge, dashboard UI 자체는 변경하지 않는다.
- KRX 전체 데이터 수집 방식이나 MIN_UNI 정책 의미는 바꾸지 않는다.

## 작업 범위
- `E:\1_Data\krx_update_clean_incremental.py`
- `E:\1_Data\p0_daily_check.py`
- 필요 시 현재 잘못 생성된 `E:\1_Data\_krx_manual\krx_daily_20260421_20260421_clean.parquet` 정리

## 현재 동작
- `krx_update_clean_incremental.py`는 `soft_skipped_days`를 기록하면서도 same-day clean parquet를 계속 쓸 수 있다.
- `generate_candidates_v41_1.py`는 `effective_ncode`를 사용해 tolerated zero-OHLC deficit을 soft allow 한다.
- `p0_daily_check.py`는 raw `krx_clean.ncode`만 사용해 `risk_off`에 `krx_clean_universe_degraded(...)`를 추가한다.

## 위험 요소
- KRX clean write 경로 수정은 배치 산출물 최신 선택에 직접 영향을 준다.
- `p0_daily_check.py` 판정 수정은 risk_off 이유 문자열과 gate 결과에 영향을 준다.
- FAIL-CLOSED 의미 변경 없이 tolerated zero-OHLC deficit에 한해 기존 soft allow 의미와 맞춰야 한다.

## 구현 계획
1. `krx_update_clean_incremental.py`에서 soft-skip day는 `all_frames/fetched_days`에 포함하지 않도록 수정한다.
2. `p0_daily_check.py`에서 `krx_price_integrity_status_latest.json`의 soft-skip/effective count를 읽어 same-day tolerated deficit이면 effective count로 판정한다.
3. 현재 남아 있는 degraded same-day clean parquet는 백업 후 quarantine 처리한다.

## 검증 계획
1. 기능 검증: `py_compile`로 수정 파일 문법 확인
2. 정합성 검증: `krx_price_integrity_status_latest.json`과 `p0_daily_check_*.json` 값 대조
3. 운영 반영 검증: `run_paper_daily.bat` 재실행
4. 정책 검증: `MIN_UNI`와 risk_off 의미가 tolerated zero-OHLC deficit 외에는 유지되는지 확인
5. FAIL-CLOSED 검증: effective count도 `MIN_UNI` 미만이면 계속 차단되는지 확인
6. 회귀 검증: direct run과 batch 모두 종료 코드/산출물 확인

## 롤백 계획
- 백업본으로 즉시 복원
- quarantine 처리 파일은 백업본으로 재복구 가능
