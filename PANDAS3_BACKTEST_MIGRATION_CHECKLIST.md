# Pandas 3.0 백테스트 이행 점검표 (실코드 매핑)

기준일: 2026-04-16  
적용 루트: `E:\1_Data`, `E:\vibe\buffett`

## 0) 현재 기준선 확인
- [x] 현재 고정 버전: `pandas==2.3.3`
  - 근거: `E:\1_Data\requirements.txt`
- [ ] 2.3 기준 경고 0건(`FutureWarning`/`DeprecationWarning`) 검증 로그 확보
- [ ] 3.0 업그레이드 전/후 백테스트 결과 비교 리포트 확보

## 1) 사전 게이트
- [ ] 브랜치/태그 고정(`pandas3-migration-baseline`)
- [ ] 2.3 환경에서 테스트 실행(경고를 에러로 승격)

실행 명령(권장):
```bash
cd E:\1_Data
pytest -W error::FutureWarning -W error::DeprecationWarning
```

## 2) 점검표 매핑 (파일 단위)

### A. 시계열 인덱싱 / 리샘플 / 롤링
- [ ] Rolling 결과 shape/NaN 경계 회귀
  - `E:\1_Data\tools\backtest_validation_framework.py`
  - `E:\1_Data\tools\macro_signal_daily.py`
  - `E:\1_Data\tools\surge_backtest_report.py`
  - `E:\1_Data\tools\surge_ml_train.py`
  - `E:\1_Data\tools\surge_param_validator.py`
- [ ] (있다면) resample/asfreq 경계 포함/제외 테스트
  - 현재 주요 백테스트 경로에서 `resample` 사용 흔적은 낮음(추가 확인 필요)

### B. groupby semantics
- [ ] groupby 집계 반환 shape/컬럼명/인덱스 고정
  - `E:\1_Data\paper_engine.py`
  - `E:\1_Data\tools\backtest_validation_framework.py`
  - `E:\1_Data\tools\signal_integration_daily.py`
  - `E:\1_Data\tools\market_data_adapter.py`
  - `E:\1_Data\tools\p0_onepass_from_fills.py`
- [ ] `as_index`, `dropna`, `group_keys` 의도 명시/테스트

### C. timezone / 날짜 경계
- [ ] tz-naive/tz-aware 혼용 점검
  - `E:\vibe\buffett\dashboard.py` (표시 레이어 포함)
  - `E:\1_Data\tools\backtest_validation_framework.py`
- [ ] 장중/장마감(09:00/15:20/15:30) 경계 회귀
  - `E:\1_Data\paper_engine.py`
  - `E:\vibe\buffett\data\live\live_fills.csv` 기반 검증 루틴

### D. dtype / nullable
- [ ] `pd.NA` 연산/비교/마스킹 회귀
  - `E:\1_Data\paper_engine.py`
  - `E:\1_Data\tools\build_backtest_symbol_panel_csv.py`
  - `E:\1_Data\tools\surge_backtest_report.py`
  - `E:\1_Data\tools\surge_lob_ingest.py`
- [ ] CSV/Parquet IO 이후 dtype drift 확인
  - 산출물 루트: `E:\1_Data\2_Logs\`

### E. 백테스트 결과 정합성
- [ ] 비교 대상 지표 고정: 체결건수/승률/PF/MDD/Sharpe/월별손익
  - 생성기:
    - `E:\1_Data\tools\build_backtest_validation_checklist.py`
    - `E:\1_Data\tools\build_backtest_final_output.py`
    - `E:\1_Data\tools\build_backtest_analysis_structure_check.py`
- [ ] 결과 파일 비교:
  - `E:\1_Data\2_Logs\backtest_validation_latest.json`
  - `E:\1_Data\2_Logs\backtest_final_output_latest.json`
  - `E:\1_Data\2_Logs\backtest_analysis_structure_latest.json`

## 3) 테스트 커버리지(현재)
- [x] 백테스트 프레임워크 테스트 파일 존재
  - `E:\1_Data\tests\test_backtest_validation_framework_dsr.py`
- [ ] Pandas 경고 승격 테스트(전역) 없음
- [ ] timezone/rolling/groupby semantics 전용 회귀 테스트 부족

## 4) 단계별 실행 절차
1. [ ] 2.3 고정 환경에서 경고 에러 테스트 통과
2. [ ] 핵심 산출물 1회 생성(기준선 저장)
3. [ ] Pandas 3.0 환경에서 동일 파이프라인 실행
4. [ ] 산출물/지표 비교(JSON diff + KPI diff)
5. [ ] 편차 임계 초과 시 FAIL-CLOSED(업그레이드 중단)

## 5) FAIL-CLOSED 기준
- [ ] 경고/예외 발생 시 실패
- [ ] 핵심 지표 편차 임계 초과 시 실패
- [ ] 결과물 스키마 불일치 시 실패

## 6) 빠른 실행 커맨드 모음
```bash
cd E:\1_Data

# 1) 테스트(경고를 에러로)
pytest -W error::FutureWarning -W error::DeprecationWarning

# 2) 백테스트 검증 산출
python tools\build_backtest_validation_checklist.py
python tools\build_backtest_final_output.py
python tools\build_backtest_analysis_structure_check.py
```

