# P0: gate1(risk_off) FAIL 해결용 패치

## 완료조건
- `p0_daily_check.py`에서 `risk_off=False`로 떨어지고, `gate_daily.py`의 `gate1=PASS`가 나온다.
- 원인(데이터 신선도 부족)이 해결되면 `run_paper_daily.bat`가 `ERRORLEVEL=1`로 종료되지 않는다.

## 지금 FAIL의 사실(당신 로그 기준)
- `risk_off=True`
- 이유 2개:
  - `krx_clean_date_max(20251225) < prev_weekday(20251226)`
  - `meta_latest_date(20251224) < prev_weekday(20251226)`
즉, **KRX clean parquet(전종목 데이터)와 candidates(meta)가 전 영업일(12/26)까지 갱신되지 않아** gate1이 FAIL로 막힌 상태입니다.

## 제공 파일
- `krx_update_clean_incremental.py`  
  - 현재 존재하는 `krx_daily_*_clean.parquet`의 **최신 date_max 이후**부터
  - **전 영업일(prev weekday)**까지의 누락 일자를 `pykrx`로 받아
  - **새로운** `krx_daily_YYYYMMDD_YYYYMMDD_clean.parquet` 파일로 저장합니다(기존 파일은 건드리지 않음).
- `run_krx_refresh_and_candidates.bat`  
  - 위 스크립트 실행 → `generate_candidates_v41_1.py` 실행까지 한 번에 수행합니다.

## 실행 순서(명령 그대로)
```bat
cd /d E:\1_Data
run_krx_refresh_and_candidates.bat
run_paper_daily.bat
```

## 기대 결과
- `p0_daily_check.py` 출력의 `krx_clean_date_max`가 `prev_weekday` 이상으로 올라가면서 `risk_off=False`
- `gate_daily.py`에서 `gate1=PASS`

