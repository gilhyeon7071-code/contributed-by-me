Paper "실거래 vs 백테스트 괴리" 모니터링 (최소 기능)

이 패키지는 Paper fills.csv를 "실체결"로 보고,
동일 일자의 OHLC(Open/Close) 기준가와 비교한 슬리피지(%)를 일일 리포트로 남깁니다.

설치(CMD)
1) zip을 E:\1_Data 로 복사
2) CMD에서:
   cd /d E:\1_Data
   python -c "import zipfile; z=zipfile.ZipFile(r'E:\1_Data\_fix_outputs_live_vs_bt_paper_v1.zip'); z.extractall(r'E:\1_Data'); z.close(); print('OK: extracted')"

실행(CMD)
- cd /d E:\1_Data
- run_live_vs_bt_paper_daily.bat

산출물
- 2_Logs\live_vs_bt_paper_YYYYMMDD.csv
- 2_Logs\live_vs_bt_paper_YYYYMMDD.json

참고
- price parquet(ohlcv_paper.parquet) 컬럼명이 프로젝트마다 다를 수 있어 자동 매핑을 시도합니다.
  (date/ymd + code/ticker + open/close or 시가/종가)
- 가격 매칭 실패 행은 ref_open/ref_close가 비어 있고, summary에 missing으로 집계됩니다.
