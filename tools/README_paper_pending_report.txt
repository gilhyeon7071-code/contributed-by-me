README - paper_pending_report.py

이 ZIP은 다음 파일을 추가합니다.
- tools\paper_pending_report.py
- tools\README_paper_pending_report.txt (본 파일)

목적
- 진입은 되었지만(entry_date 존재), entry_date 이후(> entry_date) 가격 데이터가 아직 없어
  paper_engine가 청산/평가를 못하는 포지션을 [PENDING] 섹션으로 분리 출력합니다.

사용(Windows CMD)
1) 파이프라인 실행 후(또는 paper_engine 직후) 아래 실행:
   cd /d E:\1_Data && python tools\paper_pending_report.py

2) 출력 파일:
   E:\1_Data\2_Logs\paper_pending_report_<YYYYMMDD_HHMMSS>.txt
   E:\1_Data\2_Logs\paper_pending_report_<YYYYMMDD_HHMMSS>.json
