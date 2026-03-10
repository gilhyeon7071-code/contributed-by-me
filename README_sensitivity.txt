v41.1 민감도(±20%) 리포트 패키지

목적
- stable_params_v41_1.json을 기준으로 핵심 파라미터를 ±20% 흔들었을 때
  IS/VAL/OOS 성과(특히 OOS)가 얼마나 붕괴/유지되는지 자동 리포트 생성

설치/반영 (CMD)
1) zip을 E:\1_Data 로 복사
2) CMD에서 아래 실행
   cd /d E:\1_Data
   python -c "import zipfile; z=zipfile.ZipFile(r'E:\1_Data\_fix_outputs_sensitivity_v41_1.zip'); z.extractall(r'E:\1_Data'); z.close(); print('OK: extracted')"

실행 (CMD)
- cd /d E:\1_Data
- run_sensitivity_v41_1.bat

산출물
- 12_Risk_Controlled\sensitivity_report_v41_1.csv
- 12_Risk_Controlled\sensitivity_report_v41_1.json

비고
- optimize_params_v41_1.py 내부의 load_data/build_windows/eval_params를 그대로 호출합니다.
- 내부 구현이 바뀌어 함수명이 사라지면 스크립트가 FAIL로 종료합니다.
