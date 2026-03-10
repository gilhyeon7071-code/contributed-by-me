FIX18 (kill_switch.mode -> REDUCE)

목표
- p0_daily_check의 kill_switch mode를 BLOCK -> REDUCE로 변경
- kill_switch 트리거가 걸려도, "B(REDUCE)" 모드로 신규데이터를 쌓아 검증 가능하게 하는 설정 단계

파일
- tools\apply_fix18_kill_switch_reduce.cmd
- tools\apply_fix18_kill_switch_reduce.py

적용(Windows CMD)
1) ZIP을 E:\1_Data에 풀기
2) 실행
   cd /d E:\1_Data
   tools\apply_fix18_kill_switch_reduce.cmd

만약 자동탐색이 실패하면(= kill_switch 들어있는 config를 못 찾으면)
   tools\apply_fix18_kill_switch_reduce.cmd E:\1_Data\paper\paper_engine_config.json

적용 후 검증
1) p0_daily_check에서 mode가 REDUCE로 찍히는지 확인
   cd /d E:\1_Data
   python p0_daily_check.py
   python -c "import json,glob; p=sorted(glob.glob(r'2_Logs\\p0_daily_check_*.json'))[-1]; j=json.load(open(p,'r',encoding='utf-8')); print('mode=',j['kill_switch']['limits'].get('mode')); print('triggered=',j['kill_switch'].get('triggered')); print('reasons=',j['kill_switch'].get('reasons'))"

2) run_paper_daily에서 gate_daily note / paper_engine 로그가 어떻게 변하는지 확인
   call run_paper_daily.bat
