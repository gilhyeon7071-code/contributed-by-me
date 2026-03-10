[목적]
- '1000만원 / 10개 후보 / 5개 매매 + 5개 비교군' 플랜을 코드/설정에 반영하려면,
  현재 프로젝트(E:\1_Data)에서 후보 수/매매 슬롯/자본금 관련 설정이 어디에 있는지 먼저 찾아야 합니다.
- 이 패키지는 '변경'이 아니라 '검색/진단'만 합니다.

[사용법] (Windows CMD)
1) zip을 E:\1_Data\plan_10_5_grep_pkg.zip 로 복사
2) 압축 해제:
   cd /d E:\1_Data && powershell -NoProfile -Command "Expand-Archive -Force 'E:\1_Data\plan_10_5_grep_pkg.zip' 'E:\1_Data'"
3) 실행:
   cd /d E:\1_Data\tools && call run_plan_10_5_grep.cmd
4) 결과 파일:
   E:\1_Data\_diag\plan_10_5_grep.txt

[다음]
- plan_10_5_grep.txt 내용을 그대로 채팅에 붙여주면,
  그 결과(파일/라인)만 근거로 해서 수정 지점을 지정하고 '전체 파일' 교체본을 만들어 드릴 수 있습니다.
