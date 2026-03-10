# E:\1_Data 정리(불필요 파일 D:\로 이동) - CMD 전용

## 목적
- 운영에 직접 필요 없는 산출물/백업/패키지/아카이브를 D:\1_Data_Archive\<timestamp>\ 로 이동합니다.
- paper\ 와 12_Risk_Controlled\ 는 보호(이동 금지)합니다.

## 사용법 (CMD)
### 1) DRY RUN (추천)
cd /d E:\1_Data
cleanup_1_data.cmd

### 2) 실제 이동
cd /d E:\1_Data
cleanup_1_data.cmd DOIT

## 결과
- D:\1_Data_Archive\<timestamp>\cleanup_report.json 생성
