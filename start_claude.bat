@echo off
REM E:\1_Data 를 프로젝트 루트로 Claude Code 를 연다.
REM  - .claude\skills\ (프로젝트 스킬 7개)
REM  - .claude\settings.local.json (권한 허용목록)
REM  - AGENTS.md
REM  위 셋은 **이 디렉터리에서 열었을 때만** 읽힌다. (2026-09-11)
cd /d E:\1_Data
"C:\Users\jjtop\.local\bin\claude" %*
