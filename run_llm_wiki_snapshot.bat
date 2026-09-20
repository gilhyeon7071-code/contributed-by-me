@echo off
setlocal

set "ROOT=E:\1_Data"
set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"

cd /d "%ROOT%" || exit /b 1
"%PY%" "%ROOT%\tools\build_llm_wiki_snapshot.py"
exit /b %ERRORLEVEL%

