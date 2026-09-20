@echo off
setlocal

set "ROOT=E:\1_Data"
set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"

cd /d "%ROOT%" || exit /b 1

"%PY%" "%ROOT%\tools\collect_llm_wiki_exports.py" --apply
if errorlevel 1 exit /b %ERRORLEVEL%

"%PY%" "%ROOT%\tools\validate_llm_wiki_source_contract.py" "%ROOT%\docs\llm_wiki\00_Inbox\raw_slack" "%ROOT%\docs\llm_wiki\00_Inbox\raw_meetings" "%ROOT%\docs\llm_wiki\00_Inbox\raw_docs"
if errorlevel 1 exit /b %ERRORLEVEL%

"%PY%" "%ROOT%\tools\ingest_llm_wiki_inbox.py" --apply
if errorlevel 1 exit /b %ERRORLEVEL%

"%PY%" "%ROOT%\tools\build_llm_wiki_snapshot.py"
if errorlevel 1 exit /b %ERRORLEVEL%

call "E:\vibe\buffett\run_llm_wiki_snapshot_rootb.bat"
if errorlevel 1 exit /b %ERRORLEVEL%

"%PY%" "%ROOT%\tools\build_llm_wiki_operational_classification.py"
if errorlevel 1 exit /b %ERRORLEVEL%

"%PY%" "%ROOT%\tools\build_llm_wiki_snapshot.py"
exit /b %ERRORLEVEL%
