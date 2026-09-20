@echo off
setlocal

set "ROOT=E:\1_Data"
set "PS1=%ROOT%\tools\register_llm_wiki_scheduler.ps1"

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%PS1%" -Apply
exit /b %ERRORLEVEL%
