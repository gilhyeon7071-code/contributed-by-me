@echo off
setlocal
set "ROOT=%~dp0"
set "PY=%ROOT%_runtime\python312-embed\python.exe"
if exist "%PY%" (
  "%PY%" "%ROOT%tools\build_surge_state_machine_shadow.py"
  "%PY%" "%ROOT%tools\build_surge_precursor_coverage_matrix.py"
  "%PY%" "%ROOT%tools\build_surge_preopen_precursor_readiness.py"
  "%PY%" "%ROOT%tools\preopen_5min_check.py" %*
) else (
  python "%ROOT%tools\build_surge_state_machine_shadow.py"
  python "%ROOT%tools\build_surge_precursor_coverage_matrix.py"
  python "%ROOT%tools\build_surge_preopen_precursor_readiness.py"
  python "%ROOT%tools\preopen_5min_check.py" %*
)
exit /b %ERRORLEVEL%
