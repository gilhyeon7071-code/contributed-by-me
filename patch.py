import sys

with open('E:/1_Data/run_paper_daily.bat', 'r', encoding='utf-8-sig') as f:
    content = f.read()

target = '''  call :POST_CHAIN_STEP_FAIL "[16.957e/16] build_recovery_ssot_chain_report.py"
)

REM [16.957f/16] final integrated ops snapshot after late post-chain writers'''

replacement = '''  call :POST_CHAIN_STEP_FAIL "[16.957e/16] build_recovery_ssot_chain_report.py"
)

REM [16.957e2/16] strategy parameter optimization
echo [16.957e2/16] E:\vibe\\buffett\\tools\\build_strategy_optimization_analyzer.py
if exist "E:\vibe\\buffett\\tools\\build_strategy_optimization_analyzer.py" (
  "%PY%" E:\vibe\\buffett\\tools\\build_strategy_optimization_analyzer.py
  if errorlevel 1 (
    call :POST_CHAIN_STEP_FAIL "[16.957e2/16] build_strategy_optimization_analyzer.py"
  )
)

REM [16.957f/16] final integrated ops snapshot after late post-chain writers'''

if target in content:
    content = content.replace(target, replacement)
    with open('E:/1_Data/run_paper_daily.bat', 'w', encoding='utf-8-sig') as f:
        f.write(content)
    print("SUCCESS")
elif target.replace('\n', '\r\n') in content:
    content = content.replace(target.replace('\n', '\r\n'), replacement.replace('\n', '\r\n'))
    with open('E:/1_Data/run_paper_daily.bat', 'w', encoding='utf-8-sig') as f:
        f.write(content)
    print("SUCCESS CRLF")
else:
    print("TARGET NOT FOUND")