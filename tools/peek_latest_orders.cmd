@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d E:\1_Data

if not exist 2_Logs mkdir 2_Logs

if not exist paper\orders (
  echo [FAIL] folder not found: paper\orders
  exit /b 2
)

dir /b /a:-d /o-d paper\orders\*.xlsx 1> 2_Logs\orders.list.txt 2> 2_Logs\orders.list.err.txt

for /f "usebackq delims=" %%F in ("2_Logs\orders.list.txt") do (
  set "LATEST=%%F"
  goto :HAVE
)

echo [INFO] no orders xlsx found in paper\orders
type 2_Logs\orders.list.err.txt
exit /b 0

:HAVE
echo LATEST=!LATEST!

python -c "import pandas as pd; p=r'paper\\orders\\%LATEST%'; df=pd.read_excel(p,engine='openpyxl'); print('FILE=',p); print('rows=',len(df)); print('cols=',df.columns.tolist()); print(df.head(20).to_string(index=False))" > 2_Logs\orders.latest.peek.txt 2>&1

type 2_Logs\orders.latest.peek.txt
exit /b 0
