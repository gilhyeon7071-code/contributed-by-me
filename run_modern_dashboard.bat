@echo off
setlocal

echo ========================================================
echo 1. Stopping Old Streamlit Dashboard (Port 8501)...
echo ========================================================
powershell -NoProfile -ExecutionPolicy Bypass -Command "$ps=Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -like '*streamlit run*' }; foreach($p in $ps){ try{ taskkill /F /PID $p.ProcessId | Out-Null } catch {} }" >nul 2>nul
echo Done.

echo ========================================================
echo 2. Initializing Modern Dashboard Environment...
echo ========================================================
powershell -NoProfile -ExecutionPolicy Bypass -Command "$ps=Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'node.exe' -and $_.CommandLine -like '*vite*' }; foreach($p in $ps){ try{ taskkill /F /PID $p.ProcessId | Out-Null } catch {} }" >nul 2>nul
echo Done.

echo ========================================================
echo 3. Starting Modern React Dashboard (Port 5173)...
echo ========================================================
cd /d E:\vibe\control_center_v2
start "Modern React Dashboard" /min cmd /c "npm run dev -- --host 127.0.0.1 --port 5173"

echo.
echo ========================================================
echo Start command issued! 
echo Please open your browser to:
echo http://localhost:5173
echo ========================================================
timeout /t 5
