@echo off
chcp 65001 >nul
setlocal

set ROOT=%~dp0
set PYTHON=%ROOT%.venv\Scripts\python.exe

echo [0/3] Tat cac process Python cu (neu co)...
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -like '*main_*.py*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }" 2>nul
timeout /t 2 /nobreak >nul

echo [1/3] Khoi dong Docker infrastructure...
docker compose up -d
if errorlevel 1 (
    echo [LOI] Docker compose that bai. Hay chac chan Docker Desktop dang chay.
    pause & exit /b 1
)

echo [2/3] Cho cac service san sang...
timeout /t 10 /nobreak >nul

echo [3/3] Khoi dong 4 service...

start "API Gateway"     cmd /k "cd /d %ROOT% && %PYTHON% main_api.py"
timeout /t 2 /nobreak >nul

start "Relay Service"   cmd /k "cd /d %ROOT% && %PYTHON% main_relay.py"
timeout /t 1 /nobreak >nul

start "Scheduler"       cmd /k "cd /d %ROOT% && %PYTHON% main_scheduler.py"
timeout /t 1 /nobreak >nul

start "Worker"          cmd /k "cd /d %ROOT% && %PYTHON% main_worker.py"

echo.
echo Tat ca service da khoi dong!
echo   API:      http://localhost:8000
echo   Swagger:  http://localhost:8000/docs
echo   RabbitMQ: http://localhost:15672  (admin/admin)
echo.
pause
