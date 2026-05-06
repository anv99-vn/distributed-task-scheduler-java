@echo off
chcp 65001 >nul
set ROOT=%~dp0
set PYTHON=%ROOT%.venv\Scripts\python.exe

echo.
echo [*] Chay E2E test...
echo [*] Dam bao start.bat da chay truoc!
echo.

%PYTHON% test_e2e.py
