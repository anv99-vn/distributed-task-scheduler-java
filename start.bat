@echo off
set APP_PORT=8000
powershell -Command "Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }"
docker compose up -d

echo Waiting for Cassandra to be ready...
:wait_cassandra
docker exec distributed-task-scheduler-cassandra-1 nodetool status >nul 2>&1
if errorlevel 1 (
  timeout /t 2 /nobreak >nul
  goto wait_cassandra
)
echo Cassandra is ready!

mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=%APP_PORT%
