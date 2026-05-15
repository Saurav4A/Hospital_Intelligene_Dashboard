@echo off
setlocal

REM Always run from this project folder
cd /d "%~dp0"

REM Configure environment variables
set "REDIS_URL=redis://localhost:6379/0"
set "REVENUE_FETCH_FANOUT=2"
set "REVENUE_DB_MAX_CONCURRENCY=4"
set "WARM_FETCH_FANOUT=2"
set "PYTHONUNBUFFERED=1"
set "WAITRESS_THREADS=12"
set "WAITRESS_CONNECTION_LIMIT=200"
set "WAITRESS_CHANNEL_TIMEOUT=90"
set "WAITRESS_CLEANUP_INTERVAL=30"
set "LIVE_RESPONSE_CACHE_TTL_SECONDS=30"
set "LIVE_RESPONSE_SINGLEFLIGHT_WAIT_SECONDS=8"
set "LIVE_RESPONSE_MAX_CONCURRENCY=2"
set "DB_POOL_SIZE=8"
set "DB_POOL_MAX_OVERFLOW=4"
set "DB_POOL_TIMEOUT=30"
set "DB_POOL_RECYCLE=300"
set "START_RADIOLOGY_WEBHOOK_ON_IMPORT=1"

REM Start Waitress using the venv interpreter (no manual activate needed)
".\.venv\Scripts\python.exe" -m waitress --listen=0.0.0.0:8000 --threads=%WAITRESS_THREADS% --connection-limit=%WAITRESS_CONNECTION_LIMIT% --channel-timeout=%WAITRESS_CHANNEL_TIMEOUT% --cleanup-interval=%WAITRESS_CLEANUP_INTERVAL% wsgi:app

if errorlevel 1 (
  echo.
  echo Server failed to start. Check errors above.
  pause
)
