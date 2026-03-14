@echo off
setlocal

REM Always run from this project folder
cd /d "%~dp0"

REM Configure environment variables
set "REDIS_URL=redis://localhost:6379/0"
set "REVENUE_FETCH_FANOUT=2"
set "REVENUE_DB_MAX_CONCURRENCY=4"
set "WARM_FETCH_FANOUT=2"
set "WAITRESS_THREADS=6"

REM Start Waitress using the venv interpreter (no manual activate needed)
".\.venv\Scripts\python.exe" -m waitress --listen=0.0.0.0:8000 --threads=%WAITRESS_THREADS% wsgi:app

if errorlevel 1 (
  echo.
  echo Server failed to start. Check errors above.
  pause
)
