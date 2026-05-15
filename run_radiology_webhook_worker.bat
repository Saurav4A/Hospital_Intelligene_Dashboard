@echo off
setlocal

REM Run from this project folder on the HID server.
cd /d "%~dp0"

set "PYTHONUNBUFFERED=1"

REM Optional overrides:
REM set "RADIOLOGY_WEBHOOK_URL=https://rps.asarfi.in/api/radiology/webhook/patient-create"
REM set "RADIOLOGY_WEBHOOK_API_KEY=AsarfiCall@!2345"
REM set "RADIOLOGY_WEBHOOK_POLL_SECONDS=5"
REM set "RADIOLOGY_WEBHOOK_BATCH_SIZE=20"

".\.venv\Scripts\python.exe" -m modules.radiology_webhook_worker

if errorlevel 1 (
  echo.
  echo Radiology webhook worker stopped with an error. Check Logs\radiology_webhook_worker.log.
  pause
)
