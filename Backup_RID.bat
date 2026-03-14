@echo off
setlocal EnableExtensions EnableDelayedExpansion

:: ===== PATHS =====
set "SRC=D:\Revenue_Intelligence_Dashboard"
set "BASEDEST=D:\RID Backup"

:: ===== BUILD DATE/TIME STAMP =====
:: This part tries to be independent of your regional date format
for /f "tokens=1-4 delims=/.- " %%a in ("%date%") do (
    set "d1=%%a"
    set "d2=%%b"
    set "d3=%%c"
)

:: Guess which token is year (the one with 4 chars)
if "!d1:~4,1!"=="" (
    set "yyyy=!d1!"
    set "mm=!d2!"
    set "dd=!d3!"
) else if "!d2:~4,1!"=="" (
    set "yyyy=!d2!"
    set "mm=!d1!"
    set "dd=!d3!"
) else (
    set "yyyy=!d3!"
    set "mm=!d1!"
    set "dd=!d2!"
)

for /f "tokens=1-3 delims=:." %%a in ("%time%") do (
    set "hh=%%a"
    set "nn=%%b"
    set "ss=%%c"
)

:: Remove leading space from hour if present
if "!hh:~0,1!"==" " set "hh=0!hh:~1,1!"

set "STAMP=!yyyy!-!mm!-!dd!_!hh!!nn!!ss!"

:: Final destination folder (example: D:\RID Backup\RID_2025-11-22_132530)
set "DEST=%BASEDEST%\RID_!STAMP!"

echo Creating backup folder: "%DEST%"
mkdir "%DEST%" 2>nul

:: ===== COPY USING ROBOCOPY =====
:: /MIR  : mirror entire tree
:: /R:1  : retry once on failed copy
:: /W:5  : wait 5 seconds between retries
:: /COPY:DAT : copy data, attributes, timestamps
:: /NP /NFL /NDL : cleaner console output
:: /LOG+: append log to backup_log.txt
robocopy "%SRC%" "%DEST%" /MIR /R:1 /W:5 /COPY:DAT /NP /NFL /NDL /LOG+:"%BASEDEST%\backup_log.txt"

echo.
echo Backup completed from:
echo   "%SRC%"
echo to:
echo   "%DEST%"
echo.
pause
endlocal
