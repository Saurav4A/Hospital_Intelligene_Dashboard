@echo off
setlocal EnableExtensions EnableDelayedExpansion

:: ZIP the folder this script lives in, including hidden items like .git
set "SRC=%~dp0."

:: Change this to an external drive for a safer backup
set "BASEDEST=D:\RID Backup"

for %%I in ("%SRC%") do (
    set "SRC_FULL=%%~fI"
    set "SRC_NAME=%%~nxI"
)

if not exist "!SRC_FULL!\" (
    echo Source folder not found: "!SRC_FULL!"
    exit /b 1
)

if not exist "%BASEDEST%\" mkdir "%BASEDEST%"
if errorlevel 1 (
    echo Could not create or access backup folder: "%BASEDEST%"
    exit /b 1
)

for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd_HHmmss"') do set "STAMP=%%I"
set "ZIPFILE=%BASEDEST%\!SRC_NAME!_!STAMP!.zip"

echo Creating ZIP backup:
echo   Source: "!SRC_FULL!"
echo   Output: "!ZIPFILE!"

powershell -NoProfile -Command ^
  "Add-Type -AssemblyName 'System.IO.Compression.FileSystem';" ^
  "$src = $env:SRC_FULL;" ^
  "$zip = $env:ZIPFILE;" ^
  "if (Test-Path -LiteralPath $zip) { Remove-Item -LiteralPath $zip -Force }" ^
  "try { [System.IO.Compression.ZipFile]::CreateFromDirectory($src, $zip, [System.IO.Compression.CompressionLevel]::Optimal, $true); exit 0 } catch { Write-Error $_; exit 1 }"

if errorlevel 1 (
    echo ZIP backup failed.
    exit /b 1
)

for %%I in ("!ZIPFILE!") do set "ZIPSIZE=%%~zI"

echo.
echo ZIP backup created successfully.
echo   "!ZIPFILE!"
echo   Size: !ZIPSIZE! bytes
echo.
pause
endlocal
