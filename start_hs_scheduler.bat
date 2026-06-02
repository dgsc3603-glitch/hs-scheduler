@echo off
setlocal
cd /d "%~dp0"

where py >nul 2>nul
if %errorlevel%==0 (
    start "" pyw -3 "%~dp0hs_scheduler.py"
    exit /b 0
)

where pythonw >nul 2>nul
if %errorlevel%==0 (
    start "" pythonw "%~dp0hs_scheduler.py"
    exit /b 0
)

echo Python was not found. Install Python 3.11 or later, then run this file again.
pause
exit /b 1
