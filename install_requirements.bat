@echo off
setlocal
cd /d "%~dp0"

where py >nul 2>nul
if %errorlevel%==0 (
    py -3 -m pip install -r "%~dp0requirements.txt"
    exit /b %errorlevel%
)

where python >nul 2>nul
if %errorlevel%==0 (
    python -m pip install -r "%~dp0requirements.txt"
    exit /b %errorlevel%
)

echo Python was not found. Install Python 3.11 or later, then run this file again.
pause
exit /b 1
