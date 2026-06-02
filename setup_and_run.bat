@echo off
setlocal
cd /d "%~dp0"

set "PYTHON_CMD="
where py >nul 2>nul
if %errorlevel%==0 set "PYTHON_CMD=py -3"

if "%PYTHON_CMD%"=="" (
    where python >nul 2>nul
    if %errorlevel%==0 set "PYTHON_CMD=python"
)

if "%PYTHON_CMD%"=="" (
    echo Python was not found.
    echo Install Python 3.11 or later from https://www.python.org/downloads/windows/
    pause
    exit /b 1
)

if not exist "%~dp0scheduler_data.json" (
    if exist "%~dp0scheduler_data.sample.json" (
        copy "%~dp0scheduler_data.sample.json" "%~dp0scheduler_data.json" >nul
        echo Created scheduler_data.json from scheduler_data.sample.json.
    )
)

if not exist "%~dp0scheduler_secrets.json" (
    if exist "%~dp0scheduler_secrets.sample.json" (
        copy "%~dp0scheduler_secrets.sample.json" "%~dp0scheduler_secrets.json" >nul
        echo Created scheduler_secrets.json from scheduler_secrets.sample.json.
    )
)

echo Installing Python dependencies...
%PYTHON_CMD% -m pip install -r "%~dp0requirements.txt"
if errorlevel 1 (
    echo Dependency installation failed.
    pause
    exit /b 1
)

echo Starting HS Scheduler...
%PYTHON_CMD% "%~dp0hs_scheduler.py"
exit /b %errorlevel%
