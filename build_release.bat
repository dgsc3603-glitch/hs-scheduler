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
    echo Python was not found. Install Python 3.11 or later.
    pause
    exit /b 1
)

echo Installing build dependencies...
%PYTHON_CMD% -m pip install -r "%~dp0requirements.txt" pyinstaller
if errorlevel 1 exit /b 1

echo Building Windows release package...
%PYTHON_CMD% -m PyInstaller --clean --noconfirm "%~dp0hs_scheduler.spec"
if errorlevel 1 exit /b 1

set "RELEASE_ZIP=%~dp0dist\HS-Scheduler-Windows.zip"
if exist "%RELEASE_ZIP%" del "%RELEASE_ZIP%"

echo Creating release ZIP...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Compress-Archive -Path '%~dp0dist\HS Scheduler\*' -DestinationPath '%RELEASE_ZIP%' -Force"
if errorlevel 1 exit /b 1

echo.
echo Build complete:
echo %~dp0dist\HS Scheduler
echo %RELEASE_ZIP%
pause
