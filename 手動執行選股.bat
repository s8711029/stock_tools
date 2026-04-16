@echo off
chcp 65001 >nul
echo =============================
echo  TW Stock Screener v2
echo =============================

set PYTHON=
set LOCALAPP=%USERPROFILE%\AppData\Local
for %%p in (
    "C:\Python312\python.exe"
    "C:\Python311\python.exe"
    "C:\Python310\python.exe"
    "%LOCALAPP%\Programs\Python\Python312\python.exe"
    "%LOCALAPP%\Programs\Python\Python311\python.exe"
    "%LOCALAPP%\Programs\Python\Python310\python.exe"
) do (
    if exist %%p (
        set PYTHON=%%p
        goto :found
    )
)

for /f "delims=" %%i in ('where python 2^>nul') do (
    echo %%i | findstr /i "WindowsApps" >nul || (set PYTHON=%%i && goto :found)
)

echo [ERROR] Python not found!
echo Please run setup_and_schedule.ps1 first.
pause
exit /b 1

:found
echo Python: %PYTHON%
echo.
echo Running stock analysis, please wait (3~5 min)...
%PYTHON% "%~dp0tw_stock_screener_v2.py"
echo.
echo Done!
pause
