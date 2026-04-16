@echo off
chcp 65001 >nul
echo ================================
echo  台股進場時間回測分析 — 手動執行
echo ================================

set PYTHON=
for %%p in (
    "C:\Python312\python.exe"
    "C:\Python311\python.exe"
    "C:\Python310\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python311\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python310\python.exe"
) do (
    if exist %%p (
        set PYTHON=%%p
        goto :found
    )
)

for /f "delims=" %%i in ('where python 2^>nul') do (
    echo %%i | findstr /i "WindowsApps" >nul || (set PYTHON=%%i && goto :found)
)

echo [錯誤] 找不到 Python！
pause
exit /b 1

:found
echo Python: %PYTHON%
echo.
echo 執行時間回測分析中，請稍候...
%PYTHON% "%~dp0tw_stock_time_analysis.py"
echo.
echo 完成！
pause
