@echo off
chcp 65001 > nul
echo ========================================
echo  TW Stock Backtest Tool
echo  Entry: d_score >= 60 / Hold: 5 days
echo  Estimated time: 15~20 minutes
echo ========================================
echo.
"C:\Users\s8711\AppData\Local\Programs\Python\Python311\python.exe" "%~dp0tw_stock_backtest.py"
echo.
echo Done. Press any key to close.
pause > nul
