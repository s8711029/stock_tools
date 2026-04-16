# ============================================================
# 台股選股工具 — 一鍵安裝 Python + 套件 + 設定工作排程
# 請以「系統管理員身分」執行 PowerShell，然後貼上此腳本路徑
# ============================================================

$ErrorActionPreference = "Stop"

# 取得真實桌面路徑（支援 OneDrive 重新導向）
Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;
public class Shell32 {
    [DllImport("shell32.dll", CharSet=CharSet.Unicode)]
    public static extern int SHGetFolderPath(IntPtr hwnd, int csidl, IntPtr token, int flags, System.Text.StringBuilder path);
}
"@
$sb = New-Object System.Text.StringBuilder 260
[Shell32]::SHGetFolderPath([IntPtr]::Zero, 0, [IntPtr]::Zero, 0, $sb) | Out-Null
$DESKTOP    = $sb.ToString()
$TOOLS_DIR  = Join-Path $DESKTOP "stock_tools"
$SCRIPT     = Join-Path $TOOLS_DIR "tw_stock_screener_v2.py"
$DISP_SCRIPT= Join-Path $TOOLS_DIR "tw_stock_disposition.py"

Write-Host "=============================" -ForegroundColor Cyan
Write-Host " 台股選股工具 — 環境設定程式 " -ForegroundColor Cyan
Write-Host "=============================" -ForegroundColor Cyan
Write-Host "桌面路徑: $DESKTOP"
Write-Host "腳本路徑: $SCRIPT"
Write-Host ""

# ── Step 1: 確認 Python ──────────────────────────────────
Write-Host "[1/4] 檢查 Python..." -ForegroundColor Yellow
$pythonExe = $null
$candidates = @(
    "C:\Python312\python.exe","C:\Python311\python.exe","C:\Python310\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python310\python.exe"
)
foreach ($c in $candidates) {
    if (Test-Path $c) { $pythonExe = $c; break }
}
if (-not $pythonExe) {
    try { $pythonExe = (Get-Command python -ErrorAction Stop).Source } catch {}
}
if (-not $pythonExe -or $pythonExe -like "*WindowsApps*") {
    Write-Host "  Python 未安裝，透過 winget 安裝 Python 3.11..." -ForegroundColor Yellow
    winget install Python.Python.3.11 --silent --accept-package-agreements --accept-source-agreements
    # 重新掃描
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
    foreach ($c in $candidates) {
        if (Test-Path $c) { $pythonExe = $c; break }
    }
    if (-not $pythonExe) {
        try { $pythonExe = (Get-Command python -ErrorAction Stop).Source } catch {}
    }
}
if (-not $pythonExe -or $pythonExe -like "*WindowsApps*") {
    Write-Host "  [錯誤] 仍無法找到 Python，請手動安裝後再執行此腳本" -ForegroundColor Red
    Write-Host "  下載網址: https://www.python.org/downloads/" -ForegroundColor Red
    pause; exit 1
}
Write-Host "  Python 路徑: $pythonExe" -ForegroundColor Green
& $pythonExe --version

# ── Step 2: 安裝必要套件 ─────────────────────────────────
Write-Host ""
Write-Host "[2/4] 安裝 Python 套件 (pandas, yfinance, ta, requests)..." -ForegroundColor Yellow
& $pythonExe -m pip install --upgrade pip --quiet
& $pythonExe -m pip install pandas yfinance ta requests --upgrade --quiet
Write-Host "  套件安裝完成" -ForegroundColor Green

# ── Step 3: 測試腳本能否 import ─────────────────────────
Write-Host ""
Write-Host "[3/4] 驗證套件..." -ForegroundColor Yellow
$test = & $pythonExe -c "import pandas, yfinance, ta, requests; print('OK')" 2>&1
if ($test -eq "OK") {
    Write-Host "  驗證通過: $test" -ForegroundColor Green
} else {
    Write-Host "  [警告] 套件驗證失敗: $test" -ForegroundColor Red
}

# ── Step 4: 建立工作排程 ─────────────────────────────────
Write-Host ""
Write-Host "[4/4] 建立工作排程（週一至週五 09~13 每小時執行）..." -ForegroundColor Yellow

# 刪除舊工作（若存在）
foreach ($name in @("台股選股_v2","台股處置股")) {
    if (Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $name -Confirm:$false
        Write-Host "  已刪除舊工作: $name"
    }
}

# 建立選股工具排程（09:00~13:00，週一~五）
$action1 = New-ScheduledTaskAction -Execute $pythonExe -Argument "`"$SCRIPT`""
$triggers1 = @()
foreach ($h in @(9,10,11,12,13)) {
    $t = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At "$($h):00"
    $triggers1 += $t
}
$settings1 = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 55) -StartWhenAvailable
Register-ScheduledTask -TaskName "台股選股_v2" -Action $action1 -Trigger $triggers1 -Settings $settings1 -RunLevel Highest -Force | Out-Null
Write-Host "  [完成] 台股選股_v2 — 週一~五 09:00/10:00/11:00/12:00/13:00" -ForegroundColor Green

# 建立處置股工具排程（09:00~13:00，週一~五）
$action2 = New-ScheduledTaskAction -Execute $pythonExe -Argument "`"$DISP_SCRIPT`""
$triggers2 = @()
foreach ($h in @(9,10,11,12,13)) {
    $t = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At "$($h):00"
    $triggers2 += $t
}
$settings2 = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 55) -StartWhenAvailable
Register-ScheduledTask -TaskName "台股處置股" -Action $action2 -Trigger $triggers2 -Settings $settings2 -RunLevel Highest -Force | Out-Null
Write-Host "  [完成] 台股處置股   — 週一~五 09:00/10:00/11:00/12:00/13:00" -ForegroundColor Green

# 建立週K分析排程（18:00，週一~五，盤後分析）
$WEEKLY_SCRIPT = Join-Path $TOOLS_DIR "tw_stock_weekly_analysis.py"
$action3  = New-ScheduledTaskAction -Execute $pythonExe -Argument "`"$WEEKLY_SCRIPT`""
$trigger3 = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At "18:00"
$settings3 = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 20) -StartWhenAvailable
Register-ScheduledTask -TaskName "台股週K分析" -Action $action3 -Trigger $trigger3 -Settings $settings3 -RunLevel Highest -Force | Out-Null
Write-Host "  [完成] 台股週K分析  — 週一~五 18:00（盤後壓力/支撐分析）" -ForegroundColor Green

Write-Host ""
Write-Host "=============================" -ForegroundColor Cyan
Write-Host "  設定完成！" -ForegroundColor Cyan
Write-Host "=============================" -ForegroundColor Cyan
Write-Host "  Python: $pythonExe"
Write-Host "  排程工作已建立："
Write-Host "    台股選股_v2  → 週一~五 09:00/10:00/11:00/12:00/13:00"
Write-Host "    台股處置股   → 週一~五 09:00/10:00/11:00/12:00/13:00"
Write-Host "    台股週K分析  → 週一~五 18:00（盤後）"
Write-Host ""
Write-Host "  立即手動執行選股工具："
Write-Host "  & '$pythonExe' '$SCRIPT'"
Write-Host ""
pause
