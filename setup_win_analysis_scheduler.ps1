# setup_win_analysis_scheduler.ps1
$TaskName = "TW_Stock_WinAnalysis"
$Python   = "C:\Users\s8711\AppData\Local\Programs\Python\Python311\python.exe"
$Script   = "C:\Users\s8711\OneDrive\Desktop\stock_tools\tw_stock_win_analysis.py"

if (-not (Test-Path $Script)) {
    $Script = "C:\Users\s8711\OneDrive\桌面\stock_tools\tw_stock_win_analysis.py"
}

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

$Action  = New-ScheduledTaskAction -Execute $Python -Argument "`"$Script`""
$Trigger = New-ScheduledTaskTrigger -Weekly -WeeksInterval 2 -DaysOfWeek Friday -At "18:30"
$Settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 10) -StartWhenAvailable

Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings -Description "Bi-weekly win stock pattern analysis"

Write-Host "Done. Script: $Script"
