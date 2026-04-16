$names = @("TW_Stock_Screener_v2", "TW_Stock_Disposition")

foreach ($taskName in $names) {
    $task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    if (-not $task) {
        Write-Host "[$taskName] not found, skipping"
        continue
    }
    $triggers = $task.Triggers
    $changed = 0
    foreach ($t in $triggers) {
        if ($t.StartBoundary -match "T09:00") {
            $t.StartBoundary = $t.StartBoundary -replace "T09:00:00", "T09:05:00"
            $changed++
            Write-Host "[$taskName] 09:00 -> 09:05"
        }
        if ($t.StartBoundary -match "T13:00") {
            $t.StartBoundary = $t.StartBoundary -replace "T13:00:00", "T13:20:00"
            $changed++
            Write-Host "[$taskName] 13:00 -> 13:20"
        }
    }
    if ($changed -gt 0) {
        Set-ScheduledTask -TaskName $taskName -Trigger $triggers | Out-Null
        Write-Host "[$taskName] updated OK"
    } else {
        Write-Host "[$taskName] no 09:00/13:00 triggers found"
    }
}

Write-Host ""
Write-Host "=== 確認結果 ==="
foreach ($taskName in $names) {
    $task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    if (-not $task) { continue }
    Write-Host "[$taskName]"
    foreach ($t in $task.Triggers) {
        Write-Host "  " $t.StartBoundary
    }
}
