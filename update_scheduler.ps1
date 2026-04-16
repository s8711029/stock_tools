$taskName = "TW_Stock_Screener_v2"
$task = Get-ScheduledTask -TaskName $taskName
$task.Actions[0].Arguments = $task.Actions[0].Arguments.TrimEnd() + " --scheduled"
$task | Set-ScheduledTask
Write-Host "Done. New arguments:" $task.Actions[0].Arguments
