[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$runtime = Join-Path $projectRoot 'data/dashboard/server.json'
if (-not (Test-Path -LiteralPath $runtime)) { Write-Output 'Dashboard is not registered.'; return }
$info = Get-Content -LiteralPath $runtime -Raw | ConvertFrom-Json
$dashboardPid = [int]$info.pid
if ($dashboardPid -le 0) { throw 'Invalid dashboard PID' }
$process = Get-CimInstance Win32_Process -Filter ("ProcessId = {0}" -f $dashboardPid)
if (-not $process) { Write-Output 'Dashboard is already stopped.'; return }
$entry = Join-Path $PSScriptRoot 'dashboard.py'
if (-not $process.CommandLine -or $process.CommandLine.IndexOf($entry, [StringComparison]::OrdinalIgnoreCase) -lt 0) {
    throw 'PID no longer belongs to this dashboard; refusing to stop it.'
}
Stop-Process -Id $dashboardPid -ErrorAction Stop
Write-Output 'Local dashboard stopped. Remote training and worker monitoring are unchanged.'
