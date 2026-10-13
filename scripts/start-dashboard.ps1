[CmdletBinding()]
param([int]$Port = 8765, [switch]$NoOpen, [string]$RunId, [string]$Python, [string]$Fleet)
$ErrorActionPreference = 'Stop'
if ($Port -lt 1024 -or $Port -gt 65535) { throw 'Invalid port' }
if ($RunId -and $RunId -notmatch '^run_[a-f0-9]{32}$') { throw 'Invalid RunId' }
$projectRoot = Split-Path -Parent $PSScriptRoot
$runtime = Join-Path $projectRoot 'data/dashboard/server.json'
$url = $null
if (Test-Path -LiteralPath $runtime) {
    try {
        $info = Get-Content -LiteralPath $runtime -Raw | ConvertFrom-Json
        $uri = [Uri]$info.url
        if ($uri.Host -ne '127.0.0.1' -or $uri.Port -ne $Port) { throw 'Different dashboard endpoint' }
        $result = Invoke-RestMethod -Uri ("http://127.0.0.1:{0}/api/state" -f $Port) -Headers @{Authorization = 'Bearer ' + $uri.Fragment.TrimStart('#')} -TimeoutSec 3
        if ($result.run_id -eq $info.run_id) { $url = $info.url }
    } catch { $url = $null }
}
if ($url -and $result.dashboard_runtime.restart_required) {
    Write-Warning 'Dashboard source changed since startup. Restart this local dashboard to load the new code; remote training is unaffected.'
}
if ($url -and $RunId) {
    if (-not $result.collector_focus) {
        if ($info.run_id -ne $RunId) {
            throw 'Running dashboard predates dynamic focus. Restart only this local dashboard once to enable it; do not stop remote training.'
        }
    } else {
        # This authenticated same-origin preference changes only read collection.
        # It cannot submit, cancel, restart or move any remote Run.
        $base = "http://127.0.0.1:{0}" -f $Port
        $focus = Invoke-RestMethod -Uri ($base + '/api/focus') -Method Post `
            -Headers @{Authorization = 'Bearer ' + $uri.Fragment.TrimStart('#'); Origin = $base} `
            -ContentType 'application/json' -Body (@{run_id = $RunId} | ConvertTo-Json -Compress) -TimeoutSec 5
        if ($focus.run_id -ne $RunId) { throw 'Dashboard focus acknowledgement differs from the selected Run.' }
        $url = $base + '/?run_id=' + [Uri]::EscapeDataString($RunId) + '&follow=1' + $uri.Fragment
    }
}
if (-not $url) {
    if (-not $Python) {
        $localPython = Join-Path $projectRoot '.venv/Scripts/python.exe'
        $Python = if (Test-Path -LiteralPath $localPython) { $localPython } else { (Get-Command python -ErrorAction Stop).Source }
    }
    if (-not $Fleet) { $Fleet = Join-Path $projectRoot 'config/worker-pool.json' }
    if (-not (Test-Path -LiteralPath $Fleet)) { throw 'Worker pool missing; copy and edit config/worker-pool.example.json before starting the dashboard.' }
    $entry = Join-Path $PSScriptRoot 'dashboard.py'
    $outDir = Join-Path $projectRoot 'data/dashboard'
    New-Item -ItemType Directory -Force -Path $outDir | Out-Null
    $arguments = @('-u', ('"' + $entry + '"'), '--port', [string]$Port,
        '--local-root', ('"' + $projectRoot + '"'), '--fleet', ('"' + $Fleet + '"'))
    if ($RunId) { $arguments += @('--run-id', $RunId) }
    $process = Start-Process -FilePath $Python -ArgumentList $arguments -WorkingDirectory $projectRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $outDir 'server.stdout.log') -RedirectStandardError (Join-Path $outDir 'server.stderr.log')
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        if ($process.HasExited) { throw 'Dashboard exited; inspect data/dashboard/server.stderr.log' }
        if (Test-Path -LiteralPath $runtime) {
            try {
                $info = Get-Content -LiteralPath $runtime -Raw | ConvertFrom-Json
                # Some Windows Python launchers start a child with a different PID.
                $uri = [Uri]$info.url
                if ($uri.Host -eq '127.0.0.1' -and $uri.Port -eq $Port) {
                    $result = Invoke-RestMethod -Uri ("http://127.0.0.1:{0}/api/state" -f $Port) -Headers @{Authorization = 'Bearer ' + $uri.Fragment.TrimStart('#')} -TimeoutSec 2
                    if ($result.run_id -eq $info.run_id) { $url = $info.url; break }
                }
            } catch { }
        }
        Start-Sleep -Milliseconds 200
        $process.Refresh()
    }
    if (-not $url) { throw 'Dashboard not ready; inspect its local logs before retrying' }
}
Write-Output $url
if (-not $NoOpen) { Start-Process $url }
