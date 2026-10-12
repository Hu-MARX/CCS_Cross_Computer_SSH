[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
& (Join-Path $PSScriptRoot 'research-ccs.ps1') `
    request --file (Join-Path $projectRoot 'examples/ccs-fleet-status.json')

if ($LASTEXITCODE -ne 0) { throw "research fleet status exited with code $LASTEXITCODE" }
