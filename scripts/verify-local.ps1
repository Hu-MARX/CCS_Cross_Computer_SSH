[CmdletBinding()]
param(
    [ValidateSet('core', 'mcp', 'research', 'all')]
    [string]$Mode = 'core',
    [string]$Python = 'python',
    [string]$ResearchRoot,
    [string]$Report,
    [switch]$RequireLinux
)

$ErrorActionPreference = 'Stop'
$arguments = @((Join-Path $PSScriptRoot 'verify.py'), '--group', $Mode)
if ($ResearchRoot) { $arguments += @('--research-root', $ResearchRoot) }
if ($Report) { $arguments += @('--report', $Report) }
if ($RequireLinux) { $arguments += '--require-linux' }
& $Python @arguments
if ($LASTEXITCODE -ne 0) { throw "CCS $Mode verification failed with exit code $LASTEXITCODE" }
