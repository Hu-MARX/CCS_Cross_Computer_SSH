[CmdletBinding(PositionalBinding = $false)]
param(
    [string]$IdentityFile = (Join-Path $env:USERPROFILE '.ssh\id_ed25519'),
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$CommandArgs = @()
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$dataRoot = Join-Path $projectRoot 'data'
$knownHosts = Join-Path $projectRoot 'config\known_hosts'

if (-not (Test-Path -LiteralPath $knownHosts)) {
    throw "Trusted host-key file is missing: $knownHosts"
}
if (-not (Test-Path -LiteralPath $IdentityFile)) {
    throw "SSH identity file is missing: $IdentityFile"
}
if ($CommandArgs.Count -eq 0) {
    $CommandArgs = @('--help')
}

New-Item -ItemType Directory -Force -Path $dataRoot | Out-Null
& (Join-Path $PSScriptRoot 'ccs.ps1') `
    --fleet (Join-Path $projectRoot 'config\worker-pool.json') `
    --db (Join-Path $dataRoot 'orchestrator.db') `
    --audit-db (Join-Path $dataRoot 'ccs-audit.db') `
    --known-hosts $knownHosts `
    --identity-file $IdentityFile `
    @CommandArgs

if ($LASTEXITCODE -ne 0) { throw "ccs exited with code $LASTEXITCODE" }
