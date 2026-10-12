$ErrorActionPreference = 'Stop'
$CommandArgs = @($args)
if ($CommandArgs.Count -eq 0) {
    $CommandArgs = @('--help')
}
$projectRoot = Split-Path -Parent $PSScriptRoot
$sourceRoot = Join-Path $projectRoot 'src'
$previousPythonPath = $env:PYTHONPATH
try {
    $env:PYTHONPATH = $sourceRoot
    & python -m controller.gateway_cli @CommandArgs
    exit $LASTEXITCODE
}
finally {
    $env:PYTHONPATH = $previousPythonPath
}
