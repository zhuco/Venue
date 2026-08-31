[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'venue_build_guard.ps1')
$venueBuildLease = Enter-VenueBuildGuard -RepoRoot (Split-Path -Parent $PSScriptRoot)
try {
Push-Location -LiteralPath (Split-Path -Parent $PSScriptRoot)
try {

function Invoke-CheckedProgram {
    param([Parameter(Mandatory)] [string]$FilePath, [Parameter(Mandatory)] [string[]]$Arguments)
    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$FilePath failed with exit code $LASTEXITCODE" }
}

Invoke-CheckedProgram -FilePath 'cargo' -Arguments @('fmt', '--all', '--', '--check')
Invoke-CheckedProgram -FilePath 'cargo' -Arguments @('check', '--workspace', '--all-targets', '--locked')
Invoke-CheckedProgram -FilePath 'cargo' -Arguments @('test', '--workspace', '--locked')
Invoke-CheckedProgram -FilePath 'cargo' -Arguments @('clippy', '--workspace', '--all-targets', '--all-features', '--locked', '--', '-D', 'warnings')
Write-Output 'workspace quality verified: fmt, check, test, and strict clippy'
} finally { Pop-Location }
} finally { Exit-VenueBuildGuard $venueBuildLease }
