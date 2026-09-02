[CmdletBinding()]
param(
    [switch]$OfflineFixture
)

$ErrorActionPreference = 'Stop'
if (-not $OfflineFixture) {
    throw 'This drill is offline only. Real Binance Canary execution requires separate explicit authorization.'
}
foreach ($name in 'BINANCE_API_KEY', 'BINANCE_API_SECRET') {
    if ([Environment]::GetEnvironmentVariable($name, 'Process')) {
        throw "Offline drill refuses process credential $name."
    }
}

& (Join-Path $PSScriptRoot 'Invoke-VenueBuild.ps1') -CargoArguments @(
    'test', '--locked', '-p', 'venue-control', '--test', 'kol_executor_capacity', '--', '--nocapture'
)
if ($LASTEXITCODE -ne 0) { throw "Offline KOL fixture failed with exit code $LASTEXITCODE." }
Write-Output 'Offline KOL fan-out fixture passed. No credential was read and no order was sent.'
