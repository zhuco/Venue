[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$scriptPath = Join-Path $PSScriptRoot 'package_venue_node_linux_release.sh'
if (-not (Test-Path -LiteralPath $scriptPath -PathType Leaf)) {
    throw 'Linux venue-node release script is missing.'
}
$content = Get-Content -LiteralPath $scriptPath -Raw
$expected = @(
    'venue-node-binance',
    'venue-node-bitget',
    'venue-node-bybit',
    'venue-node-gate',
    'venue-node-hyperliquid',
    'venue-node-okx'
)
foreach ($binary in $expected) {
    if ($content.IndexOf($binary, [StringComparison]::Ordinal) -lt 0) {
        throw "Linux release script does not allow-list $binary."
    }
}
foreach ($forbidden in @(
    'systemctl ',
    'service ',
    'kill ',
    'pkill ',
    'ssh '
)) {
    if ($content.IndexOf($forbidden, [StringComparison]::OrdinalIgnoreCase) -ge 0) {
        throw "Linux release script contains prohibited production operation: $forbidden"
    }
}
foreach ($required in @(
    '--locked --release -p venue-node',
    '--no-default-features --features',
    'sha256sum',
    'SHA256SUMS',
    'manifest.json',
    '--preflight-only',
    'release directory already exists',
    'release contains a non-allow-listed file',
    'retired hedged-grid production binary',
    'retired root production binary is present'
)) {
    if ($content.IndexOf($required, [StringComparison]::Ordinal) -lt 0) {
        throw "Linux release script is missing required safety contract: $required"
    }
}

Write-Output 'Linux venue-node release script verified: six allow-listed binaries, hashes, preflight, and no process operations'
