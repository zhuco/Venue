[CmdletBinding()]
param(
    [switch]$SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$expectedBins = [ordered]@{
    'venue' = @{ path = 'src/main.rs'; features = @() }
    'hedged-grid-binance' = @{ path = 'src/bin/hedged-grid-binance.rs'; features = @('hedged-grid-binance') }
    'hedged-grid-gate' = @{ path = 'src/bin/hedged-grid-gate.rs'; features = @('hedged-grid-gate') }
    'hedged-grid-bitget' = @{ path = 'src/bin/hedged-grid-bitget.rs'; features = @('hedged-grid-bitget') }
    'verify-grid-inventory-recovery' = @{ path = 'src/bin/verify-grid-inventory-recovery.rs'; features = @() }
    'verify-grid-exposure-shadow' = @{ path = 'src/bin/verify-grid-exposure-shadow.rs'; features = @() }
}
$deploymentExchanges = @('binance', 'gate', 'bitget')

function Invoke-CheckedProgram {
    param([Parameter(Mandatory)] [string]$FilePath, [Parameter(Mandatory)] [string[]]$Arguments)
    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$FilePath failed with exit code $LASTEXITCODE" }
}

$metadataRaw = & cargo metadata --locked --no-deps --format-version 1
if ($LASTEXITCODE -ne 0) { throw "cargo metadata failed with exit code $LASTEXITCODE" }
$metadata = $metadataRaw | ConvertFrom-Json
$rootManifest = (Join-Path $repoRoot 'Cargo.toml').Replace('\', '/')
$rootPackage = @($metadata.packages | Where-Object { $_.manifest_path.Replace('\', '/') -eq $rootManifest })
if ($rootPackage.Count -ne 1) { throw 'could not resolve the root package from cargo metadata' }
$actualBins = @($rootPackage[0].targets | Where-Object { $_.kind -contains 'bin' })
if ($actualBins.Count -ne $expectedBins.Count) { throw "root package must define exactly $($expectedBins.Count) fixed binaries; found $($actualBins.Count)" }

foreach ($expected in $expectedBins.GetEnumerator()) {
    $target = @($actualBins | Where-Object { $_.name -eq $expected.Key })
    if ($target.Count -ne 1) { throw "missing fixed binary target: $($expected.Key)" }
    $actualPath = $target[0].src_path.Replace('\', '/')
    $expectedPath = (Join-Path $repoRoot $expected.Value.path).Replace('\', '/')
    if ($actualPath -ne $expectedPath) { throw "binary $($expected.Key) has unexpected source path: $actualPath" }
    $featureProperty = $target[0].PSObject.Properties['required-features']
    $actualFeatures = if ($null -eq $featureProperty) { @() } else { @($featureProperty.Value | Sort-Object) }
    $expectedFeatures = @($expected.Value.features | Sort-Object)
    if (($actualFeatures -join ',') -ne ($expectedFeatures -join ',')) {
        throw "binary $($expected.Key) has unexpected required features: $($actualFeatures -join ',')"
    }
}

foreach ($exchange in $deploymentExchanges) {
    $binaryName = "hedged-grid-$exchange"
    $source = Get-Content -LiteralPath (Join-Path $repoRoot "src/bin/$binaryName.rs") -Raw
    if ($source -notmatch "start_hedged_grid_$exchange`_deployment") {
        throw "$binaryName is not bound to its exact $exchange deployment entrypoint"
    }
    foreach ($other in $deploymentExchanges | Where-Object { $_ -ne $exchange }) {
        if ($source -match "start_hedged_grid_$other`_deployment") {
            throw "$binaryName references the $other deployment entrypoint"
        }
    }
}

if (-not $SkipBuild) {
    $targetDirectoryRaw = & cargo metadata --locked --no-deps --format-version 1
    if ($LASTEXITCODE -ne 0) { throw "cargo metadata failed with exit code $LASTEXITCODE" }
    $targetDirectory = ($targetDirectoryRaw | ConvertFrom-Json).target_directory
    foreach ($exchange in $deploymentExchanges) {
        $binaryName = "hedged-grid-$exchange"
        Invoke-CheckedProgram -FilePath 'cargo' -Arguments @(
            'build', '--locked', '--release', '--bin', $binaryName,
            '--no-default-features', '--features', $binaryName
        )
        $binaryCandidates = @(
            (Join-Path $targetDirectory "release/$binaryName.exe"),
            (Join-Path $targetDirectory "release/$binaryName")
        )
        $binaryPath = @($binaryCandidates | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } | Select-Object -First 1)
        if ($binaryPath.Count -ne 1) { throw "compiled deployment binary was not found: $binaryName" }
        & (Join-Path $PSScriptRoot 'verify_hedged_grid_binary_isolation.ps1') -Exchange $exchange -BinaryPath $binaryPath[0]
        if ($LASTEXITCODE -ne 0) { throw "endpoint isolation failed: $binaryName" }
    }
}

Write-Output 'fixed binary contract verified: six root binaries; three single-adapter deployables; production endpoint isolation'
