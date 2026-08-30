[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$maxSourceLines = 2000
$expectedRustVersion = '1.98.0'
$allowedDependencies = @(
    'base64', 'bitflags', 'bytes', 'clap', 'crossbeam-channel', 'ctrlc', 'dotenvy', 'eframe',
    'egui', 'egui_tiles', 'fs2', 'futures-util', 'hmac', 'js-sys', 'k256', 'reqwest', 'rmp-serde',
    'rust_decimal', 'secrecy', 'serde', 'serde_json', 'sha2', 'sha3', 'sqlx', 'tempfile', 'thiserror', 'tokio',
    'tokio-tungstenite', 'toml', 'tracing', 'tracing-subscriber', 'tungstenite', 'venue-control-protocol',
    'venue-copy', 'venue-domain', 'venue-execution', 'venue-gateway-api', 'venue-gateway-binance', 'venue-gateway-bitget',
    'venue-gateway-bybit', 'venue-gateway-gate', 'venue-gateway-hyperliquid', 'venue-gateway-okx',
    'venue', 'venue-indicators', 'venue-runtime', 'venue-storage', 'venue-strategies', 'wasm-bindgen', 'wasm-bindgen-futures', 'web-sys'
)

function Get-TrackedActivePaths {
    $paths = @(& git -C $repoRoot ls-files)
    if ($LASTEXITCODE -ne 0) { throw 'git ls-files failed' }
    $legacy = @($paths | Where-Object { $_.Replace('\', '/').StartsWith('bak/', [StringComparison]::OrdinalIgnoreCase) })
    if ($legacy.Count -gt 0) {
        throw "frozen bak paths are tracked; this policy gate will not read them: $($legacy -join ', ')"
    }
    return @($paths | Where-Object { -not $_.Replace('\', '/').StartsWith('bak/', [StringComparison]::OrdinalIgnoreCase) })
}

function Assert-RustToolchain {
    $toolchain = Get-Content -LiteralPath (Join-Path $repoRoot 'rust-toolchain.toml') -Raw
    $workspaceManifest = Get-Content -LiteralPath (Join-Path $repoRoot 'Cargo.toml') -Raw
    if ($toolchain -notmatch '(?m)^\s*channel\s*=\s*"1\.98\.0"\s*$') {
        throw 'rust-toolchain.toml must pin Rust 1.98.0 exactly.'
    }
    if ($workspaceManifest -notmatch '(?m)^\s*rust-version\s*=\s*"1\.98\.0"\s*$') {
        throw 'Cargo workspace.package.rust-version must pin Rust 1.98.0 exactly.'
    }
    $rustcVersion = @(& rustc --version)
    if ($LASTEXITCODE -ne 0 -or $rustcVersion.Count -ne 1 -or $rustcVersion[0] -notmatch "^rustc $([regex]::Escape($expectedRustVersion)) ") {
        throw "active rustc must be $expectedRustVersion; got: $($rustcVersion -join ' ')"
    }
    $cargoVersion = @(& cargo --version)
    if ($LASTEXITCODE -ne 0 -or $cargoVersion.Count -ne 1 -or $cargoVersion[0] -notmatch "^cargo $([regex]::Escape($expectedRustVersion)) ") {
        throw "active cargo must be $expectedRustVersion; got: $($cargoVersion -join ' ')"
    }
}

function Assert-DependencyAllowlist {
    $metadataRaw = & cargo metadata --locked --no-deps --format-version 1
    if ($LASTEXITCODE -ne 0) { throw "cargo metadata failed with exit code $LASTEXITCODE" }
    $metadata = $metadataRaw | ConvertFrom-Json
    $workspacePackageIds = @($metadata.workspace_members)
    $workspacePackages = @($metadata.packages | Where-Object { $workspacePackageIds -contains $_.id })
    $violations = [System.Collections.Generic.List[string]]::new()
    foreach ($package in $workspacePackages) {
        if ($package.manifest_path.Replace('\', '/').Contains('/bak/')) {
            $violations.Add("workspace package resolves through frozen bak: $($package.name)")
        }
        foreach ($dependency in $package.dependencies) {
            if ($allowedDependencies -notcontains $dependency.name) {
                $violations.Add("dependency is not allowlisted: package=$($package.name) dependency=$($dependency.name)")
            }
            if ($dependency.name -eq 'tungstenite' -and $package.name -ne 'venue') {
                $violations.Add("direct tungstenite is only permitted in the frozen root Stage 7 migration: package=$($package.name)")
            }
        }
    }
    if ($violations.Count -gt 0) { throw ($violations -join [Environment]::NewLine) }
}

function Assert-SourceFileLimit {
    $sourcePaths = @(Get-TrackedActivePaths | Where-Object { $_ -match '\.(rs|ps1)$' })
    $violations = [System.Collections.Generic.List[string]]::new()
    foreach ($relativePath in $sourcePaths) {
        $fullPath = Join-Path $repoRoot $relativePath
        if (-not (Test-Path -LiteralPath $fullPath -PathType Leaf)) {
            $violations.Add("tracked source file is missing: $relativePath")
            continue
        }
        $lineCount = @([System.IO.File]::ReadLines($fullPath)).Count
        if ($lineCount -gt $maxSourceLines) {
            $violations.Add("handwritten source exceeds $maxSourceLines physical lines: $relativePath ($lineCount)")
        }
    }
    if ($violations.Count -gt 0) { throw ($violations -join [Environment]::NewLine) }
}

function Assert-FrozenAndArtifactBoundaries {
    $status = @(& git -C $repoRoot status --porcelain=v1 --untracked-files=all --ignored=no)
    if ($LASTEXITCODE -ne 0) { throw 'git status failed' }
    $violations = [System.Collections.Generic.List[string]]::new()
    foreach ($entry in $status) {
        if ($entry.Length -lt 4) { continue }
        $path = $entry.Substring(3).Replace('\', '/')
        if ($path.StartsWith('bak/', [StringComparison]::OrdinalIgnoreCase)) {
            $violations.Add("frozen bak path changed or is untracked: $path")
        }
        if ($entry.Substring(0, 2).Contains('D') -and $path.StartsWith('artifacts/', [StringComparison]::OrdinalIgnoreCase)) {
            $violations.Add("protected runtime artifact deletion is forbidden: $path")
        }
    }
    if ($violations.Count -gt 0) { throw ($violations -join [Environment]::NewLine) }
}

Assert-RustToolchain
Assert-DependencyAllowlist
Assert-SourceFileLimit
Assert-FrozenAndArtifactBoundaries
Write-Output "workspace policy verified: Rust $expectedRustVersion, dependency allowlist, source limit, frozen bak, and artifact boundaries"
