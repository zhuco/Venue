[CmdletBinding()]
param(
    [string]$WasmBindgen = 'G:\Build\Venue\tools\wasm-bindgen-0.2.126-x86_64-pc-windows-msvc\wasm-bindgen.exe'
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot 'venue_build_guard.ps1')
$plan = Get-VenueBuildPlan -RepoRoot $repo
$admission = Test-VenueBuildAdmission $plan
if ($admission.CacheBytes -gt 200GB) { throw 'Web preview build requires the 200 GiB cache admission budget.' }
if (-not (Test-Path -LiteralPath $WasmBindgen -PathType Leaf)) {
    throw 'Provide wasm-bindgen 0.2.126 from the official release matching Cargo.lock.'
}
$version = & $WasmBindgen --version
if ($LASTEXITCODE -ne 0 -or $version -ne 'wasm-bindgen 0.2.126') { throw 'wasm-bindgen version does not match Cargo.lock.' }
& (Join-Path $PSScriptRoot 'Invoke-VenueBuild.ps1') -CargoArguments @('build','--locked','-p','venueflow','--target','wasm32-unknown-unknown','--no-default-features','--features','web,preview','--lib')
& (Join-Path $PSScriptRoot 'Invoke-VenueBuild.ps1') -CargoArguments @('build','--locked','-p','venueflow','--features','preview','--bin','venueflow-web-preview')
$lease = Enter-VenueBuildGuard -RepoRoot $repo
try {
    $output = Join-Path $repo 'output\venueflow-web'
    Assert-VenuePlainPath $output
    $null = New-Item -ItemType Directory -Force -Path $output
    $wasm = Join-Path $lease.Plan.TargetDirectory 'wasm32-unknown-unknown\debug\venueflow.wasm'
    & $WasmBindgen $wasm --target web --out-dir $output --out-name venueflow --no-typescript
    if ($LASTEXITCODE -ne 0) { throw 'WASM browser packaging failed.' }
    Copy-Item -LiteralPath (Join-Path $repo 'apps\ui\desktop\web\preview.html') -Destination (Join-Path $output 'index.html')
    Copy-Item -LiteralPath (Join-Path $repo 'apps\ui\desktop\web\preview-bootstrap.js') -Destination $output
    $font = Join-Path $env:WINDIR 'Fonts\msyh.ttc'
    if (Test-Path -LiteralPath $font) { Copy-Item -LiteralPath $font -Destination (Join-Path $output 'preview-font.ttc') }
    Copy-Item -LiteralPath (Join-Path $lease.Plan.TargetDirectory 'debug\venueflow-web-preview.exe') -Destination $output
    Get-ChildItem -LiteralPath $output -File | Select-Object Name,Length
    Write-Output "Start: & '$output\venueflow-web-preview.exe' '$output'"
} finally { Exit-VenueBuildGuard $lease }
