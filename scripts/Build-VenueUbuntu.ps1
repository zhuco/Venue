[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$ExpectedRevision,
    [Parameter(Mandatory)][string]$ReleaseId,
    [string]$SourceRoot = (Split-Path -Parent $PSScriptRoot),
    [switch]$CheckOnly
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'venue_build_guard.ps1')
. (Join-Path $PSScriptRoot 'ubuntu_build_helpers.ps1')

$source = [IO.Path]::GetFullPath($SourceRoot).TrimEnd([char[]]'\/')
Assert-VenuePlainPath $source
Assert-VenueUbuntuRevision $source $ExpectedRevision
Assert-VenueUbuntuEnvironment
$paths = Get-VenueUbuntuPaths $ReleaseId
$plan = Get-VenueBuildPlan -RepoRoot $source -Slot 'slot-2'
Assert-VenueUbuntuSourcePaths $source $paths.Root $plan.TargetDirectory
$null = Test-VenueBuildAdmission $plan

# Do not install tools or alter global Cargo configuration as a side effect of packaging.
$versions = @{}
Push-Location -LiteralPath $source
try {
    foreach ($tool in @('cargo','rustc','rustup','cargo-zigbuild','zig')) {
        $null = Get-Command $tool -CommandType Application -ErrorAction Stop
    }
    $versions.rustc = (& rustc --version)
    if ($LASTEXITCODE -ne 0 -or $versions.rustc -notmatch '^rustc 1\.98\.0 ') { throw 'rustc 1.98.0 is required.' }
    $versions.cargo = (& cargo --version)
    if ($LASTEXITCODE -ne 0 -or $versions.cargo -notmatch '^cargo 1\.98\.0 ') { throw 'cargo 1.98.0 is required.' }
    $versions.zig = (& zig version)
    if ($LASTEXITCODE -ne 0 -or $versions.zig -cne '0.16.0') { throw 'Zig 0.16.0 is required.' }
    $versions.zigbuild = (& cargo-zigbuild --version)
    if ($LASTEXITCODE -ne 0 -or $versions.zigbuild -cne 'cargo-zigbuild 0.23.0') { throw 'cargo-zigbuild 0.23.0 is required.' }
    $targets = @(& rustup target list --installed)
    if ($LASTEXITCODE -ne 0 -or 'x86_64-unknown-linux-gnu' -notin $targets) { throw 'Install the Rust x86_64-unknown-linux-gnu target first.' }
} finally { Pop-Location }

$target = 'x86_64-unknown-linux-gnu.2.35'
$binaries = @('binance','bitget','bybit','gate','hyperliquid','okx')
$builderFiles = @($PSCommandPath,(Join-Path $PSScriptRoot 'ubuntu_build_helpers.ps1'),(Join-Path $PSScriptRoot 'venue_build_guard.ps1'))
$builderHashes = @{}
foreach ($file in $builderFiles) { $builderHashes[[IO.Path]::GetFileName($file)] = (Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash.ToLowerInvariant() }
if ($CheckOnly) {
    [PSCustomObject]@{ Source=$source; Revision=$ExpectedRevision; Target=$target; Cache=$plan.TargetDirectory; Release=$paths.Release; Tools=$versions }
    return
}

$lease = Enter-VenueBuildGuard -RepoRoot $source -Slot 'slot-2'
$saved = @{}
try {
    Assert-VenueUbuntuRevision $source $ExpectedRevision
    $null = Get-VenueUbuntuPaths $ReleaseId
    foreach ($directory in @($paths.Root,$paths.Releases)) { [void][IO.Directory]::CreateDirectory($directory) }
    $settings = @{
        CARGO_ZIGBUILD_ZIG_PATH=(Get-Command zig -CommandType Application).Source
        CARGO_ZIGBUILD_CACHE_DIR=(Join-Path $paths.Root 'zigbuild-cache')
        ZIG_GLOBAL_CACHE_DIR=(Join-Path $paths.Root 'zig-cache')
        ZIG_LOCAL_CACHE_DIR=(Join-Path $paths.Root 'zig-local-cache')
        CARGO_BUILD_JOBS='2'
    }
    foreach ($name in $settings.Keys) {
        $saved[$name] = [Environment]::GetEnvironmentVariable($name,'Process')
        [Environment]::SetEnvironmentVariable($name,$settings[$name],'Process')
    }
    foreach ($name in @('CARGO_ZIGBUILD_CACHE_DIR','ZIG_GLOBAL_CACHE_DIR','ZIG_LOCAL_CACHE_DIR')) {
        Assert-VenuePlainPath $settings[$name]
        [void][IO.Directory]::CreateDirectory($settings[$name])
    }
    $stage = Join-Path $paths.Releases ('.' + $ReleaseId + '.stage.' + [Guid]::NewGuid().ToString('N'))
    [void][IO.Directory]::CreateDirectory($stage)
    $records = @()
    Push-Location -LiteralPath $source
    try {
        foreach ($venue in $binaries) {
            $binary = 'venue-node-' + $venue
            & cargo zigbuild --locked --release -p venue-node --no-default-features --features $venue --bin $binary --target $target
            if ($LASTEXITCODE -ne 0) { throw "Ubuntu build failed for $binary; cache and partial stage are retained." }
            $artifact = Join-Path $lease.TargetDirectory ('x86_64-unknown-linux-gnu\release\' + $binary)
            Assert-VenuePlainPath $artifact
            Assert-VenueUbuntuElf $artifact
            $destination = Join-Path $stage $binary
            [IO.File]::Copy($artifact,$destination,$false)
            $records += [ordered]@{name=$binary;sha256=(Get-FileHash -LiteralPath $destination -Algorithm SHA256).Hash.ToLowerInvariant()}
            $null = Test-VenueBuildAdmission $lease.Plan
        }
    } finally { Pop-Location }
    Assert-VenueUbuntuRevision $source $ExpectedRevision
    foreach ($file in $builderFiles) {
        if ((Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash.ToLowerInvariant() -cne $builderHashes[[IO.Path]::GetFileName($file)]) {
            throw 'Build entry changed during compilation; partial stage retained, release not published.'
        }
    }
    $manifest = [ordered]@{
        release_id=$ReleaseId; git_revision=$ExpectedRevision; platform='linux'; target=$target
        minimum_glibc='2.35'; tools=$versions; binaries=$records
        builder=$builderHashes
    }
    [IO.File]::WriteAllText((Join-Path $stage 'manifest.json'), (($manifest | ConvertTo-Json -Depth 5) + "`n"), [Text.UTF8Encoding]::new($false))
    $checksums = ($records | ForEach-Object { $_.sha256 + '  ' + $_.name }) -join "`n"
    [IO.File]::WriteAllText((Join-Path $stage 'SHA256SUMS'), ($checksums + "`n"), [Text.UTF8Encoding]::new($false))
    $expected = @($records | ForEach-Object { $_.name }) + @('manifest.json','SHA256SUMS')
    $actual = @(Get-ChildItem -LiteralPath $stage -Force)
    if ($actual.Count -ne 8 -or @($actual | Where-Object { $_.PSIsContainer -or $_.Name -cnotin $expected }).Count) {
        throw 'Ubuntu stage contains a non-allow-listed entry.'
    }
    # Directory.Move refuses an existing destination, including a release created concurrently.
    $null = Get-VenueUbuntuPaths $ReleaseId
    [IO.Directory]::Move($stage,$paths.Release)
    Write-Output "Created Ubuntu release: $($paths.Release)"
    Write-Output 'No upload, service activation, credential access or account operation was performed.'
} finally {
    Restore-VenueBuildEnvironment $saved
    Exit-VenueBuildGuard $lease
}
