# Pure validation shared by the local Ubuntu build entry and its offline tests.
function Assert-VenueUbuntuRevision {
    param([string]$SourceRoot, [string]$ExpectedRevision)
    if ($ExpectedRevision -cnotmatch '^[0-9a-f]{40}$') { throw 'ExpectedRevision must be a full lowercase Git commit.' }
    $top = & git -C $SourceRoot rev-parse --show-toplevel
    if ($LASTEXITCODE -ne 0 -or [IO.Path]::GetFullPath($top) -ne [IO.Path]::GetFullPath($SourceRoot)) {
        throw 'SourceRoot must be the root of a Git checkout.'
    }
    $head = & git -C $SourceRoot rev-parse --verify HEAD
    if ($LASTEXITCODE -ne 0 -or $head -cne $ExpectedRevision) { throw 'Source HEAD does not equal ExpectedRevision.' }
    $status = @(& git -C $SourceRoot status --porcelain --untracked-files=all)
    if ($LASTEXITCODE -ne 0 -or $status.Count) { throw 'Ubuntu release source must be clean, including untracked files.' }
}

function Assert-VenueUbuntuElf {
    param([Parameter(Mandatory)][string]$Path)
    $stream = [IO.File]::OpenRead($Path)
    try {
        $header = [byte[]]::new(64)
        if ($stream.Read($header, 0, $header.Length) -ne 64 -or
            $header[0] -ne 0x7f -or $header[1] -ne 0x45 -or $header[2] -ne 0x4c -or $header[3] -ne 0x46 -or
            $header[4] -ne 2 -or $header[5] -ne 1 -or $header[6] -ne 1 -or
            $header[16] -notin @(2,3) -or $header[17] -ne 0 -or
            $header[18] -ne 0x3e -or $header[19] -ne 0) {
            throw 'Expected an x86-64 little-endian ELF executable, not a Windows PE or another architecture.'
        }
    } finally { $stream.Dispose() }
}

function Assert-VenueUbuntuEnvironment {
    # These variables can bypass Zig, select a different CPU, or leak outputs outside the guard.
    $names = @('RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','CARGO_BUILD_RUSTFLAGS',
        'CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS','CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER',
        'CARGO_BUILD_TARGET','CARGO_TARGET','RUSTC_WORKSPACE_WRAPPER','CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER',
        'CFLAGS','CXXFLAGS','LDFLAGS','CC','CXX','AR','TARGET_CC','TARGET_CXX','TARGET_AR',
        'CARGO_ZIGBUILD_RUSTC_VERSION')
    foreach ($name in $names) {
        if ([Environment]::GetEnvironmentVariable($name, 'Process')) {
            throw "Ubuntu build refuses an external $name override."
        }
    }
}

function Get-VenueUbuntuPaths {
    param([string]$ReleaseId)
    if ($ReleaseId -cnotmatch '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$' -or $ReleaseId.EndsWith('.') -or
        $ReleaseId -match '^(CON|PRN|AUX|NUL|COM[0-9]|LPT[0-9])($|\.)') {
        throw 'ReleaseId must be 1-64 ASCII alphanumeric, dot, underscore or dash characters.'
    }
    $root = 'G:\Build\Venue\ubuntu'
    $release = Join-Path (Join-Path $root 'releases') $ReleaseId
    Assert-VenuePlainPath $root
    Assert-VenuePlainPath $release
    if (Test-Path -LiteralPath $release) { throw 'Ubuntu release already exists; never overwrite a release.' }
    [PSCustomObject]@{ Root=$root; Release=$release; Releases=(Join-Path $root 'releases') }
}

function Assert-VenueUbuntuSourcePaths {
    param([string]$Source, [string]$UbuntuRoot, [string]$CargoTarget)
    $outputs = @($CargoTarget, (Join-Path $UbuntuRoot 'releases'), (Join-Path $UbuntuRoot 'zig-cache'),
        (Join-Path $UbuntuRoot 'zig-local-cache'), (Join-Path $UbuntuRoot 'zigbuild-cache'))
    foreach ($output in $outputs) {
        if ($Source -eq $output -or $Source.StartsWith($output + '\', [StringComparison]::OrdinalIgnoreCase) -or
            $output.StartsWith($Source + '\', [StringComparison]::OrdinalIgnoreCase)) {
            throw 'Source and compiler/output directories must not contain each other.'
        }
    }
}
