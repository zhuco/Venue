[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$bash = 'C:\Program Files\Git\bin\bash.exe'
if (-not (Test-Path -LiteralPath $bash)) {
    throw 'Git Bash is required for the Linux release-script fixture test.'
}

function Assert-ReleaseTest {
    param([Parameter(Mandatory)][bool]$Condition,[Parameter(Mandatory)][string]$Message)
    if (-not $Condition) { throw "release-script test failed: $Message" }
}

function ConvertTo-GitBashPath {
    param([Parameter(Mandatory)][string]$Path)
    $full = [IO.Path]::GetFullPath($Path)
    if ($full -notmatch '^[A-Za-z]:\\') { throw "fixture path is not drive-qualified: $full" }
    '/' + $full.Substring(0,1).ToLowerInvariant() + $full.Substring(2).Replace('\','/')
}

function Invoke-ReleaseScript {
    param([Parameter(Mandatory)][string[]]$Arguments)
    $previousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = @(& $script:bash $script:packageScript @Arguments 2>&1)
        $exitCode = $LASTEXITCODE
    } finally { $ErrorActionPreference = $previousPreference }
    $output | Write-Output
    if ($exitCode -ne 0) { throw "release script failed with exit code $exitCode" }
}

function Assert-ReleaseRejects {
    param([Parameter(Mandatory)][string[]]$Arguments,[Parameter(Mandatory)][string]$Message)
    $previousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = @(& $script:bash $script:packageScript @Arguments 2>&1)
        $exitCode = $LASTEXITCODE
    } finally { $ErrorActionPreference = $previousPreference }
    $output | Write-Output
    Assert-ReleaseTest ($exitCode -ne 0) $Message
}

$tempBase = [IO.Path]::GetTempPath()
$fixture = Join-Path $tempBase ('venue-linux-release-test-' + [Guid]::NewGuid().ToString('N'))
$packageScript = $null
$originalPath = $env:PATH
$originalLog = $env:FAKE_CARGO_LOG
$targetOverrideNames = @('CARGO_TARGET_DIR','CARGO_BUILD_TARGET_DIR','CARGO_BUILD_BUILD_DIR','CARGO_BUILD_TARGET','CARGO_TARGET')
$originalTargetOverrides = @{}
foreach ($name in $targetOverrideNames) {
    $originalTargetOverrides[$name] = [Environment]::GetEnvironmentVariable($name,'Process')
    Remove-Item -LiteralPath ('Env:' + $name) -ErrorAction SilentlyContinue
}
try {
    $repo = Join-Path $fixture 'repo'
    $fakeBin = Join-Path $fixture 'fake-bin'
    $output = Join-Path $fixture 'releases'
    $build = Join-Path $fixture 'build-cache'
    $buildLink = Join-Path $repo 'build-link'
    $mismatchOutput = Join-Path $fixture 'mismatch-release'
    $legacyOutput = Join-Path $fixture 'legacy-release'
    $changedOutput = Join-Path $fixture 'changed-release'
    New-Item -ItemType Directory -Path (Join-Path $repo 'scripts'), (Join-Path $repo 'apps/venue-node'), $fakeBin -Force | Out-Null
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'package_venue_node_linux_release.sh') -Destination (Join-Path $repo 'scripts/package_venue_node_linux_release.sh')
    Set-Content -LiteralPath (Join-Path $repo 'Cargo.toml') -NoNewline -Value "[workspace]`nmembers = []`n"
    Set-Content -LiteralPath (Join-Path $repo 'Cargo.lock') -NoNewline -Value "version = 4`n"
    Set-Content -LiteralPath (Join-Path $repo 'apps/venue-node/Cargo.toml') -NoNewline -Value @'
[package]
name = "venue-node"
version = "0.1.0"
'@
    $fakeTool = Join-Path $PSScriptRoot 'fixtures/linux_release_fake_tool.sh'
    foreach ($name in 'cargo','rustc','flock') {
        Copy-Item -LiteralPath $fakeTool -Destination (Join-Path $fakeBin $name)
    }
    $repoPosix = ConvertTo-GitBashPath $repo
    $fakeBinPosix = ConvertTo-GitBashPath $fakeBin
    & $bash -c "chmod 700 '$fakeBinPosix/cargo' '$fakeBinPosix/rustc' '$fakeBinPosix/flock'"
    if ($LASTEXITCODE -ne 0) { throw 'could not prepare fixture tools' }
    & git -C $repo init -q
    & git -C $repo config user.email fixture@example.invalid
    & git -C $repo config user.name fixture
    & git -C $repo add .
    & git -C $repo commit -qm fixture
    if ($LASTEXITCODE -ne 0) { throw 'could not initialize release-script fixture repository' }

    $outputPosix = ConvertTo-GitBashPath $output
    $buildPosix = ConvertTo-GitBashPath $build
    $buildLinkPosix = ConvertTo-GitBashPath $buildLink
    $mismatchOutputPosix = ConvertTo-GitBashPath $mismatchOutput
    $legacyOutputPosix = ConvertTo-GitBashPath $legacyOutput
    $packageScript = ConvertTo-GitBashPath (Join-Path $repo 'scripts/package_venue_node_linux_release.sh')
    $head = (& git -C $repo rev-parse HEAD).Trim()
    $env:PATH = $fakeBin + [IO.Path]::PathSeparator + $originalPath
    $env:FAKE_CARGO_LOG = Join-Path $fixture 'fake-cargo.log'

    Invoke-ReleaseScript @('--release-id','preflight','--output-root',$outputPosix,'--build-root',$buildPosix,'--expected-revision',$head,'--preflight-only')
    Assert-ReleaseTest (-not (Test-Path -LiteralPath $output) -and -not (Test-Path -LiteralPath $build)) 'preflight does not create release or cache paths'

    Assert-ReleaseRejects @('--release-id','wrong-head','--output-root',$mismatchOutputPosix,'--build-root',$buildPosix,'--expected-revision',('0' * 40),'--preflight-only') 'wrong expected revision is rejected'
    Assert-ReleaseTest (-not (Test-Path -LiteralPath $mismatchOutput)) 'rejected preflight leaves output absent'
    $env:CARGO_BUILD_TARGET_DIR = '/outside-target'
    Assert-ReleaseRejects @('--release-id','override','--output-root',$mismatchOutputPosix,'--build-root',$buildPosix,'--expected-revision',$head,'--preflight-only') 'target override is rejected'
    Remove-Item -LiteralPath Env:CARGO_BUILD_TARGET_DIR -ErrorAction SilentlyContinue
    Assert-ReleaseRejects @('--release-id','root','--output-root','/','--build-root',$buildPosix,'--expected-revision',$head,'--preflight-only') 'filesystem root is rejected'
    $symlinkCoverage = $false
    try {
        New-Item -ItemType SymbolicLink -Path $buildLink -Target $repo -ErrorAction Stop | Out-Null
        Assert-ReleaseRejects @('--release-id','symlink','--output-root',$mismatchOutputPosix,'--build-root',$buildLinkPosix,'--expected-revision',$head,'--preflight-only') 'symbolic-link build root is rejected'
        $symlinkCoverage = $true
    } catch [UnauthorizedAccessException] {
        # This Windows host has no unprivileged symbolic-link privilege; Linux preflight keeps
        # the executable check, while the fixture still checks its static contract below.
        Assert-ReleaseTest ((Get-Content -LiteralPath (Join-Path $repo 'scripts/package_venue_node_linux_release.sh') -Raw).Contains('must not be symbolic links')) 'symbolic-link rejection contract is present'
    } finally {
        Remove-Item -LiteralPath $buildLink -Force -ErrorAction SilentlyContinue
    }
    Assert-ReleaseTest (-not (Test-Path -LiteralPath $build)) 'root and symbolic-link preflight failures do not create cache paths'

    Push-Location -LiteralPath $fixture
    try {
        Invoke-ReleaseScript @('--release-id','release-a','--output-root',$outputPosix,'--build-root',$buildPosix,'--expected-revision',$head)
    } finally { Pop-Location }
    $releaseA = Join-Path $output 'venue-node/release-a'
    $expectedFiles = @('venue-node-binance','venue-node-bitget','venue-node-bybit','venue-node-gate','venue-node-hyperliquid','venue-node-okx','SHA256SUMS','manifest.json')
    $actualFiles = @(Get-ChildItem -LiteralPath $releaseA -File | Select-Object -ExpandProperty Name | Sort-Object)
    Assert-ReleaseTest (@($actualFiles) -join ',' -eq (@($expectedFiles | Sort-Object) -join ',')) 'release contains exactly six Node binaries plus manifest and hashes'
    $target = Join-Path $build 'cargo-target'
    Assert-ReleaseTest (Test-Path -LiteralPath $target -PathType Container) 'fixed cargo target remains after release'

    Invoke-ReleaseScript @('--release-id','release-b','--output-root',$outputPosix,'--build-root',$buildPosix,'--expected-revision',$head)
    $cargoTargets = @(Get-Content -LiteralPath $env:FAKE_CARGO_LOG)
    $targetPosix = ConvertTo-GitBashPath $target
    Assert-ReleaseTest ($cargoTargets.Count -eq 12 -and @($cargoTargets | Where-Object { $_ -ne "$repoPosix|$targetPosix" }).Count -eq 0) 'both releases reuse one fixed cargo target from the workspace cwd'

    $changedOutputPosix = ConvertTo-GitBashPath $changedOutput
    $env:FAKE_MUTATE_REPOSITORY = '1'
    $env:FAKE_REPOSITORY = $repoPosix
    Assert-ReleaseRejects @('--release-id','changed','--output-root',$changedOutputPosix,'--build-root',$buildPosix,'--expected-revision',$head) 'revision change during build is rejected before manifest'
    Remove-Item -LiteralPath Env:FAKE_MUTATE_REPOSITORY -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath Env:FAKE_REPOSITORY -ErrorAction SilentlyContinue
    Assert-ReleaseTest (-not (Test-Path -LiteralPath (Join-Path $changedOutput 'venue-node/changed'))) 'revision-change failure publishes no release'
    Assert-ReleaseTest (@(Get-ChildItem -LiteralPath (Join-Path $changedOutput 'venue-node') -Directory -Filter '.changed.stage.*').Count -eq 0) 'revision-change failure removes only its stage'
    $changedHead = (& git -C $repo rev-parse HEAD).Trim()

    $raceOutput = Join-Path $fixture 'race-release'
    $raceOutputPosix = ConvertTo-GitBashPath $raceOutput
    $raceDirectory = Join-Path $raceOutput 'venue-node/race'
    $env:FAKE_RACE_RELEASE_DIR = ConvertTo-GitBashPath $raceDirectory
    Assert-ReleaseRejects @('--release-id','race','--output-root',$raceOutputPosix,'--build-root',$buildPosix,'--expected-revision',$changedHead) 'release-directory race is rejected'
    Remove-Item -LiteralPath Env:FAKE_RACE_RELEASE_DIR -ErrorAction SilentlyContinue
    Assert-ReleaseTest ((Get-Content -LiteralPath (Join-Path $raceDirectory 'racer') -Raw).Trim() -eq 'external release') 'release race does not modify the existing release directory'
    Assert-ReleaseTest (@(Get-ChildItem -LiteralPath (Join-Path $raceOutput 'venue-node') -Directory -Filter '.race.stage.*').Count -eq 0) 'release-race failure removes only its own stage'

    New-Item -ItemType Directory -Path (Join-Path $repo 'src/bin') -Force | Out-Null
    Set-Content -LiteralPath (Join-Path $repo 'src/bin/hedged-grid-binance.rs') -NoNewline -Value '// retired fixture'
    & git -C $repo add src/bin/hedged-grid-binance.rs
    & git -C $repo commit -qm legacy
    if ($LASTEXITCODE -ne 0) { throw 'could not commit legacy fixture' }
    $legacyHead = (& git -C $repo rev-parse HEAD).Trim()
    Assert-ReleaseRejects @('--release-id','legacy','--output-root',$legacyOutputPosix,'--build-root',$buildPosix,'--expected-revision',$legacyHead,'--preflight-only') 'retired binary is rejected'
    Assert-ReleaseTest (-not (Test-Path -LiteralPath $legacyOutput)) 'legacy rejection leaves output absent'
    [PSCustomObject]@{ Passed=$true; CargoInvocations=$cargoTargets.Count; RealCargoStarted=$false; SymbolicLinkCoverage=$symlinkCoverage } | ConvertTo-Json
} finally {
    $env:PATH = $originalPath
    if ($null -eq $originalLog) { Remove-Item -LiteralPath Env:FAKE_CARGO_LOG -ErrorAction SilentlyContinue } else { $env:FAKE_CARGO_LOG = $originalLog }
    Remove-Item -LiteralPath Env:FAKE_MUTATE_REPOSITORY -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath Env:FAKE_REPOSITORY -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath Env:FAKE_RACE_RELEASE_DIR -ErrorAction SilentlyContinue
    foreach ($name in $targetOverrideNames) {
        if ($null -eq $originalTargetOverrides[$name]) { Remove-Item -LiteralPath ('Env:' + $name) -ErrorAction SilentlyContinue }
        else { [Environment]::SetEnvironmentVariable($name,$originalTargetOverrides[$name],'Process') }
    }
    if (Test-Path -LiteralPath $fixture) {
        $resolved = [IO.Path]::GetFullPath($fixture)
        $base = [IO.Path]::GetFullPath($tempBase).TrimEnd([IO.Path]::DirectorySeparatorChar)
        if ($resolved.StartsWith($base + [IO.Path]::DirectorySeparatorChar,[StringComparison]::OrdinalIgnoreCase) -and [IO.Path]::GetFileName($resolved) -match '^venue-linux-release-test-[0-9a-f]{32}$') {
            Remove-Item -LiteralPath $resolved -Recurse -Force
        }
    }
}
