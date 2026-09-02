[CmdletBinding()]
param()
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'venue_build_guard.ps1')
. (Join-Path $PSScriptRoot 'ubuntu_build_helpers.ps1')
$passed = 0
function Assert-Rejected {
    param([scriptblock]$Action)
    $rejected = $false
    try { & $Action } catch { $rejected = $true }
    if (-not $rejected) { throw 'Expected validation to reject the fixture.' }
    $script:passed++
}

foreach ($id in @('..','../outside','a\b','',('x' * 65),'a.','CON','nul.txt')) {
    Assert-Rejected { Get-VenueUbuntuPaths $id }
}
$validId = 'fixture-' + [Guid]::NewGuid().ToString('N')
$paths = Get-VenueUbuntuPaths $validId
if ($paths.Release -ne "G:\Build\Venue\ubuntu\releases\$validId") { throw 'Output escaped the fixed Ubuntu root.' }
$passed++
Assert-VenueUbuntuSourcePaths 'G:\Build\Venue\ubuntu\source' 'G:\Build\Venue\ubuntu' 'G:\Build\Venue\slot-2'
$passed++
foreach ($overlap in @('G:\Build\Venue','G:\Build\Venue\ubuntu\releases\source','G:\Build\Venue\slot-2\source','G:\Build\Venue\ubuntu\zig-cache')) {
    Assert-Rejected { Assert-VenueUbuntuSourcePaths $overlap 'G:\Build\Venue\ubuntu' 'G:\Build\Venue\slot-2' }
}

$savedFlags = [Environment]::GetEnvironmentVariable('RUSTFLAGS','Process')
try {
    [Environment]::SetEnvironmentVariable('RUSTFLAGS','-C target-cpu=native','Process')
    Assert-Rejected { Assert-VenueUbuntuEnvironment }
} finally { Restore-VenueBuildEnvironment @{RUSTFLAGS=$savedFlags} }

# Generated protocol fixtures only; no compiler, slot, live account, or existing release is used.
$fixture = Join-Path 'G:\Build\Venue\ubuntu' ('validation-' + [Guid]::NewGuid().ToString('N'))
Assert-VenuePlainPath $fixture
[void][IO.Directory]::CreateDirectory($fixture)
$elf = Join-Path $fixture 'executable'
$bytes = [byte[]]::new(64)
$bytes[0]=0x7f; $bytes[1]=0x45; $bytes[2]=0x4c; $bytes[3]=0x46
$bytes[4]=2; $bytes[5]=1; $bytes[6]=1; $bytes[16]=3; $bytes[18]=0x3e
[IO.File]::WriteAllBytes($elf,$bytes)
Assert-VenueUbuntuElf $elf
$passed++
$bytes[0]=0x4d; $bytes[1]=0x5a
[IO.File]::WriteAllBytes($elf,$bytes)
Assert-Rejected { Assert-VenueUbuntuElf $elf }
$bytes[0]=0x7f; $bytes[1]=0x45; $bytes[18]=0xb7
[IO.File]::WriteAllBytes($elf,$bytes)
Assert-Rejected { Assert-VenueUbuntuElf $elf }
[IO.File]::WriteAllBytes($elf,[byte[]]::new(2))
Assert-Rejected { Assert-VenueUbuntuElf $elf }

$repo = Join-Path $fixture 'source'
[void][IO.Directory]::CreateDirectory($repo)
& git -C $repo init --quiet
if ($LASTEXITCODE -ne 0) { throw 'Fixture Git init failed.' }
& git -C $repo -c user.name=VenueFixture -c user.email=fixture@invalid.local commit --allow-empty --quiet -m fixture
if ($LASTEXITCODE -ne 0) { throw 'Fixture Git commit failed.' }
$revision = & git -C $repo rev-parse HEAD
Assert-VenueUbuntuRevision $repo $revision
$passed++
Assert-Rejected { Assert-VenueUbuntuRevision $repo ('0' * 40) }
[IO.File]::WriteAllText((Join-Path $repo 'untracked.txt'),'fixture')
Assert-Rejected { Assert-VenueUbuntuRevision $repo $revision }
& git -C $repo add -- untracked.txt
if ($LASTEXITCODE -ne 0) { throw 'Fixture Git add failed.' }
Assert-Rejected { Assert-VenueUbuntuRevision $repo $revision }

$entry = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'Build-VenueUbuntu.ps1') -Raw
foreach ($required in @("-Slot 'slot-2'",'Enter-VenueBuildGuard','Exit-VenueBuildGuard',
    'Restore-VenueBuildEnvironment','--locked --release -p venue-node',
    "[ValidateSet('Nodes','Control')]",'--locked --release -p venue-control',
    '--no-default-features --features $venue','--target $target',
    'x86_64-unknown-linux-gnu.2.35','Assert-VenueUbuntuElf','[IO.Directory]::Move',
    'Assert-VenueUbuntuRevision $source $ExpectedRevision')) {
    if (-not $entry.Contains($required,[StringComparison]::Ordinal)) { throw "Missing build contract: $required" }
}
if ($entry -notmatch '\$controlBinaries\s*=\s*@\(''venue-control-server'',''venue-executor-binance''\)') {
    throw 'Control release must contain venue-control-server and venue-executor-binance.'
}
if ($entry.Contains("'venue-copy-worker'",[StringComparison]::Ordinal)) {
    throw 'Frozen venue-copy-worker must not remain in the KOL Control release.'
}
if ($entry -match '(?im)^\s*(ssh|scp|Remove-Item|Stop-Process|cargo clean)\b') { throw 'Build entry must not deploy, delete or stop processes.' }
$passed++
Write-Output "Ubuntu build validation passed: $passed checks. Small fixtures retained at $fixture"
