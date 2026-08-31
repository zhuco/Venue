[CmdletBinding()]
param()
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'venue_build_guard.ps1')
$script:assertions = 0
function Assert-GuardTest([bool]$Condition,[string]$Message) {
    if (-not $Condition) { throw "Guard test failed: $Message" }
    $script:assertions++
}
function Assert-GuardThrows([scriptblock]$Action,[string]$Pattern) {
    try { & $Action } catch {
        Assert-GuardTest ($_.Exception.Message -match $Pattern) "Expected '$Pattern', got '$($_.Exception.Message)'"
        return
    }
    throw "Guard test failed: expected refusal matching '$Pattern'"
}

$repo = Split-Path -Parent $PSScriptRoot
$first = Get-VenueBuildPlan -RepoRoot $repo
$second = Get-VenueBuildPlan -RepoRoot $repo
Assert-GuardTest ($first.TargetDirectory -eq $second.TargetDirectory) 'Stable cache selection'
Assert-GuardTest ($first.TargetDirectory -in @('G:\Build\Venue\main','G:\Build\Venue\slot-1','G:\Build\Venue\slot-2')) 'Only three targets'
Assert-GuardThrows { Get-VenueBuildPlan -RepoRoot $repo -RequestedTarget 'G:\Build\Venue\new-target-12345' } 'Arbitrary target'
Assert-GuardThrows { Get-VenueBuildPlan -RepoRoot $repo -RequestedTarget 'G:\Venue' } 'Arbitrary target'
Assert-GuardThrows { Assert-VenueCargoArguments @('clean') } 'approved Cargo'
Assert-GuardThrows { Assert-VenueCargoArguments @('check','--target-dir=G:\another') } 'overrides'
Assert-GuardThrows { Assert-VenueCargoArguments @('check','--config','build.target-dir="G:/another"') } 'overrides'
Assert-GuardThrows { Assert-VenueCargoArguments @('check','-Zbuild-dir-new-layout') } 'overrides'
Assert-VenueCargoArguments @('check','--locked','-p','venue-runtime')
$script:assertions++

$tempBase = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd([char[]]'\/')
$fixture = Join-Path $tempBase ('venue-build-guard-test-' + [Guid]::NewGuid().ToString('N'))
[void][IO.Directory]::CreateDirectory($fixture)
$externalHandles = [Collections.Generic.List[IDisposable]]::new()
$activeLease = $null
$junction = $null
$originalTarget = $env:CARGO_TARGET_DIR
$originalTemp = $env:TEMP
$originalIncremental = $env:CARGO_INCREMENTAL
$originalWrapper = [Environment]::GetEnvironmentVariable('RUSTC_WRAPPER','Process')
$originalCargoWrapper = [Environment]::GetEnvironmentVariable('CARGO_BUILD_RUSTC_WRAPPER','Process')
$script:fixturePlan = [PSCustomObject]@{
    RepoRoot=$repo;Root=$fixture;Slot='main';TargetDirectory=(Join-Path $fixture 'main')
    TempDirectory=(Join-Path $fixture 'tmp');GuardDirectory=(Join-Path $fixture 'locks')
    HostedCI=$false;BudgetBytes=150GB;MinimumHostFree=0;MinimumGuestFree=0
    HostRoot=[IO.Path]::GetPathRoot($fixture);GuestRoot=[IO.Path]::GetPathRoot($fixture)
}
try {
    Restore-VenueBuildEnvironment @{RUSTC_WRAPPER=$null;CARGO_BUILD_RUSTC_WRAPPER=$null}
    # Only this test scope substitutes temporary storage; production parameters expose no override.
    function Get-VenueBuildPlan { param($RepoRoot,$Slot,$RequestedTarget) return $script:fixturePlan }
    $null = Test-VenueBuildAdmission $script:fixturePlan
    $script:assertions++
    $script:fixturePlan.MinimumHostFree = [long]::MaxValue
    Assert-GuardThrows { Test-VenueBuildAdmission $script:fixturePlan } 'backing volume'
    $script:fixturePlan.MinimumHostFree = 0
    $script:fixturePlan.MinimumGuestFree = [long]::MaxValue
    Assert-GuardThrows { Test-VenueBuildAdmission $script:fixturePlan } 'target volume'
    $script:fixturePlan.MinimumGuestFree = 0
    [IO.File]::WriteAllText((Join-Path $fixture 'budget-fixture.txt'),'budget-fixture')
    $script:fixturePlan.BudgetBytes = 1
    Assert-GuardThrows { Enter-VenueBuildGuard -RepoRoot $repo -WaitSeconds 0 } '150 GiB'
    Assert-GuardTest (-not (Test-Path -LiteralPath $script:fixturePlan.GuardDirectory)) 'Budget failure occurs before creating locks or starting work'
    $script:fixturePlan.BudgetBytes = 150GB

    $activeLease = Enter-VenueBuildGuard -RepoRoot $repo -WaitSeconds 0
    Assert-GuardTest ($env:CARGO_TARGET_DIR -eq $script:fixturePlan.TargetDirectory) 'Target is overridden inside lease'
    Assert-GuardTest ($env:CARGO_BUILD_BUILD_DIR -eq $env:CARGO_TARGET_DIR) 'Intermediate build directory cannot escape'
    Assert-GuardTest ($env:CARGO_INCREMENTAL -eq '1') 'Main retains incremental cache'
    Assert-GuardTest ([object]::Equals([Environment]::GetEnvironmentVariable('RUSTC_WRAPPER','Process'),'')) 'Main explicitly disables the outer wrapper, including Cargo config defaults'
    Assert-GuardThrows { Enter-VenueBuildGuard -RepoRoot $repo -WaitSeconds 0 } 'Nested build'

    # Verify the lock is visible in another OS process, not just this PowerShell runspace.
    $helper = (Join-Path $PSScriptRoot 'venue_build_guard.ps1').Replace("'","''")
    $lockPath = (Join-Path $script:fixturePlan.GuardDirectory 'main.lock').Replace("'","''")
    $code = ". '$helper'; `$handle=Open-VenueBuildLock '$lockPath'; if (`$null -eq `$handle) { exit 0 }; `$handle.Dispose(); exit 2"
    $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($code))
    $engine = (Get-Process -Id $PID).Path
    $child = Start-Process -FilePath $engine -ArgumentList @('-NoProfile','-EncodedCommand',$encoded) -WindowStyle Hidden -PassThru
    if (-not $child.WaitForExit(10000)) { Stop-Process -Id $child.Id; throw 'Lock probe timed out.' }
    Assert-GuardTest ($child.ExitCode -eq 0) 'Cross-process slot exclusion'
    Exit-VenueBuildGuard $activeLease
    $activeLease = $null
    Assert-GuardTest ($env:CARGO_TARGET_DIR -eq $originalTarget -and $env:TEMP -eq $originalTemp -and $env:CARGO_INCREMENTAL -eq $originalIncremental) 'Environment is restored'
    Assert-GuardTest ($null -eq [Environment]::GetEnvironmentVariable('RUSTC_WRAPPER','Process')) 'Unset wrapper is restored'

    foreach ($wrapperSample in @($null,'','sccache','C:\tools\sccache.exe')) {
        Restore-VenueBuildEnvironment @{RUSTC_WRAPPER=$wrapperSample}
        $activeLease = Enter-VenueBuildGuard -RepoRoot $repo -WaitSeconds 0
        Assert-GuardTest ([object]::Equals([Environment]::GetEnvironmentVariable('RUSTC_WRAPPER','Process'),'')) 'Every main lease selects direct incremental compilation'
        try { throw 'simulated main validation failure' } catch { } finally { Exit-VenueBuildGuard $activeLease; $activeLease=$null }
        Assert-GuardTest ([object]::Equals([Environment]::GetEnvironmentVariable('RUSTC_WRAPPER','Process'),$wrapperSample)) 'Failure restores unset, empty and sccache wrapper values exactly'
    }
    [Environment]::SetEnvironmentVariable('RUSTC_WRAPPER','C:\tools\custom-wrapper.exe','Process')
    Assert-GuardThrows { Enter-VenueBuildGuard -RepoRoot $repo -WaitSeconds 0 } 'explicit custom compiler wrapper'
    Assert-GuardTest ($env:RUSTC_WRAPPER -ceq 'C:\tools\custom-wrapper.exe') 'Refusal preserves explicit custom wrapper'
    Restore-VenueBuildEnvironment @{RUSTC_WRAPPER=$null;CARGO_BUILD_RUSTC_WRAPPER='sccache'}
    Assert-GuardThrows { Enter-VenueBuildGuard -RepoRoot $repo -WaitSeconds 0 } 'explicit custom compiler wrapper'
    Assert-GuardTest ($env:CARGO_BUILD_RUSTC_WRAPPER -ceq 'sccache') 'Refusal preserves explicit Cargo wrapper setting'
    Restore-VenueBuildEnvironment @{CARGO_BUILD_RUSTC_WRAPPER=$null}

    $busy = Open-VenueBuildLock (Join-Path $script:fixturePlan.GuardDirectory 'main.lock')
    $externalHandles.Add($busy)
    Assert-GuardThrows { Enter-VenueBuildGuard -RepoRoot $repo -WaitSeconds 0 } 'slot is busy'
    $busy.Dispose(); $externalHandles.Clear()
    foreach ($index in 1,2) { $externalHandles.Add((Open-VenueBuildLock (Join-Path $script:fixturePlan.GuardDirectory ("parallel-$index.lock")))) }
    Assert-GuardThrows { Enter-VenueBuildGuard -RepoRoot $repo -WaitSeconds 0 } 'Both build permits'
    foreach ($handle in $externalHandles) { $handle.Dispose() }; $externalHandles.Clear()
    $activeLease = Enter-VenueBuildGuard -RepoRoot $repo -WaitSeconds 0
    Exit-VenueBuildGuard $activeLease
    $activeLease = $null
    $script:assertions++ # Failed permit acquisition did not leak the slot lock.

    $script:fixturePlan.Slot = 'slot-1'
    [Environment]::SetEnvironmentVariable('RUSTC_WRAPPER','venue-test-wrapper','Process')
    $activeLease = Enter-VenueBuildGuard -RepoRoot $repo -WaitSeconds 0
    Assert-GuardTest ($env:CARGO_INCREMENTAL -eq '0') 'Isolated slot disables incremental cache'
    Assert-GuardTest ($env:RUSTC_WRAPPER -ceq 'venue-test-wrapper') 'Isolated slot keeps the configured wrapper'
    try { throw 'simulated validation failure' } catch { } finally { Exit-VenueBuildGuard $activeLease; $activeLease=$null }
    Assert-GuardTest ($env:CARGO_TARGET_DIR -eq $originalTarget) 'Exception path restores environment'
    Assert-GuardTest ($env:RUSTC_WRAPPER -ceq 'venue-test-wrapper') 'Isolated slot failure does not change the wrapper'
    Restore-VenueBuildEnvironment @{RUSTC_WRAPPER=$null}

    $scanRoot = Join-Path $fixture 'cache-scan'
    $outside = Join-Path $fixture 'protected-data'
    [void][IO.Directory]::CreateDirectory($scanRoot)
    [void][IO.Directory]::CreateDirectory($outside)
    [IO.File]::WriteAllText((Join-Path $scanRoot 'cache.bin'),'12345')
    [IO.File]::WriteAllText((Join-Path $outside 'keep.txt'),'protected-data')
    $junction = Join-Path $scanRoot 'redirect'
    New-Item -ItemType Junction -Path $junction -Target $outside | Out-Null
    Assert-GuardThrows { Assert-VenuePlainPath (Join-Path $junction 'child') } 'reparse-point'
    Assert-GuardTest ((Get-VenueCacheBytes $scanRoot) -eq 5) 'Cache scan does not follow junctions'
    Assert-GuardTest ([IO.File]::ReadAllText((Join-Path $outside 'keep.txt')) -eq 'protected-data') 'Protected fixture remains unchanged'
    [PSCustomObject]@{Passed=$true;Assertions=$script:assertions;CargoStarted=$false;ProductionCacheDeleted=$false;FixtureRoot=$fixture} | ConvertTo-Json
} finally {
    if ($null -ne $activeLease) { Exit-VenueBuildGuard $activeLease }
    Restore-VenueBuildEnvironment @{RUSTC_WRAPPER=$originalWrapper;CARGO_BUILD_RUSTC_WRAPPER=$originalCargoWrapper}
    foreach ($handle in $externalHandles) { $handle.Dispose() }
    if ($junction -and (Test-Path -LiteralPath $junction)) { [IO.Directory]::Delete($junction) }
    $resolved = (Resolve-Path -LiteralPath $fixture).ProviderPath
    if ($resolved -ne [IO.Path]::GetFullPath($fixture) -or -not $resolved.StartsWith($tempBase + [IO.Path]::DirectorySeparatorChar,[StringComparison]::OrdinalIgnoreCase) -or [IO.Path]::GetFileName($resolved) -notmatch '^venue-build-guard-test-[0-9a-f]{32}$') { throw 'Refusing to remove an unexpected fixture directory.' }
    # This removes only the test-owned temporary fixture, never a production cache.
    Remove-Item -LiteralPath $resolved -Recurse -Force
}
