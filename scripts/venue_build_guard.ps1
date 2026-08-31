# Local build admission policy. No cache deletion or process termination is performed.
function Assert-VenuePlainPath {
    param([Parameter(Mandatory)][string]$Path)
    $cursor = [IO.Path]::GetFullPath($Path)
    while ($cursor) {
        if (Test-Path -LiteralPath $cursor) {
            $item = Get-Item -LiteralPath $cursor -Force -ErrorAction Stop
            if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) {
                throw "Build policy refuses a reparse-point path: $cursor"
            }
        }
        if ($cursor -eq [IO.Path]::GetPathRoot($cursor)) { break }
        $parent = [IO.Path]::GetDirectoryName($cursor.TrimEnd([char[]]'\/'))
        if ($parent -eq $cursor) { break }
        $cursor = $parent
    }
}

function Get-VenueBuildPlan {
    param(
        [Parameter(Mandatory)][string]$RepoRoot,
        [ValidateSet('auto','main','slot-1','slot-2')][string]$Slot = 'auto',
        [string]$RequestedTarget
    )
    $repo = [IO.Path]::GetFullPath($RepoRoot).TrimEnd([char[]]'\/')
    if (-not (Test-Path -LiteralPath (Join-Path $repo 'Cargo.toml') -PathType Leaf)) {
        throw 'Run the build entry point from a Venue workspace.'
    }
    # Hosted CI has no local F/G backing disk. Keep its existing job-owned target.
    $hostedCI = $env:GITHUB_ACTIONS -eq 'true' -and $env:RUNNER_ENVIRONMENT -eq 'github-hosted' -and $env:COMPUTERNAME -ne 'ZCODE'
    if ($hostedCI) {
        if (-not $env:RUNNER_TEMP -or -not $env:CARGO_TARGET_DIR) { throw 'Hosted CI must provide RUNNER_TEMP and CARGO_TARGET_DIR.' }
        $ciRoot = [IO.Path]::GetFullPath($env:RUNNER_TEMP).TrimEnd([char[]]'\/')
        $ciTarget = [IO.Path]::GetFullPath($env:CARGO_TARGET_DIR)
        if (-not $ciTarget.StartsWith($ciRoot + [IO.Path]::DirectorySeparatorChar,[StringComparison]::OrdinalIgnoreCase)) { throw 'CI target must remain inside RUNNER_TEMP.' }
        if ($RequestedTarget -and [IO.Path]::GetFullPath($RequestedTarget) -ne $ciTarget) { throw 'CI must reuse its job-owned target.' }
        return [PSCustomObject]@{RepoRoot=$repo;Root=$ciRoot;Slot='ci';TargetDirectory=$ciTarget;TempDirectory=(Join-Path $ciRoot 'venue-build-tmp');GuardDirectory=(Join-Path $ciRoot 'venue-build-guard');HostedCI=$true;BudgetBytes=150GB;MinimumHostFree=2GB;MinimumGuestFree=2GB;HostRoot=[IO.Path]::GetPathRoot($ciRoot);GuestRoot=[IO.Path]::GetPathRoot($ciRoot)}
    }
    if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) { throw 'This entry point manages local Windows builds; use the separate Linux release workflow on Linux.' }
    $root = 'G:\Build\Venue'
    if ($Slot -eq 'auto') {
        if ($repo -eq 'G:\Venue') { $Slot = 'main' }
        else {
            $sha = [Security.Cryptography.SHA256]::Create()
            try { $digest = $sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($repo.ToLowerInvariant())) } finally { $sha.Dispose() }
            $Slot = 'slot-' + (1 + ($digest[0] % 2))
        }
    }
    if ($RequestedTarget) {
        $requested = [IO.Path]::GetFullPath($RequestedTarget).TrimEnd([char[]]'\/')
        $allowed = @('main','slot-1','slot-2') | Where-Object { (Join-Path $root $_) -eq $requested }
        if (@($allowed).Count -ne 1) { throw 'Arbitrary target directories are disabled. Use G:\Build\Venue\main, slot-1 or slot-2.' }
        $Slot = [string]$allowed
    }
    $target = Join-Path $root $Slot
    Assert-VenuePlainPath $target
    Assert-VenuePlainPath (Join-Path $root '.guard')
    Assert-VenuePlainPath (Join-Path (Join-Path $root '.tmp') $Slot)
    return [PSCustomObject]@{RepoRoot=$repo;Root=$root;Slot=$Slot;TargetDirectory=$target;TempDirectory=(Join-Path (Join-Path $root '.tmp') $Slot);GuardDirectory=(Join-Path $root '.guard');HostedCI=$false;BudgetBytes=150GB;MinimumHostFree=100GB;MinimumGuestFree=20GB;HostRoot='F:\';GuestRoot='G:\'}
}

function Get-VenueCacheBytes {
    param([Parameter(Mandatory)][string]$Root,[long]$StopAfter = [long]::MaxValue)
    if (-not (Test-Path -LiteralPath $Root)) { return [long]0 }
    Assert-VenuePlainPath $Root
    $stack = [Collections.Generic.Stack[string]]::new()
    $stack.Push($Root)
    [long]$total = 0
    while ($stack.Count) {
        $directory = $stack.Pop()
        foreach ($entry in [IO.DirectoryInfo]::new($directory).EnumerateFileSystemInfos()) {
            # Never traverse legacy junctions into projects, databases or other volumes.
            if ($entry.Attributes -band [IO.FileAttributes]::ReparsePoint) { continue }
            if ($entry.Attributes -band [IO.FileAttributes]::Directory) { $stack.Push($entry.FullName) }
            else { $total += $entry.Length }
            if ($total -gt $StopAfter) { return $total }
        }
    }
    return $total
}

function Test-VenueBuildAdmission {
    param([Parameter(Mandatory)]$Plan)
    $hostFree = [IO.DriveInfo]::new($Plan.HostRoot).AvailableFreeSpace
    $guestFree = [IO.DriveInfo]::new($Plan.GuestRoot).AvailableFreeSpace
    if ($hostFree -lt $Plan.MinimumHostFree) { throw "Build refused: backing volume free space is below $($Plan.MinimumHostFree / 1GB) GiB." }
    if ($guestFree -lt $Plan.MinimumGuestFree) { throw "Build refused: target volume free space is below $($Plan.MinimumGuestFree / 1GB) GiB." }
    $bytes = if ($Plan.HostedCI) { Get-VenueCacheBytes $Plan.TargetDirectory $Plan.BudgetBytes } else { Get-VenueCacheBytes $Plan.Root $Plan.BudgetBytes }
    if ($bytes -gt $Plan.BudgetBytes) { throw 'Build refused: the 150 GiB cache admission budget is exceeded. Review idle registered caches; do not clean source or recovery data.' }
    [PSCustomObject]@{Slot=$Plan.Slot;TargetDirectory=$Plan.TargetDirectory;CacheBytes=$bytes;HostFreeBytes=$hostFree;GuestFreeBytes=$guestFree;BudgetBytes=$Plan.BudgetBytes;HostedCI=$Plan.HostedCI}
}

function Open-VenueBuildLock {
    param([Parameter(Mandatory)][string]$Path)
    Assert-VenuePlainPath $Path
    try { return [IO.File]::Open($Path,[IO.FileMode]::OpenOrCreate,[IO.FileAccess]::ReadWrite,[IO.FileShare]::None) }
    catch [IO.IOException] {
        $code = $_.Exception.HResult -band 65535
        if ($code -in @(32,33,11)) { return $null }
        throw
    }
}

function Enter-VenueBuildGuard {
    param(
        [Parameter(Mandatory)][string]$RepoRoot,
        [ValidateSet('auto','main','slot-1','slot-2')][string]$Slot='auto',
        [string]$RequestedTarget,
        [ValidateRange(0,60)][int]$WaitSeconds=60
    )
    if (Get-Variable VenueActiveBuildLease -Scope Global -ErrorAction SilentlyContinue) { throw 'Nested build guards are not allowed. Invoke guarded verification scripts directly.' }
    $plan = Get-VenueBuildPlan -RepoRoot $RepoRoot -Slot $Slot -RequestedTarget $RequestedTarget
    if (-not $plan.HostedCI -and $plan.Slot -eq 'main') {
        $wrapper = [Environment]::GetEnvironmentVariable('RUSTC_WRAPPER','Process')
        $cargoWrapper = [Environment]::GetEnvironmentVariable('CARGO_BUILD_RUSTC_WRAPPER','Process')
        if ($cargoWrapper -or ($wrapper -and [IO.Path]::GetFileName($wrapper) -notin @('sccache','sccache.exe'))) {
            throw 'Main incremental policy cannot override an explicit custom compiler wrapper. Review the wrapper policy before building.'
        }
    }
    $null = Test-VenueBuildAdmission $plan
    [void][IO.Directory]::CreateDirectory($plan.GuardDirectory)
    $handles = [Collections.Generic.List[IDisposable]]::new()
    $saved = @{}
    $deadline = [DateTime]::UtcNow.AddSeconds($WaitSeconds)
    try {
        $slotLock = $null
        do {
            $slotLock = Open-VenueBuildLock (Join-Path $plan.GuardDirectory ($plan.Slot + '.lock'))
            if ($null -ne $slotLock) { break }
            if ([DateTime]::UtcNow -ge $deadline) { throw 'Build slot is busy. Retry later; do not create another target directory.' }
            Start-Sleep -Milliseconds 250
        } while ($true)
        $handles.Add($slotLock)
        $poolLock = $null
        do {
            foreach ($index in 1,2) {
                $poolLock = Open-VenueBuildLock (Join-Path $plan.GuardDirectory ("parallel-$index.lock"))
                if ($null -ne $poolLock) { break }
            }
            if ($null -ne $poolLock) { break }
            if ([DateTime]::UtcNow -ge $deadline) { throw 'Both build permits are busy. Retry later; parallelism is capped at two.' }
            Start-Sleep -Milliseconds 250
        } while ($true)
        $handles.Add($poolLock)
        $null = Test-VenueBuildAdmission $plan
        foreach ($directory in @($plan.TargetDirectory,$plan.TempDirectory)) { Assert-VenuePlainPath $directory; [void][IO.Directory]::CreateDirectory($directory) }
        $settings = @{
            CARGO_TARGET_DIR=$plan.TargetDirectory; CARGO_BUILD_TARGET_DIR=$plan.TargetDirectory; CARGO_BUILD_BUILD_DIR=$plan.TargetDirectory
            CARGO_INCREMENTAL=$(if ($plan.Slot -eq 'main') {'1'} else {'0'})
            CARGO_PROFILE_DEV_DEBUG='line-tables-only'; CARGO_PROFILE_TEST_DEBUG='line-tables-only'
            TEMP=$plan.TempDirectory; TMP=$plan.TempDirectory
        }
        if (-not $plan.HostedCI -and $plan.Slot -eq 'main') {
            # sccache rejects CARGO_INCREMENTAL=1 even for Cargo's compiler probe.
            # Main uses direct incremental compilation; isolated slots retain wrappers.
            $settings.RUSTC_WRAPPER = ''
        }
        foreach ($name in $settings.Keys) {
            $saved[$name] = [Environment]::GetEnvironmentVariable($name,'Process')
            [Environment]::SetEnvironmentVariable($name,$settings[$name],'Process')
        }
        $lease = [PSCustomObject]@{Plan=$plan;TargetDirectory=$plan.TargetDirectory;TempDirectory=$plan.TempDirectory;Handles=$handles;SavedEnvironment=$saved;Released=$false}
        $global:VenueActiveBuildLease = $lease
        return $lease
    } catch {
        Restore-VenueBuildEnvironment $saved
        foreach ($handle in $handles) { $handle.Dispose() }
        throw
    }
}

function Restore-VenueBuildEnvironment {
    param([Parameter(Mandatory)][hashtable]$Saved)
    foreach ($name in $Saved.Keys) {
        if ($null -eq $Saved[$name]) {
            # PowerShell can coerce null to an empty string; .NET 9 preserves empty variables.
            Remove-Item -LiteralPath ('Env:' + $name) -ErrorAction SilentlyContinue
        } else {
            [Environment]::SetEnvironmentVariable($name,$Saved[$name],'Process')
        }
    }
}

function Exit-VenueBuildGuard {
    param([Parameter(Mandatory)]$Lease)
    if ($Lease.Released) { return }
    try {
        Restore-VenueBuildEnvironment $Lease.SavedEnvironment
    } finally {
        foreach ($handle in $Lease.Handles) { $handle.Dispose() }
        $Lease.Released = $true
        Remove-Variable VenueActiveBuildLease -Scope Global -ErrorAction SilentlyContinue
    }
}

function Assert-VenueCargoArguments {
    param([Parameter(Mandatory)][string[]]$Arguments)
    if ($Arguments.Count -eq 0 -or $Arguments[0] -notin @('check','test','build','clippy','fmt','metadata','tree','--version')) { throw 'Use an approved Cargo validation/build command; clean and arbitrary subcommands are disabled.' }
    foreach ($argument in $Arguments) {
        if ($argument -match '^(--target-dir|--build-dir|--artifact-dir|--config|--manifest-path|--lockfile-path)(=|$)' -or $argument.StartsWith('-Z') -or $argument.StartsWith('-C')) {
            throw 'Cargo path/config/unstable overrides are not accepted by the managed build entry point.'
        }
    }
}
