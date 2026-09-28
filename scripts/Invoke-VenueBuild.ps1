[CmdletBinding()]
param(
    [ValidateSet('auto','main','slot-1','slot-2')][string]$Slot='auto',
    [string[]]$CargoArguments,
    [switch]$CheckOnly
)
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
. (Join-Path $PSScriptRoot 'venue_build_guard.ps1')
$repo = Split-Path -Parent $PSScriptRoot
if ($CheckOnly) {
    $plan = Get-VenueBuildPlan -RepoRoot $repo -Slot $Slot
    Test-VenueBuildAdmission $plan
    return
}
if (-not $CargoArguments) { throw 'Provide -CargoArguments, for example @("check","--locked","-p","venue-runtime").' }
Assert-VenueCargoArguments $CargoArguments
$lease = Enter-VenueBuildGuard -RepoRoot $repo -Slot $Slot
try {
    Push-Location -LiteralPath $repo
    try {
        $cargoInvocation = @()
        if (-not $lease.Plan.HostedCI -and $lease.Plan.Slot -eq 'main') {
            # A global Cargo config can choose sccache even when the corresponding
            # environment variable is empty. This CLI config has higher precedence.
            # TOML's literal empty string survives PowerShell's native argument quoting.
            $cargoInvocation += @('--config', "build.rustc-wrapper=''" )
        }
        $cargoInvocation += $CargoArguments
        & cargo @cargoInvocation
        $cargoExit = $LASTEXITCODE
        if ($cargoExit -ne 0) { throw "Cargo failed with exit code $cargoExit." }
        $null = Test-VenueBuildAdmission $lease.Plan
    } finally { Pop-Location }
} finally { Exit-VenueBuildGuard $lease }
