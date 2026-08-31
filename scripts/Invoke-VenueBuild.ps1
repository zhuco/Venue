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
        & cargo @CargoArguments
        $cargoExit = $LASTEXITCODE
        if ($cargoExit -ne 0) { throw "Cargo failed with exit code $cargoExit." }
        $null = Test-VenueBuildAdmission $lease.Plan
    } finally { Pop-Location }
} finally { Exit-VenueBuildGuard $lease }
