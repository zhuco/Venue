[CmdletBinding()]
param(
    [ValidateSet('auto', 'main', 'slot-1', 'slot-2')]
    [string]$Slot = 'auto',
    [string]$ControlUrl = 'https://clawdbotweb.site',
    [switch]$SshTunnel,
    [string]$Server = 'cta@45.77.253.180',
    [int]$ControlPort = 39180,
    [switch]$BuildOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repo = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$build = Join-Path $PSScriptRoot 'Invoke-VenueBuild.ps1'

# Keep the release build inside the shared fixed-cache guard. The follow-up plan is
# deterministic for this worktree, so the executable is read from the same slot.
& $build -Slot $Slot -CargoArguments @(
    'build', '--locked', '--release', '-p', 'venueflow', '--bin', 'venueflow'
)

. (Join-Path $PSScriptRoot 'venue_build_guard.ps1')
$plan = Get-VenueBuildPlan -RepoRoot $repo -Slot $Slot
$uiPath = Join-Path $plan.TargetDirectory 'release\venueflow.exe'
if (-not (Test-Path -LiteralPath $uiPath -PathType Leaf)) {
    throw "VenueFlow build completed but its executable was not found: $uiPath"
}

if ($BuildOnly) {
    Write-Output "VenueFlow built: $uiPath"
    return
}

$launch = @{
    UiPath = $uiPath
    ControlUrl = $ControlUrl
    Server = $Server
    ControlPort = $ControlPort
}
if ($SshTunnel) { $launch.SshTunnel = $true }
& (Join-Path $PSScriptRoot 'Start-VenueFlow.ps1') @launch
