[CmdletBinding()]
param(
    [string]$Server = 'cta@45.77.253.180',
    [int]$ControlPort = 39180,
    [string]$UiPath = 'G:\Build\Venue\main\release\venueflow.exe'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $UiPath -PathType Leaf)) {
    throw "VenueFlow executable was not found: $UiPath"
}

$forward = "127.0.0.1:${ControlPort}:127.0.0.1:${ControlPort}"
$listener = Get-NetTCPConnection -State Listen -LocalPort $ControlPort -ErrorAction SilentlyContinue
if (-not $listener) {
    $ssh = Start-Process ssh.exe -ArgumentList @(
        '-N',
        '-o', 'BatchMode=yes',
        '-o', 'ExitOnForwardFailure=yes',
        '-o', 'ServerAliveInterval=15',
        '-o', 'ServerAliveCountMax=4',
        '-L', $forward,
        $Server
    ) -PassThru -WindowStyle Hidden
    for ($attempt = 0; $attempt -lt 20; $attempt++) {
        Start-Sleep -Milliseconds 250
        if ($ssh.HasExited) {
            throw "SSH Control tunnel exited with code $($ssh.ExitCode)."
        }
        if (Get-NetTCPConnection -State Listen -LocalPort $ControlPort -ErrorAction SilentlyContinue) {
            break
        }
    }
    if (-not (Get-NetTCPConnection -State Listen -LocalPort $ControlPort -ErrorAction SilentlyContinue)) {
        throw "SSH Control tunnel did not listen on 127.0.0.1:$ControlPort."
    }
}

$ui = Start-Process -FilePath $UiPath -WorkingDirectory (Split-Path -Parent $UiPath) -PassThru
Write-Output "VenueFlow started (PID $($ui.Id)); Control tunnel is available on 127.0.0.1:$ControlPort."
