[CmdletBinding()]
param(
    [string]$ControlUrl = 'https://clawdbotweb.site',
    [switch]$SshTunnel,
    [string]$Server = 'cta@45.77.253.180',
    [int]$ControlPort = 39180,
    [string]$UiPath = 'G:\Build\Venue\main\release\venueflow.exe'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $UiPath -PathType Leaf)) {
    throw "VenueFlow executable was not found: $UiPath"
}

if ($SshTunnel) {
    $ControlUrl = "http://127.0.0.1:$ControlPort"
    $forward = "127.0.0.1:${ControlPort}:127.0.0.1:${ControlPort}"
    $listener = Get-NetTCPConnection -State Listen -LocalPort $ControlPort -ErrorAction SilentlyContinue
    if (-not $listener) {
        $tunnelLogDirectory = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'VenueFlow'
        [IO.Directory]::CreateDirectory($tunnelLogDirectory) | Out-Null
        $ssh = Start-Process ssh.exe -ArgumentList @(
            '-N',
            '-E', ('"{0}"' -f (Join-Path $tunnelLogDirectory 'ssh-tunnel.log')),
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
}

$serviceUri = [Uri]$ControlUrl.Trim().TrimEnd('/')
if (-not $serviceUri.IsAbsoluteUri -or
    ($serviceUri.Scheme -ne 'https' -and -not ($serviceUri.Scheme -eq 'http' -and $serviceUri.IsLoopback)) -or
    $serviceUri.UserInfo -or $serviceUri.Query -or $serviceUri.Fragment) {
    throw 'ControlUrl must be HTTPS or local HTTP, without credentials, query or fragment.'
}
$ControlUrl = $serviceUri.AbsoluteUri.TrimEnd('/')
$endpoint = "$ControlUrl/v2/account/session"

try {
    $response = Invoke-WebRequest -UseBasicParsing -Uri $endpoint -TimeoutSec 5
    if ([int]$response.StatusCode -lt 200 -or [int]$response.StatusCode -ge 500) {
        throw "Control account endpoint returned HTTP $([int]$response.StatusCode)."
    }
}
catch {
    $status = $_.Exception.Response | ForEach-Object { [int]$_.StatusCode }
    if ($status -notin @(401, 403)) {
        throw 'The configured Venue server is not reachable.'
    }
}

$startInfo = [System.Diagnostics.ProcessStartInfo]::new()
$startInfo.FileName = $UiPath
$startInfo.WorkingDirectory = Split-Path -Parent $UiPath
$startInfo.UseShellExecute = $false
$startInfo.EnvironmentVariables['VENUE_CONTROL_URL'] = $ControlUrl
$ui = [System.Diagnostics.Process]::Start($startInfo)
Write-Output "VenueFlow started (PID $($ui.Id)); default server: $ControlUrl"
