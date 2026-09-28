[CmdletBinding()]
param([string]$UiPath = 'G:\Build\Venue\main\release\venueflow.exe')
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$executable = (Resolve-Path -LiteralPath $UiPath).Path
if ([IO.Path]::GetFileName($executable) -ne 'venueflow.exe') {
    throw 'UiPath must identify a built venueflow.exe.'
}
$desktop = [Environment]::GetFolderPath('Desktop')
$shortcutPath = Join-Path $desktop 'VenueFlow.lnk'
$shell = New-Object -ComObject WScript.Shell
$shortcut = $shell.CreateShortcut($shortcutPath)
$shortcut.TargetPath = $executable
$shortcut.WorkingDirectory = Split-Path -Parent $executable
$shortcut.IconLocation = "$executable,0"
$shortcut.Description = 'VENUE — Markets move further here'
$shortcut.Save()
Write-Output "VenueFlow shortcut: $shortcutPath -> $executable"
