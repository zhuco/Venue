[CmdletBinding()]
param([string]$CargoTargetDir)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
if (-not $CargoTargetDir) {
    $CargoTargetDir = Join-Path ([System.IO.Path]::GetTempPath()) 'venue-node-verification-target'
}
$targetRoot = [System.IO.Path]::GetFullPath($CargoTargetDir)
$repoRootFull = [System.IO.Path]::GetFullPath($repoRoot)
if ($targetRoot.Equals($repoRootFull, [StringComparison]::OrdinalIgnoreCase) -or
    $targetRoot.StartsWith(
        $repoRootFull + [System.IO.Path]::DirectorySeparatorChar,
        [StringComparison]::OrdinalIgnoreCase
    )) {
    throw 'venue-node 专项构建目录必须位于 worktree 外。'
}
New-Item -ItemType Directory -Path $targetRoot -Force | Out-Null
$cargoTemp = Join-Path `
    (Split-Path -Parent $targetRoot) `
    "$([System.IO.Path]::GetFileName($targetRoot))-tmp"
New-Item -ItemType Directory -Path $cargoTemp -Force | Out-Null
$env:CARGO_TARGET_DIR = $targetRoot
$env:CARGO_INCREMENTAL = '0'
$env:TEMP = $cargoTemp
$env:TMP = $cargoTemp

function Test-FailClosedNode {
    param(
        [Parameter(Mandatory)] [string]$Venue,
        [Parameter(Mandatory)] [string]$BinaryPath
    )

    $symbols = @{
        bybit = 'BTC/USDT'
        hyperliquid = 'BTC/USDC'
        okx = 'BTC/USDT'
    }
    $credentialNames = @{
        bybit = @('BYBIT_API_KEY', 'BYBIT_API_SECRET')
        hyperliquid = @(
            'HYPERLIQUID_MASTER_ADDRESS',
            'HYPERLIQUID_USER_ADDRESS',
            'HYPERLIQUID_VAULT_ADDRESS',
            'HYPERLIQUID_AGENT_NAME',
            'HYPERLIQUID_AGENT_ADDRESS',
            'HYPERLIQUID_AGENT_PRIVATE_KEY'
        )
        okx = @('OKX_API_KEY', 'OKX_API_SECRET', 'OKX_API_PASSPHRASE')
    }
    $requiredEvidence = @(
        'Owner',
        'WAL',
        'unique account writer fence',
        'signed readback',
        'UNKNOWN reconciliation',
        'Stop/Flatten',
        'operator-confirmed Canary evidence'
    )

    foreach ($mode in @('TEST', 'LIVE')) {
        $probeBase = Join-Path `
            (Split-Path -Parent $targetRoot) `
            "venue-node-fail-closed-$Venue-$mode-$([Guid]::NewGuid().ToString('N'))"
        if (Test-Path -LiteralPath $probeBase) {
            throw "失败关闭探针路径意外已存在：$probeBase"
        }

        $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
        $startInfo.FileName = $BinaryPath
        $startInfo.UseShellExecute = $false
        $startInfo.RedirectStandardOutput = $true
        $startInfo.RedirectStandardError = $true
        foreach ($argument in @(
            '--mode', $mode,
            '--trading-account-id', '00000000-0000-4000-8000-000000000001',
            '--symbol', $symbols[$Venue],
            '--artifacts-base', $probeBase
        )) {
            $startInfo.ArgumentList.Add($argument)
        }
        foreach ($credentialName in $credentialNames[$Venue]) {
            [void]$startInfo.Environment.Remove($credentialName)
        }

        $process = [System.Diagnostics.Process]::new()
        $process.StartInfo = $startInfo
        if (-not $process.Start()) {
            throw "无法启动 $Venue $mode 失败关闭探针。"
        }
        $stdout = $process.StandardOutput.ReadToEnd()
        $stderr = $process.StandardError.ReadToEnd()
        $process.WaitForExit()
        if ($process.ExitCode -eq 0) {
            throw "$Venue $mode 在安全证据未闭环时错误地成功启动。"
        }
        $output = "$stdout`n$stderr"
        foreach ($marker in $requiredEvidence) {
            if (-not $output.Contains($marker, [StringComparison]::Ordinal)) {
                throw "$Venue $mode 失败关闭输出缺少证据标记：$marker"
            }
        }
        if (Test-Path -LiteralPath $probeBase) {
            throw "$Venue $mode 失败关闭前创建了工件路径：$probeBase"
        }
    }
}

$evidence = [System.Collections.Generic.List[object]]::new()
foreach ($venue in @('binance', 'bitget', 'bybit', 'gate', 'hyperliquid', 'okx')) {
    $binaryName = "venue-node-$venue"
    & cargo.exe build --locked -p venue-node --no-default-features --features $venue --bin $binaryName
    if ($LASTEXITCODE -ne 0) {
        throw "构建 $binaryName 失败，退出码 $LASTEXITCODE"
    }
    $binaryPath = Join-Path $targetRoot 'debug' "$binaryName.exe"
    if (-not (Test-Path -LiteralPath $binaryPath -PathType Leaf)) {
        $binaryPath = Join-Path $targetRoot 'debug' $binaryName
    }
    $result = & (Join-Path $PSScriptRoot 'verify_venue_node_binary_isolation.ps1') `
        -Venue $venue `
        -BinaryPath $binaryPath
    $venueEvidence = $result | ConvertFrom-Json
    if ($venue -in @('bybit', 'hyperliquid', 'okx')) {
        Test-FailClosedNode -Venue $venue -BinaryPath $binaryPath
        $venueEvidence | Add-Member -NotePropertyName fail_closed_modes -NotePropertyValue @('TEST', 'LIVE')
        $venueEvidence | Add-Member -NotePropertyName fail_closed_no_io -NotePropertyValue $true
    }
    $evidence.Add($venueEvidence)
}

$evidence | ConvertTo-Json -Depth 4
