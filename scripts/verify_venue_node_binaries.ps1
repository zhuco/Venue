[CmdletBinding()]
param([string]$CargoTargetDir)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
if (-not $CargoTargetDir) {
    $targetParent = if ([string]::IsNullOrWhiteSpace($env:CARGO_TARGET_DIR)) {
        'G:\Build\Venue'
    } else {
        $env:CARGO_TARGET_DIR
    }
    $CargoTargetDir = Join-Path $targetParent "venue-node-verification-target-$PID"
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

function Test-UnauthorizedLiveRejected {
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
            'HYPERLIQUID_ACCOUNT_ADDRESS',
            'HYPERLIQUID_API_WALLET_ADDRESS',
            'HYPERLIQUID_API_WALLET_PRIVATE_KEY',
            'HYPERLIQUID_VAULT_ADDRESS'
        )
        okx = @('OKX_API_KEY', 'OKX_API_SECRET', 'OKX_API_PASSPHRASE')
    }
    foreach ($mode in @('LIVE')) {
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
            '--artifacts-base', $probeBase,
            '--', 'preflight', '--confirm-live', 'wrong-venue'
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
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
        if (-not $process.WaitForExit(15000)) {
            $process.Kill($true)
            $process.WaitForExit()
            [void]$stdoutTask.GetAwaiter().GetResult()
            [void]$stderrTask.GetAwaiter().GetResult()
            throw "$Venue $mode 缺证据探针未在 15 秒内失败关闭。"
        }
        $stdout = $stdoutTask.GetAwaiter().GetResult()
        $stderr = $stderrTask.GetAwaiter().GetResult()
        if ($process.ExitCode -eq 0) {
            throw "$Venue $mode 在安全证据未闭环时错误地成功启动。"
        }
        $output = "$stdout`n$stderr"
        if (-not $output.Contains(
            '--confirm-live must exactly match the lowercase venue id',
            [StringComparison]::Ordinal
        )) {
            throw "$Venue $mode 未在凭证读取前拒绝错误的人工确认。"
        }
        if (Test-Path -LiteralPath $probeBase) {
            throw "$Venue $mode 失败关闭前创建了工件路径：$probeBase"
        }
    }
}

function Test-LegacyPredecessorRequired {
    param(
        [Parameter(Mandatory)] [string]$Venue,
        [Parameter(Mandatory)] [string]$BinaryPath
    )

    $symbols = @{ binance = 'BTC/USDT'; bitget = 'BTC/USDT'; gate = 'DOGE/USDT' }
    $probeBase = Join-Path `
        (Split-Path -Parent $targetRoot) `
        "venue-node-legacy-predecessor-$Venue-$([Guid]::NewGuid().ToString('N'))"
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $BinaryPath
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    foreach ($argument in @(
        '--mode', 'LIVE',
        '--trading-account-id', '00000000-0000-4000-8000-000000000001',
        '--symbol', $symbols[$Venue],
        '--artifacts-base', $probeBase
    )) {
        $startInfo.ArgumentList.Add($argument)
    }
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    if (-not $process.Start()) {
        throw "无法启动 $Venue legacy predecessor 拒绝探针。"
    }
    $stdout = $process.StandardOutput.ReadToEndAsync()
    $stderr = $process.StandardError.ReadToEndAsync()
    if (-not $process.WaitForExit(15000)) {
        $process.Kill($true)
        $process.WaitForExit()
        throw "$Venue 缺 predecessor 探针未在 15 秒内结束。"
    }
    $output = "$($stdout.GetAwaiter().GetResult())`n$($stderr.GetAwaiter().GetResult())"
    if ($process.ExitCode -eq 0 -or (Test-Path -LiteralPath $probeBase) -or
        -not $output.Contains(
            'legacy v1 predecessor handoff is required only for Binance, Gate, and Bitget and must validate exactly',
            [StringComparison]::Ordinal
        )) {
        throw "$Venue 未在任何凭证或工件 I/O 前拒绝缺少 v1 predecessor。"
    }
}

function Test-NonProductionModeRejected {
    param(
        [Parameter(Mandatory)] [string]$Venue,
        [Parameter(Mandatory)] [string]$BinaryPath
    )

    $probeBase = Join-Path `
        (Split-Path -Parent $targetRoot) `
        "venue-node-reject-test-$Venue-$([Guid]::NewGuid().ToString('N'))"
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $BinaryPath
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $credentialNames = @{
        binance = @('BINANCE_API_KEY', 'BINANCE_API_SECRET')
        bitget = @('BITGET_API_KEY', 'BITGET_API_SECRET', 'BITGET_API_PASSPHRASE', 'BITGET_PASSPHRASE')
        bybit = @('BYBIT_API_KEY', 'BYBIT_API_SECRET')
        gate = @('GATEIO_API_KEY', 'GATEIO_API_SECRET')
        hyperliquid = @(
            'HYPERLIQUID_ACCOUNT_ADDRESS', 'HYPERLIQUID_API_WALLET_ADDRESS',
            'HYPERLIQUID_API_WALLET_PRIVATE_KEY', 'HYPERLIQUID_VAULT_ADDRESS'
        )
        okx = @('OKX_API_KEY', 'OKX_API_SECRET', 'OKX_API_PASSPHRASE')
    }
    foreach ($argument in @(
        '--mode', 'TEST',
        '--trading-account-id', '00000000-0000-4000-8000-000000000001',
        '--symbol', 'BTC/USDT',
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
        throw "无法启动 $Venue TEST 拒绝探针。"
    }
    $stdoutTask = $process.StandardOutput.ReadToEndAsync()
    $stderrTask = $process.StandardError.ReadToEndAsync()
    if (-not $process.WaitForExit(15000)) {
        $process.Kill($true)
        $process.WaitForExit()
        throw "$Venue TEST 拒绝探针未在 15 秒内结束。"
    }
    $stdout = $stdoutTask.GetAwaiter().GetResult()
    $stderr = $stderrTask.GetAwaiter().GetResult()
    if ($process.ExitCode -eq 0 -or (Test-Path -LiteralPath $probeBase)) {
        throw "$Venue 接受了 TEST 或在拒绝前创建了工件。"
    }
    if (-not "$stdout`n$stderr".Contains(
        'gateway mode must be exactly LIVE',
        [StringComparison]::Ordinal
    )) {
        throw "$Venue TEST 未由最前置 mode parser 拒绝。"
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
    Test-NonProductionModeRejected -Venue $venue -BinaryPath $binaryPath
    $venueEvidence | Add-Member -NotePropertyName rejected_modes -NotePropertyValue @('TEST')
    $venueEvidence | Add-Member -NotePropertyName mode_rejection -NotePropertyValue `
        'exact parser error; credentials removed; artifact root absent'
    if ($venue -in @('bybit', 'hyperliquid', 'okx')) {
        Test-UnauthorizedLiveRejected -Venue $venue -BinaryPath $binaryPath
        $venueEvidence | Add-Member -NotePropertyName live_commands -NotePropertyValue `
            @('preflight', 'canary-place', 'canary-cancel')
        $venueEvidence | Add-Member -NotePropertyName wrong_confirmation_no_io -NotePropertyValue $true
    } elseif ($venue -in @('binance', 'bitget', 'gate')) {
        Test-LegacyPredecessorRequired -Venue $venue -BinaryPath $binaryPath
        $venueEvidence | Add-Member -NotePropertyName legacy_predecessor_required_no_io -NotePropertyValue $true
    }
    $evidence.Add($venueEvidence)
}

$evidence | ConvertTo-Json -Depth 4
