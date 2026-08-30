[CmdletBinding()]
param([string]$CargoTargetDir)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = [System.IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))

if (-not $CargoTargetDir) {
    $CargoTargetDir = Join-Path 'G:\Build\Venue' "venue-gateway-candidate-contract-target-$PID"
}
$targetRoot = [System.IO.Path]::GetFullPath($CargoTargetDir)
if ($targetRoot.Equals($repoRoot, [StringComparison]::OrdinalIgnoreCase) -or
    $targetRoot.StartsWith(
        $repoRoot + [System.IO.Path]::DirectorySeparatorChar,
        [StringComparison]::OrdinalIgnoreCase
    )) {
    throw '网关候选专项构建目录必须位于 worktree 外。'
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

function Invoke-Cargo {
    param([Parameter(Mandatory)] [string[]]$Arguments)

    & cargo.exe @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "cargo $($Arguments -join ' ') 失败，退出码 $LASTEXITCODE"
    }
}

Invoke-Cargo -Arguments @(
    'test', '--locked', '-p', 'venue-node', '--no-default-features', '--lib'
)
Invoke-Cargo -Arguments @(
    'test', '--locked', '-p', 'venue-node', '--no-default-features',
    '--test', 'gateway_candidate_conformance'
)
Invoke-Cargo -Arguments @(
    'test', '--locked', '-p', 'venue-execution', 'account_host::tests'
)
Invoke-Cargo -Arguments @(
    'test', '--locked', '-p', 'venue-gateway-okx', 'account_gateway::tests'
)
Invoke-Cargo -Arguments @(
    'test', '--locked', '-p', 'venue-gateway-hyperliquid',
    'binding_rejects_wrong_venue_and_account'
)

$symbols = @{
    binance = 'BTC/USDT'
    bitget = 'BTC/USDT'
    bybit = 'BTC/USDT'
    gate = 'DOGE/USDT'
    hyperliquid = 'BTC/USDC'
    okx = 'BTC/USDT'
}
$credentialNames = @{
    binance = @('BINANCE_API_KEY', 'BINANCE_API_SECRET')
    bitget = @(
        'BITGET_API_KEY', 'BITGET_API_SECRET', 'BITGET_API_PASSPHRASE', 'BITGET_PASSPHRASE'
    )
    bybit = @('BYBIT_API_KEY', 'BYBIT_API_SECRET')
    gate = @('GATEIO_API_KEY', 'GATEIO_API_SECRET')
    hyperliquid = @(
        'HYPERLIQUID_ACCOUNT_ADDRESS', 'HYPERLIQUID_API_WALLET_ADDRESS',
        'HYPERLIQUID_API_WALLET_PRIVATE_KEY', 'HYPERLIQUID_VAULT_ADDRESS'
    )
    okx = @('OKX_API_KEY', 'OKX_API_SECRET', 'OKX_API_PASSPHRASE')
}
$preCredentialVenues = @('bybit', 'hyperliquid', 'okx')

function Test-MissingEvidenceFailClosed {
    param(
        [Parameter(Mandatory)] [string]$Venue,
        [Parameter(Mandatory)] [string]$BinaryPath
    )

    foreach ($mode in @('LIVE')) {
        $probeBase = Join-Path `
            (Split-Path -Parent $targetRoot) `
            "venue-gateway-no-io-$Venue-$mode-$([Guid]::NewGuid().ToString('N'))"
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
            '--trading-account-id', '00000000-0000-4000-8000-000000000041',
            '--symbol', $symbols[$Venue],
            '--artifacts-base', $probeBase
        )) {
            $startInfo.ArgumentList.Add($argument)
        }
        if ($Venue -in $preCredentialVenues) {
            foreach ($argument in @('--', 'preflight', '--confirm-live', 'wrong-venue')) {
                $startInfo.ArgumentList.Add($argument)
            }
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
            throw "$Venue $mode 在缺少共享证据时错误地成功启动。"
        }
        if (Test-Path -LiteralPath $probeBase) {
            throw "$Venue $mode 在失败关闭前创建了工件路径：$probeBase"
        }

        $output = "$stdout`n$stderr"
        if ($Venue -in $preCredentialVenues) {
            if (-not $output.Contains(
                '--confirm-live must exactly match the lowercase venue id',
                [StringComparison]::Ordinal
            )) {
                throw "$Venue $mode 未在凭证读取前拒绝错误的人工确认。"
            }
        } elseif (-not $output.Contains(
            'runtime arguments must select exactly one fixed deployment command',
            [StringComparison]::Ordinal
        )) {
            throw "$Venue $mode 未在 legacy runtime 参数解析前失败关闭。"
        }
    }
}

function Test-NonProductionModeRejected {
    param(
        [Parameter(Mandatory)] [string]$Venue,
        [Parameter(Mandatory)] [string]$BinaryPath
    )

    $probeBase = Join-Path `
        (Split-Path -Parent $targetRoot) `
        "venue-gateway-reject-test-$Venue-$([Guid]::NewGuid().ToString('N'))"
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $BinaryPath
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    foreach ($argument in @(
        '--mode', 'TEST',
        '--trading-account-id', '00000000-0000-4000-8000-000000000041',
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

$matrix = [System.Collections.Generic.List[object]]::new()
foreach ($venue in @('binance', 'bitget', 'bybit', 'gate', 'hyperliquid', 'okx')) {
    $binaryName = "venue-node-$venue"
    Invoke-Cargo -Arguments @(
        'test', '--locked', '-p', 'venue-node', '--no-default-features',
        '--features', $venue, '--bin', $binaryName
    )
    Invoke-Cargo -Arguments @(
        'build', '--locked', '-p', 'venue-node', '--no-default-features',
        '--features', $venue, '--bin', $binaryName
    )

    $binaryPath = Join-Path $targetRoot 'debug' "$binaryName.exe"
    if (-not (Test-Path -LiteralPath $binaryPath -PathType Leaf)) {
        $binaryPath = Join-Path $targetRoot 'debug' $binaryName
    }
    $isolationJson = & (Join-Path $PSScriptRoot 'verify_venue_node_binary_isolation.ps1') `
        -Venue $venue `
        -BinaryPath $binaryPath
    if ($LASTEXITCODE -ne 0) {
        throw "$venue 固定二进制隔离验证失败。"
    }
    $isolation = $isolationJson | ConvertFrom-Json
    Test-NonProductionModeRejected -Venue $venue -BinaryPath $binaryPath
    Test-MissingEvidenceFailClosed -Venue $venue -BinaryPath $binaryPath

    $candidateEvidence = switch ($venue) {
        'binance' {
            [ordered]@{
                order_families = 'binary_test_exact_three_family_coverage'
                one_shot_mutation = 'shared_host_only; candidate_test_prepares_three_mutation_kinds'
                ack_unknown = 'binary_test_exact_readback_or_unknown'
                capability = 'binary_test_empty'
            }
        }
        'bitget' {
            [ordered]@{
                order_families = 'candidate_readback_exercised; no_direct_family_assertion'
                one_shot_mutation = 'binary_test_dispatch_count_equals_one'
                ack_unknown = 'binary_test_exact_readback_failure_is_unknown'
                capability = 'binary_test_empty'
            }
        }
        'gate' {
            [ordered]@{
                order_families = 'binary_test_regular_complete_conditional_algo_profile_unsupported'
                one_shot_mutation = 'binary_test_place_cancel_reduce_once_and_replay_rejected'
                ack_unknown = 'binary_test_unknown_not_retried'
                capability = 'candidate_fixture_nonempty; fixed_binary_not_candidate_admitted'
            }
        }
        'bybit' {
            [ordered]@{
                order_families = 'signed_exact_order_readback_in_account_gateway'
                one_shot_mutation = 'opaque_single_use_host_permit; post_only_place_and_exact_cancel'
                ack_unknown = 'transport_ambiguity_is_unknown_without_retry'
                capability = 'account_host_bound; public raw POST unavailable'
            }
        }
        'hyperliquid' {
            [ordered]@{
                order_families = 'exact_cloid_order_status_readback'
                one_shot_mutation = 'opaque_single_use_host_permit; persisted nonce before signing'
                ack_unknown = 'exchange_transport_ambiguity_is_unknown_without_retry'
                capability = 'account_host_bound; public exchange POST unavailable'
            }
        }
        'okx' {
            [ordered]@{
                order_families = 'signed exact clOrdId readback in account gateway'
                one_shot_mutation = 'opaque single-use host permit; base-to-contract conversion'
                ack_unknown = 'transport ambiguity is unknown without retry'
                capability = 'account_host_bound; public raw POST unavailable'
            }
        }
    }
    $matrix.Add([ordered]@{
        venue = $venue
        live_binding = 'integration_test_exact_live_and_reject_nonproduction'
        rejected_modes = @('TEST')
        mode_rejection = 'exact parser error; credentials removed; artifact root absent'
        order_family_evidence = $candidateEvidence.order_families
        one_shot_evidence = $candidateEvidence.one_shot_mutation
        ack_unknown_evidence = $candidateEvidence.ack_unknown
        capability_evidence = $candidateEvidence.capability
        missing_evidence_probe = 'process_nonzero_under_15s; credentials_removed; artifact_root_absent'
        endpoint_isolation = $isolation.endpoint_isolation
        credential_namespace_isolation = $isolation.credential_namespace_isolation
        executable_sha256 = $isolation.executable_sha256
    })
}

[ordered]@{
    writer_enabled = 'bybit_okx_hyperliquid_live_mvp_only'
    shared_host_tests = 'actual venue-node --lib test suite'
    coverage = $matrix
} | ConvertTo-Json -Depth 5
