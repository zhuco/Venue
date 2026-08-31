[CmdletBinding()]
param([string]$CargoTargetDir)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'venue_build_guard.ps1')
$repoRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$venueBuildLease = Enter-VenueBuildGuard -RepoRoot $repoRoot -Slot 'slot-1' -RequestedTarget $CargoTargetDir
try {
$targetRoot = $venueBuildLease.TargetDirectory
$cargoTemp = $venueBuildLease.TempDirectory
Push-Location -LiteralPath $repoRoot
try {
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
        'HYPERLIQUID_MASTER_ADDRESS', 'HYPERLIQUID_USER_ADDRESS',
        'HYPERLIQUID_VAULT_ADDRESS', 'HYPERLIQUID_AGENT_NAME',
        'HYPERLIQUID_AGENT_ADDRESS', 'HYPERLIQUID_AGENT_PRIVATE_KEY'
    )
    okx = @('OKX_API_KEY', 'OKX_API_SECRET', 'OKX_API_PASSPHRASE')
}
$preCredentialVenues = @('bybit', 'hyperliquid', 'okx')

function Test-MissingEvidenceFailClosed {
    param(
        [Parameter(Mandatory)] [string]$Venue,
        [Parameter(Mandatory)] [string]$BinaryPath
    )

    foreach ($mode in @('TEST', 'LIVE')) {
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
        if (-not $process.WaitForExit(15000)) {
            $process.Kill($true)
            throw "$Venue $mode 缺证据探针未在 15 秒内失败关闭。"
        }
        if ($process.ExitCode -eq 0) {
            throw "$Venue $mode 在缺少共享证据时错误地成功启动。"
        }
        if (Test-Path -LiteralPath $probeBase) {
            throw "$Venue $mode 在失败关闭前创建了工件路径：$probeBase"
        }

        $output = "$stdout`n$stderr"
        if ($Venue -in $preCredentialVenues) {
            foreach ($marker in @(
                'Owner', 'WAL', 'unique account writer fence', 'signed readback',
                'UNKNOWN reconciliation', 'Stop/Flatten', 'operator-confirmed Canary evidence'
            )) {
                if (-not $output.Contains($marker, [StringComparison]::Ordinal)) {
                    throw "$Venue $mode 失败关闭输出缺少共享证据标记：$marker"
                }
            }
        } elseif (-not $output.Contains(
            'runtime arguments must select exactly one fixed deployment command',
            [StringComparison]::Ordinal
        )) {
            throw "$Venue $mode 未在 legacy runtime 参数解析前失败关闭。"
        }
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
                order_families = 'not_reached; precredential_fail_closed'
                one_shot_mutation = 'not_reached; precredential_fail_closed'
                ack_unknown = 'not_reached; precredential_fail_closed'
                capability = 'binary_test_empty'
            }
        }
        'hyperliquid' {
            [ordered]@{
                order_families = 'not_reached; shared_integration_fail_closed'
                one_shot_mutation = 'not_reached; shared_integration_fail_closed'
                ack_unknown = 'not_reached; shared_integration_fail_closed'
                capability = 'binary_test_empty'
            }
        }
        'okx' {
            [ordered]@{
                order_families = 'not_reached; no_post_recovery_collector'
                one_shot_mutation = 'not_reached; no_post_recovery_collector'
                ack_unknown = 'not_reached; no_post_recovery_collector'
                capability = 'fixed_binary_adapter_flags_empty; candidate_bridge_not_constructed'
            }
        }
    }
    $matrix.Add([ordered]@{
        venue = $venue
        test_live_binding = 'integration_test_exact_and_disjoint'
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
    writer_enabled = $false
    shared_host_tests = 'actual venue-node --lib test suite'
    coverage = $matrix
} | ConvertTo-Json -Depth 5
} finally { Pop-Location }
} finally { Exit-VenueBuildGuard $venueBuildLease }
