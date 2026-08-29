[CmdletBinding()]
param([string]$CargoTargetDir)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$expectedBaseline = 'af54c157400ff819c0027c06cd96c6fcf6e101c8'
$repoRoot = [System.IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$allowedPaths = @(
    'apps/venue-node/tests/gateway_candidate_conformance.rs',
    'scripts/verify_gateway_candidate_contract.ps1'
)

function Invoke-GitLines {
    param([Parameter(Mandatory)] [string[]]$Arguments)

    $lines = & git.exe -C $repoRoot @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "git $($Arguments -join ' ') 失败，退出码 $LASTEXITCODE"
    }
    @($lines)
}

Invoke-GitLines -Arguments @('cat-file', '-e', "$expectedBaseline`^{commit}") | Out-Null
$mergeBase = (Invoke-GitLines -Arguments @('merge-base', $expectedBaseline, 'HEAD') | Select-Object -First 1)
if ($mergeBase -ne $expectedBaseline) {
    throw "当前 HEAD 不是 Goal41 精确基线 $expectedBaseline 的后代。"
}

$changed = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
foreach ($path in Invoke-GitLines -Arguments @('diff', '--name-only', $expectedBaseline, '--')) {
    if ($path) {
        [void]$changed.Add($path.Replace('\', '/'))
    }
}
foreach ($path in Invoke-GitLines -Arguments @('ls-files', '--others', '--exclude-standard')) {
    if ($path) {
        [void]$changed.Add($path.Replace('\', '/'))
    }
}
foreach ($path in $changed) {
    if ($path -notin $allowedPaths) {
        throw "Goal41 基线之后出现租约外路径：$path"
    }
}
foreach ($path in $allowedPaths) {
    if (-not (Test-Path -LiteralPath (Join-Path $repoRoot $path) -PathType Leaf)) {
        throw "Goal41 租约路径缺失：$path"
    }
}

if (-not $CargoTargetDir) {
    $CargoTargetDir = Join-Path ([System.IO.Path]::GetTempPath()) 'venue-goal41-gateway-contract-target'
}
$targetRoot = [System.IO.Path]::GetFullPath($CargoTargetDir)
if ($targetRoot.Equals($repoRoot, [StringComparison]::OrdinalIgnoreCase) -or
    $targetRoot.StartsWith(
        $repoRoot + [System.IO.Path]::DirectorySeparatorChar,
        [StringComparison]::OrdinalIgnoreCase
    )) {
    throw 'Goal41 专项构建目录必须位于 worktree 外。'
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
            "venue-goal41-no-io-$Venue-$mode-$([Guid]::NewGuid().ToString('N'))"
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

    $candidateLevel = if ($venue -in @('binance', 'bitget', 'gate')) {
        'native_candidate_behavior'
    } else {
        'precredential_fail_closed'
    }
    $matrix.Add([ordered]@{
        venue = $venue
        candidate_level = $candidateLevel
        test_live_binding_isolation = $true
        three_order_families = if ($candidateLevel -eq 'native_candidate_behavior') {
            'complete_or_profile_unsupported'
        } else {
            'readback_unreachable_without_complete_evidence'
        }
        one_shot_mutation = if ($candidateLevel -eq 'native_candidate_behavior') {
            'candidate_and_shared_host_regression'
        } else {
            'mutation_unreachable'
        }
        ack_unknown_no_resubmit = $true
        capability_empty = $true
        missing_evidence_zero_artifact_io = $true
        endpoint_isolation = $isolation.endpoint_isolation
        credential_namespace_isolation = $isolation.credential_namespace_isolation
        executable_sha256 = $isolation.executable_sha256
    })
}

[ordered]@{
    baseline = $expectedBaseline
    writer_enabled = $false
    coverage = $matrix
} | ConvertTo-Json -Depth 5
