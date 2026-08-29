[CmdletBinding()]
param(
    [Parameter(Mandatory)] [ValidateSet('binance', 'gate', 'bitget')] [string]$Exchange,
    [Parameter(Mandatory)] [ValidatePattern('^[A-Z0-9]+/[A-Z0-9]+$')] [string]$Symbol,
    [Parameter(Mandatory)] [string]$ConfigPath,
    [Parameter(Mandatory)] [ValidatePattern('^[0-9a-fA-F]{64}$')] [string]$CanonicalRootSha256,
    [ValidatePattern('^[0-9a-fA-F]{64}$')] [string]$PredecessorCanonicalRootSha256,
    [Parameter(Mandatory)] [ValidatePattern('^[0-9a-fA-F]{64}$')] [string]$PredecessorExecutableSha256,
    [Parameter(Mandatory)] [ValidatePattern('^[0-9a-fA-F]{64}$')] [string]$PredecessorAdmissionSha256,
    [string]$BinaryPath,
    [string]$TargetTriple,
    [string]$OutputRoot
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
if (-not $OutputRoot) { $OutputRoot = Join-Path $repoRoot 'releases' }
$outputRootFull = [System.IO.Path]::GetFullPath($OutputRoot)
$artifactsRootFull = [System.IO.Path]::GetFullPath((Join-Path $repoRoot 'artifacts'))
if ($outputRootFull.Equals($artifactsRootFull, [StringComparison]::OrdinalIgnoreCase) -or
    $outputRootFull.StartsWith($artifactsRootFull + [System.IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
    throw '发布输出目录不得位于 live/recovery artifacts 下。'
}

function Invoke-CheckedProgram {
    param(
        [Parameter(Mandatory)] [string]$FilePath,
        [Parameter(Mandatory)] [string[]]$Arguments
    )
    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$FilePath 失败，退出码 $LASTEXITCODE" }
}

function Resolve-ExistingFile {
    param([Parameter(Mandatory)] [string]$Path, [Parameter(Mandatory)] [string]$Label)
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { throw "缺少${Label}：$Path" }
    return (Resolve-Path -LiteralPath $Path).Path
}

function Assert-NoCredentialMaterial {
    param([Parameter(Mandatory)] [string]$Path)
    $leaf = [System.IO.Path]::GetFileName($Path)
    if ($leaf -match '^\.env(?:\.|$)' -or $leaf -match '(?i)(secret|credential|private[-_]?key)') {
        throw "拒绝把凭证文件加入发布包：$Path"
    }
}

$config = Resolve-ExistingFile -Path $ConfigPath -Label '配置文件'
Assert-NoCredentialMaterial -Path $config
$configText = Get-Content -LiteralPath $config -Raw
$symbolMatch = [regex]::Match($configText, '(?m)^\s*symbol\s*=\s*["''](?<value>[A-Za-z0-9]+/[A-Za-z0-9]+)["'']\s*(?:#.*)?$')
if (-not $symbolMatch.Success -or $symbolMatch.Groups['value'].Value.ToUpperInvariant() -ne $Symbol) {
    throw "配置交易对与发布绑定不一致：expected=$Symbol"
}
$configuredExchanges = @(@('binance', 'gate', 'bitget') | Where-Object {
    [regex]::IsMatch($configText, "(?m)^\s*\[$_\]\s*(?:#.*)?$")
})
if ($configuredExchanges.Count -ne 1 -or $configuredExchanges[0] -ne $Exchange) {
    throw "配置交易所与发布绑定不一致：expected=$Exchange actual=$($configuredExchanges -join ',')"
}

if (-not $BinaryPath) {
    $binaryTarget = "hedged-grid-$Exchange"
    $metadataRaw = & cargo.exe metadata --no-deps --format-version 1
    if ($LASTEXITCODE -ne 0) { throw "cargo metadata 失败，退出码 $LASTEXITCODE" }
    $targetRoot = ($metadataRaw | ConvertFrom-Json).target_directory
    $isNonWindowsTarget = $TargetTriple -and $TargetTriple -notmatch 'windows'
    $buildArguments = @(
        $(if ($isNonWindowsTarget) { 'zigbuild' } else { 'build' }),
        '--locked', '--release', '--bin', $binaryTarget,
        '--no-default-features', '--features', $binaryTarget
    )
    if ($TargetTriple) { $buildArguments += @('--target', $TargetTriple) }
    Invoke-CheckedProgram -FilePath 'cargo.exe' -Arguments $buildArguments

    $targetDirectoryTriple = if ($TargetTriple) {
        $TargetTriple -replace '\.\d+\.\d+$', ''
    }
    $profileRoot = if ($targetDirectoryTriple) {
        Join-Path (Join-Path $targetRoot $targetDirectoryTriple) 'release'
    }
    else {
        Join-Path $targetRoot 'release'
    }
    $candidateNames = if ($TargetTriple -and $TargetTriple -notmatch 'windows') {
        @($binaryTarget)
    }
    else {
        @("$binaryTarget.exe", $binaryTarget)
    }
    foreach ($name in $candidateNames) {
        $candidate = Join-Path $profileRoot $name
        if (Test-Path -LiteralPath $candidate -PathType Leaf) { $BinaryPath = $candidate; break }
    }
    if (-not $BinaryPath) { throw "构建完成但未找到 $binaryTarget 二进制：$profileRoot" }
}

$binary = Resolve-ExistingFile -Path $BinaryPath -Label '二进制'
Assert-NoCredentialMaterial -Path $binary
$isolationVerifier = Join-Path $PSScriptRoot 'verify_hedged_grid_binary_isolation.ps1'
$isolationEvidence = & $isolationVerifier -Exchange $Exchange -BinaryPath $binary | ConvertFrom-Json
if (-not $isolationEvidence.endpoint_isolation) {
    throw '固定部署 endpoint 隔离验证失败。'
}
$binaryHash = (Get-FileHash -LiteralPath $binary -Algorithm SHA256).Hash.ToLowerInvariant()
$configHash = (Get-FileHash -LiteralPath $config -Algorithm SHA256).Hash.ToLowerInvariant()
$rootHash = $CanonicalRootSha256.ToLowerInvariant()
$predecessorRootHash = if ($PredecessorCanonicalRootSha256) {
    $PredecessorCanonicalRootSha256.ToLowerInvariant()
} else {
    $null
}
$isRelocation = $predecessorRootHash -and $predecessorRootHash -ne $rootHash
$predecessorHash = $PredecessorExecutableSha256.ToLowerInvariant()
$admissionHash = $PredecessorAdmissionSha256.ToLowerInvariant()

if ($binaryHash -eq $predecessorHash) {
    throw 'successor 与 predecessor 二进制摘要相同，不能形成内容寻址 handoff。'
}

$releaseId = "$Exchange-$($Symbol.Replace('/', '-').ToLowerInvariant())-$($binaryHash.Substring(0, 16))"
$releaseRoot = Join-Path $outputRootFull $releaseId
if (Test-Path -LiteralPath $releaseRoot) {
    throw "发布目录已存在；为避免覆盖证据，本脚本拒绝复用：$releaseRoot"
}
New-Item -ItemType Directory -Path $releaseRoot | Out-Null

try {
    $binaryLeaf = [System.IO.Path]::GetFileName($binary)
    $configLeaf = [System.IO.Path]::GetFileName($config)
    Copy-Item -LiteralPath $binary -Destination (Join-Path $releaseRoot $binaryLeaf)
    Copy-Item -LiteralPath $config -Destination (Join-Path $releaseRoot $configLeaf)

    $commit = (& git.exe -C $repoRoot rev-parse HEAD 2>$null)
    if ($LASTEXITCODE -ne 0) { $commit = $null }
    elseif ($commit) { $commit = "$commit".Trim() }
    $dirty = $null
    if ($commit) {
        $dirty = @(& git.exe -C $repoRoot status --porcelain=v1 --untracked-files=normal).Count -gt 0
        if ($LASTEXITCODE -ne 0) { throw 'git status 无法生成可复核的源码状态。' }
    }

    $metadata = [ordered]@{
        schema_version = 1
        release_id = $releaseId
        exchange = $Exchange
        symbol = $Symbol
        executable_file = $binaryLeaf
        executable_sha256 = $binaryHash
        config_file = $configLeaf
        config_sha256 = $configHash
        target_triple = if ($TargetTriple) { $TargetTriple } else { 'host' }
        source_commit = $commit
        source_dirty = $dirty
        contains_credentials = $false
        contains_artifacts = $false
        adapter_endpoint_isolation = $true
    }
    $handoffInput = [ordered]@{
        schema_version = if ($isRelocation) { 2 } else { 1 }
        authorization = if ($isRelocation) {
            'preserve-hedge-positions-cross-host-relocation-v1'
        } else {
            'preserve-hedge-positions-cancel-owned-orders-only'
        }
        exchange = $Exchange
        symbol = $Symbol
        canonical_root_sha256 = $rootHash
        predecessor_executable_sha256 = $predecessorHash
        successor_executable_sha256 = $binaryHash
        predecessor_admission_sha256 = $admissionHash
        authorized_at_ms = $null
        valid_until_ms = $null
        note = 'Set both timestamps at the handoff window; this input is intentionally not an executable manifest.'
    }
    if ($isRelocation) {
        $handoffInput.Insert(5, 'predecessor_canonical_root_sha256', $predecessorRootHash)
    }

    $metadata | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $releaseRoot 'release-metadata.json') -Encoding utf8NoBOM
    $handoffInput | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $releaseRoot 'handoff-manifest-input.json') -Encoding utf8NoBOM

    $stagedBinaryHash = (Get-FileHash -LiteralPath (Join-Path $releaseRoot $binaryLeaf) -Algorithm SHA256).Hash.ToLowerInvariant()
    $stagedConfigHash = (Get-FileHash -LiteralPath (Join-Path $releaseRoot $configLeaf) -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($stagedBinaryHash -ne $binaryHash -or $stagedConfigHash -ne $configHash) {
        throw '发布目录复核摘要与源文件不一致。'
    }

    [ordered]@{
        release_root = $releaseRoot
        executable_sha256 = $binaryHash
        config_sha256 = $configHash
        handoff_input = (Join-Path $releaseRoot 'handoff-manifest-input.json')
    } | ConvertTo-Json -Depth 3
}
catch {
    if (Test-Path -LiteralPath $releaseRoot) {
        Remove-Item -LiteralPath $releaseRoot -Recurse -Force
    }
    throw
}
