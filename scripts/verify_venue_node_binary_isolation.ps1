[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet('binance', 'bitget', 'bybit', 'gate', 'hyperliquid', 'okx')]
    [string]$Venue,
    [Parameter(Mandatory)] [string]$BinaryPath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$binary = (Resolve-Path -LiteralPath $BinaryPath -ErrorAction Stop).Path
if (-not (Test-Path -LiteralPath $binary -PathType Leaf)) {
    throw "固定节点二进制不存在：$binary"
}

$families = [ordered]@{
    binance = [ordered]@{
        endpoints = @(
            'papi.binance.com',
            'fapi.binance.com',
            'fstream.binance.com'
        )
        credentials = @('BINANCE_API_KEY', 'BINANCE_API_SECRET')
        binding = @('portfolio_margin_um')
    }
    bitget = [ordered]@{
        endpoints = @('api.bitget.com', 'ws.bitget.com')
        credentials = @(
            'BITGET_API_KEY',
            'BITGET_API_SECRET',
            'BITGET_API_PASSPHRASE',
            'BITGET_PASSPHRASE'
        )
        binding = @('uta_usdt_futures_hedge')
    }
    bybit = [ordered]@{
        endpoints = @(
            'api.bybit.com',
            'stream.bybit.com'
        )
        credentials = @('BYBIT_API_KEY', 'BYBIT_API_SECRET')
        binding = @('uta2_linear')
    }
    gate = [ordered]@{
        endpoints = @(
            'api.gateio.ws',
            'fx-ws.gateio.ws'
        )
        credentials = @('GATEIO_API_KEY', 'GATEIO_API_SECRET')
        binding = @('usdt_futures_dual')
    }
    hyperliquid = [ordered]@{
        endpoints = @('api.hyperliquid.xyz')
        credentials = @(
            'HYPERLIQUID_ACCOUNT_ADDRESS',
            'HYPERLIQUID_API_WALLET_ADDRESS',
            'HYPERLIQUID_API_WALLET_PRIVATE_KEY',
            'HYPERLIQUID_VAULT_ADDRESS'
        )
        binding = @('usdc_perpetual_api_wallet')
    }
    okx = [ordered]@{
        endpoints = @('www.okx.com', 'ws.okx.com')
        credentials = @('OKX_API_KEY', 'OKX_API_SECRET', 'OKX_API_PASSPHRASE')
        binding = @('linear_swap')
    }
}

$content = [Text.Encoding]::GetEncoding(28591).GetString([IO.File]::ReadAllBytes($binary))
$selected = $families[$Venue]
$forbiddenNonProductionMarkers = @(
    'testnet',
    'sandbox',
    'paper_trading',
    'simulated-trading',
    'testnet.binancefuture.com',
    'wspap.bitget.com',
    'paptrading',
    'api-testnet.bybit.com',
    'stream-testnet.bybit.com',
    'api-testnet.gateapi.io',
    'ws-testnet.gate.com',
    'api.hyperliquid-testnet.xyz',
    'wspap.okx.com',
    'x-simulated-trading'
)
# Do not scan the bare token `demo`: case-insensitive binary search also matches Rust identifiers
# such as `TradeMode`. Exact demo/test endpoints and headers above are the deployable evidence.
foreach ($needle in $forbiddenNonProductionMarkers) {
    if ($content.IndexOf($needle, [StringComparison]::OrdinalIgnoreCase) -ge 0) {
        throw "固定节点 $Venue 包含非生产 endpoint/header 标记：$needle"
    }
}
foreach ($category in @('endpoints', 'credentials', 'binding')) {
    foreach ($needle in $selected[$category]) {
        if ($content.IndexOf($needle, [StringComparison]::Ordinal) -lt 0) {
            throw "固定节点缺少目标 $Venue $category 标记：$needle"
        }
    }
}

foreach ($other in $families.Keys | Where-Object { $_ -ne $Venue }) {
    # The legacy root configuration parser contains all three existing binding enum spellings so
    # it can reject a mismatched config. Foreign endpoint or credential markers, unlike those
    # parser spellings, prove that another physical adapter boundary leaked into the artifact.
    foreach ($category in @('endpoints', 'credentials')) {
        foreach ($needle in $families[$other][$category]) {
            if ($content.IndexOf($needle, [StringComparison]::Ordinal) -ge 0) {
                throw "固定节点 $Venue 错误链接了 $other $category 标记：$needle"
            }
        }
    }
}

[ordered]@{
    venue = $Venue
    binary = $binary
    endpoint_isolation = $true
    credential_namespace_isolation = $true
    binding_isolation = $true
    executable_sha256 = (Get-FileHash -LiteralPath $binary -Algorithm SHA256).Hash.ToLowerInvariant()
} | ConvertTo-Json -Compress
