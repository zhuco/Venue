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
            'testnet.binancefuture.com',
            'papi.binance.com',
            'fapi.binance.com',
            'fstream.binance.com'
        )
        credentials = @('BINANCE_API_KEY', 'BINANCE_API_SECRET')
        binding = @('portfolio_margin_um')
    }
    bitget = [ordered]@{
        endpoints = @('api.bitget.com', 'wspap.bitget.com', 'ws.bitget.com')
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
            'api-testnet.bybit.com',
            'api.bybit.com',
            'stream-testnet.bybit.com',
            'stream.bybit.com'
        )
        credentials = @('BYBIT_API_KEY', 'BYBIT_API_SECRET')
        binding = @('uta2_linear')
    }
    gate = [ordered]@{
        endpoints = @(
            'api-testnet.gateapi.io',
            'api.gateio.ws',
            'ws-testnet.gate.com',
            'fx-ws.gateio.ws'
        )
        credentials = @('GATEIO_API_KEY', 'GATEIO_API_SECRET')
        binding = @('usdt_futures_dual')
    }
    hyperliquid = [ordered]@{
        endpoints = @('api.hyperliquid-testnet.xyz', 'api.hyperliquid.xyz')
        credentials = @(
            'HYPERLIQUID_MASTER_ADDRESS',
            'HYPERLIQUID_USER_ADDRESS',
            'HYPERLIQUID_VAULT_ADDRESS',
            'HYPERLIQUID_AGENT_NAME',
            'HYPERLIQUID_AGENT_ADDRESS',
            'HYPERLIQUID_AGENT_PRIVATE_KEY'
        )
        binding = @('usdc_perpetual_agent')
    }
    okx = [ordered]@{
        endpoints = @('www.okx.com', 'wspap.okx.com', 'ws.okx.com')
        credentials = @('OKX_API_KEY', 'OKX_API_SECRET', 'OKX_API_PASSPHRASE')
        binding = @('linear_swap')
    }
}

$content = [Text.Encoding]::GetEncoding(28591).GetString([IO.File]::ReadAllBytes($binary))
$selected = $families[$Venue]
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
