[CmdletBinding()]
param(
    [Parameter(Mandatory)] [ValidateSet('binance', 'gate', 'bitget')] [string]$Exchange,
    [Parameter(Mandatory)] [string]$BinaryPath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$binary = (Resolve-Path -LiteralPath $BinaryPath -ErrorAction Stop).Path
if (-not (Test-Path -LiteralPath $binary -PathType Leaf)) {
    throw "固定部署二进制不存在：$binary"
}

$families = [ordered]@{
    binance = @('fapi.binance.com', 'papi.binance.com')
    gate = @('api.gateio.ws', 'fx-ws.gateio.ws')
    bitget = @('api.bitget.com', 'ws.bitget.com')
}
$content = [Text.Encoding]::GetEncoding(28591).GetString([IO.File]::ReadAllBytes($binary))

foreach ($needle in $families[$Exchange]) {
    if ($content.IndexOf($needle, [StringComparison]::Ordinal) -lt 0) {
        throw "固定部署缺少目标 $Exchange endpoint：$needle"
    }
}
foreach ($other in $families.Keys | Where-Object { $_ -ne $Exchange }) {
    foreach ($needle in $families[$other]) {
        if ($content.IndexOf($needle, [StringComparison]::Ordinal) -ge 0) {
            throw "固定部署 $Exchange 错误链接了 $other endpoint：$needle"
        }
    }
}

[ordered]@{
    exchange = $Exchange
    binary = $binary
    endpoint_isolation = $true
    executable_sha256 = (Get-FileHash -LiteralPath $binary -Algorithm SHA256).Hash.ToLowerInvariant()
} | ConvertTo-Json -Compress
