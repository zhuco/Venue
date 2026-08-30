[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$databaseUrl = $env:VENUE_CONTROL_TEST_DATABASE_URL
if ([string]::IsNullOrWhiteSpace($databaseUrl)) {
    throw 'VENUE_CONTROL_TEST_DATABASE_URL is required for the PostgreSQL integration gate.'
}

try {
    $databaseUri = [Uri]$databaseUrl
}
catch {
    throw 'VENUE_CONTROL_TEST_DATABASE_URL must be an absolute PostgreSQL connection URI.'
}

if ($databaseUri.Scheme -notin @('postgres', 'postgresql') -or [string]::IsNullOrWhiteSpace($databaseUri.Host)) {
    throw 'VENUE_CONTROL_TEST_DATABASE_URL must be an absolute PostgreSQL connection URI.'
}

$env:VENUE_CONTROL_POSTGRES_REQUIRED = '1'
Write-Output 'PostgreSQL integration gate: configured database connection (connection string redacted).'

function Invoke-PostgresIntegrationTest {
    param([Parameter(Mandatory)] [string]$TestTarget)

    $output = @(& cargo test --locked -p venue-control --test $TestTarget 2>&1)
    $exitCode = $LASTEXITCODE
    $output | ForEach-Object { Write-Output $_ }
    if ($exitCode -ne 0) {
        throw "PostgreSQL integration test $TestTarget failed with exit code $exitCode"
    }
    if ($output | Select-String -SimpleMatch 'SKIP:') {
        throw "PostgreSQL integration test $TestTarget reported a skipped test"
    }
}

Invoke-PostgresIntegrationTest -TestTarget 'account_delivery_postgres_integration'
Invoke-PostgresIntegrationTest -TestTarget 'copy_postgres_integration'

Write-Output 'PostgreSQL integration gate passed: delivery and Copy tests connected to the test database.'
