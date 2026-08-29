[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$maxTrackedBytes = 10MB
$maxSingleFileBytes = 2MB
$forbiddenRoots = @(
    "bak/",
    "target/",
    "target-",
    ".codex-target-",
    ".codex-local-toolchain/",
    "releases/",
    "artifacts/",
    ".secrets/"
)
$forbiddenFiles = @(".env", "venue.local.toml")
$forbiddenExtensions = @(
    ".7z",
    ".db",
    ".dll",
    ".dylib",
    ".exe",
    ".gz",
    ".jsonl",
    ".key",
    ".log",
    ".pdb",
    ".pem",
    ".rlib",
    ".rmeta",
    ".so",
    ".sqlite",
    ".tar",
    ".zip"
)

$tracked = @(git ls-files)
if ($LASTEXITCODE -ne 0) {
    throw "git ls-files failed"
}

$violations = [System.Collections.Generic.List[string]]::new()
$totalBytes = [int64]0
foreach ($relativePath in $tracked) {
    $normalized = $relativePath.Replace("\", "/")
    if ($forbiddenFiles -contains $normalized) {
        $violations.Add("secret/local file is tracked: $normalized")
    }
    foreach ($root in $forbiddenRoots) {
        if ($normalized.StartsWith($root, [StringComparison]::OrdinalIgnoreCase)) {
            $violations.Add("runtime/build directory is tracked: $normalized")
            break
        }
    }
    $extension = [IO.Path]::GetExtension($normalized).ToLowerInvariant()
    if ($forbiddenExtensions -contains $extension) {
        $violations.Add("generated, binary, runtime, or secret extension is tracked: $normalized")
    }
    if (-not (Test-Path -LiteralPath $relativePath -PathType Leaf)) {
        $violations.Add("tracked path is missing from the worktree: $normalized")
        continue
    }
    $bytes = (Get-Item -LiteralPath $relativePath).Length
    $totalBytes += $bytes
    if ($bytes -gt $maxSingleFileBytes) {
        $violations.Add("tracked file exceeds 2 MiB: $normalized ($bytes bytes)")
    }
}

if ($totalBytes -gt $maxTrackedBytes) {
    $violations.Add("tracked worktree exceeds 10 MiB: $totalBytes bytes")
}
if ($violations.Count -ne 0) {
    $violations | ForEach-Object { Write-Error $_ }
    exit 1
}

Write-Output "repository hygiene verified: $($tracked.Count) files, $totalBytes bytes"
