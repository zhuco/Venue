[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
# Match the current workspace source budget; recovery artifacts remain excluded.
# This is a source-only budget; single-file and generated/secret exclusions stay unchanged.
$maxTrackedBytes = 20MB
$maxSingleFileBytes = 2MB
$forbiddenRoots = @(
    "bak/",
    "target/",
    "target-",
    ".codex-target-",
    ".codex-local-toolchain/",
    "releases/",
    "content-releases/",
    "handoff-staging/",
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
$protectedArtifactRoots = @("artifacts/")

$tracked = @(git ls-files)
if ($LASTEXITCODE -ne 0) {
    throw "git ls-files failed"
}

$violations = [System.Collections.Generic.List[string]]::new()
$totalBytes = [int64]0
foreach ($relativePath in $tracked) {
    $normalized = $relativePath.Replace("\", "/")
    if ($normalized.StartsWith("bak/", [StringComparison]::OrdinalIgnoreCase)) {
        # `bak` is frozen legacy. Report its presence without reading its contents or metadata.
        $violations.Add("frozen legacy path is tracked: $normalized")
        continue
    }
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

$changedPaths = @(
    & git diff --name-status --no-ext-diff -- . ':!bak/**'
    if ($LASTEXITCODE -ne 0) { throw "git diff failed" }
    & git diff --cached --name-status --no-ext-diff -- . ':!bak/**'
    if ($LASTEXITCODE -ne 0) { throw "git diff --cached failed" }
)
foreach ($change in $changedPaths) {
    if ($change -match '^(?<status>[A-Z]+)\s+(?<path>.+)$') {
        $path = $Matches.path.Replace("\", "/")
        if ($Matches.status.StartsWith("D") -and $protectedArtifactRoots | Where-Object {
                $path.StartsWith($_, [StringComparison]::OrdinalIgnoreCase)
            }) {
            $violations.Add("protected runtime artifact deletion is forbidden: $path")
        }
    }
}

if ($totalBytes -gt $maxTrackedBytes) {
    $violations.Add("tracked worktree exceeds 13 MiB: $totalBytes bytes")
}
if ($violations.Count -ne 0) {
    $violations | ForEach-Object { Write-Error $_ }
    exit 1
}

Write-Output "repository hygiene verified: $($tracked.Count) files, $totalBytes bytes"
