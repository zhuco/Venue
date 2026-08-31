#!/usr/bin/env bash
# Build a reviewable Linux release directory only.  It never reads credentials, copies runtime
# artifacts, or operates a service/process; deployment and signed preflight are separate steps.
set -euo pipefail
umask 077

readonly BINARY_NAMES=(
  venue-node-binance
  venue-node-bitget
  venue-node-bybit
  venue-node-gate
  venue-node-hyperliquid
  venue-node-okx
)

release_id=''
output_root=''
preflight_only=false

usage() {
  cat <<'USAGE'
Usage: scripts/package_venue_node_linux_release.sh --release-id <id> --output-root <absolute-path> [--preflight-only]

Creates <output-root>/venue-node/<id>/ containing only the six fixed venue-node Linux binaries,
manifest.json, and SHA256SUMS.  It does not start, stop, signal, or inspect a live process.
USAGE
}

while (($# > 0)); do
  case "$1" in
    --release-id)
      (($# >= 2)) || { usage >&2; exit 2; }
      release_id="$2"
      shift 2
      ;;
    --output-root)
      (($# >= 2)) || { usage >&2; exit 2; }
      output_root="$2"
      shift 2
      ;;
    --preflight-only)
      preflight_only=true
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
done

[[ "$release_id" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$ ]] || {
  echo 'release id must be 1-64 ASCII alphanumeric, dot, underscore, or dash characters' >&2
  exit 2
}
[[ -n "$output_root" && "$output_root" = /* ]] || {
  echo 'output root must be an absolute Linux path' >&2
  exit 2
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
output_root="$(realpath -m -- "$output_root")"
if [[ "$output_root" == "$repo_root" || "$output_root" == "$repo_root/"* ]]; then
  echo 'output root must be outside the workspace' >&2
  exit 2
fi
node_manifest="$repo_root/apps/venue-node/Cargo.toml"
[[ -f "$repo_root/Cargo.lock" && -f "$node_manifest" ]] || {
  echo 'run this script from a complete Venue workspace' >&2
  exit 1
}
command -v cargo >/dev/null 2>&1 || { echo 'cargo is required' >&2; exit 1; }
command -v sha256sum >/dev/null 2>&1 || { echo 'sha256sum is required' >&2; exit 1; }

# A retired root binary must never regain a packaging path through a future Cargo edit.
if grep -R --fixed-strings --quiet -- 'hedged-grid-binance' \
    "$repo_root/Cargo.toml" "$node_manifest"; then
  echo 'retired hedged-grid production binary is present in a package manifest' >&2
  exit 1
fi
for legacy_binary in hedged-grid-binance hedged-grid-gate hedged-grid-bitget; do
  if [[ -e "$repo_root/src/bin/$legacy_binary.rs" ]]; then
    echo "retired root production binary is present: $legacy_binary" >&2
    exit 1
  fi
done

release_parent="$output_root/venue-node"
release_dir="$release_parent/$release_id"
if [[ -e "$release_dir" ]]; then
  echo "release directory already exists: $release_dir" >&2
  exit 1
fi

if $preflight_only; then
  printf 'preflight passed: release=%s binaries=%s\n' \
    "$release_id" "${BINARY_NAMES[*]}"
  exit 0
fi

mkdir -p "$release_parent"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/venue-node-release.XXXXXX")"
stage_dir="$(mktemp -d "$release_parent/.${release_id}.stage.XXXXXX")"
cleanup() {
  rm -rf -- "$work_dir"
  if [[ -d "$stage_dir" ]]; then
    rm -rf -- "$stage_dir"
  fi
}
trap cleanup EXIT

target_dir="$work_dir/cargo-target"
for venue in binance bitget bybit gate hyperliquid okx; do
  binary="venue-node-$venue"
  CARGO_TARGET_DIR="$target_dir" cargo build --locked --release -p venue-node \
    --no-default-features --features "$venue" --bin "$binary"
  install -m 0755 "$target_dir/release/$binary" "$stage_dir/$binary"
done

mapfile -t staged < <(find "$stage_dir" -maxdepth 1 -type f -printf '%f\n' | LC_ALL=C sort)
expected="$(printf '%s\n' "${BINARY_NAMES[@]}" | LC_ALL=C sort)"
actual="$(printf '%s\n' "${staged[@]}")"
[[ "$actual" == "$expected" ]] || {
  echo 'release staging contains an unexpected or missing binary' >&2
  exit 1
}

(
  cd "$stage_dir"
  sha256sum "${BINARY_NAMES[@]}" > SHA256SUMS
)
revision="$(git -C "$repo_root" rev-parse --verify HEAD)"
{
  printf '{\n'
  printf '  "release_id": "%s",\n' "$release_id"
  printf '  "git_revision": "%s",\n' "$revision"
  printf '  "platform": "linux",\n'
  printf '  "binaries": [\n'
  for index in "${!BINARY_NAMES[@]}"; do
    binary="${BINARY_NAMES[$index]}"
    digest="$(awk -v name="$binary" '$2 == name { print $1 }' "$stage_dir/SHA256SUMS")"
    separator=','
    if ((index + 1 == ${#BINARY_NAMES[@]})); then
      separator=''
    fi
    printf '    {"name":"%s","sha256":"%s"}%s\n' "$binary" "$digest" "$separator"
  done
  printf '  ]\n'
  printf '}\n'
} > "$stage_dir/manifest.json"

# The release is deliberately a flat allow-list.  Credentials, artifacts, configs, logs, and
# retired binaries cannot enter through a recursive copy.
mapfile -t final_files < <(find "$stage_dir" -maxdepth 1 -type f -printf '%f\n' | LC_ALL=C sort)
expected_files="$(printf '%s\n' "${BINARY_NAMES[@]}" SHA256SUMS manifest.json | LC_ALL=C sort)"
actual_files="$(printf '%s\n' "${final_files[@]}")"
[[ "$actual_files" == "$expected_files" ]] || {
  echo 'release contains a non-allow-listed file' >&2
  exit 1
}

mv -- "$stage_dir" "$release_dir"
stage_dir=''
printf 'created Linux venue-node release: %s\n' "$release_dir"
