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
build_root=''
expected_revision=''
preflight_only=false

usage() {
  cat <<'USAGE'
Usage: scripts/package_venue_node_linux_release.sh --release-id <id> --output-root <absolute-path> --build-root <absolute-path> --expected-revision <40-hex> [--preflight-only]

Creates <output-root>/venue-node/<id>/ containing only the six fixed venue-node Linux binaries,
manifest.json, and SHA256SUMS. Build cache is contained in <build-root>; this script does not
start, stop, signal, or inspect a live process.
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
    --build-root)
      (($# >= 2)) || { usage >&2; exit 2; }
      build_root="$2"
      shift 2
      ;;
    --expected-revision)
      (($# >= 2)) || { usage >&2; exit 2; }
      expected_revision="$2"
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
[[ -n "$build_root" && "$build_root" = /* ]] || {
  echo 'build root must be an absolute Linux path' >&2
  exit 2
}
[[ "$output_root" != / && "$build_root" != / ]] || {
  echo 'output root and build root must not be the filesystem root' >&2
  exit 2
}
[[ "$expected_revision" =~ ^[0-9a-f]{40}$ ]] || {
  echo 'expected revision must be a lowercase 40-character Git commit' >&2
  exit 2
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
[[ ! -L "$output_root" && ! -L "$build_root" ]] || {
  echo 'output root and build root must not be symbolic links' >&2
  exit 2
}
output_root="$(realpath -m -- "$output_root")"
build_root="$(realpath -m -- "$build_root")"

paths_overlap() {
  local left="$1"
  local right="$2"
  [[ "$left" == "$right" || "$left" == "$right/"* || "$right" == "$left/"* ]]
}

if paths_overlap "$repo_root" "$output_root"; then
  echo 'output root must be outside the workspace' >&2
  exit 2
fi
if paths_overlap "$repo_root" "$build_root"; then
  echo 'build root must be outside the workspace' >&2
  exit 2
fi
if paths_overlap "$output_root" "$build_root"; then
  echo 'build root and output root must not overlap' >&2
  exit 2
fi

existing_parent() {
  local path="$1"
  while [[ ! -e "$path" ]]; do
    local parent
    parent="$(dirname "$path")"
    [[ "$parent" != "$path" ]] || break
    path="$parent"
  done
  [[ -e "$path" ]] || return 1
  printf '%s\n' "$path"
}

assert_free_space() {
  local path="$1"
  local label="$2"
  local parent
  local available
  parent="$(existing_parent "$path")" || {
    echo "cannot resolve an existing parent for $label" >&2
    return 1
  }
  available="$(df -B1 --output=avail "$parent" | tail -n 1 | tr -d '[:space:]')"
  [[ "$available" =~ ^[0-9]+$ ]] || {
    echo "cannot determine free space for $label" >&2
    return 1
  }
  if ((available < 20 * 1024 * 1024 * 1024)); then
    echo "$label requires at least 20 GiB free space" >&2
    return 1
  fi
}

assert_clean_revision() {
  local revision
  revision="$(git -C "$repo_root" rev-parse --verify HEAD)"
  [[ "$revision" == "$expected_revision" ]] || {
    echo 'workspace HEAD does not equal expected revision' >&2
    return 1
  }
  git -C "$repo_root" diff --quiet
  git -C "$repo_root" diff --cached --quiet
  [[ -z "$(git -C "$repo_root" status --porcelain --untracked-files=all)" ]] || {
    echo 'workspace must be clean before packaging' >&2
    return 1
  }
}

assert_no_target_overrides() {
  local name
  for name in CARGO_TARGET_DIR CARGO_BUILD_TARGET_DIR CARGO_BUILD_BUILD_DIR CARGO_BUILD_TARGET CARGO_TARGET; do
    if [[ ${!name+x} ]]; then
      echo "$name must be unset; the release script owns its target paths" >&2
      return 1
    fi
  done
}

assert_preflight() {
  local command
  local node_manifest="$repo_root/apps/venue-node/Cargo.toml"
  [[ -f "$repo_root/Cargo.lock" && -f "$node_manifest" ]] || {
    echo 'run this script from a complete Venue workspace' >&2
    return 1
  }
  for command in cargo rustc sha256sum git flock df mktemp find sort awk install realpath grep tr; do
    command -v "$command" >/dev/null 2>&1 || {
      echo "$command is required" >&2
      return 1
    }
  done
  [[ "$(rustc --version | awk '{ print $2 }')" == '1.98.0' ]] || {
    echo 'rustc 1.98.0 is required' >&2
    return 1
  }
  [[ "$(cargo --version | awk '{ print $2 }')" == '1.98.0' ]] || {
    echo 'cargo 1.98.0 is required' >&2
    return 1
  }
  assert_clean_revision
  assert_no_target_overrides
  assert_free_space "$build_root" 'build root'
  assert_free_space "$output_root" 'output root'

  # A retired root binary must never regain a packaging path through a future Cargo edit.
  for legacy_binary in hedged-grid-binance hedged-grid-gate hedged-grid-bitget; do
    if grep -R --fixed-strings --quiet -- "$legacy_binary" \
        "$repo_root/Cargo.toml" "$node_manifest"; then
      echo 'retired hedged-grid production binary is present in a package manifest' >&2
      return 1
    fi
    if [[ -e "$repo_root/src/bin/$legacy_binary.rs" ]]; then
      echo "retired root production binary is present: $legacy_binary" >&2
      return 1
    fi
  done
  release_parent="$output_root/venue-node"
  release_dir="$release_parent/$release_id"
  assert_existing_owned_path "$build_root" 'build root directory'
  assert_existing_owned_path "$build_root/cargo-target" 'cargo target directory'
  assert_existing_owned_path "$build_root/tmp" 'build temporary directory'
  assert_existing_owned_path "$build_root/venue-node-build.lock" 'build lock file'
  assert_existing_owned_path "$release_parent" 'release parent directory'
  [[ ! -e "$release_dir" ]] || {
    echo "release directory already exists: $release_dir" >&2
    return 1
  }
}

stage_dir=''
release_parent=''
release_dir=''

assert_existing_owned_path() {
  local path="$1"
  local kind="$2"
  if [[ ! -e "$path" && ! -L "$path" ]]; then
    return
  fi
  [[ ! -L "$path" && -O "$path" ]] || {
    echo "$kind must be owned and not a symbolic link" >&2
    return 1
  }
  if [[ "$kind" == *directory* ]]; then
    [[ -d "$path" ]] || {
      echo "$kind must be a directory" >&2
      return 1
    }
  else
    [[ -f "$path" ]] || {
      echo "$kind must be a regular file" >&2
      return 1
    }
  fi
}

cleanup_stage() {
  local canonical_parent
  local canonical_stage
  [[ -n "$stage_dir" && -d "$stage_dir" && ! -L "$stage_dir" ]] || return 0
  canonical_parent="$(realpath -e -- "$release_parent")" || return 0
  canonical_stage="$(realpath -e -- "$stage_dir")" || return 0
  [[ "$(dirname "$canonical_stage")" == "$canonical_parent" ]] || return 0
  [[ "$(basename "$canonical_stage")" == ".${release_id}.stage."* ]] || return 0
  rm -rf -- "$canonical_stage"
}

assert_preflight

if $preflight_only; then
  printf 'preflight passed: release=%s binaries=%s\n' \
    "$release_id" "${BINARY_NAMES[*]}"
  exit 0
fi

if [[ -e "$build_root" && (! -d "$build_root" || -L "$build_root" || ! -O "$build_root") ]]; then
  echo 'build root must be an owned, non-symlink directory' >&2
  exit 1
fi
mkdir -p -m 700 "$build_root"
[[ -O "$build_root" && ! -L "$build_root" ]] || {
  echo 'build root must be an owned, non-symlink directory' >&2
  exit 1
}
exec {lock_fd}>"$build_root/venue-node-build.lock"
flock -w 60 "$lock_fd" || {
  echo 'venue-node build cache is busy' >&2
  exit 1
}

# Admission can change while a sibling release is holding the shared cache.
assert_preflight

if [[ -e "$output_root" && (! -d "$output_root" || -L "$output_root" || ! -O "$output_root") ]]; then
  echo 'output root must be an owned, non-symlink directory' >&2
  exit 1
fi
mkdir -p -m 700 "$release_parent"
[[ -O "$release_parent" && ! -L "$release_parent" ]] || {
  echo 'release parent must be an owned, non-symlink directory' >&2
  exit 1
}
target_dir="$build_root/cargo-target"
tmp_dir="$build_root/tmp"
mkdir -p -m 700 "$target_dir" "$tmp_dir"
stage_dir="$(mktemp -d "$release_parent/.${release_id}.stage.XXXXXX")"
trap cleanup_stage EXIT

cd "$repo_root"
assert_clean_revision
for venue in binance bitget bybit gate hyperliquid okx; do
  binary="venue-node-$venue"
  CARGO_TARGET_DIR="$target_dir" CARGO_BUILD_JOBS=1 TMPDIR="$tmp_dir" cargo build --locked --release -p venue-node \
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
assert_clean_revision
revision="$expected_revision"
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

[[ ! -e "$release_dir" ]] || {
  echo "release directory already exists: $release_dir" >&2
  exit 1
}
mv -T -n -- "$stage_dir" "$release_dir"
[[ ! -e "$stage_dir" && -d "$release_dir" ]] || {
  echo 'stage directory did not move into a new release directory' >&2
  exit 1
}
stage_dir=''
printf 'created Linux venue-node release: %s\n' "$release_dir"
