#!/usr/bin/env bash
set -euo pipefail

case "$(basename "$0")" in
  rustc)
    echo 'rustc 1.98.0 (fixture)'
    ;;
  cargo)
    if [[ "${1:-}" == '--version' ]]; then
      echo 'cargo 1.98.0 (fixture)'
      exit 0
    fi
    binary=''
    while (($# > 0)); do
      if [[ "$1" == '--bin' ]]; then
        binary="$2"
        break
      fi
      shift
    done
    [[ -n "$binary" && -n "${CARGO_TARGET_DIR:-}" ]]
    mkdir -p "$CARGO_TARGET_DIR/release"
    printf 'fixture %s\n' "$binary" > "$CARGO_TARGET_DIR/release/$binary"
    printf '%s|%s\n' "$PWD" "$CARGO_TARGET_DIR" >> "$FAKE_CARGO_LOG"
    if [[ -n "${FAKE_RACE_RELEASE_DIR:-}" && "$binary" == 'venue-node-binance' ]]; then
      mkdir -p "$FAKE_RACE_RELEASE_DIR"
      printf 'external release\n' > "$FAKE_RACE_RELEASE_DIR/racer"
    fi
    if [[ "${FAKE_MUTATE_REPOSITORY:-}" == '1' && "$binary" == 'venue-node-binance' ]]; then
      git -C "$FAKE_REPOSITORY" commit --allow-empty -qm 'fixture revision changed'
    fi
    ;;
  flock)
    # The Windows fixture only verifies packaging orchestration; Linux flock semantics are
    # checked by the remote preflight before any real build.
    exit 0
    ;;
  *)
    exit 64
    ;;
esac
