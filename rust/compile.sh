#!/usr/bin/env bash
# Compile all Rust DevStats binaries (release, stripped).
#
# Works on Linux and FreeBSD. Because the checkout may be shared between the two
# (bhyve VM + host), every OS builds into its own directory:
#   target/<os>/release/<name>      (<os> = linux | freebsd | ..., from uname -s)
# Override with CARGO_TARGET_DIR if you want another location.
#
# Usage: ./compile.sh [--debug] [--offline]
#   --debug    build the dev profile instead of release
#   --offline  pass --offline to cargo (no network for crates.io)
# Env:
#   BINDIR     if set, copy the resulting binaries there (like `make install BINDIR=...`)
#   DEVSTATS_BUILD_STAMP / DEVSTATS_GIT_HASH / DEVSTATS_HOST_NAME / DEVSTATS_RUST_VERSION
#              build information compiled into the binaries (defaults computed like the Go Makefile)
set -euo pipefail
cd "$(dirname "$0")"
# shellcheck source=./env.sh
. ./env.sh

profile=release
cargo_flags=(--workspace)
for arg in "$@"; do
  case "$arg" in
    --debug) profile=debug ;;
    --offline) cargo_flags+=(--offline) ;;
    -h|--help) sed -n '2,14p' "$0"; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 1 ;;
  esac
done
[ "$profile" = release ] && cargo_flags+=(--release)

devstats_build_info
cargo build "${cargo_flags[@]}"

echo "Built ($profile, $DEVSTATS_OS):"
for b in $(devstats_binaries); do
  ls -la "$CARGO_TARGET_DIR/$profile/$b"
done
if [ -n "${BINDIR:-}" ]; then
  install -d "$BINDIR"
  for b in $(devstats_binaries); do
    install -m 0755 "$CARGO_TARGET_DIR/$profile/$b" "$BINDIR/$b"
  done
  echo "Installed to $BINDIR"
fi
