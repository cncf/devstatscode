#!/usr/bin/env bash
# Compile all Rust DevStats binaries (release, stripped).
#
# Works on Linux and FreeBSD. Because the checkout may be shared between the two
# (bhyve VM + host), every OS builds into its own directory:
#   target/<os>/release/<name>      (<os> = linux | freebsd | ..., from uname -s)
# Override with CARGO_TARGET_DIR if you want another location.
#
# Usage: ./compile.sh [--debug] [--offline] [--musl | --target TRIPLE] [-p NAME]...
#   --debug    build the dev profile instead of release
#   --offline  pass --offline to cargo (no network for crates.io)
#   --musl     static Linux binaries (--target x86_64-unknown-linux-musl): what the docker images ship and
#              what the grafana pods (ubuntu:22.04, old glibc) can run -> target/<os>/x86_64-unknown-linux-musl/release/
#              needs `rustup target add x86_64-unknown-linux-musl` and a musl C compiler for the bundled
#              SQLite and ring (Ubuntu: `apt-get install musl-tools`)
#   --target T any other cargo --target triple (-> target/<os>/T/release/)
#   -p NAME    build only the cmd/NAME crate (repeat for several); default: every binary
# Env:
#   BINDIR     if set, copy the resulting binaries there (like `make install BINDIR=...`)
#   DEVSTATS_BUILD_STAMP / DEVSTATS_GIT_HASH / DEVSTATS_HOST_NAME / DEVSTATS_RUST_VERSION
#              build information compiled into the binaries (defaults computed like the Go Makefile)
set -euo pipefail
cd "$(dirname "$0")"
# shellcheck source=./env.sh
. ./env.sh

profile=release
target=
packages=
cargo_flags=()
while [ $# -gt 0 ]; do
  case "$1" in
    --debug) profile=debug ;;
    --offline) cargo_flags+=(--offline) ;;
    --musl) target=x86_64-unknown-linux-musl ;;
    --target) shift; target=${1:?--target needs a triple} ;;
    -p) shift; packages="$packages ${1:?-p needs a crate name}" ;;
    -h|--help) sed -n '2,21p' "$0"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 1 ;;
  esac
  shift
done
[ "$profile" = release ] && cargo_flags+=(--release)

if [ -n "$packages" ]; then
  for b in $packages; do
    [ -f "cmd/$b/Cargo.toml" ] || { echo "unknown binary: $b (no cmd/$b/Cargo.toml)" >&2; exit 1; }
    cargo_flags+=(-p "$b")
  done
  binaries=$packages
else
  cargo_flags+=(--workspace)
  binaries=$(devstats_binaries)
fi

outdir="$CARGO_TARGET_DIR/$profile"
if [ -n "$target" ]; then
  cargo_flags+=(--target "$target")
  outdir="$CARGO_TARGET_DIR/$target/$profile"
  if command -v rustup >/dev/null 2>&1 && ! rustup target list --installed 2>/dev/null | grep -qx "$target"; then
    echo "rust target $target is not installed: rustup target add $target" >&2; exit 1
  fi
  case "$target" in
    *-linux-musl)
      if [ "$(rustc -vV | sed -n 's/^host: //p')" != "$target" ] && ! command -v musl-gcc >/dev/null 2>&1 \
         && ! command -v "${target%%-*}-linux-musl-gcc" >/dev/null 2>&1; then
        echo "no C compiler for $target (needed by the bundled SQLite and ring); Ubuntu: apt-get install musl-tools" >&2; exit 1
      fi ;;
  esac
fi

devstats_build_info
cargo build "${cargo_flags[@]}"

echo "Built ($profile, ${target:-$DEVSTATS_OS}):"
for b in $binaries; do
  ls -la "$outdir/$b"
done
if [ -n "${BINDIR:-}" ]; then
  install -d "$BINDIR"
  for b in $binaries; do
    install -m 0755 "$outdir/$b" "$BINDIR/$b"
  done
  echo "Installed to $BINDIR"
fi
