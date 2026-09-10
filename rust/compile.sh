#!/usr/bin/env bash
# Compile all Rust DevStats binaries (release, stripped) into rust/target/release/.
#
# Usage: ./compile.sh [--debug] [--offline]
#   --debug    build the dev profile instead of release (target/debug/)
#   --offline  pass --offline to cargo (no network for crates.io)
# Env:
#   BINDIR     if set, copy the resulting binaries there (like `make install BINDIR=...`)
set -euo pipefail
cd "$(dirname "$0")"

profile=release
cargo_flags=(--workspace)
for arg in "$@"; do
  case "$arg" in
    --debug) profile=debug ;;
    --offline) cargo_flags+=(--offline) ;;
    -h|--help) sed -n '2,10p' "$0"; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 1 ;;
  esac
done
[ "$profile" = release ] && cargo_flags+=(--release)

cargo build "${cargo_flags[@]}"

# Every [[bin]] in the workspace ends up in target/<profile>/<name>.
bins=$(cargo metadata --no-deps --format-version 1 \
  | python3 -c 'import json,sys; m=json.load(sys.stdin); print("\n".join(t["name"] for p in m["packages"] for t in p["targets"] if "bin" in t["kind"]))' \
  | sort)
echo "Built ($profile):"
for b in $bins; do
  ls -la "target/$profile/$b"
done
if [ -n "${BINDIR:-}" ]; then
  install -d "$BINDIR"
  for b in $bins; do
    install -m 0755 "target/$profile/$b" "$BINDIR/$b"
  done
  echo "Installed to $BINDIR"
fi
