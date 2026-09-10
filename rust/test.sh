#!/usr/bin/env bash
# Run every kind of test we have for the Rust port (Linux and FreeBSD):
#   1. rustfmt --check                      (formatting)
#   2. cargo clippy -D warnings             (lints, all targets)
#   3. cargo test --workspace               (library unit tests, binary unit tests,
#                                            doc tests, and the Go⇄Rust compatibility
#                                            tests in cmd/<name>/tests/compat.rs which
#                                            build the Go binaries from ../cmd/<name>)
# Artifacts go to target/<os>/ (see env.sh), so VM and host runs do not collide.
#
# Usage: ./test.sh [--skip-go] [--no-lint] [--lint-only] [--release] [-- <cargo test args>]
#   --skip-go    do not build/compare against the Go binaries (DEVSTATS_SKIP_GO_COMPAT=1)
#   --no-lint    skip rustfmt/clippy
#   --lint-only  run only rustfmt/clippy
#   --release    run tests with the release profile
#   --           everything after is passed to `cargo test` (e.g. `-- -p replacer compat`)
# Env:
#   DEVSTATS_GO  path to the go tool if it is not on PATH
#   PG_* etc.    forwarded to DB-dependent tests (see README.md)
set -euo pipefail
cd "$(dirname "$0")"
# shellcheck source=./env.sh
. ./env.sh

lint=1
tests=1
release=()
extra=()
while [ $# -gt 0 ]; do
  case "$1" in
    --skip-go) export DEVSTATS_SKIP_GO_COMPAT=1 ;;
    --no-lint) lint=0 ;;
    --lint-only) tests=0 ;;
    --release) release=(--release) ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    --) shift; extra=("$@"); break ;;
    *) echo "unknown option: $1" >&2; exit 1 ;;
  esac
  shift
done

step() { echo; echo "==> $*"; }

if [ "$lint" = 1 ]; then
  step "rustfmt --check"
  cargo fmt --all -- --check
  step "clippy -D warnings"
  cargo clippy --workspace --all-targets "${release[@]}" -- -D warnings
fi

if [ "$tests" = 1 ]; then
  devstats_ensure_go
  step "cargo test --workspace ${release[*]:-} ${extra[*]:-} (os: $DEVSTATS_OS, target: $CARGO_TARGET_DIR)"
  cargo test --workspace "${release[@]}" "${extra[@]}"
fi

step "OK — all checks passed"
