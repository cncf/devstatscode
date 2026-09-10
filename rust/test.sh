#!/usr/bin/env bash
# Run every kind of test we have for the Rust port:
#   1. rustfmt --check                      (formatting)
#   2. cargo clippy -D warnings             (lints, all targets)
#   3. cargo test --workspace               (library unit tests, binary unit tests,
#                                            doc tests, and the Go⇄Rust compatibility
#                                            tests in cmd/<name>/tests/compat.rs which
#                                            build the Go binaries from ../cmd/<name>)
#
# Usage: ./test.sh [--skip-go] [--no-lint] [--release] [-- <cargo test args>]
#   --skip-go   do not build/compare against the Go binaries (DEVSTATS_SKIP_GO_COMPAT=1)
#   --no-lint   skip rustfmt/clippy
#   --release   run tests with the release profile
#   --          everything after is passed to `cargo test` (e.g. `-- -p replacer compat`)
# Env:
#   DEVSTATS_GO   path to the go tool if it is not on PATH (default probes /usr/local/go/bin/go)
#   PG_* etc.     forwarded to DB-dependent tests (see README.md)
set -euo pipefail
cd "$(dirname "$0")"

lint=1
release=()
extra=()
while [ $# -gt 0 ]; do
  case "$1" in
    --skip-go) export DEVSTATS_SKIP_GO_COMPAT=1 ;;
    --no-lint) lint=0 ;;
    --release) release=(--release) ;;
    -h|--help) sed -n '2,17p' "$0"; exit 0 ;;
    --) shift; extra=("$@"); break ;;
    *) echo "unknown option: $1" >&2; exit 1 ;;
  esac
  shift
done

# Go is needed to build the reference binaries for the compatibility tests.
if [ -z "${DEVSTATS_SKIP_GO_COMPAT:-}" ] && ! command -v go >/dev/null 2>&1; then
  if [ -x /usr/local/go/bin/go ]; then
    export PATH="$PATH:/usr/local/go/bin"
  else
    echo "warning: go not found — Go⇄Rust comparison tests will be skipped (DEVSTATS_SKIP_GO_COMPAT=1)" >&2
    export DEVSTATS_SKIP_GO_COMPAT=1
  fi
fi

step() { echo; echo "==> $*"; }

if [ "$lint" = 1 ]; then
  step "rustfmt --check"
  cargo fmt --all -- --check
  step "clippy -D warnings"
  cargo clippy --workspace --all-targets "${release[@]}" -- -D warnings
fi

step "cargo test --workspace ${release[*]:-} ${extra[*]:-}"
cargo test --workspace "${release[@]}" "${extra[@]}"

step "OK — all tests passed"
