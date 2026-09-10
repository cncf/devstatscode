# Shared shell helpers for compile.sh / test.sh (sourced, not executed).
# Portable across Linux and FreeBSD (bash).

# Lower-cased OS name: linux, freebsd, darwin, ...
DEVSTATS_OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
export DEVSTATS_OS

# Per-OS cargo target directory so a checkout shared between the bhyve VM (Linux)
# and the FreeBSD host never mixes build artifacts. Honors a pre-set CARGO_TARGET_DIR.
if [ -z "${CARGO_TARGET_DIR:-}" ]; then
  CARGO_TARGET_DIR="$(pwd)/target/$DEVSTATS_OS"
fi
export CARGO_TARGET_DIR

# Prefer user-local toolchains (rustup in ~/.cargo/bin, Go in ~/.local/go or /usr/local/go)
# over distro packages; matters for non-interactive ssh sessions that skip ~/.bashrc.
for d in "$HOME/.cargo/bin" "$HOME/.local/go/bin" /usr/local/go/bin; do
  if [ -d "$d" ]; then
    case ":$PATH:" in *":$d:"*) ;; *) PATH="$d:$PATH" ;; esac
  fi
done
export PATH

# Names of all [[bin]] crates: one crate per directory under cmd/, named after it.
devstats_binaries() {
  local d
  for d in cmd/*/; do
    d="${d%/}"
    [ -f "$d/Cargo.toml" ] && basename "$d"
  done
}

# Make sure `go` is reachable for the Go⇄Rust comparison tests; otherwise skip them.
devstats_ensure_go() {
  [ -n "${DEVSTATS_SKIP_GO_COMPAT:-}" ] && return 0
  command -v go >/dev/null 2>&1 && return 0
  local c
  for c in /usr/local/go/bin/go /usr/local/bin/go "${HOME}/go/bin/go"; do
    if [ -x "$c" ]; then
      PATH="$PATH:$(dirname "$c")"
      export PATH
      return 0
    fi
  done
  echo "warning: go not found — Go⇄Rust comparison tests will be skipped (DEVSTATS_SKIP_GO_COMPAT=1)" >&2
  export DEVSTATS_SKIP_GO_COMPAT=1
}
