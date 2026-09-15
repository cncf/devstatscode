#!/usr/bin/env bash
# cleanup.sh — delete every Go and Rust build/test artifact in this checkout and keep
# ONLY the final binaries. Works on Linux and FreeBSD (bash); also `make cleanup`.
#
# Kept (the final binaries):
#   Go    ./<name>                        for every name in the Makefile BINARIES list (`make`)
#   Rust  rust/target/<os>/release/<name> for every rust/cmd/<name> crate (`rust/compile.sh`,
#         one <os> directory per OS that builds from this shared checkout) and
#         rust/target/release/<name> if cargo was ever run without rust/env.sh
#   Installed copies in $GOPATH/bin (`make install`) are never touched.
#
# Removed:
#   Go    build, test and fuzz caches (`go clean -cache -testcache -fuzzcache`) and in-tree
#         leftovers: *.test, *.prof, *.pprof, *.coverprofile, coverage.out, cover.out, c.out,
#         __debug_bin*, ./*.g (debug builds, see the Makefile)
#   Rust  everything under rust/target except the final binaries: deps/, build/, .fingerprint/,
#         incremental/, examples/, *.d and cargo lock files next to the binaries; the whole
#         debug/, doc/, tmp/ and go-bin/ (Go reference binaries built by the compat tests),
#         .rustc_info.json, .rustdoc_fingerprint.json, CACHEDIR.TAG; *.rs.bk, *.profraw,
#         *.profdata, *.pdb anywhere under rust/
#
# A git-tracked path is never removed (checked before every in-tree deletion).
#
# Usage: ./cleanup.sh [-n|--dry-run] [--all] [--docker] [-q|--quiet]
#   -n, --dry-run  print what would be removed, remove nothing
#   --all          also purge the dependency download caches: the Go module cache
#                  (`go clean -modcache`) and $CARGO_HOME/{registry,git} — the next build
#                  needs network access again
#   --docker       also prune the local Docker build cache and dangling (untagged) images
#                  left behind by ../devstats-docker-images/images/build_images.sh;
#                  tagged images are kept
#   -q, --quiet    print only the summary
set -euo pipefail
cd "$(dirname "$0")"

dry=0
all=0
docker=0
quiet=0
while [ $# -gt 0 ]; do
  case "$1" in
    -n|--dry-run) dry=1 ;;
    --all) all=1 ;;
    --docker) docker=1 ;;
    -q|--quiet) quiet=1 ;;
    -h|--help) awk 'NR > 1 && !/^#/ { exit } NR > 1 { sub(/^# ?/, ""); print }' "$0"; exit 0 ;;
    *) echo "unknown option: $1 (try --help)" >&2; exit 1 ;;
  esac
  shift
done

# Same toolchain lookup as rust/env.sh (non-interactive ssh sessions skip ~/.bashrc).
for d in "$HOME/.cargo/bin" "$HOME/.local/go/bin" /usr/local/go/bin; do
  if [ -d "$d" ]; then
    case ":$PATH:" in *":$d:"*) ;; *) PATH="$d:$PATH" ;; esac
  fi
done
export PATH

say() { [ "$quiet" = 1 ] || echo "$*"; }
warn() { echo "warning: $*" >&2; }

human() {
  awk -v k="$1" 'BEGIN {
    u = "KMGT"; i = 1; v = k
    while (v >= 1024 && i < 4) { v /= 1024; i++ }
    if (i == 1) printf "%dK", v; else printf "%.1f%s", v, substr(u, i, 1)
  }'
}

kb_of() {
  local p kb total=0
  for p in "$@"; do
    [ -e "$p" ] || [ -L "$p" ] || continue
    kb=$({ du -sk -- "$p" 2>/dev/null || true; } | awk 'NR == 1 { print $1 }')
    total=$((total + ${kb:-0}))
  done
  echo "$total"
}

is_in() {
  local x=$1 y
  shift
  for y in "$@"; do
    [ "$x" = "$y" ] && return 0
  done
  return 1
}

have_git=1
if ! command -v git >/dev/null 2>&1 || ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  have_git=0
  warn "not a git checkout (or git missing): the tracked-path safety check is disabled"
fi

# True when git tracks PATH itself or anything below it; paths outside the checkout are untracked.
tracked() {
  [ "$have_git" = 1 ] || return 1
  [ -n "$(git ls-files -- "$1" 2>/dev/null | head -n 1)" ]
}

removed_n=0
removed_kb=0
# remove PATH... — honours --dry-run, refuses git-tracked paths, accounts the reclaimed size.
remove() {
  local p kb
  for p in "$@"; do
    [ -e "$p" ] || [ -L "$p" ] || continue
    if tracked "$p"; then
      warn "keeping git-tracked path: $p"
      continue
    fi
    kb=$(kb_of "$p")
    removed_n=$((removed_n + 1))
    removed_kb=$((removed_kb + kb))
    if [ "$dry" = 1 ]; then
      say "would remove: $p ($(human "$kb"))"
    else
      say "removing: $p ($(human "$kb"))"
      rm -rf -- "$p"
    fi
  done
}

# Final binaries: Go from the Makefile, Rust from the workspace binary crates (like rust/env.sh).
go_bins=$(sed -n 's/^BINARIES=//p' Makefile)
rust_bins=""
for d in rust/cmd/*/; do
  d=${d%/}
  if [ -f "$d/Cargo.toml" ]; then
    rust_bins="$rust_bins ${d##*/}"
  fi
done
[ -n "$go_bins" ] || { echo "BINARIES not found in Makefile" >&2; exit 1; }
[ -n "$rust_bins" ] || { echo "no binary crates found under rust/cmd/" >&2; exit 1; }

kept_rust=()

# Keep only the final binaries inside a cargo release directory.
prune_release() {
  local dir=$1 f name
  for f in "$dir"/* "$dir"/.[!.]* "$dir"/..?*; do
    [ -e "$f" ] || [ -L "$f" ] || continue
    name=${f##*/}
    # shellcheck disable=SC2086
    if [ -f "$f" ] && [ ! -L "$f" ] && [ -x "$f" ] && is_in "$name" $rust_bins; then
      kept_rust+=("$f")
    else
      remove "$f"
    fi
  done
  [ "$dry" = 1 ] || rmdir -- "$dir" 2>/dev/null || true
}

# rust/target/{release, <os>/release} keep their final binaries; everything else goes.
clean_rust_target() {
  local t=rust/target e sub
  [ -d "$t" ] || return 0
  for e in "$t"/* "$t"/.[!.]* "$t"/..?*; do
    [ -e "$e" ] || [ -L "$e" ] || continue
    if [ "${e##*/}" = release ] && [ -d "$e" ] && [ ! -L "$e" ]; then
      prune_release "$e"
    elif [ -d "$e" ] && [ ! -L "$e" ] && [ -d "$e/release" ] && [ ! -L "$e/release" ]; then
      for sub in "$e"/* "$e"/.[!.]* "$e"/..?*; do
        [ -e "$sub" ] || [ -L "$sub" ] || continue
        if [ "${sub##*/}" = release ] && [ -d "$sub" ] && [ ! -L "$sub" ]; then
          prune_release "$sub"
        else
          remove "$sub"
        fi
      done
      [ "$dry" = 1 ] || rmdir -- "$e" 2>/dev/null || true
    else
      remove "$e"
    fi
  done
  [ "$dry" = 1 ] || rmdir -- "$t" 2>/dev/null || true
}

clean_rust_tree() {
  local f
  while IFS= read -r f; do
    remove "$f"
  done < <(find rust -path rust/target -prune -o -type f \
    \( -name '*.rs.bk' -o -name '*.profraw' -o -name '*.profdata' -o -name '*.pdb' \) -print)
}

clean_go_tree() {
  local f
  while IFS= read -r f; do
    remove "$f"
  done < <(find . \( -path ./.git -o -path ./rust/target \) -prune -o -type f \
    \( -name '*.test' -o -name '*.prof' -o -name '*.pprof' -o -name '*.coverprofile' \
       -o -name 'coverage.out' -o -name 'cover.out' -o -name 'c.out' -o -name '__debug_bin*' \) -print)
  for f in ./*.g; do
    [ -f "$f" ] && remove "$f"
  done
  return 0
}

clean_go_caches() {
  if ! command -v go >/dev/null 2>&1; then
    warn "go not found: Go caches not cleaned"
    return 0
  fi
  local flags=(-cache -testcache -fuzzcache) dirs kb
  dirs=$(go env GOCACHE)
  if [ "$all" = 1 ]; then
    flags+=(-modcache)
    dirs="$dirs
$(go env GOMODCACHE)"
  fi
  # shellcheck disable=SC2086
  kb=$(kb_of $dirs)
  removed_kb=$((removed_kb + kb))
  if [ "$dry" = 1 ]; then
    say "would run: go clean ${flags[*]}  ($(echo "$dirs" | tr '\n' ' ')— $(human "$kb"))"
  else
    say "running: go clean ${flags[*]}  ($(echo "$dirs" | tr '\n' ' ')— $(human "$kb"))"
    go clean "${flags[@]}"
  fi
}

clean_cargo_caches() {
  local home=${CARGO_HOME:-$HOME/.cargo}
  remove "$home/registry" "$home/git"
}

clean_docker() {
  if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
    warn "docker not available: skipped"
    return 0
  fi
  if [ "$dry" = 1 ]; then
    say "would run: docker builder prune -af && docker image prune -f"
    [ "$quiet" = 1 ] || docker system df
  else
    say "running: docker builder prune -af && docker image prune -f"
    if [ "$quiet" = 1 ]; then
      docker builder prune -af >/dev/null
      docker image prune -f >/dev/null
    else
      docker builder prune -af
      docker image prune -f
    fi
  fi
}

clean_rust_target
clean_rust_tree
clean_go_tree
clean_go_caches
[ "$all" = 1 ] && clean_cargo_caches
[ "$docker" = 1 ] && clean_docker

# Summary.
verb="removed"
[ "$dry" = 1 ] && verb="would remove"
echo
echo "cleanup: $verb $removed_n path(s), about $(human "$removed_kb")"
echo "kept Go binaries (./<name>):"
kept_go=0
# shellcheck disable=SC2086
for b in $go_bins; do
  if [ -x "./$b" ] && [ -f "./$b" ]; then
    ls -la -- "./$b"
    kept_go=$((kept_go + 1))
  fi
done
[ "$kept_go" -gt 0 ] || echo "  (none built)"
echo "kept Rust binaries (rust/target/<os>/release/<name>):"
if [ ${#kept_rust[@]} -gt 0 ]; then
  ls -la -- ${kept_rust[@]+"${kept_rust[@]}"}
else
  echo "  (none built)"
fi
if [ "$dry" = 0 ] && [ -d rust/target ]; then
  leftovers=$(find rust/target -type f | wc -l | tr -d ' ')
  if [ "$leftovers" != "${#kept_rust[@]}" ]; then
    warn "rust/target still holds $leftovers file(s) but ${#kept_rust[@]} final binaries were kept"
    exit 1
  fi
fi
