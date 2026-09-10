# DevStats — Rust port

Rust rewrite of the DevStats Go binaries from `cmd/*`, developed program by
program. Everything Rust-related lives in this `rust/` directory; the Go sources
in the repository root stay the reference until the port is complete.

Every ported binary is a **drop-in replacement**: same executable name, same
environment variables, same command-line arguments, same stdout on success,
same exit codes and same stdout/stderr placement of messages. Scripts in
`cncf/devstats`, `devstats-docker-images`, `devstats-helm`, … must not notice
the difference.

## Status

| Binary     | Go source           | Rust crate         | Tests (unit / Go⇄Rust) |
|------------|---------------------|--------------------|------------------------|
| `tsplit`   | `cmd/tsplit`        | `cmd/tsplit`       | 10 / 10                |
| `replacer` | `cmd/replacer`      | `cmd/replacer`     | 10 / 27                |

Shared library code goes to the `devstatscode` crate (currently: fatal error
handling à la `lib.FatalOnError`/`lib.Fatalf`, and the `goregex` Go→Rust regexp
adapter).

Supported platforms: **Linux and FreeBSD** (the checkout is shared between an
Ubuntu bhyve VM and its FreeBSD host, so every OS builds into its own
`target/<os>/` directory — see `env.sh`).

## Layout

```
rust/
├── Cargo.toml          workspace (all crates below)
├── compile.sh          build all binaries (release) -> target/<os>/release/<name>
├── test.sh             fmt + clippy + unit + Go⇄Rust compatibility tests
├── env.sh              shared helpers for the scripts (per-OS target dir, toolchain PATH)
├── Makefile            thin portable (bmake/gmake) wrapper around the two scripts
├── devstatscode/       shared library crate (port of the root Go package)
│   └── src/{error,goregex}.rs
├── cmd/<name>/         one binary crate per Go program
│   ├── src/main.rs
│   └── tests/compat.rs Go⇄Rust differential tests for that binary
└── compat/             test harness: builds the Go reference binary, runs both,
    └── fixtures/       compares outcomes; real-world fixtures
```

## Build

```sh
cd rust
./compile.sh                 # release build, stripped: target/<os>/release/{tsplit,replacer}
BINDIR=$GOPATH/bin ./compile.sh   # ... and copy them there (same as `make install`)
make / make install          # equivalents via the Makefile
```

Requires a stable Rust toolchain (`rust-version` in `Cargo.toml`); no C
dependencies, no network access at run time.

## Test

```sh
./test.sh                # rustfmt --check, clippy -D warnings, unit + Go⇄Rust compat tests
./test.sh --skip-go      # same without building/comparing the Go binaries
./test.sh --lint-only    # only rustfmt/clippy
./test.sh -- -p replacer # extra args go to `cargo test`
make test / make check   # equivalents via the Makefile
```

The compatibility tests (`cmd/<name>/tests/compat.rs`) build the Go program
from `../cmd/<name>/*.go` into `target/go-bin/` (needs `go` on `PATH`,
`/usr/local/go/bin/go`, or `DEVSTATS_GO=/path/to/go`) and run **both**
binaries on the same inputs — real fixtures and invocations copied from the
DevStats shell scripts — asserting identical exit codes, stdout (and stderr or
resulting files where that is part of the contract). Set
`DEVSTATS_SKIP_GO_COMPAT=1` to run without a Go toolchain.

## Compatibility notes

* Fatal errors (`lib.FatalOnError` in Go) keep the production behaviour: message
  to stderr, 60 s pause unless `NO_FATAL_DELAY` is set, exit code 2.
  Go's stack trace is not reproduced.
* Wording of *parse* errors produced by the standard library may differ
  (e.g. an invalid `SIZE`); the exit code and the stream (stderr) are the same.
* Regular expressions written for Go's `regexp` (RE2 syntax) are translated by
  `devstatscode::goregex` before compilation with the `regex` crate, so patterns
  used in the DevStats scripts work unchanged:
  * a `{` that does not start a `{n}`/`{n,}`/`{n,m}` repetition is literal in
    Go (`{{exclude_bots}}`) but a syntax error in `regex` → escaped;
  * `\d`, `\w`, `\s`, `\b` (and negations) are ASCII-only in Go → emitted as
    ASCII-only;
  * `[`, `&&`, `~~` inside a character class are literal in Go but set
    operators in `regex` → escaped.
  Not translated (unused in DevStats): `\Q…\E`, octal escapes like `\123`.

## Bugs found in the Go code and fixed in both implementations

* `tsplit`: errors were printed to **stdout** with exit code **0**; now stderr
  and exit 1. `SIZE=0` panicked (division by zero) and negative sizes
  produced empty output; now `SIZE` must be positive. A different number of
  link and image lines was silently accepted (misaligned output); now an error.
* `replacer`: the `REPLACEFROM` bound error said "filename length"; it is the
  file's length.
