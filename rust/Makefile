# Rust port of the DevStats binaries — lives entirely under rust/.
#
# Targets mirror the Go Makefile in the repository root:
#   make            build all ported binaries (release, stripped)  -> target/release/<name>
#   make test       unit + Go⇄Rust compatibility tests (needs `go`; DEVSTATS_SKIP_GO_COMPAT=1 to skip)
#   make check      rustfmt --check + clippy -D warnings
#   make fmt        rustfmt
#   make install    copy release binaries to BINDIR (default: $GOPATH/bin, like the Go `make install`)
#   make clean

CARGO ?= cargo
GOPATH ?= $(shell go env GOPATH 2>/dev/null || echo $(HOME)/go)
BINDIR ?= $(GOPATH)/bin

# Ported so far (keep in sync with [workspace] members in Cargo.toml).
BINARIES = tsplit replacer
RELEASE_BINS = $(addprefix target/release/,$(BINARIES))

.PHONY: all build $(BINARIES) test test-rust-only check fmt clippy install clean

all: build

build:
	$(CARGO) build --release --workspace

$(BINARIES):
	$(CARGO) build --release -p $@

test:
	$(CARGO) test --workspace

# Same tests without building/comparing against the Go reference binaries.
test-rust-only:
	DEVSTATS_SKIP_GO_COMPAT=1 $(CARGO) test --workspace

fmt:
	$(CARGO) fmt --all

clippy:
	$(CARGO) clippy --workspace --all-targets -- -D warnings

check:
	$(CARGO) fmt --all -- --check
	$(CARGO) clippy --workspace --all-targets -- -D warnings

install: build
	install -d $(BINDIR)
	install -m 0755 $(RELEASE_BINS) $(BINDIR)

clean:
	$(CARGO) clean
