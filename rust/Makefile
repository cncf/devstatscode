# Rust port of the DevStats binaries — lives entirely under rust/.
# Portable across GNU make (Linux, FreeBSD `gmake`) and BSD make (FreeBSD `make`):
# the real work is done by compile.sh / test.sh, which also pick a per-OS
# target directory (target/<os>/) so the VM and the FreeBSD host can share a checkout.
#
#   make            build all ported binaries (release, stripped) -> target/<os>/release/<name>
#   make test       lint + unit + Go⇄Rust compatibility tests (needs `go`)
#   make check      rustfmt --check + clippy -D warnings only
#   make fmt        rustfmt
#   make install    copy release binaries to BINDIR (default: $GOPATH/bin, like the Go `make install`)
#   make static-bins    all binaries as static Linux (x86_64-unknown-linux-musl) executables, like the docker
#                       images ship them -> target/<os>/x86_64-unknown-linux-musl/release/<name>
#   make grafana-tools  static replacer, sqlitedb and runq for the grafana pods -> RUST_BINS
#                       (default ../../devstats-docker-images/rust-bins; then in ../devstats:
#                       RUST_BINS=../devstats-docker-images/rust-bins ./devel/create_grafana_shared_data.sh)
#                       both need: rustup target add x86_64-unknown-linux-musl; Ubuntu: apt-get install musl-tools
#   make clean      remove the whole target/ directory (binaries included)
#   make cleanup    remove every build/test artifact but keep target/<os>/release/<name> (../cleanup.sh)

GOPATH ?= $(HOME)/go
BINDIR ?= $(GOPATH)/bin
RUST_BINS ?= ../../devstats-docker-images/rust-bins

# Ported so far (one crate per cmd/<name>; keep in sync with [workspace] members in Cargo.toml).
BINARIES = tsplit replacer splitcrons structure tags runq vars columns devstats hide_data website_data webhook sqlitedb merge_dbs reconcile_dbs gha2db_sync import_affs calc_metric annotations get_repos sync_issues ghapi2db gha2db api
# Run by the grafana pods at start (grafana/shared/grafana_start.sh in ../devstats).
GRAFANA_TOOLS = replacer sqlitedb runq

.PHONY: all build $(BINARIES) test test-rust-only check fmt install static-bins grafana-tools clean cleanup

all: build

build:
	./compile.sh

$(BINARIES):
	./compile.sh && echo "built $@"

test:
	./test.sh

# Same tests without building/comparing against the Go reference binaries.
test-rust-only:
	./test.sh --skip-go

check:
	./test.sh --lint-only

fmt:
	cargo fmt --all

install:
	BINDIR=$(BINDIR) ./compile.sh

static-bins:
	./compile.sh --musl

grafana-tools:
	set -- ; for b in $(GRAFANA_TOOLS); do set -- "$$@" -p "$$b"; done; BINDIR=$(RUST_BINS) ./compile.sh --musl "$$@"

clean:
	rm -rf target

cleanup:
	../cleanup.sh
