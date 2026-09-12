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
#   make clean

GOPATH ?= $(HOME)/go
BINDIR ?= $(GOPATH)/bin

# Ported so far (one crate per cmd/<name>; keep in sync with [workspace] members in Cargo.toml).
BINARIES = tsplit replacer splitcrons structure tags runq vars columns devstats hide_data website_data webhook sqlitedb merge_dbs gha2db_sync import_affs calc_metric annotations get_repos sync_issues ghapi2db gha2db api

.PHONY: all build $(BINARIES) test test-rust-only check fmt install clean

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

clean:
	rm -rf target
