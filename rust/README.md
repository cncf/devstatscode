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

| Binary       | Go source         | Rust crate        | Tests (unit / Go⇄Rust) |
|--------------|-------------------|-------------------|------------------------|
| `tsplit`     | `cmd/tsplit`      | `cmd/tsplit`      | 10 / 10                |
| `replacer`   | `cmd/replacer`    | `cmd/replacer`    | 10 / 27                |
| `splitcrons` | `cmd/splitcrons`  | `cmd/splitcrons`  | 3 / 25                 |
| `structure`  | `cmd/structure`   | `cmd/structure`   | — / 18 (PostgreSQL)    |
| `tags`       | `cmd/tags`        | `cmd/tags`        | 2 / 21 (PostgreSQL)    |
| `runq`       | `cmd/runq`        | `cmd/runq`        | 3 / 49 (PostgreSQL)    |
| `vars`       | `cmd/vars`        | `cmd/vars`        | 3 / 35 (PostgreSQL)    |
| `columns`    | `cmd/columns`     | `cmd/columns`     | 2 / 28 (PostgreSQL)    |
| `devstats`   | `cmd/devstats`    | `cmd/devstats`    | 2 / 37 (PostgreSQL)    |
| `hide_data`  | `cmd/hide_data`   | `cmd/hide_data`   | 2 / 36 (PostgreSQL)    |
| `website_data` | `cmd/website_data` | `cmd/website_data` | 3 / 36 (PostgreSQL) |
| `webhook`    | `cmd/webhook`     | `cmd/webhook`     | 5 / 21 (HTTP servers)  |
| `sqlitedb`   | `cmd/sqlitedb`    | `cmd/sqlitedb`    | 5 / 36 (SQLite)        |
| `merge_dbs`  | `cmd/merge_dbs`   | `cmd/merge_dbs`   | 4 / 49 (PostgreSQL)    |
| `gha2db_sync` | `cmd/gha2db_sync` | `cmd/gha2db_sync` | 7 / 71 (PostgreSQL)   |
| `import_affs` | `cmd/import_affs` | `cmd/import_affs` | 6 / 47 (PostgreSQL)   |
| `calc_metric` | `cmd/calc_metric` | `cmd/calc_metric` | 6 / 98 (PostgreSQL)   |
| `annotations` | `cmd/annotations` | `cmd/annotations` | 5 / 73 (PostgreSQL + git) |
| `get_repos`  | `cmd/get_repos`   | `cmd/get_repos`   | 3 / 117 (PostgreSQL + git) |
| `sync_issues` | `cmd/sync_issues` | `cmd/sync_issues` | 10 / 53 (PostgreSQL + fake GitHub API) |
| `ghapi2db`   | `cmd/ghapi2db`    | `cmd/ghapi2db`    | 109 (PostgreSQL + fake GitHub REST/GraphQL API) |
| `gha2db`     | `cmd/gha2db`      | `cmd/gha2db`      | 5 (+9 lib) / 60 (PostgreSQL + fake GH Archive) |
| `api`        | `cmd/api`         | `cmd/api`         | 10 / 17 scenarios ≈ 330 requests (HTTP servers + PostgreSQL) |

All 23 binaries of `cmd/*` are ported.

Beyond the test suites the Rust binaries were validated live in the
`devstats-test` Kubernetes namespace (2026-09-12) side by side with the Go
ones, using the `RUST=1` images of `../devstats-docker-images`: every project
was synced with both image kinds, the shared affiliations import was alternated
Go→Rust→Go, a per-project affiliations recompute (`riff`, 100 tables) was
compared table by table between Go and Rust — 100 % content-identical (the
only physical difference is the column order of the recreated `s*` tables:
Go's random map iteration vs. Rust's sorted order, see the `WriteTSPoints`
note below) — and backups, the API server, the static site and the reports
image were exercised on both kinds. Items 46–53 of the bugs list were found by
that validation.

Shared library code lives in the `devstatscode` crate — the port of the root Go
package (`lib`): `Ctx` (`context.go`, all `GHA2DB_*` knobs), logging, env
syncing, signals, `ExecCommand`, string/time/hash/unicode helpers, the JSON and
YAML formatters (`yamlv2` is a byte-exact `gopkg.in/yaml.v2` encoder + decoder
rules), `gomath` (bit-exact Go `math.Pow/Exp/Log`) and the `goregex` Go→Rust
regexp adapter, and `pg` — the PostgreSQL layer (`pg_conn.go`, a `database/sql`
+ lib/pq work-alike on our own wire-protocol client; 120 unit tests + 11
DB-backed tests including a differential run against a Go probe). Every DB-free
Go table test is ported as a Rust unit test. `github` is the subset of
`google/go-github` v38 the tools use (client with per-category rate limit
cache, `RateLimitError`/`AbuseRateLimitError`/`AcceptedError`/`ErrorResponse`
with Go's wording, Go-style redirect following, `Issues.Get`,
`PullRequests.Get`, `Issues.ListRepositoryEvents`, `Repositories.ListLanguages`,
`Repositories.License`, `Repositories.ListCommits`, `Issues.ListComments`,
`PullRequests.ListComments`, `Repositories.ListComments`, `PullRequests.List`,
`PullRequests.ListReviews`, `Repositories.ListForks`,
`Repositories.ListReleases`, `RateLimits`, raw GraphQL POSTs) and `ghapi` the
port of `ghapi.go` (`GHClient`, `GetRateLimits`, `HandlePossibleError`,
`ArtificialEvent`/`ArtificialPREvent`, `SyncIssuesState`, …); `restore` is the
port of the root `restore.go` (`RestoreIssueComment`/`ReviewComment`/
`CommitComment`/`Review`/`Fork`/`Release`/`Star`, `findRawEventID`,
`RunEventIDsPostprocess`).

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
├── devstatscode/       shared library crate (port of the root Go package `lib`)
│   ├── src/*.rs        one module per Go file (see the table in src/lib.rs)
│   └── tests/          Go-generated vectors (yaml.v2 encoder, math.Pow/Exp/Log)
├── cmd/<name>/         one binary crate per Go program
│   ├── src/main.rs
│   └── tests/compat.rs Go⇄Rust differential tests for that binary
├── compat/             test harness: builds the Go reference binary, runs both,
│   └── fixtures/       compares outcomes; real-world fixtures
└── docs/               Rust-only design notes (no code):
    └── ghapi2db-gha-gaps.md  2026-09-14 research: GH Archive degradation measured, what the
                              GitHub API still offers, phased proposal to extend ghapi2db/get_repos
```

The bot-exclusion fixtures (`compat/fixtures/{tags/data,website_data,runq/data}/util_sql/exclude_bots.sql`,
`calc_metric/exclude_bots.sql`, `structure/util_sql/exclude_bots_table_insert.sql`, `runq/data/util_sql/actors.sql`)
are verbatim copies of `../devstats/util_sql/*`; `../devstats/devel/regen_bot_lists.py` re-syncs them whenever
the bot list changes (the tests compare Go vs Rust dynamically, so any list works, but keeping the real one exercises
the real `%[%bot]%`-style patterns).

## Build

```sh
cd rust
./compile.sh                 # release build, stripped: target/<os>/release/{tsplit,replacer,splitcrons,structure,tags,runq,vars,columns,devstats,hide_data,website_data,webhook,sqlitedb,merge_dbs,gha2db_sync,import_affs,calc_metric,annotations,get_repos,sync_issues,ghapi2db,gha2db,api}
BINDIR=$GOPATH/bin ./compile.sh   # ... and copy them there (same as `make install`)
make / make install          # equivalents via the Makefile
```

Requires a stable Rust toolchain (`rust-version` in `Cargo.toml`) and a C
compiler for `ring` (the TLS crypto used by `webhook` to fetch the Travis
public key — the only tool that talks to the network at run time) and for the
bundled SQLite amalgamation of `rusqlite` (`sqlitedb`, like `mattn/go-sqlite3`
in Go); no other native dependencies.

## Test

```sh
./test.sh                # rustfmt --check, clippy -D warnings, unit + Go⇄Rust compat tests
./test.sh --skip-go      # same without building/comparing the Go binaries
./test.sh --skip-db      # same without the PostgreSQL-backed tests
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

Every Go test of the original repository has a Rust twin: the table-driven
unit tests (`context`, `gha`, `time`, `string`, `pg` helpers, …) live in the
`mod tests` of the corresponding module with the Go tables copied 1:1
(`*_go_table` tests), and the PostgreSQL-backed ones in
`devstatscode/tests/`: `pg_db.rs` (`TestPostgres`), `series_db.rs`
(`TestProcessAnnotations`, 15 cases), `annotation_regexp.rs`
(`TestAnnotationRegexp`, 264 rows) and `metrics_yaml.rs` — the port of the
sibling repository's `devstats/metrics_test.go`, which runs the 70 Kubernetes
metric cases of `devstats/tests.yaml` (fixtures, tags, `{{…}}` substitutions,
`CompareSlices2D` semantics) against a scratch `dbtest_metrics` database. It
needs a `devstats` checkout (`DEVSTATS_DIR=/path/to/devstats`, default
`../../../devstats`; skipped when absent). The `tests.yaml` cases that also
fail under Go because the fixtures are older than the metric SQL are listed in
`KNOWN_STALE` (executed and reported, fatal only with `METRICS_TEST_STRICT=1`);
one collation-dependent case is ignored on non-glibc PostgreSQL servers.
`TEST_METRICS=metric1,metric2` selects cases like in Go.

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
* JSON written by the library (`PrettyPrintJSON`, `ObjectToJSON`) has **sorted**
  object keys, 2-space indentation, `encoding/json` number formatting (`1e-7`,
  exact 64-bit integers) and `&`/`<`/`>` escaped as `\u0026` etc. — exactly
  what the Go library produces after the fix of bug 28 (jsoniter used to emit
  map keys in random order and float64-rounded integers).
* A Rust panic (Go: runtime panic with a goroutine dump) prints
  `panic: <message> [file:line]` to stderr and exits with code 2, like Go.
* A write to a closed stdout/stderr pipe (`prog | head -1` after `head` has
  exited) kills the process with `SIGPIPE` exactly like Go's `os.epipecheck`
  (nothing printed, shell status 141) — `devstatscode::error::die_on_stdio_epipe`
  / the panic hooks; Rust's default would be a `Broken pipe` panic or, for the
  `lib.Printf` port, silently continuing.
* `splitcrons`:
  * `values.yaml` is decoded with yaml.v2 rules (`devstatscode::yamlv2::de`:
    raw scalar text into string fields, YAML 1.1 ints/bools, `[3]int` length
    checks) and re-encoded **byte-for-byte** like `yaml.Marshal` (field order,
    `omitempty`, quoting, flow style `[]`); 249 Go-generated encoder vectors +
    the real `../devstats-helm/devstats-helm/values.yaml` (when present) are
    compared. Duplicate keys (last value wins, a repeated whole mapping
    replaces the earlier one) and multi-document streams (first document only)
    are decoded like yaml.v2 — `devstatscode::yamlv2::dedup` re-parses such
    input with `saphyr-parser` and re-emits it canonically before the serde
    decode (the live `devstats-helm/projects.yaml` once shipped a repeated
    `annotation_regexp`, which the Go binaries silently accepted). Deliberate
    decoder differences on malformed input only: quoted numbers are accepted
    in int fields, error wording follows serde.
  * The scheduling weights (`size^WEIGHT_POWER`, `SPLIT_ALGO=geom|invgeom|…`)
    are printed, so `math.Pow` is reproduced bit-exactly (`devstatscode::gomath`
    ports Go's `pow.go`, `exp_amd64.s`, `log_amd64.s`; the `Exp` code path —
    fused multiply-add or not — is chosen at run time from the CPU flags
    exactly like the Go binary does; verified against 1689 Go vectors).
  * Go breaks ties between equal affiliations "gap" positions in random map
    order (`sort.Slice` over a map); Rust breaks them deterministically
    (position, then project order). Only the informational `gap=` figures can
    differ, and only when two projects share a slot.
  * `kubectl` is invoked with the same argument vectors (checked by a fake
    `kubectl` that logs its calls); the child's stdout/stderr are relayed as
    in Go (`lib.ExecCommand`).

* PostgreSQL (`devstatscode::pg`, replaces `pg_conn.go` + lib/pq + `database/sql`):
  * own implementation of the v3 wire protocol (trust / clear-text / MD5 /
    SCRAM-SHA-256 authentication; no TLS: `PG_SSL`/`sslmode` `require`/`verify-*`
    are errors, `disable`/`allow`/`prefer` connect in clear text — DevStats
    always uses `disable`). Connection parameters follow lib/pq's DSN rules,
    including the `PG*` environment fallbacks (`PGTZ`, `PGAPPNAME`, ...).
  * `PgConn` behaves like `*sql.DB`: connects lazily, retries a statement once
    on a dead connection, keeps at most 2 idle connections; `Rows`/`Row`/`PgTx`
    mirror `*sql.Rows`/`*sql.Row`/`*sql.Tx` (a dropped `PgTx` rolls back).
  * values are decoded exactly as lib/pq does (all results in text format):
    `int2/4/8` → `i64`, `float4/8` → `f64`, `bool`, `text`/`varchar`/`"char"` →
    `String`, `bytea` → bytes, `timestamp`/`timestamptz`/`date`/`time` → time,
    and **everything else** (`char(n)`, `name`, `numeric`, `json(b)`, arrays,
    `uuid`, `interval`, `hll`) → raw bytes, as `[]byte` in Go. Scanning into
    `String`/`Vec<u8>`/`i64`/`i32`/`i16`/`f64`/`bool`/`DateTime`/`Option<…>`
    produces the same values and the same error texts as `database/sql`
    (`sql: Scan error on column index …`, `converting NULL to … is unsupported`,
    `sql: no rows in result set`, `pq: …` server errors with SQLSTATE code and
    condition name). Verified by a differential test that runs the same
    statements through a Go program using the real `lib.PgConn` (`rust/compat/
    go/testdata/pgprobe`) and compares every output line.
  * Time parameters keep their UTC offset and are sent exactly like lib/pq's
    `FormatTimestamp` (`2026-09-11 09:21:49.7+02:00`, `Z` for UTC), so a local
    `time.Now()` stored into a `timestamp` column keeps the wall-clock time on
    non-UTC hosts too. Wherever Go stores or prints `time.Now()` (the
    `gha_logs` rows, the `GHA2DB_LOGTIME` prefix, fatal-error timestamps) the
    Rust code uses the local zone as well.
  * Deviations: `timestamp`/`date` values print (`%v`) as `… +0000 +0000`
    exactly like Go, but `timestamptz` values print the numeric offset where Go
    prints the session zone abbreviation (`+0000 UTC`), and so does `%v` of a
    local time argument echoed by `GHA2DB_QOUT` (`+0200 +0200` instead of
    `+0200 CEST`, and no monotonic-clock `m=+0.012` suffix); text values that are
    not valid UTF-8 are decoded lossily (Go keeps the raw bytes); `DateTime<Utc>`
    destinations convert to UTC; PostgreSQL dates beyond chrono's range
    (±262143 years) are decode errors; where Go iterates a map in random order
    (tags/fields of `WriteTSPoints`, columns to drop) the Rust code uses a
    sorted order.
  * `FatalOnError`'s PostgreSQL branches (wait 15 min on `too many connections`
    / `the database system is starting up`, then retry) are kept; the delay can
    be shortened with `DEVSTATS_PG_SETTLE_SECONDS` for tests.
  * The DB-backed tests need a PostgreSQL server and only run with
    `PG_DB=dbtest` (they create and drop `dbtest_<name>` databases); `test.sh`
    finds a server on `127.0.0.1:5432` or `:15432` automatically and skips
    them otherwise (`--skip-db` forces the skip).

* `structure`:
  * `structure.go` is transcribed statement for statement (399 DDL statements
    in the same order, same `GHA2DB_SKIPTABLE`/`GHA2DB_INDEX`/`GHA2DB_SKIPTOOLS`
    gating, the affiliation tables skipped in shared-affiliations mode) into
    `devstatscode::structure`; `GHA2DB_QOUT=1` therefore echoes the identical
    statement list. The tools pass (`gha_postprocess_scripts` seeding, the
    `util_sql/*.sql` scripts with the `_shared` substitution, the bounded
    `GHA2DB_POSTPROCESS_FROM/TO` range mode) follows the Go code path by path.
  * The Go⇄Rust tests run both binaries against a PostgreSQL server (databases
    `dbtest_structure_<scenario>_{go,rs}`) and compare exit code, stdout,
    `Error: '…'` stderr lines, database existence and a full schema/data
    snapshot (`information_schema` + `pg_dump --schema-only`) — including the
    interactive `(y/n)` prompt (`GHA2DB_MGETC` or stdin), the second
    indexes-only pass, idempotency, the real `../devstats/util_sql` scripts
    (copied into `compat/fixtures/structure/util_sql`) and every fatal path.
  * Only the run-time durations (`Time: 1.234s`) differ between the two
    binaries.

* `tags`:
  * `devstatscode::tags` is the port of `tags.go` (`Tags`/`Tag` yaml structure,
    `ProcessTag`); the binary mirrors `cmd/tags/tags.go` including the
    goroutine limiter (one worker per tag, at most `GetThreadsNum` running,
    synchronised through an unbuffered channel) and the `Final N threads join`
    / debug `threading:` lines.
  * The Go⇄Rust tests (`compat/fixtures/tags`) run both binaries on databases
    seeded with the real DevStats schema and a small dataset, using the real
    `metrics/shared/tags.yaml` + `*_tags.sql` from `cncf/devstats` and a
    test project exercising every yaml field (`other_tags` with and without
    normalisation, `limit`/`{{lim}}`, `{{exclude_bots}}`, `disabled`, the
    `/shared/` fallback, NULL columns, tags without values), plus
    `GHA2DB_SKIPTSDB`, `GHA2DB_DATADIR`, multi-threaded runs and the lock
    fallbacks (a reader holding the series table → `truncate` times out and
    `delete` is used; an exclusive lock → both warned, upserts wait). Every
    `t<series>` table is compared.
  * Deliberately **not** reproduced — Go's randomness: `WriteTSPointsBatch`
    iterates Go maps, so the order of the tag columns in `create table`, of
    the `create index` statements and of the resulting table columns changes
    from run to run in Go; Rust uses sorted order. Likewise the main thread's
    `threading: …` debug lines race the workers' `Synced tag …` lines in both
    implementations. The tests compare modulo these orders.

* `runq`:
  * `cmd/runq/runq.go` transcribed: `readfile:` parameters (`len ≥ 10`), the
    `qr` quick-range parameter (`PrepareQuickRangeQuery`), `{{range}}`,
    `{{rnd}}`, `explain` mode, `GHA2DB_DRY_RUN` (`Printf` vs `fmt.Println`
    depending on `debug < 0`), the framed ASCII table, `GHA2DB_CSVOUT` output
    through `devstatscode::gocsv` — a port of Go's `encoding/csv` `Writer`
    (quoting rules incl. the leading-space and `\.` cases, `\r\n` mode, the
    Go `writerTests` table as unit tests) — `Rows:`, `<csv> written` and
    `Time:` lines.
  * Every column is scanned as raw bytes like Go's `*[]byte` destination —
    `runq` prints exactly what lib/pq handed to Go: RFC3339Nano timestamps
    (`2012-07-01T00:00:00Z`, `0000-01-01T12:34:56Z` for `time`, session
    offset for `timestamptz`), `%g` floats (`1e+06`), `true`/`false`, NULL as
    empty, and everything else (numeric, interval, arrays, json, uuid, inet,
    `bytea`) as the server's text — including invalid UTF-8, which reaches
    stdout unchanged. Column widths are byte lengths and the padding is
    computed with Go's rune count (an invalid byte counts as one rune), so the
    frames match byte for byte.
  * The Go⇄Rust tests (`compat/fixtures/runq`) run both binaries on databases
    seeded with the DevStats schema plus a typed dataset (27 column types,
    103-column tables, artificial events, duplicated rows, NaN/infinity) using
    the real `util_sql/*.sql` and `metrics/shared/hist_commenters.sql` from
    `cncf/devstats` with their production parameters, plus purpose-written
    SQL exercising every code path (`readfile:`, `qr`, `{{rnd}}`, multi
    statement files, DML before/after a select, `explain`, dry run, CSV,
    bytea, unicode widths, syntax/ambiguity errors, missing files, unusable
    CSV path, no connection) and compare exit code, stdout bytes, `Error:`
    stderr lines, the CSV bytes and the affected tables.
  * Deviations: Go applies the `{{param}}` replacements in random (map)
    order — Rust uses the sorted order (only matters for parameters that are
    prefixes of each other, e.g. `{{a}}` and `{{ab}}`); Go's `ErrorType:`
    stderr lines are not reproduced; `GHA2DB_QOUT` echoes of `time.Now()`
    lack the `m=+…` monotonic suffix (see `pg`).

* `vars`:
  * `cmd/vars/vars.go` transcribed: `metrics/<project>/vars.yaml` (or
    `GHA2DB_VARS_YAML` / `GHA2DB_VARS_FN_YAML`, `./` in `GHA2DB_LOCAL` mode,
    `GHA2DB_DATADIR` otherwise, `lib.ReadFile`'s `/shared/` fallback) decoded
    with the yaml.v2 rules (`value: 007` stays `007`, `yes` stays `yes`,
    `disabled: yes` is a bool); per variable: `GHA2DB_EXCLUDE_VARS` /
    `disabled` skip, the `Incorrect variable configuration, skipping` check,
    `queries` (raw SQL, every column scanned as text, NULL → empty,
    `name:column:value:row:col` placeholders, duplicate names and unknown
    columns fatal), the command (`{{datadir}}`/`{{project}}` expansion, stdout
    and stderr merged like `CombinedOutput` — `devstatscode::exec::combined_output`
    — `TrimSpace`d; empty output keeps the yaml `value` and skips the
    replacements; failures print `Failed command: …` and are fatal with Go's
    `exit status N` / `executable file not found in $PATH` / `fork/exec …`
    wording), `loops_before` → `queries_before` → `replaces` (`[[from]]` with
    a previous variable, an environment variable `$NAME` or a `:literal`; a
    `:from` replaces raw text; results become variables) → `loops_after` →
    `queries_after`, then the `insert … on conflict(name) do update`
    (`value_s|i|f|dt`, filtered by `GHA2DB_ONLY_VARS` / `no_write`, skipped by
    `GHA2DB_SKIPPDB`) and all `GHA2DB_DEBUG` lines (`Variable Name '…', Value
    '…', Type '…', Command […], Replaces […], Queries: […], Loops: […],
    Disabled: …, Skip: …, NoWrite: …`, `Name '…', New Value '…', Type '…'`,
    `Skipping postgres vars write`).
  * The Go⇄Rust tests (`compat/fixtures/vars`) run both binaries in a
    devstats-like tree holding the real `all`, `kubernetes` and `prestodb`
    `vars.yaml`/`sync_vars.yaml` with the `docs/dashboards/*.md` and
    `partials/*.html` they read (the projects-health partial expands 253 × 76
    query cells over real `sprojects_health` rows for 12 projects plus
    synthetic ones for the remaining 247 series), and a test project covering
    every feature (all four types, upserts into pre-existing rows,
    `$ENV`/literal/chained replacements, `{{datadir}}` in the command itself,
    unicode, quoting, merged stderr, NULLs and non-text query columns, loop
    edge cases: empty/reversed ranges, unmatched markers, repeated blocks) and
    every fatal path (malformed yaml, replacement/query/loop definitions,
    undefined variable, failing/missing commands, bad `type`, unparsable
    values, unreachable server); they compare exit code, stdout, the `Error:`
    lines and the whole `gha_vars` table.
  * Deviations: the variable dump of a failed replacement (`defined: map[…]`)
    is printed in sorted order (Go: map order); queries whose placeholder
    keys prefix each other are replaced in sorted order too; command output
    and query values that are not valid UTF-8 are decoded lossily.
* `columns`
  * `cmd/columns/columns.go` transcribed: `metrics/<project>/columns.yaml` (or
    `GHA2DB_COLUMNS_YAML`, `metrics/columns.yaml` without a project, `./` in
    `GHA2DB_LOCAL` mode, `GHA2DB_DATADIR` otherwise, `lib.ReadFile`'s
    `/shared/` fallback; `GHA2DB_SKIPTSDB` only prints `Time:`) decoded with
    the yaml.v2 rules; phase one, per `{table_regexp, tag, column, hll}`
    config on a `GHA2DB_NCPUS` pool (`GHA2DB_ST` = one thread, in yaml order):
    `Ensure column config: &{…}` (hidden at `GHA2DB_DEBUG=-1`), the tag values
    from `select "<column>" from "<tag>" order by time asc` (`Warning: no tag
    values for …` when empty), the matching tables via `pg_tables … ~ $1`
    (`Warning: '&{…}': no table hits`), the current columns, the stale ones
    (`Need to delete N columns: […]`, `Deleted column "…"`; `time`, `series`,
    `period`, `all` and `none` are never deleted) and `alter table … add
    column if not exists "<value>" double precision|hll` (`Added column`) —
    failures go through `HandleRowIsTooBig` → `DropLeastUsedCol` (`Table 't'
    has N column(s)…`, `Two least used columns are: …`, `Dropped 'a' and 'b'…`;
    fewer than 80 candidate columns: nothing dropped) with the three-trial
    `Give up 'mass add columns' after 3 trials`; the `Ensure columns(N): … -->
    […]`/`Ensure N columns in '…'`, `Current columns(N): … --> […]`/`Currently
    N columns in '…'`, `Tables:`/`Columns:`/`HLLs:` (debug > 1) and `Cfg:
    map[…]` (debug > 0) lines; phase two, per table: `update "t" set "c" =
    0.0|hll_empty(), …` (`Mass updated "t", columns: N, took: …`) and `alter
    table "t" alter column "c" set not null, alter column "c" set default
    0.0|hll_empty(), …` (`Altered "t" defaults and restrictions, …`) with the
    same retry/give-up handling (`Error handle row is too big mass alter
    defaults: pq: column "…" contains null values`).
  * The Go⇄Rust tests (`compat/fixtures/columns`) run both binaries against a
    seeded database (tag tables with unicode, quotes, NULLs, a 104-value tag;
    series tables missing, having and having stale columns; three "row is too
    big" tables whose rows are within a few bytes of PostgreSQL's 8160-byte
    heap-tuple limit so that adding a hundred `double precision` columns
    fails and triggers the drop-least-used / give-up paths) with the real
    `shared`, `all` and `kubernetes` yamls plus a test project covering every
    debug level, `GHA2DB_ST`/multithreaded runs, re-runs (idempotence),
    `GHA2DB_QOUT`, `GHA2DB_SKIPTSDB`, `hll` columns, the `/shared/` fallback,
    `GHA2DB_DATADIR` and every fatal path (missing tag table/column, NULL tag
    value, invalid regexp, missing/malformed yaml, unreachable server); they
    compare exit code, stdout as a multiset of lines plus the exact order of
    the single-threaded first phase, the `Error:` lines and the whole schema
    and data of every table.
  * Deviations: the second phase visits the tables, and lists the columns in
    its `update`/`alter table` statements, in sorted order (Go: map order) —
    visible only in the `GHA2DB_QOUT` echo and the line order. The
    `pg_catalog.pg_tables … ~ $1` lookup of the first phase has `order by
    tablename` on both sides (bug 48: without it the table order followed the
    catalog's physical row order, which differs between databases). `hll` columns
    need the `hll` extension installed in the server (not available as a
    FreeBSD package — built from `citusdata/postgresql-hll`); without it both
    implementations report `Error handle row is too big add column X/hll: pq:
    type "hll" does not exist` and the tests check that path instead.
* `devstats`
  * `cmd/devstats/devstats.go` transcribed: `projects.yaml` (`GHA2DB_PROJECTS_YAML`,
    `./` in `GHA2DB_LOCAL` mode, `GHA2DB_DATADIR` otherwise, no `/shared/`
    fallback) decoded with the yaml.v2 rules into `devstatscode::projects`
    (`AllProjects`/`Project`, `GetProjectsList` with `disabled`,
    `GHA2DB_PROJECTS_OVERRIDE` and `ONLY`); `GHA2DB_CHECK_PROVISION_FLAG`
    (`provisioned` in every project's `gha_computed`: `No '<db>' database,
    missing provisioning flag`, `Missing provisioned flag on '<db>' database…`,
    `Not all databases provisioned, pending: N, exiting`);
    `GHA2DB_CHECK_RUNNING_FLAG` (`Running flag on '<db>' set, age <d>, maximum
    allowed age: <GHA2DB_MAX_RUNNING_FLAG_AGE>` then `set, exiting` or
    `expired, removing…`/`force removed`, `No '<db>' database, cannot check
    running flag`); `GHA2DB_SET_RUNNING_FLAG` (`devstats_running` inserted in
    every database and cleared at the end — with the `Setting/Set/Deleting/
    Cleared running flag` debug lines, the `Failed to clear running flag on
    <db>: <err>, retrying after N seconds` retry loop and `Not all databases
    present, missing: N, exiting`); the PID file `/tmp/<GHA2DB_PID_FILE_ROOT>.pid`
    (`Another \`devstats\` instance is running, PID file '…' exists, exiting
    (not an error)`, `GHA2DB_SKIP_PIDFILE`, fatal `remove …: no such file or
    directory` when it disappeared); `get_repos` with
    `GHA2DB_PROCESS_REPOS=1 GHA2DB_FETCH_COMMITS_MODE=0|2` (unless
    `GHA2DB_GETREPOSSKIP`; `Updating git repos for all projects`, `Updated git
    repos, took: …` / `Error updating git repos (took …): …` on stdout and
    time-stamped on stderr); `ClearOrphanedLocks`; per project (`sync_probabilty`
    → `Skipping #N name`) `gha2db_sync` with `GHA2DB_PROJECT`, `PG_DB`,
    `ENV_SET=1` and the project's `env` (`Syncing #N name`, `Synced name,
    took: …` / `Error result for name (took …): …`, the sync of the next
    project continues); `GHA2DB_WEBSITEDATA` → `website_data` (`Generating…`,
    `Generated website data, took: …` / `Error generating website data (took
    …): …` on stdout and `Error generating website (took …)` — sic — on
    stderr); `Synced all projects in: …` / `There were sync errors, took: …`.
    Commands are `./`-prefixed with `GHA2DB_LOCAL_CMD`, found in `$PATH`
    otherwise, and run through the ported `ExecCommand` (`GHA2DB_CMDDEBUG`
    piping, captured output printed on failure, `exit status N`).
  * The Go⇄Rust tests (`compat/fixtures/devstats`, the real
    `cncf/devstats` `projects.yaml` included) run both binaries in a scratch
    "checkout" with fake `get_repos`/`gha2db_sync`/`website_data` scripts that
    record their arguments and environment (and whether the PID file names
    their parent) against per-project scratch databases holding
    `gha_computed` flag rows; they cover plain syncs and ordering, project
    `env` merging (including values overriding `GHA2DB_PROJECT`), disabled
    projects, `GHA2DB_PROJECTS_OVERRIDE`, `ONLY`, `sync_probabilty` 0/1,
    projects sharing an `order`, custom/missing/malformed/empty yaml, the real
    yaml (254 enabled projects; the three `0.99` ones excluded from the
    comparison), `get_repos`/`website_data`/per-project failures,
    `GHA2DB_FETCH_COMMITS_MODE`, `GHA2DB_CMDDEBUG`, `GHA2DB_DATADIR` + `$PATH`
    mode, every provision/running/set-flag path (fresh, expired, negative age,
    missing database), the PID file paths, orphaned-lock clearing and an
    unreachable server; compared: exit code, stdout (durations, database
    names and the flag age masked), the program's stderr lines (time stamp
    masked) and `Error:` lines, the recorded invocations and the final
    `gha_computed` rows.
  * Deviations: the stderr time stamp prints the zone as `+0200 +0200` (Go:
    `+0200 CEST`; masked in the tests); Go's `ErrorType:` lines and stack
    traces of fatal errors are not reproduced; the deferred PID-file removal
    error is reported after the running flags are cleared in both, but Go
    prints its fatal report during the panic unwinding (interleaving with
    stdout differs only when both streams go to the same terminal).

* `hide_data`
  * `cmd/hide_data/hide_data.go` transcribed. Without arguments: the SHA1s of
    `hide/hide.csv` (current directory, then `GHA2DB_DATADIR/hide/hide.csv`;
    `sha1` header rows skipped, missing file = nothing to hide) are
    anonymized in every enabled project database of `projects.yaml`
    (`Processing databases: [db1 db2 …]`): one worker per (database, SHA1)
    pair — `GHA2DB_NCPUS`/`GHA2DB_ST` bounded — opens its own connection and
    runs `update <table> set <column> = 'anon-<sha1>' where
    encode(digest(<column>, 'sha1'), 'hex') = '<sha1>'` (pgcrypto) for the 39
    (table, column) pairs of the Go table (`gha_actors.login/name`,
    `gha_actors_emails.email`, …, `gha_issues_events_labels.actor_login`;
    `gha_issues.dup_actor_login` is listed twice like in Go), reporting `DB:
    <db>, table: <t>, column: <c>, sha: <sha1>, updated N rows` for every
    statement that changed rows. With arguments: the SHA1 of every (trimmed)
    argument is added to `./hide/hide.csv` (`Skipping '<arg>', SHA1 '<sha1>' -
    already added` for known ones; the file is only rewritten — header `sha1`
    plus one SHA1 per line — when something new was added; it is always
    written to the current directory, even when it was read from
    `GHA2DB_DATADIR`).
  * The Go⇄Rust tests (`compat/fixtures/hide_data/schema.sql` — every
    anonymized table with the column types of `structure` and pgcrypto)
    cover the file mode (new/known/trimmed/empty/special-character arguments,
    an untouched file when nothing is new, `GHA2DB_DATADIR` fallback for
    reading while writing to the current directory, blank lines and missing
    header, two-column files, a wrong field count, a missing `hide/`
    directory) and the database mode (every column against seed rows with
    hidden, visible, NULL, multi-row, multi-column and already-anonymized
    values; `GHA2DB_ST`/`GHA2DB_NCPUS`, `ONLY`, disabled projects and
    `GHA2DB_PROJECTS_OVERRIDE`, projects sharing an `order`, `GHA2DB_DATADIR`
    mode, custom/missing/malformed/empty yaml, the real `cncf/devstats`
    `projects.yaml` (254 databases in order), no/empty/non-matching hide
    file, empty databases, idempotency, `GHA2DB_QOUT`, a missing database, a
    database without pgcrypto and an unreachable server); compared: exit
    code, stdout as a set of lines (the Go tool reports from concurrent
    workers walking a randomly ordered map — the Rust port walks the SHA1s
    sorted), `PqError:`/`Error:` stderr lines and every table of every
    database afterwards.
  * Deviations: the SHA1s are processed (and written to `hide/hide.csv`) in
    sorted order instead of Go's random map order; Go's `ErrorType:` lines and
    stack traces of fatal errors are not reproduced; yaml syntax error
    wording differs (yaml.v2 vs the Rust decoder).

* `website_data`
  * `cmd/website_data/website_data.go` transcribed. Reads `projects.yaml`
    (`GHA2DB_LOCAL` → `./projects.yaml`, else `GHA2DB_DATADIR`), writes
    `projects.json` (`{"projects": [{"name", "title", "status", "repo",
    "dashboardUrl" (`https://<name>.<hostname>`, `k8s.` for `kubernetes`),
    "dbDumpUrl" (`https://<hostname>/<db>.dump`), "projectVersion" (the
    trimmed stdout of `last_tag.sh`/`./git/last_tag.sh` run on
    `GHA2DB_REPOS_DIR/<main_repo>` with `GIT_TERMINAL_PROMPT=0`, `-` when the
    project has no `main_repo`)}, …]}` for every enabled project in `order`)
    and then, for every project — concurrently (`GHA2DB_NCPUS` workers) or
    sequentially (`GHA2DB_ST`: `Using single threaded version`) — connects to
    the database **named after the project** (`kubernetes` → `gha`, `all` →
    `allprj`), runs 44 queries (24 hourly, 7 daily and 4 weekly commit
    buckets, the last month's commits, contributors/contributions/companies
    of the discussion window, star deltas for a month/quarter/year, current
    stars, open issues; bots from `util_sql/exclude_bots.sql` excluded) and
    writes `<name>.json` (`{"timestamp", "commits": {"day": [24], "week":
    [7], "month": [4]}, "totals": {"month": {"commits", "stars", …},
    "quarter": {…}, "year": {…}}}`) into `GHA2DB_JSONS_DIR` (`jsons/`);
    `Generated website data in: <duration>` at the end. Output is Go's
    `MarshalIndent` layout (2-space indent, `\u0026`/`\u003c`/`\u003e` HTML
    escapes, no trailing newline, RFC3339Nano timestamps in the local zone).
  * The Go⇄Rust tests (`compat/fixtures/website_data/schema.sql` — the four
    queried tables with the column types of `structure` — and a copy of the
    real `util_sql/exclude_bots.sql`) cover the projects file (order, disabled
    projects, `ONLY`, `GHA2DB_PROJECTS_OVERRIDE`, duplicate `order`, the
    `kubernetes`/`all` database mapping, HTML characters in titles, a project
    version from a tag script/`GHA2DB_LOCAL_CMD`/a failing script/no
    `main_repo`, `GHA2DB_REPOS_DIR`, `GHA2DB_DATADIR`, custom/missing/empty
    yaml), the statistics (seeded commits, texts, forkees and issues at
    bucket boundaries, bots, no forkees at all, empty databases, several
    projects with different data, `GHA2DB_ST` and up to 4096 workers,
    `GHA2DB_JSONS_DIR`, an existing/missing output directory, `GHA2DB_QOUT`
    query echo, a missing database, a missing `exclude_bots.sql`, an
    unreachable server); compared: exit code, stdout as a multiset of lines
    (durations, paths and the `GHA2DB_QOUT` time argument masked, banner
    first and `Generated website data in:` last), `Error:`/`PqError:` stderr
    lines, and `projects.json` plus every `<project>.json`: same file names,
    semantically equal contents (`timestamp` checked for Go's RFC3339Nano
    format and the same UTC offset, then masked) and byte-identical
    formatting once the random Go key order is normalised.
  * Deviations: JSON object keys are emitted in sorted order (Go's
    `jsoniter.MarshalIndent` of a map iterates randomly — the files are
    semantically identical); the per-project files are written in
    `projects.yaml` order in the multi-threaded mode too (Go's is the order
    in which the workers finish); Go's stack traces of fatal errors are not
    reproduced.

* `webhook`
  * `cmd/webhook/webhook.go` transcribed on top of `devstatscode::http` — a
    small `net/http`-compatible server (`ServeMux` semantics: exact/subtree
    patterns, `301`/`307` redirects to the cleaned path, `404 page not found`,
    `*`/`CONNECT` handling; `Date`/`Content-Length`/`Connection` headers,
    HTTP/1.0 and keep-alive, chunked bodies, `Expect: 100-continue`,
    `417`/`400`/`501`/`505` errors with Go's exact status lines and bodies —
    verified byte-for-byte against Go 1.27 on some 60 edge-case requests),
    `devstatscode::gourl` (`url.QueryUnescape`, `ParseQuery`,
    `PostFormValue`) and `devstatscode::httpclient` (`ureq`, used for the
    Travis public key). `GHA2DB_PROJECT_ROOT` is required (`You need to define
    reposiory path via GHA2DB_PROJECT_ROOT=/path/to/repo <argv0>`, exit 0);
    listens on `GHA2DB_WHHOST` (`127.0.0.1`) + `GHA2DB_WHPORT` (`:1982`, a
    missing `:` is prepended) and serves `GHA2DB_WHROOT` (`/hook`); prints
    `WebHook processing event <remote> at <time>`, the config line and the
    `WebHook: repo/branch/status/type/result/author/message` lines with Go's
    `%v` formatting of the allowed lists. Without
    `GHA2DB_SKIP_VERIFY_PAYLOAD` the Travis `Signature` header is verified
    with the key from `https://api.travis-ci.org/config` (SHA1 PKCS#1 v1.5 —
    `rsa`/`sha1`/`base64`); a payload is deployed when the repository is
    `cncf/devstats` or `cncf/devstatscode`, and branch/status/type/result are
    in `GHA2DB_DEPLOY_BRANCHES/STATUSES/TYPES/RESULTS` (defaults `master`,
    `Passed,Fixed`, `push`, `0`), otherwise `401 {"message": "webhook:
    webhook: skipping deploy …"}` (the doubled prefix is Go's). Deploys run
    in `GHA2DB_PROJECT_ROOT` (`chdir` error → 401) after waiting for another
    instance's `/tmp/webhook.pid` (up to 3800 s, `Another `webhook` instance
    was running, waited N seconds`): `git checkout <branch>`, `git pull`,
    `make`, `make install` (`make` is skipped for `devstatscode`), and with
    `[deploy]` in the commit message (unless `GHA2DB_SKIP_FULL_DEPLOY`)
    `./devel/deploy_all.sh` with `FROM_WEBHOOK=1` after checking `PG_PASS`
    (`environment variable PG_PASS must be set`). Failing commands answer
    `401 {"message": "webhook: exit status N"}` after `ExecCommand`'s
    stdout/`STDERR:`/`Command, arguments, environment:` dump, success is
    `200 {"message": "ok"}` after `WebHook: deployed via 'make install' in
    <duration>`; removing the PID file under a running deploy is a handler
    panic (`http: panic serving <addr>: stacktrace: …`, the connection is
    closed) and the server keeps serving, exactly like Go's recovered panic.
  * The Go⇄Rust tests start both servers on free ports with fake `git`,
    `make` and `devel/deploy_all.sh` scripts (recording arguments, working
    directory, `FROM_WEBHOOK`, `PG_PASS`, whether the PID file exists; each
    can be made to fail) and compare every response byte-for-byte (`Date`
    masked), stdout (durations/addresses masked), the `webhook:` stderr
    lines, the recorded commands and the PID file: successful deploys of
    both repositories (Unicode commit messages, `[deploy]` with/without
    `PG_PASS`, `GHA2DB_SKIP_FULL_DEPLOY`, `GHA2DB_WHROOT`/custom allowed
    lists, a port without `:`), every reason to skip, a failing `git`
    (`exit status 3` + captured output), a failing `deploy_all.sh`
    (`Environment Override` map), a missing `make`, a nonexistent project
    root, waiting for another instance, the PID-file-removed panic, bodies
    shorter than `payload=`/invalid escapes, jsoniter-style decoding
    (case-insensitive keys, `null`s, a wrong type — wording masked),
    routing (`/hook/`, `/x/../hook`, `/HOOK`, query strings, `HEAD`, a `/`
    root pattern), the wire (missing/malformed `Host`, `HTTP/9.9`, chunked,
    HTTP/1.0 with and without keep-alive, `Expect: 100-continue`, two
    pipelined requests), check-payload mode (dead Travis endpoint) and a bind
    failure.
  * Deviations: JSON decoding error *wording* is `serde_json`'s instead of
    jsoniter's (both `401 {"message": "webhook: …"}`); Go's `ErrorType: …`
    stderr lines and stack traces are not reproduced; Go's `FatalOnError`
    sleeps 15 minutes on `cannot assign requested address` (the tool would
    then exit 0) — the Rust tool exits 2 at once; resolver errors say `lookup
    X: no such host` without Go's `on 127.0.0.53:53` suffix; request bodies
    are read before the handler runs (Go reads lazily — only observable as
    a `100 Continue` that Go does not send when the Travis key fetch fails
    first); multipart form bodies are not parsed (Travis sends
    `application/x-www-form-urlencoded`). Note that `api.travis-ci.org` is
    gone, so the signature verification mode cannot succeed with either
    implementation today.
* `sqlitedb`
  * `cmd/sqlitedb/sqlitedb.go` on `rusqlite` (bundled SQLite, the counterpart
    of `mattn/go-sqlite3`): export (`sqlitedb grafana.db` → `sqlite/<slug>.json`
    for every `dashboard` row, pretty-printed), import (`sqlitedb grafana.db
    <json>…` — new uids are inserted with the Go tool's exact column values,
    known uids are compared by title, slug and pretty-printed data, updated
    with a `<json>.was` snapshot of the previous data, tags synchronised in
    `dashboard_tag`, the original file backed up once as `<db>.<UnixNano>`
    before the first change) and delete (`sqlitedb grafana.db 1,2,3` — exactly
    one extra argument whose comma-separated items all pass `strconv.Atoi`,
    so `sqlitedb db 5` deletes uid 5 while `sqlitedb db 1,,2` tries to import a
    file named `1,,2`). All messages, the `GHA2DB_QOUT` query echo (`%+v` of
    the arguments including `time.Now()` with its `m=+…` monotonic part, no
    argument line for argument-less statements) and the `GHA2DB_DEBUG` 1/2
    dumps (`%+v` of `dashboardData` is its `String()`) are reproduced, as are
    go-sqlite3's timestamp binding (`2006-01-02 15:04:05.999999999-07:00`),
    `database/sql`'s error texts and go-sqlite3's `: <os error>` suffix on
    `unable to open database file` (recovered by replaying SQLite's `open(2)`
    sequence, since rusqlite appends the path instead). Dashboard JSONs are
    decoded like jsoniter into the `{title, uid, tags}` struct
    (case-insensitive keys, `null` → zero values, wrong types fatal).
  * Tests: 36 Go⇄Rust scenarios on databases built from the real Grafana
    schema (`compat/fixtures/sqlitedb/schema.sql`) seeded with four real
    Prometheus dashboards: export (also of an empty DB, without the `sqlite/`
    directory, with a missing/garbage/directory database path), import into
    an empty database, unchanged re-import (no backup, no `.was`), the real
    `import_jsons_to_sqlite.sh` scenario (title→slug change, tags change,
    panel change, unchanged and brand-new dashboards; also with `GHA2DB_DEBUG`
    1/2 and `GHA2DB_QOUT`), jsoniter decoding quirks, inconsistent `dashboard`
    rows, failures (missing/invalid JSON, wrong JSON types, duplicate uid among
    the JSONs, duplicate new uid / existing title hitting the unique indexes,
    read-only database, missing database or database directory), delete mode
    (comma lists, single numeric argument, `strconv.Atoi` edge cases such as
    `+68`, `-5`, ` 5`, int64 overflow) and an export→import round trip.
    Compared: exit code, stdout (durations, backup names and `time.Now()`
    masked; as a sorted multiset where Go walks a map), the `Error:` lines,
    every `dashboard` row and the `(dashboard_id, term)` tag set, the exported
    files, `.was` snapshots and backups (which must equal the original file).
    Building the Go reference binary needs cgo (a C compiler).
  * Deviations: tags are inserted/deleted and updates are processed in sorted
    order (Go: random map order — only the interleaving of the messages and
    the `dashboard_tag.id` values differ); jsoniter's error wording for
    invalid JSON is not reproduced (exit code and stream are).
* `merge_dbs`
  * `cmd/merge_dbs/merge_dbs.go` transcribed: `GHA2DB_INPUT_DBS` (or `-all-`
    expanded from `projects.yaml` — enabled projects whose trimmed `shared_db`
    equals the output DB, sorted by `order`/name/db and deduplicated, with
    `SKIP_DBS` and `IGNORE_NO_DB=1` pinging each DB and skipping the missing
    ones) merged into `GHA2DB_OUTPUT_DB` table by table in two passes
    (`gha_actors` — only without `GHA2DB_AFFILIATIONS_DB` —, `gha_events`,
    `gha_issues`, `gha_labels`, `gha_payloads` split on `id > 0` / `id <= 0`);
    `ONLY_TABLES`/`SKIP_TABLES`; `MERGE_DT_FROM`/`MERGE_DT_DROM` (five Go
    `time.Parse` layouts emulated, the per-table date column / `event_id in
    (select …)` mapping and the "no merge date mapping" notices); row mode
    (positional `insert … values($1…)`, unique violations counted as
    collisions, any other server error prints `Failing values:` with Go's
    `%+v` of the scanned values — driver timestamps `+0000 +0000`, `<nil>`,
    raw `numeric`/`text[]`/`jsonb` bytes as `[49 46 53]` — then dies) and
    `USE_BATCH` mode (`insert … values (…),(…) on conflict do nothing`,
    `BATCH_SIZE` clamped to [2, 1000] and capped at 65535 parameters per
    statement — 70-column `gha_pages` → 936 rows —, `RowsAffected` counting),
    `PARALLEL` ∈ [1, 16] worker threads per pass, `lib.ProgressInfo`
    reports, the `PqError:`/`DURABLE_PQ` behaviour of `FatalOnError` and the
    closing `Consider running './devel/remove_db_dups.sh'…` hint.
  * `GHA2DB_QOUT` echoes are byte-identical (lib/pq's `FormatRawBytes` for
    byte arguments, `(null)` for NULLs, the driver's nameless zone for
    timestamps read from a DB vs `+0000 UTC` for the parsed `MERGE_DT_FROM`).
  * Go⇄Rust tests: `cmd/merge_dbs/tests/compat.rs` — 2/3 generated input DBs
    (`compat/fixtures/merge_dbs/schema.sql`, the 32 merged tables including
    the key-less `gha_issues_events_labels`/`gha_texts` and the wide
    `gha_pages`, plus the never merged `gha_companies`) with overlapping keys,
    negative ids, NULL variants and raw-bytes columns; empty, pre-seeded and
    column-mismatched output DBs; `-all-` mode with a `projects.yaml`
    (blank-padded `shared_db`, duplicate DBs, disabled/overridden projects,
    missing DBs with and without `IGNORE_NO_DB`, `SKIP_DBS`, no matching
    projects, missing/invalid yaml); date filter formats and errors; table
    filters and their fatals; batch sizes, caps and clamps; `PARALLEL`
    clamps; missing input/output DBs; `DURABLE_PQ`; `GHA2DB_QOUT`. Compared:
    exit code, stdout (durations, DB names and the banner time masked; sorted
    when `PARALLEL` > 1), `Error:`/`PqError:` stderr lines and every table
    of the output database.
  * Deviations: with several unknown `ONLY_TABLES`/`SKIP_TABLES` entries Go
    names a random one (Rust the alphabetically first); the interleaving of
    the per-table messages with `PARALLEL` > 1 is scheduling dependent on
    both sides; yaml syntax error wording (yaml.v2 vs the Rust decoder).
* `gha2db_sync`
  * `cmd/gha2db_sync/gha2db_sync.go` transcribed: the per-project arguments
    (`GHA2DB_PROJECT` looked up in `projects.yaml` — `GHA2DB_PROJECTS_YAML`,
    `GHA2DB_DATADIR` unless `GHA2DB_LOCAL`), `GHA2DB_LOCAL_CMD` (`./`
    prefixed sub-commands), `GHA2DB_PROJECTS_COMMITS`, the `ClearDBLogs`
    call, the `gha_parsed`/`gha_events` max-date probe (`GHA2DB_STARTDT`,
    `GHA2DB_DEFAULT_START_DATE`) and the `gha2db` → `get_repos` (with
    `GHA2DB_PROCESS_COMMITS`/`GHA2DB_PROCESS_REPOS`) → `ghapi2db` →
    `structure` (`GHA2DB_SKIPTABLE`… env) → `vars` chain (`GHA2DB_SKIPPDB`
    skips it), the randomised `dailyRecalcHour` logic gating `tags`,
    `annotations` and `columns` (`GHA2DB_SKIP_TAGS/ANNOTATIONS/COLUMNS`,
    `GHA2DB_RUN_COLUMNS`), `GHA2DB_RESETTSDB`/`GHA2DB_RESETRANGES`,
    `GHA2DB_TSDB_PROJECT`, the quick ranges / TS range computation and the
    `metrics.yaml` (`GHA2DB_METRICS_YAML`, `/shared/` fallback) driven
    `calc_metric` invocations: `periods` × `aggregate` with `skip`,
    `ComputePeriodAtThisDate` (`GHA2DB_COMPUTE_ALL`, `GHA2DB_FORCE_PERIODS`
    keyed by the bare period, `GHA2DB_RECALC_RECIPROCAL`, `always_recalc`),
    `annotations_ranges`, `add_period_to_name`, `multi_value`, `escape_value_name`,
    `desc`, `series_name_map`, `drop:` (`GHA2DB_ENABLE_METRICS_DROP`), the
    `env:` map with the `[a-z]+:` period-conditional keys, `GHA2DB_SKIP_RAND`
    (metric shuffling), `GHA2DB_EXCLUDE_METRICS`/`GHA2DB_ONLY_METRICS` and
    project exclusions, `GHA2DB_ALLOW_METRIC_FAIL`/`allow_fail`/
    `wait_after_fail` and the histogram jobs collected and run at the end —
    `GHA2DB_ST`/`GHA2DB_NCPUS`/`GHA2DB_MAX_HIST` threads —, `GHA2DB_ONLY_ENV`,
    the closing `Time:` line.
  * Go⇄Rust tests: `cmd/gha2db_sync/tests/compat.rs` — every sub-command is a
    fake script recording its arguments and `GHA2DB_*` environment; scratch
    project databases (`gha_events`/`gha_parsed`/`gha_computed`/quick ranges)
    per side plus an old row in `devstats.gha_logs` to observe `ClearDBLogs`;
    the whole default flow, cron (`PATH`/`GHA2DB_DATADIR`) vs local mode,
    every skip/force knob, dates and quick ranges, metric filtering, period
    and aggregate combinations, `FORCE_PERIODS`, `COMPUTE_ALL`, reciprocal
    recalcs, histograms in ST and MT modes with tolerated/fatal failures and
    `wait_after_fail`, env maps, `series_name_map`/`desc`/`drop`, all the
    fatal paths (missing/unknown project or yaml, bad periods/aggregates,
    failing sub-commands, invalid env keys). Compared: exit code, stdout
    (durations, timestamps, random hours and the scratch paths masked; sorted
    where parallel histograms make the order scheduling dependent), the
    `Error:` lines of stderr, the recorded invocations and the `gha_logs`
    clearing.
  * Deviations: Go prints local non-UTC timestamps with a zone *name*
    (`CEST`), Rust with the numeric offset (`+0200 +0200`); a failing
    histogram is reported before the deferred "Calculated histogram" line on
    both sides but Go's unbuffered result channel delays that line until the
    main thread receives it (ordering only); Go's random map order in
    `processEnvMap` when a conditional key collides with a plain one; yaml
    error wording; the dead helpers `addPeriodSuffix`, `joinedCartesian` and
    `createSeriesFromFormula` are not ported.
* `import_affs`
  * `cmd/import_affs/import_affs.go` transcribed: the JSON (`GHA2DB_AFFILIATIONS_JSON`
    or argv[1], default `github_users.json`; `GHA2DB_DATADIR` prefixed unless
    `GHA2DB_LOCAL`) is decoded with a Go/jsoniter-compatible struct decoder
    (case-insensitive keys, last duplicate wins, `null` elements, `int`
    fields, the "unexpected EOF"/"invalid character" fatals), the
    `companies.yaml` acquisitions map (`GHA2DB_COMPANY_ACQ_YAML`, missing →
    no mapping, malformed / self-mapping / duplicate destination / cyclic
    definitions are fatal, regexps compiled through `goregex`,
    `GHA2DB_SKIP_COMPANY_ACQ`), `hide.csv` (`hide/hide.csv` or
    `GHA2DB_DATADIR/hide/hide.csv`, sha1 of the lowercased login), the
    `GHA2DB_CHECK_IMPORTED_SHA`/`GHA2DB_ONLY_CHECK_IMPORTED_SHA` flow on
    `gha_imported_shas` (sha256 of the file), `GHA2DB_DRY_RUN` (exit 2 after
    the summary), then the seven phases: actors (login/name score, `null`
    names, ≤120-byte truncation, per-login email/name/country/sex/tz/age
    aggregation with the `tz` → `tz_offset` lookup through
    `pg_timezone_names`), the `gha_actors` insert/update (`Added actors:`,
    `updated actors:`, `non-changed:`), `gha_actors_emails`/`gha_actors_names`
    (`emails added up to`/`names added up to` count attempts), the two-way
    login/id correlations (`findActors`, `gone too deep:` non-fatal error at
    depth 10, `new logins`), companies (`processCompany`: shortened
    original and mapped names, `Mapped to 'X' … checked regexp: N, cache
    hit: M` / `Non-acquired companies:` statistics), affiliations
    (`processRoll`: `< date` markers, quoted names, `Unknown`/`NotFound`
    placeholders, `Affiliations added up to:`), `GHA2DB_ST` / `GHA2DB_NCPUS`
    (≤10 workers), `Time:`.
  * Go⇄Rust tests: `cmd/import_affs/tests/compat.rs` — 46 scenarios on scratch
    databases (the six tables' DDL from `structure`), probe JSON, the real
    `util_json/test_affs.json` + `companies.yaml` (ST and MT), two-phase runs
    with a correlation SQL step in between (ST and MT), argv/env/`DATADIR`
    file resolution, missing/malformed JSON and YAML (7 JSON syntax cases),
    every acquisitions validation fatal, invalid regexps, skipped
    acquisitions, long company/affiliation names, the imported-sha flow
    (check / only-check / new file), dry run, hidden logins (with and without
    the csv header), scoring, updates of existing actors, deep correlation
    chains, markers/quotes in affiliations, JSON key/null handling,
    idempotent re-imports, deterministic tie-breaking (several names /
    equally long affiliation definitions for one login, imported twice),
    names longer than the 120-byte column / hidden names re-imported.
    Compared: exit code, stdout (`Time:` masked; the per-company statistics
    and `gone too deep` lines as a multiset), the `Error:`/`PqError:` stderr
    lines and all six tables — byte for byte, including `gha_actors.name`.
  * Deviations: jsoniter error wording (`Error: '…'` line) for malformed JSON
    is not reproduced (exit code is). Bug 49 (fixed in Go, Rust already
    behaved so): with several names for one login, or several equally long
    affiliation definitions of the same source priority, Go stored a random
    map key — in the real `github_users.json` that is 215 logins whose name
    and 397 logins whose company (393 of them *different* companies, e.g.
    CloudBees vs Red Hat) changed on every daily import; both sides now take
    the smallest (byte order) candidate, and acquisition regexps are applied
    in `companies.yaml` order (first match wins). Bug 51 (both sides): the
    re-import compared the raw name with the stored one (truncated to 120
    bytes, hidden if configured), so such actors were "updated" to the same
    value on every run.

* `calc_metric`
  * `cmd/calc_metric/calc_metric.go` transcribed: `series_name_or_func sql_file
    from to period [options]` (fewer arguments → banner + usage on stdout, exit
    1), the comma-separated options (`hist`, `desc:time_diff_as_string`,
    `multivalue`, `escape_value_name`, `skip_escape_series_name`,
    `annotations_ranges`, `skip_past`, `merge_series:name`, `custom_data`,
    `custom_data_unique_time` (alone → fatal), `drop:t1;t2` (needs
    `GHA2DB_ENABLE_METRICS_DROP`, forbidden with `hist`, `Truncating table`
    unless `GHA2DB_DEBUG=-1`, `warning: failed dropping table` on dependent
    objects), `project_scale:f` (invalid/negative → `1.0`, printed with `%f`),
    `series_name_map` (`map[k:v …]` exact lookup); every interval of
    `GetIntervalFunctions` (`hN` steps hourly with an N-hour window, `dN`,
    `w`, `m`, `q`, `y`; unknown → exit 1), `to` inclusive, from > to → nothing
    computed (Go bug 36); the query placeholders (`{{from}}`/`{{to}}`, `{{n}}`,
    `{{period}}`, `{{range}}`, `{{exclude_bots}}` from `util_sql/exclude_bots.sql`
    under `./` (`GHA2DB_LOCAL`) or `GHA2DB_DATADIR`, `{{project_scale}}`,
    `{{rnd}}`); the naming functions (`multi_row_single_column` /
    `multi_row_multi_column` / `single_row_multi_column` with the `prefix,`,
    `prefix;repo;a,b`, backtick and `series_name_map` forms, escaped or
    verbatim names, `Info:` lines for unusable row names); single values
    (`NULL` → 0, several single-number rows → non-fatal `Error:` lines and the
    last row); descriptions (`descr` column), multivalue columns (`name_t/_v/_s`
    with `custom_data`), custom data (`dt`/`str` columns, `series` when merged,
    `custom_data_unique_time` de-duplication); histograms (`Histogram running
    interval …`, `{{period}}` = `N interval` (`3N month` for quarters), fake
    times counting down hourly from 2012-07-01, the period's rows deleted
    first, `annotations_ranges` from `tquick_ranges` (`Found quick range:`,
    `quick range not found` fatal, `skip_past` + `gha_computed` bookkeeping
    with the last two path components as key, `Skipping past quick range: …
    (already computed)`), `range:YYYY-MM-DD,YYYY-MM-DD` (normalised to full
    timestamps, one date → fatal), typed multivalue specs `name:f|s`, merged
    histogram series); `hll` values (`hll_empty()` for `NULL`, text round
    trip); `GHA2DB_SKIPTSDB` (no tables, no bookkeeping); `GHA2DB_NCPUS`
    workers (`Running (on N CPUs)`, sorted interleaving); `gha_last_computed`
    written by the deferred `setLastComputed` also on fatal errors (Go
    `defer` emulated through `error::defer`, which also captures the `range:`
    abbreviation normalised later); the `Ignored grant select on` lines;
    `GHA2DB_DEBUG` output (`NewTSPoint:`/`AddTSPoint:`/`#N` points, `structural
    sqls:` DDL, `upserts: N`).
  * Go⇄Rust tests: `cmd/calc_metric/tests/compat.rs` — 98 scenarios on scratch
    databases (a 60-event fixture with bots and three repositories,
    `tquick_ranges`, 44 metric SQL shapes in `compat/fixtures/calc_metric/`):
    usage, every interval, MT, project scale, multi-row/-column naming,
    `series_name_map`, multivalue (escaped / verbatim / merged / custom),
    custom data (merged, unique time, multi-column, `NULL` time fatal), temp
    tables in metric SQL, bad SQL (ST and MT), missing SQL / exclude-bots
    files, `GHA2DB_DATADIR` mode, `GHA2DB_SKIPTSDB`, `GHA2DB_DEBUG`, the drop
    option (enabled / disabled / quiet / SKIPTSDB / dependent view / with
    `hist`), histograms (d/q/d7 with `{{n}}`, rerun replaces the period's
    rows, annotation ranges, `skip_past` reruns, open-ended range, unknown
    range, explicit `range:` (valid / malformed), multi-row single/multi
    column, merged, custom data, multivalue with typed specs, the `NULL` /
    bad-type / two-names / `NULL` name fatals, project scale, SKIPTSDB, DEBUG)
    and `hll` (single, `NULL`, multi-row, multi-column, multivalue, custom,
    MT; skipped when the extension is missing). Compared: exit code, stdout
    (`Time:`, timestamps and the `added` time of debug points masked; the
    `Ignored grant`/DDL lines and MT output as a multiset), the
    `Error:`/`PqError:` stderr lines, every created series table (columns,
    indexes, rows) and the `gha_last_computed`/`gha_computed` bookkeeping.
  * Deviations: Go's random map order for multivalue columns, the `Ignored
    grant select on` / debug DDL lines and `Quick ranges: %+v` (Rust: sorted);
    Go's `strconv`/`sql: Scan error` wording of the multivalue `NULL` fatals;
    the interleaving of MT output; the dead `mergeESSeriesName` helper is not
    ported.
* `annotations`
  * `cmd/annotations/annotations.go` + the library's `annotations.go`
    transcribed (`devstatscode::annotations`): `GHA2DB_PROJECT` required,
    `projects.yaml` (`GHA2DB_PROJECTS_YAML`) from `./` (`GHA2DB_LOCAL`) or
    `GHA2DB_DATADIR`, unknown project → fatal `project 'x' not found in
    'projects.yaml'`; with a `main_repo` the tags of
    `<GHA2DB_REPOS_DIR><org/repo>` are listed through `git_tags.sh`
    (`./git/git_tags.sh` with `GHA2DB_LOCAL_CMD`, else from the PATH; env
    `GIT_TERMINAL_PROMPT=0`; `org/repo` format checked first), filtered by
    `annotation_regexp` (invalid → exit 2), tags with an empty time skipped
    (debug line), a non-numeric time reported (`Invalid time returned for
    repo: …`, always) and skipped, tags before 2012-07-01 skipped (debug
    line), the message cut at 40 **bytes** with `\n`/`\r`/`\t` → space,
    sorted by date and de-duplicated per hour (`Skipping annotation {…}
    because its hour date is the same as the previous one`; the dedupe starts
    at 2012-07-01 00:00, so a tag in that very hour is dropped too — as in
    Go); without a `main_repo` the fake `Project start` / `First CNCF project
    join date` annotations (only for sane dates ≥ 2012-07-01, join > start)
    or the single `Project start` one. `ProcessAnnotations`: the
    `annotations` points (fields `title`/`description`), the milestone
    annotations (`Project start date`, `CNCF join date` — both only when join
    > start; `Moved to incubating state`, `Graduated`, `Archived`), the
    `quick_ranges` tag points (12 fixed periods from 2012-07-01 00:00 hourly,
    `a_i_j` / `a_i_n` between consecutive annotations and to
    `NextDayStart(now)`, `c_b`/`c_n` and — in the right order and after the
    join date — `c_j_i`, `c_i_g`/`c_i_n`, `c_j_g`, `c_g_n`), the stale
    `like '%_n'` rows deleted first (when the table and column exist),
    `WriteTSPoints`, and the `annotations` points copied as
    `annotations_shared` (period = project, field `repo` = main repo) into
    `shared_db` unless `GHA2DB_SKIP_SHAREDDB`; `GHA2DB_SKIPTSDB` writes
    nothing (`Skipping annotations series write` debug line). Time handling
    is Go's: tag times are local (`time.Unix`) and `HourStart` relabels the
    wall clock as UTC, so the stored times and range strings depend on `TZ`
    exactly like the Go binary's (`time::wall_as_utc`). Go strings are byte
    strings, so the script output is processed as bytes
    (`exec::exec_command_bytes`): the 40-byte cut may split a multi-byte
    character, which — like Go's `SafeUTF8String` — is dropped when written,
    NUL bytes are removed, invalid UTF-8 disappears.
  * Go⇄Rust tests: `cmd/annotations/tests/compat.rs` — 73 scenarios, each
    on scratch databases (plus a scratch shared database where needed) and a
    scratch directory with `projects.yaml`, `git/git_tags.sh` (the real
    script from `compat/fixtures/annotations/`, a stand-in `sh` script, or
    none) and a real git repository built with fixed dates
    (`GIT_CONFIG_GLOBAL=/dev/null`, lightweight + annotated tags): the full
    project (all dates, regexp, debug), every milestone-date combination
    (start/join only, join ≤ start, incubating/graduated in the wrong order or
    before the join date, archived, incubating without join, dates with
    offsets), the fake-annotation paths, `shared_db` (written, skipped, no
    main repo, rerun), `GHA2DB_SKIPTSDB`, reruns (idempotent, stale `%_n`
    rows — `_` being a LIKE wildcard —, pre-existing tables with a missing or
    an extra column, edited rows restored), tag edge cases (first hour of
    2012-07-01, several tags per hour, 40-byte cuts of 2-/3-/4-byte
    characters, unicode names, no tags, 30 tags), broken `git_tags.sh`
    output (empty / non-numeric / negative / epoch / year-2100 / `+`-signed
    times, 1/2/4 fields fatal, trimming incl. NBSP and CRLF, control
    characters and NULs, invalid UTF-8 in messages and names, duplicate
    names, unsorted output, exit ≠ 0 after output, stderr on success, the
    script's argument and environment), the error paths (invalid regexp, bad
    `main_repo`, missing repository / script (`./git/` and PATH) / yaml /
    `GHA2DB_PROJECT`, unknown project, invalid or empty yaml), PATH mode,
    `GHA2DB_DATADIR` mode, a custom yaml name and `TZ=Europe/Warsaw`,
    `America/New_York`, `Asia/Kolkata` (shifted hours, half-hour zone, tags
    moving into one local hour). Compared: exit code, stdout (durations, the
    `added` stamp of debug points and zone names masked; the DDL/`grant` lines
    as a multiset with the `create table` columns sorted), the `Error:`
    stderr lines and afterwards `sannotations`, `tquick_ranges` (columns,
    indexes, rows) and the shared `sannotations_shared`.
  * Deviations: `%v` of a non-UTC local time prints `+0200 +0200` instead of
    Go's `+0200 CEST` (the `Skipping annotation {…}` debug line only); an
    invalid `annotation_regexp` is a fatal `Error:` (exit 2) instead of Go's
    `MustCompile` panic (exit 2 too), with the regex crate's wording; the
    debug `Series: annotations:` line shows the message after the invalid
    bytes were dropped (Go prints the raw bytes); tags with identical
    timestamps are sorted stably (Go's `sort.Sort` is not stable).
* `get_repos`
  * `cmd/get_repos/get_repos.go` → `src/main.rs` and
    `cmd/get_repos/fetch_commits.go` → `src/fetch_commits.rs` transcribed.
    `GHA2DB_GETREPOSSKIP` makes the tool print only the final timing line.
    Otherwise `projects.yaml` (`GHA2DB_LOCAL` / `GHA2DB_DATADIR` /
    `GHA2DB_PROJECTS_YAML`) gives the enabled projects (`GHA2DB_PROJECTS_COMMITS`
    filter, `disabled`, `files_skip_pattern`, `main_repo`), their databases
    and the `gha_repos` of every database (`org/repo` names only, orgs
    grouped, all-project `dbs`/`repos`/`repoDBs` maps) and the four phases run
    in order: **repos** (`GHA2DB_PROCESS_REPOS`: `git/git_reset_pull.sh` on
    an existing clone or `git clone` into `GHA2DB_REPOS_DIR/org/repo`,
    per-org goroutines/threads, `GHA2DB_EXTERNAL_INFO` prints the
    `all_repos.sh`/`all_orgs.sh` style listings, `Pulled …`/`Cloned …`
    debug and the `Successfully processed N/M repos` summary), **backfill**
    (`GHA2DB_FETCH_COMMITS_MODE` 1 = PushEvents without `gha_commits`
    rows, 2 = also truncated payloads; per-DB `selectPushEventsNeedingCommits`
    since the last processed event (or `GHA2DB_STARTDT`),
    `git/git_commits_range.sh` before..head per event (a `128` exit is
    retried once with `git fetch`, empty ranges are warned about, ancestry is
    not enforced), `git/git_commits.sh` metadata in `GHA2DB_GIT_COMMITS_BATCH`
    batches with bisection on failure (base64 fields, `♂♀` separators),
    actor lookup chain login → `gha_actors.name` → `gha_actors_names` →
    `gha_actors_emails` (highest id) with `hide.csv` anonymisation
    (`anon-<sha1>`), `author_id`/`committer_id` 0 when unknown, trailer roles
    into `gha_commits_roles`, `Warning: … payload size=N, computed commits=M`),
    **orphan restore** (`GHA2DB_RESTORE_ORPHAN_COMMITS`: commits that
    *landed* on `refs/remotes/origin/HEAD` → `origin/main` → `HEAD` and on
    every other `refs/remotes/origin/*` branch whose tip moved within
    `GHA2DB_ORPHAN_COMMITS_RANGE` (a PostgreSQL interval, default `8 hours`;
    `GHA2DB_ORPHAN_COMMITS_DEFAULT_BRANCH_ONLY` limits the scan to the
    default branch) and have no `gha_commits` row — the window is the
    branch's first-parent history since `git rev-list -1 --first-parent
    --before=<since>` (`<boundary>..<ref>`, so merged commits with old
    author/committer dates are found too), grouped by the first-parent
    step that brought them in into GHA-shaped artificial negative
    `PushEvent`s (`negative_artificial_id(PushEvent, repo, head)`, payload
    `ref=refs/heads/<branch>`, `head`, `befor` = first parent, `size` =
    commits landed, `created_at` = the step's committer date, actor = the
    step's committer or, for GitHub's web-flow `noreply@github.com`
    committer, its author); an existing PushEvent of the same repo and head
    is reused (its commits join it), other events with the same id are
    conflicts and skipped, commits are handled once per clone across
    branches, `gha_skip_commits` honoured, then the targeted
    `RunEventIDsPostprocessDB` (or `targeted postprocess skipped: gha_texts
    is empty…`); `GHA2DB_ORPHAN_COMMITS_NO_GROUPING` restores the legacy
    shape — one event per commit whose *commit date* is within the range
    (`git log --since=YYYY-MM-DD`), ref = the remote ref, no `befor`,
    author and author date) and **commits** (`GHA2DB_PROCESS_COMMITS`:
    `util_sql/list_unprocessed_commits_files.sql` → `git/git_files.sh`
    per commit into `gha_commits_files` (`files_skip_pattern`, `Invalid time`
    breaks, a line without `♂♀` is fatal, no files → `gha_skip_commits`
    reason 1), `util_sql/create_events_commits.sql`, then
    `list_unprocessed_commits_loc.sql` → `git/git_loc.sh` shortstat parsing
    (`N file(s) changed`, `insertion(s)(+)`, `deletion(s)(-)`, garbage →
    zeros) into `gha_commits` (no row → reason 2), `GHA2DB_SKIP_COMMITS_FILES`
    / `GHA2DB_SKIP_COMMITS_LOC`, `Got N (P%) new commit's files/BOC stats, …`
    summaries). `GHA2DB_ST`/`GHA2DB_NCPUS` select the thread count for every
    phase, `GHA2DB_DEBUG` 1/2 the verbosity.
  * Go⇄Rust tests: `cmd/get_repos/tests/compat.rs` — 129 scenarios, each on
    scratch databases (`full_structure.sql`, seeded `gha_repos`, actors with
    emails/names and two PushEvents with payloads) and a scratch directory
    with `projects.yaml`, the real `git/*.sh` + `util_sql/*.sql` fixtures
    from `compat/fixtures/get_repos/` (or wrapped / fake / missing scripts)
    and a real git repository (an upstream with 5 dated commits by two
    authors, trailers and a unicode message, a bare origin at commit 4 and a
    working clone reset to a chosen commit): the repos phase (clone, pull,
    reset failure, no repos dir, repo path is a file, a second org, external
    info, script missing/failing, `GETREPOSSKIP`), backfill mode 1 and 2
    (fresh, rerun, a new event after the rerun, `STARTDT`, hidden emails and
    names, every step of the actor lookup chain, weird payloads — empty,
    equal before/head, unknown head, non fast-forward —, the 128 retry, range
    failures, batch sizes 1/2 with bisection, all-failing / partial /
    corrupt-base64 metadata, one clone shared by two DBs, MT), orphan restore
    (wide/default/invalid ranges, quiet, after a backfill, seeded `gha_texts`,
    rerun, `origin/HEAD` fallback, `gha_skip_commits`, event conflicts, same
    id same event, hide, actor resolution, metadata failures, batches, two
    repos sharing commits, renamed repos (historical alias clones), the push
    shape — landing window with 2020-dated commits merged today, legacy
    `NO_GROUPING` shape and its reuse by a grouped run, web-flow committer,
    all `origin/*` branches / `DEFAULT_BRANCH_ONLY`, stale branches, shared
    history between branches —, MT, two DBs) and the commits phase (fresh, after a
    backfill, files/LOC/both skipped, nothing to do, rerun, debug 2, every
    `git_files.sh` / `git_loc.sh` failure mode incl. empty/invalid times,
    invalid lines, special sizes, garbage and singular/plural shortstats, not
    cloned repo, missing sql, MT, two DBs) plus full-flow runs (ST, MT, two
    DBs × two repos, `GHA2DB_DATADIR` mode). Compared: exit code, stdout
    (durations, `since <ts>` and `[N:<now>` SQL argument dumps masked;
    as a multiset for multi-repo/DB runs), the `Error:` stderr lines, the
    clone's HEAD and afterwards `gha_commits`, `gha_commits_roles`,
    `gha_commits_files`, `gha_events_commits_files`, `gha_skip_commits` (modulo
    `dt = now()`), and the artificial `gha_events`/`gha_payloads` rows.
  * Deviations: Go iterates its `dbs`/`repos`/`repoDBs` maps in random order,
    Rust uses sorted maps — the per-DB/per-repo output order (and which of two
    repositories sharing the same commits restores them as orphans) is
    deterministic in Rust; multi-threaded output interleaving differs; the
    `%q` quoting of actor names/emails is exact for ASCII control characters
    only; prepared statements became per-call executions (same SQL); the
    Rust stderr has no Go stack trace after an `Error:` line.

* `sync_issues`
  * `cmd/sync_issues/sync_issues.go` → `src/main.rs` transcribed on top of the
    new `devstatscode::{github, ghapi}` modules. Same behaviour: the
    `GHA2DB_ISSUES_SYNC_SQL` query (with `FROM1`/`TO1`, `FROM2`/`TO2`, …
    replacements up to the first missing `FROMn`) yields `(repo, number)`
    rows, duplicates are dropped (`Duplicated issue: …` in debug mode),
    invalid repo names are skipped, every issue is fetched with `Issues.Get`
    (and PRs additionally with `PullRequests.Get`) through the
    `GetRateLimits` / `MinGHAPIPoints` / `MaxGHAPIWaitSeconds` /
    `MaxGHAPIRetry` retry loop with `HandlePossibleError` (`404`/`409` →
    `Warning: not found`, `410` → `Warning: issue is deleted`, `301` →
    `Warning: This issue has been transferred`, `502` → retried, rate limit →
    retried, abuse → `2^(try+3)` s sleep, anything else → `<argv0> error:
    <Go type>:<message>, non fatal, exiting 0 status`), a transferred issue
    (number mismatch) is reported, and the final `SyncIssuesState(manual =
    true)` adds artificial `sync` events (`gha_events`/`gha_payloads`/
    `gha_issues`(+labels, assignees)/`gha_milestones`/`gha_actors`,
    `gha_pull_requests`(+assignees, requested reviewers)) only when the latest
    stored state differs, printing the `Issues to process:`/`Issues:`/`PRs:`
    info blocks and the `Manually processed …` summaries. Knobs:
    `GHA2DB_GITHUB_OAUTH` (token list, `-` or a file), `GHA2DB_MIN_GHAPI_POINTS`,
    `GHA2DB_MAX_GHAPI_WAIT`, `GHA2DB_MAX_GHAPI_RETRY`, `GHA2DB_GHAPI_RATE_LIMITS_CACHE`
    (see `ghapi2db` below), `GHA2DB_GITHUB_DEBUG`, `GHA2DB_SKIPPDB`,
    `GHA2DB_ST`/`GHA2DB_NCPUS`, `GHA2DB_DEBUG`, `hide/hide.csv`.
  * `GHA2DB_GITHUB_API_URL` (new, in Go and Rust): overrides the API base URL
    (`https://api.github.com/` by default) — GitHub Enterprise or a test
    server; unset → unchanged behaviour.
  * Go⇄Rust tests: `cmd/sync_issues/tests/compat.rs` — 53 scenarios, each side
    on a scratch database (`full_structure.sql` + a seeded repo event and
    actors) against its own scripted fake GitHub API server
    (`compat/src/github.rs`: routed responses in sequence, per-token rate
    limit state, `/rate_limit`, request log): input handling (no/bad/empty
    SQL, `FROMn`/`TOn`, duplicates, invalid repos), new / unchanged / changed
    issues (state, title, lock, closed_at, milestone, assignee, labels,
    assignees, removals, several stored events, rerun, NULL body, unicode,
    65535-byte truncation), PRs (new, unchanged, merged with milestone /
    assignees / reviewers), every `HandlePossibleError` branch (404, 410,
    409, 502 retried, retry limit, 301 without and with `Location`,
    transferred number, 500, 202, malformed JSON), a dead API (abort / wait
    then give up), `/rate_limit` 403 / `{}` / 500, `Issues.Get` rate limited
    once / always, abuse with `GHA2DB_GITHUB_DEBUG`, `GHA2DB_MIN_GHAPI_POINTS`
    wait / abort, two tokens (hint by points, tie by reset), token file, `-`,
    unset, `GHA2DB_SKIPPDB`, `GHA2DB_DEBUG=2`, `hide.csv`, two repos (repo id
    `-1`), MT. Compared: exit code, stdout (durations, now-derived event ids /
    timestamps, the API URL and binary path masked; multiset for MT), the
    `Error:` stderr lines, the full database contents and the sequence of API
    requests (method, path, `Accept`, token).
  * Deviations: `User-Agent: devstatscode-rust` (Go: `go-github/38.1.0`);
    JSON decoder messages differ (the Go type `*json.SyntaxError` /
    `*json.UnmarshalTypeError` and the exit path are the same); Go's
    `%T` type names are reproduced for the go-github / `url.Error` / JSON
    errors only; malformed API payloads (e.g. a label without `id`) end in a
    Go nil-pointer panic vs a Rust `expect` panic (both exit 2); Go map
    iteration order in `SyncIssuesState` loops vs sorted maps (output blocks
    are sorted on both sides anyway); MT output interleaving and the
    artificial event id collisions (`UnixNano / 31622`) are timing dependent
    on both sides.

* `ghapi2db`
  * `cmd/ghapi2db/ghapi2db.go` + `restore.go` → `src/main.rs` + `src/restore.rs`
    on top of `devstatscode::{github, ghapi, restore}`. Same passes in the same
    order, each skippable with its `GHA2DB_GHAPISKIP*` flag (`GHA2DB_GHAPISKIP`
    skips everything but the final `Time:` line): **licenses**
    (`Repositories.License` for recent repos without one,
    `GHA2DB_FORCE_API_LICENSES` rechecks all, `gha_repos.license_*` update),
    **languages** (`ListLanguages` → `gha_repos_langs` rows incl. zero-byte
    languages, the `unknown,0,0.0` marker when nothing is returned,
    `GHA2DB_FORCE_API_LANGS`), **events** (`Issues.ListRepositoryEvents` paged
    until an event older than `recentDt`, issue/PR state per event,
    `GHA2DB_ONLY_EVENTS`/`ONLY_ISSUES`/`ONLY_MILESTONE`/`GHA2DB_GHAPISKIPISSUES`/
    `GHAPISKIPPRS`, `SyncIssuesState` with artificial `281474976710656 + <REST
    event id>` events, PR fetched once per issue, `GHA2DB_GITHUB_DEBUG` rate /
    EventID output), **commits** (`Repositories.ListCommits` in the autofetch
    range `[max(dup_created_at with author email) − 2 min, max(dup_created_at) +
    2 min]` or `DTFROM`/`DTTO`, `gha_commits` author/committer enrichment with
    the `difference for sha …` reports, `gha_actors`/`gha_actors_emails`/
    `gha_actors_names` writes — to the shared `GHA2DB_AFFILIATIONS_DB` when
    configured — and the API-calls summary), then the five **restore** passes
    (`ghapi2db comments|reviews|forks|releases|stars restore: processing N repos,
    recent date: …` / `processed N repos, P pages, checked C, restored R`):
    issue, review and commit comments (the latter walked from the last page
    backwards while a page still holds a recent comment), reviews of PRs
    updated since `recentDt`, forks, releases with assets, and stargazers over
    GraphQL (`GHA2DB_GITHUB_OAUTH` tokens tried in turn; `stars restore needs
    GHA2DB_GITHUB_OAUTH token(s), skipping` without one). Restored objects reuse
    the raw GH Archive event when exactly one matches (verified through the
    payload's `comment_id`/`forkee_id`/`release_id`) and get the artificial
    `ArtificialIDBase + kind offset + object id` (stars: `NegativeArtificialID`)
    event otherwise; comment/review restores end with the targeted
    `util_sql/postprocess_{texts,labels,issues_prs}_ids.sql` run (`targeted
    postprocess executed for N restored event id(s)` or `… skipped: gha_texts is
    empty, full structure rebuild pending`). Every API call goes through the
    `GetRateLimits`/`MinGHAPIPoints`/`MaxGHAPIWaitSeconds`/`MaxGHAPIRetry`/
    `GHA2DB_GHAPI_ERROR_FATAL` handling with Go's messages (`API limit reached,
    waiting …`, `abuse detected, waiting …, retry i/n`, `rate limited, reset in
    …, skipping`, `status 500, skipping`, `giving up after N retries`, `error:
    …, skipping`). Knobs: `GHA2DB_RECENT_RANGE`, `GHA2DB_RECENT_REPOS_RANGE`,
    `GHA2DB_MIN_GHAPI_POINTS`, `GHA2DB_MAX_GHAPI_WAIT`, `GHA2DB_MAX_GHAPI_RETRY`,
    `GHA2DB_GHAPI_RATE_LIMITS_CACHE`, `GHA2DB_NO_AUTOFETCHCOMMITS`,
    `GHA2DB_SKIPPDB`, `GHA2DB_ST`/`GHA2DB_NCPUS`, `GHA2DB_DEBUG`,
    `GHA2DB_GITHUB_DEBUG`, `GHA2DB_GITHUB_OAUTH`, `GHA2DB_GITHUB_API_URL`,
    `GHA2DB_AFFILIATIONS_DB`, `GHA2DB_LOCAL`, `hide/hide.csv`.
  * Repository scope (2026-09-14, Go and Rust alike — see
    `docs/ghapi2db-gha-gaps.md` P1-A): by default every `gha_repos` repository
    is in scope (lib `GetTrackedRepos`/`get_tracked_repos`: one current name per
    id = the name of its newest native event, other names are "historical" and
    skipped; `ghapi2db scope: N repos from gha_repos (I ids), H historical names
    skipped`), and one GraphQL **heartbeat** per process (`heartbeat.go` /
    `heartbeat.rs`, 50 repos per query, `ghapi2db heartbeat: N repos in Q
    GraphQL queries: F found, N not found, M moved, U unknown, A archived; active
    since …: pushes P, issues I, PRs R, forks F, releases L, stars S`) decides
    which repos each pass may skip (`… processing N repos (heartbeat: K
    skipped) …`, per-repo `skipped by heartbeat (no <gate> since …)` with
    `GHA2DB_DEBUG`): events need issue/PR updates, commits pushes, comments
    either, reviews PR updates, forks/releases a newer fork/release, stars a
    `stargazerCount` differing from the newest `gha_forkees` snapshot at or
    before the recent date. Repos GitHub cannot resolve (or whose name now
    belongs to another repository id — `WARNING: … resolves to … but is tracked
    as id …`) are skipped, a heartbeat failure or a missing token processes
    everything (fail open). `GHA2DB_GHAPI_RECENT_REPOS_ONLY` restores the
    legacy "repos with events in `GHA2DB_RECENT_REPOS_RANGE`" scope without a
    heartbeat; `REPO=` and `DTFROM`/`DTTO` bypass the heartbeat. Restore passes
    resolve `repo_id`/`org_id` from `gha_repos` when a repository has no
    `gha_events` rows yet (bug 63).
  * Repository counters (2026-09-14, Go and Rust alike — gaps report P1-D): a
    last pass `ghapi2db repo stats` (`GHA2DB_GHAPISKIPREPOSTATS` disables it)
    writes one `gha_forkees` snapshot per tracked repository per run —
    `stargazers_count`/`watchers` (= stars, the GH Archive semantics), `forks`,
    `open_issues` (issues + PRs, the REST semantics), `name`/`full_name`/
    `owner_id` — taken from the heartbeat (the fragment also asks
    `owner { … databaseId }`), so it costs no REST request in the default
    scope; repos without a heartbeat (`REPO=`, the legacy scope, *unknown*)
    cost one `GET /repos/{o}/{r}` each and the returned id must be the tracked
    one (`WARNING: … resolves to … (id N) which is not tracked, skipping`).
    The row hangs off the newest `gha_events` row of the repository (under
    the tracked name, else under any name — renamed repos), copies its
    `dup_actor_id`/`dup_created_at`, keeps the tracked `dup_repo_name`, sets
    `updated_at = now`, and is upserted on `(id, event_id)`: `ghapi2db repo
    stats: processed N repos, snapshots: I inserted, R refreshed; skipped: W
    without events, U unavailable; GH API calls: C` (per repo with
    `GHA2DB_DEBUG`: `… N stars, F forks, O open issues from the heartbeat|API,
    snapshot inserted|refreshed (event E)`). The counter-less rows GH Archive
    attaches to PR events since 2024-09 are upgraded in place, and the stars
    heartbeat gate now picks the newest snapshot by `updated_at`, so those
    rows cannot hide a real one.
  * Repository events feed (2026-09-14, Go and Rust alike — gaps report P1-B):
    a pass `ghapi2db repo events` (`GHA2DB_GHAPISKIPREPOEVENTS` disables it)
    running right after the licenses/languages passes and before every other
    API pass. For each repository with *any* heartbeat activity since the
    recent date (issue/PR update, push, fork, release, or a star count the
    snapshot does not know; unknown → processed, not found/moved → skipped)
    it reads `GET /repos/{o}/{r}/events?per_page=100&page=1..3` — the very
    objects GH Archive is built from — decodes each element with the gha2db
    `Event` type and writes it with the shared gha2db writer (`lib.WriteToDB`
    / `devstatscode::ghawriter::write_to_db`) under its **native id, actor and
    time stamp**, hide.csv anonymisation and the gha2db actor filters
    (`GHA2DB_ACTORS_FILTER`/`ALLOW`/`FORBID`) included, all event types
    (`PushEvent` stubs without commits and 5-field PR stubs too). Ids GH Archive already
    delivered are skipped, a different event under a known id logs the
    writer's `event id collision` line. Paging follows the `Link: next`
    header only, up to page 3 (GitHub answers 422 for page 4): the live feed
    is ordered by id — which is no longer monotonic in time — and filtered
    after pagination (a "full" page holds 84–96 events while more follow), so
    neither a short page nor an old event on a page ends it; every event of a
    fetched page is written, months-old ones included. A feed carrying an
    untracked repository id is skipped with a
    `WARNING`, 404/410 silently. Restored ids go to the targeted postprocess.
    Summary: `ghapi2db repo events: processed N repos, P pages, checked C,
    restored R` + `… restored events by type: ForkEvent 1, IssuesEvent 3, …`
    (sorted); with `GHA2DB_DEBUG`: `… restored <type> <id> (<time>)` and
    `… page N: E events, oldest <time>, restored so far R` per repo.
  * `GHA2DB_GHAPI_RATE_LIMITS_CACHE` (new, in Go and Rust, default `5`, `0`
    disables): `GetRateLimits`/`get_rate_limits` polls all tokens
    **concurrently** (was: one sequential `/rate_limit` round trip per token
    before *every* API call — ~5 s per call with the 49 production tokens, bug
    54) and caches the answer for that many seconds; each served call
    decrements the hinted token's cached points by one (that is what the
    caller is about to spend), cached durations shrink by the elapsed time,
    and the cache is bypassed (GitHub polled again) when it says the best
    token has `GHA2DB_MIN_GHAPI_POINTS` or fewer points left, when its reset
    already passed, when the token count changed, and right after a rate
    limit/abuse error (`HandlePossibleError` drops it). Durations are computed
    against one common "now" after the poll, so equally loaded tokens tie
    exactly and the first one wins the hint (before, the last polled token
    won ties by a few microseconds). The compat harnesses of `ghapi2db` and
    `sync_issues` run with the cache disabled (so the asserted `/rate_limit`
    request counts stay deterministic) plus five `rate_limits_cache_*`
    scenarios with it enabled.
  * Go⇄Rust tests: `cmd/ghapi2db/tests/compat.rs` — 109 scenarios, each side on
    a scratch database (`full_structure.sql` + seeded events/repos/actors)
    against its own scripted fake GitHub REST + GraphQL server: skip-all;
    licenses (found / not found / already set / force / debug `Stringify`
    output / 403 retried then given up / other errors and `null` bodies / low
    points wait, abort and fatal / budget refresh / MT); languages (found,
    empty, not found, force, zero sum, errors, abuse, hangup, low points, MT);
    events (new issue + PR, unchanged state, changed state/title, renamed,
    unknown type, missing fields, duplicate id, paging stop at old events, two
    pages, empty page, date ranges, single issue / milestone / repo filters,
    skip issues or PRs, 404 repo, abuse then ok, server errors exhausting
    retries / fatal, unknown error exit 0, low points wait then abort, PR fetch
    errors, GitHub debug output, MT with many repos); rate limits cache (one
    poll per token with the hinted token serving every call, token switching
    driven by the decremented cached points incl. the tie → shorter reset rule,
    re-poll after the reset wait, abuse invalidation, disabled → poll before
    every call); commits (autofetch range,
    no `gha_commits`, no autofetch, `DTFROM`/`DTTO`, author name mismatch and
    missing users, shared affiliations DB, hidden actors, paging and progress,
    error paths, MT); restores (all three comment kinds, targeted postprocess,
    raw-event reuse incl. ambiguous / unverifiable candidates, backwards
    commit-comment walk, paging, idempotent rerun, malformed / id-less repos,
    404/410/500/rate-limit/abuse/hangup/unreachable-API paths, plain 403
    give-up, fatal mode, `GHA2DB_SKIPPDB`, hidden actors, rate refresh every 20
    repos, MT; reviews with PR-list stop and paging; forks incl. present / old
    / paging; releases with assets, uploader fallback, `published_at` fallback,
    prerelease; stars over GraphQL with two pages, skipped edges, no token,
    HTTP 500 / GraphQL errors / 429 Retry-After / 403 X-RateLimit-Reset,
    `null` page cursor / edges / node fields / `data`, next-token fallback,
    hash id conflict; all passes in order); scope (12 `scope_*` scenarios:
    heartbeat gates of every pass incl. the stars snapshot rule, exact GraphQL
    body, one heartbeat for several passes, current/historical names and
    shared ids, not-found / moved / renamed / archived / malformed repos,
    fail-open on 500 / path-less errors / 429 → next token / no token,
    `RESOURCE_LIMITS_EXCEEDED` batch splitting, 121 repos → 3 batches, the
    legacy flag and the `REPO=`/`DTFROM` bypasses, the stars gate ignoring
    counter-less GHA rows); repo stats (7 `repo_stats_*` scenarios: snapshot
    from the heartbeat with zero REST calls, refresh of the same anchor row
    across runs and renames, in-place upgrade of GHA stub rows, the newest
    event under any name as the anchor incl. artificial ids, `REPO=` / legacy
    scope / unknown heartbeat → `GET /repos/{o}/{r}` with go-github's `Accept`
    header, untracked id warning and 404 → unavailable); repo events (8
    `repo_events_*` scenarios: a mixed six-type page written with native ids
    into `gha_events`/`gha_issues`/`gha_issues_labels`/`gha_comments`/
    `gha_pull_requests`/`gha_forkees`/`gha_payloads`/`gha_actors`, the short
    page ending the feed, a second run finding everything present plus an
    `event id collision`, paging across 3 pages of 100 with the recent-date
    stop and the 3-page cap, an untracked feed id and a 404, `REPO=` with
    hide.csv anonymisation, the gha2db actor filters
    (`GHA2DB_ACTORS_FILTER/ALLOW/FORBID`), the heartbeat "no activity" gate
    incl. the star-count signal, and the targeted postprocess filling
    `gha_texts` from the restored ids). Compared: exit
    code, stdout (durations, now-derived ids and timestamps, API URL and binary
    path masked; multiset for MT and where Go's map order shows), `Error:`
    stderr lines, the full database contents and the API request log (method,
    path, query, `Accept`, token, GraphQL bodies).
  * Deviations: repos, `Unique repos` / language maps and the langs debug output
    are processed / printed in sorted order (Go: random map order); the GC
    heartbeat is a no-op; an MT abort joins the in-flight workers first;
    `Timestamp` drops non-`Z` offsets like go-github; the 200-byte GraphQL error
    snippet is lossy UTF-8; `%T` names and JSON decoder wording as for
    `sync_issues`; the GraphQL POST sends `User-Agent: Go-http-client/1.1` and
    no `Accept` header like Go's `net/http`.

* `gha2db`
  * `cmd/gha2db/gha2db.go` → `src/main.rs` (hour loop, `getGHAJSON` download /
    gunzip / split / parse, `parseJSON`, retries, `today`/`now` arguments, the
    deferred `refreshCommitRoles` / `updateCommitRoles`), the shared
    `devstatscode::ghawriter` (since 2026-09-14 the writer lives in the lib —
    Go: root-package `ghawriter.go` — so `ghapi2db` can write GitHub API
    events with exactly the gha2db semantics): `ghawriter/writer.rs`
    (`writeToDB` / `writeToDBOldFmt` and every payload writer),
    `ghawriter/db.rs` (lookups, actor / repo / org / milestone / forkee /
    branch / label / comment / review / release / pages / commit-roles rows,
    the shared `(email,name) → actor` cache, `eventExistsCollision`);
    `src/roles.rs`,
    `src/gz.rs` (Go `compress/gzip` error texts: `EOF`, `unexpected EOF`,
    `gzip: invalid header`, `gzip: invalid checksum`) on top of
    `devstatscode::gha` — the port of `gha.go`: serde structs with Go
    `encoding/json` semantics (unknown keys ignored, `null` → zero value,
    `*Dummy` presence markers, RFC 3339 times kept with their offset and Go's
    `parsing time … cannot parse …` texts), `ActorHit`, `RepoHit`,
    `MakeOldRepoName`, the `*OrNil` helpers and `gha_test.go` as unit tests.
    Same arguments (`date_from hour_from date_to hour_to [orgs [repos]]`,
    `today`/`now`, `regexp:` filters, comma lists with spaces), same knobs
    (`GHA2DB_ST`/`GHA2DB_NCPUS`, `GHA2DB_OLDFMT`, `GHA2DB_EXACT`,
    `GHA2DB_EXCLUDE_REPOS`, `GHA2DB_ACTORS_FILTER`/`ALLOW`/`FORBID`,
    `GHA2DB_JSON`, `GHA2DB_NODB`, `GHA2DB_ALLOW_BROKEN_JSON`,
    `GHA2DB_HTTP_RETRY`, `GHA2DB_HTTP_TIMEOUT`, `GHA2DB_SKIP_DATES_YAML`,
    `GHA2DB_REFRESH_COMMIT_ROLES`, `GHA2DB_AFFILIATIONS_DB`, `GHA2DB_LOCAL`,
    `GHA2DB_DATADIR`, `GHA2DB_DEBUG`, `hide/hide.csv`), same stdout lines
    (`Working on` / `Skipped` / `Opened` / `Decompressed` / `Split` / `Parsed:`
    / `Retry(n)` / `Gave up on` / `event id collision` / `Final threads join
    (processed n)` / `n remain:` / `All done:` / `Time:`), same `jsons/` files
    (`<hour unix>_<event id>.json`, `error_<hour>-<i>-<n>.json`) and the same
    rows in every `gha_*` table.
  * `GHA2DB_GHARCHIVE_URL` (new, in Go and Rust): overrides the
    `http://data.gharchive.org/` base URL (tests point it at a fake server).
  * Go⇄Rust tests: `cmd/gha2db/tests/compat.rs` — 60 scenarios, each side on
    a scratch database against its own fake GH Archive server
    (`compat/src/gharchive.rs`) serving gzipped fixtures cut from real hours
    (`compat/fixtures/gha2db/`: 2013 and 2014 old-format hours, 2015, 2020,
    2025 new-format hours, a 2012 hour with the `2012/03/11 12:00:00 -0700`
    time format, an empty hour): usage / bad dates / reversed range / bad
    regexps / skip-dates file (missing, from the data dir, bad yaml, skipped
    hours); org, repo, full-name, `GHA2DB_EXACT`, `regexp:`, exclude, actor
    allow / forbid / disabled filters and hidden actors; every 2025 event type;
    debug output; `GHA2DB_JSON` (with and without `jsons/`), `GHA2DB_NODB`;
    reruns and id collisions (new and old format); old-format hours and their
    `-07:00`/`-08:00` wall clocks; old-format JSON decoded as new format;
    broken JSON fatal and `GHA2DB_ALLOW_BROKEN_JSON`; empty hour, 404, empty /
    3-byte / truncated / corrupted bodies, retries that recover and give up,
    dead archive and hang-ups; single- and multi-threaded ranges, `runGC`
    heartbeat every 24 hours, `today`/`now`, `TZ=Europe/Warsaw`; commit roles
    refresh / update with hidden actors and an empty table; the shared
    affiliations DB. Compared: exit code, stdout (durations, `n remain:`
    lists, GC lines, now-derived stamps, archive URL, binary path and database
    names masked; multiset in MT mode), `Error:` stderr lines, the full
    database contents (+ affiliations DB), the archive request log and the
    `jsons/` files.
  * Deviations: the `n remain:` list is printed sorted (Go: map order); the
    `runGC` memory lines carry RSS-based numbers and `#gc:0`; decoder wording
    (serde vs jsoniter) in `Error(<hour>):` / `Cannot unmarshal:` lines;
    connection errors read `Get "<url>": <error>` with the Rust client's
    wording, `flate:` texts of corrupted streams differ; invalid UTF-8 in an
    echoed JSON is replaced by U+FFFD; `%v` of a zero-offset non-UTC zone
    prints `+0000 UTC`; an invalid `regexp:` argument fails with an `Error:`
    line (Go: bare `regexp.MustCompile` panic; both exit 2 and both still
    print the deferred commit-roles lines).

* `api`
  * `cmd/api/api.go` → `src/main.rs` (the HTTP server: `requestInfo`, the
    jsoniter-like body decoder — first JSON value only, trailing input
    ignored, `null` → zero payload, case-insensitive `api`/`payload` keys,
    duplicate keys last-wins —, `handleAPI` dispatch with the `Request:` /
    `Request(exit[, N bg runners]):` log lines, rs/cors `AllowAll`
    (preflight → 204 without a body), `checkEnv`, `readProjects`, the
    `SIGINT`/`SIGUSR1`/`SIGALRM` → `Exiting due to signal <name>` exit 1
    handler), `src/common.rs` (projects / name→db state, the three caches,
    `handleSharedPayload`, `getPayloadStringParam` /
    `getPayloadStringArrayParam`, `timeParseAny` with Go's `time.Parse`
    digit-width rules, `periodNameToValue` (`range:from,to` manual periods,
    `maxDt` = yesterday's midnight), `ensureManualData` (runs `calc_metric`
    when a manual range has no data — synchronously, or in the background with
    `bg`: at most 3 runners, `configuration already running in background
    (…)` / `too many background calculations: N`), tag lookups, metric and
    period maps), `src/handlers.rs` (the 17 APIs: `Health`, `ListAPIs`,
    `ListProjects`, `RepoGroups`, `Ranges`, `Countries`, `Companies`,
    `Events`, `Repos`, `CumulativeCounts`, `CompaniesTable`,
    `ComContribRepoGrp`, `ComStatsRepoGrp`, `DevActCnt` (+ repository mode on
    the `gha` database), `DevActCntComp`, `SiteStats` (4 parallel queries),
    `GithubIDContributions` (3 parallel queries on `allprj`)). Same `POST
    /api/v1` protocol, same JSON bodies (jsoniter-compact: `<>&` escaped,
    nil slices → `null`, Go field order), same `{"error":"API '<name>': …"}`
    texts and status codes, same stdout log lines (`<API>(exit): project:…
    db:… payload: map[…] err:…`, cache hits / expiries with their TTLs,
    `Calculated manually:` + the `calc_metric` output, `ExecCommand` failure
    dumps), same required environment (`PG_PASS`, `PG_PASS_RO`, `PG_USER_RO`,
    `PG_HOST_RO` → `Error: '<VAR> env variable must be set'` exit 2), same
    `projects.yaml` lookup (`GHA2DB_PROJECTS_YAML`, `GHA2DB_LOCAL`,
    `GHA2DB_PROJECTS_OVERRIDE`).
  * `GHA2DB_API_HOST` (default `0.0.0.0`) / `GHA2DB_API_PORT` (default
    `:8080`, a colon is prefixed when missing) — new in Go and Rust: the
    listen address (the Go server used a hard-coded `:8080`).
  * Go⇄Rust tests: `cmd/api/tests/compat.rs` — 17 scenarios (≈330 requests),
    both servers started on free ports against scratch databases
    (`dbtest_api` with every series/tag/GHA table the APIs read, an empty
    schema-only database, and the fixed-name `gha` / `allprj` databases the
    `kubernetes` project and `GithubIDContributions` insist on — created
    only when absent or marked as harness-owned) with a fake `calc_metric` on
    `PATH` recording its arguments and environment. Compared per scenario:
    every raw HTTP response (status line, headers with `Date` masked, body —
    JSON canonicalized only where Go's output has random map order), the
    stdout log (ports, cache timestamps, `YYYY-MM-DD H` stamps and decoder
    wording masked; sorted only for the parallel / background scenarios),
    `Error…` / `panic: stacktrace:` stderr lines, the recorded `calc_metric`
    calls and the exit code or signal. Scenarios: lists / health / project
    lookup (missing, disabled, non-existent database, `GHA2DB_PROJECTS_
    OVERRIDE`), HTTP edge cases (GET / HEAD / PUT / DELETE, query strings with
    `<`, 404s, CORS preflight variants, multiple / empty / unicode
    `User-Agent`) and 25 malformed bodies, tag lists, `Repos` / `Events` with
    every parameter error, `CumulativeCounts` / `SiteStats` (+ cache hits and
    a missing database), `GithubIDContributions` with and without `allprj`,
    the three company APIs, `DevActCnt` (51 cases: quick ranges, github_id
    filter, countries, repo groups, metrics, manual ranges with every error),
    `DevActCnt` on `kubernetes` incl. repository mode and `Approves` /
    `Reviews` (33), `DevActCntComp` (31), a failing manual calculation,
    background runners (already running, limit of 3, a new run after they
    finish), a failing background run, startup failures (each required
    variable, missing / broken / alternative `projects.yaml`, port in use,
    non-numeric port) and the four signals.
  * Deviations: the JSON decoder's error text (jsoniter vs serde; the status,
    the `{"error":"API 'unknown': …"}` shape and the log lines are the same);
    `ListProjects` lists `projects` and `ComStatsRepoGrp` the `values` keys in
    a fixed order (Go: random map order); cache-hit log lines print the entry
    time (Go prints `time.Time` internals `{wall:… ext:… loc:…}`); the order
    of the parallel SQL error dumps (`SiteStats`, `GithubIDContributions`)
    and the `N bg runners` count right after a background submission are
    scheduling dependent on both sides; a fractional `shdev` value scanned
    into a count reads `to a int64` (Go: `to a int`); when several required
    parameters are missing the first one in a fixed order is reported (Go:
    random map order).

## Bugs found in the Go code and fixed in both implementations

* `tsplit`: errors were printed to **stdout** with exit code **0**; now stderr
  and exit 1. `SIZE=0` panicked (division by zero) and negative sizes
  produced empty output; now `SIZE` must be positive. A different number of
  link and image lines was silently accepted (misaligned output); now an error.
* `replacer`: the `REPLACEFROM` bound error said "filename length"; it is the
  file's length.
* `lib`: `FatalNoLog` ignored `NO_FATAL_DELAY` (always slept 60 s);
  `EnvReplace`/`EnvRestore` truncated values containing `=`; an unparsable
  `GHA2DB_STARTDT`/`GHA2DB_POSTPROCESS_FROM|TO` dead-locked the process
  (`Printf` → `ctx.Init` → `TimeParseAny` re-entering the init `sync.Once`).
* `splitcrons`: `WEIGHT_POWER=NaN` passed the range check (`NaN < 0` and
  `NaN > 4` are both false) and made every weight `NaN`; now rejected.
* `lib` `ProcessTag` (`tags`, `gha2db_sync`): when `truncate t<series>` hit the
  500 ms lock timeout (a reader was using the table — the normal case), the
  `delete` fallback was issued on the same, already aborted transaction and
  failed with `current transaction is aborted`, killing the run (exit 2)
  instead of falling back; now the transaction is rolled back and a new one
  begun first.
* `lib` `ReadFile`: the `/shared/` fallback messages (`lib.ReadFile('…'): ok`
  in debug mode, `lib.ReadFile('…'): error: …`) had no trailing newline, so
  the next output line was glued onto them.
* `runq`: with 100 or more result columns the header names lost the wrong
  number of characters (the `c<index>` suffix length was assumed to be one or
  two digits: `c1011`, `c1021`… instead of the column names); a `%` in a data
  value was printed as `%%` (the value was `%`-escaped and then printed with
  `Printf("%s")`, which does not interpret it); a `qr` parameter with fewer
  than three comma-separated parts panicked (index out of range) — now a
  fatal `qr parameter must be 'period,from,to'` error.
* `vars`: environment variables whose value contains `=` (`PATH`-like lists,
  `KEY=a=b`) were exposed to the `$NAME` replacements truncated at the second
  `=` (the same `strings.Split(e, "=")` slip as in `EnvReplace`); a `loops`
  entry with fewer than four numbers or a `queries` entry with fewer than two
  strings panicked (index out of range), and a loop increment of `0` (or a
  negative one) looped forever while growing the output until the process ran
  out of memory — all three are now fatal errors naming the offending entry
  (`Loop definition should be array with 4 elements [n, from, to, inc], got:
  …`, `Loop increment must be positive, got: …`, `Query definition should be
  array with at least 2 elements [name, sql, columns...], got: …`).
* `lib` `HandleRowIsTooBig` (`columns`): the `Error handle row is too big …`
  message had no trailing newline, so the next output line was glued onto it.
* `devstats`: with `GHA2DB_CHECK_PROVISION_FLAG` a missing project database
  crashed the check (`sql: database is closed` — the `continue` re-ran the
  query on the connection just closed) instead of being counted as not
  provisioned; with `GHA2DB_SET_RUNNING_FLAG` the deferred clearing of the
  flag was attempted on every project, including the databases that do not
  exist, each retried 89 times with growing sleeps (about 67 minutes) before a
  fatal error — now only the databases where the flag was set are cleared.
* `lib` `GetProjectsList` (`devstats`, `website_data`): projects sharing the
  same `order` were reduced to one (a random one — Go map iteration) listed
  twice, so the other was never synced; now all are kept, ordered by `order`
  then name, with a `Warning: projects 'a' and 'b' have the same order N`
  line. The real `cncf/devstats` `projects.yaml` had `agones` and
  `kaischeduler` both at `order: 245` (fixed 2026-09-13: agones → 246,
  velero…sdc renumbered 247…256, every `order` unique again). `hide_data` had
  its own copy of the
  same ordering code (same effect: one of the two databases was never
  anonymized) and now uses `GetProjectsList`.
* `lib` `GetHidden` (Rust side only): the CSV field-count error was rendered
  with the pre-Go-1.10 wording (`… wrong number of fields (expected 1, got
  2)`); it is `record on line N: wrong number of fields` like Go.
* `website_data`: a project database without any forkee of the last three
  months made `select sum(fmax) …` return NULL and the tool died with
  `converting NULL to int is unsupported` (now `coalesce(sum(fmax), 0)`); the
  single-threaded mode (`GHA2DB_ST`) never set the `timestamp` of the
  generated files (`0001-01-01T00:00:00Z`); the month's star delta was
  computed by two identical consecutive queries (one removed).
* `webhook`: the `http.ListenAndServe` error was ignored, so a port already in
  use (or a bad `GHA2DB_WHHOST`) made the tool exit **0** silently — now a
  fatal error (exit 2); bodies shorter than 8 bytes (`payload=`) panicked on
  `sBody[8:]` (`401 webhook: payload too short` now); the error message was
  interpolated into `{"message": "%s"}` without JSON escaping, producing
  invalid JSON for every jsoniter error (they quote the input) — now
  `json.Marshal`ed; `unauthorized payload` was logged without a newline; the
  Travis key's `publicKey.(*rsa.PublicKey)` assertion was unchecked (a
  non-RSA key would panic — `invalid public key` now).
* `lib.PrettyPrintJSON` (used by `sqlitedb` for the exported JSONs and, more
  importantly, to decide whether a dashboard changed): jsoniter's
  `ConfigDefault` unmarshals into `map[string]interface{}` and marshals map
  keys in **random** order (50 different outputs in 50 calls) and turns every
  number into a float64 — so `sqlitedb` never recognised an unchanged
  dashboard: every `import_jsons_to_sqlite.sh` run rewrote every dashboard,
  produced `.was` files and a database backup, and the exported JSONs differed
  from run to run (`../devstats/util_sh/sort_json.sh` post-processing was
  needed). Now decoded with `UseNumber` (integers kept exact) and encoded with
  `encoding/json`'s `MarshalIndent` (sorted keys); the Rust port produces the
  same bytes.
* `merge_dbs`: the row-by-row insert passed the `*interface{}` scan pointers
  themselves to `lib.ExecSQL` (only the batch path dereferenced them), so
  `GHA2DB_QOUT` echoed `1:*interface {}:0xc000…` pointer addresses instead of
  the values (lib/pq still sent the right data because `database/sql`
  dereferences pointers). Now the values are passed.
* `ComputePeriodAtThisDate` (`time.go`) sliced `period[0:1]`/`period[len-2:]`
  and panicked with a Go runtime error on an empty or one-character period
  instead of reporting the `unknown period` fatal.
* `gha2db_sync` printed metrics with `%+v`, which rendered the `MetricSQLs`
  pointer as an address (`sqls:0xc000…`) — now a `String()` method prints the
  pointed-to list.
* `gha2db_sync` MT histograms: the final `for thrN > 0` join loop discarded the
  workers' results, so a tolerated failure's `wait_after_fail` requested by one
  of the last `thrN` histograms was never applied to `maxRes` (the closing
  sleep).
* `import_affs` `tzOffset`: `select extract(epoch from utc_offset) / 60 …` was
  scanned into `*int`; since PostgreSQL 14 `extract()` returns `numeric`
  (`120.0000000000000000`), which `database/sql` cannot convert to `int` — a
  fatal for every user with a non-empty `tz`. Now `(… / 60)::int`.
* `import_affs`: `GHA2DB_SKIP_COMPANY_ACQ=1` panicked (`assignment to entry in
  nil map`) in `mapCompanyName` as soon as any company was processed —
  `acqMap`/`comMap`/`stat` were only created when acquisitions were read. The
  maps are now always created.
* `import_affs`: the login-change propagation loop (`for login, prios := range
  loginAffs`) added new keys to `loginAffs` while ranging over it; Go does not
  guarantee whether such keys are visited, so the number of `new logins` /
  copied affiliations (and the resulting `gha_actors_affiliations` rows for
  correlated logins) differed between runs. Now a sorted snapshot is iterated
  until a fixpoint; Rust does the same.
* `import_affs` (bug 49, found while running the shared affiliations import on
  the test cluster with both binaries): `firstKey` returned a random map key,
  and the "pick first affiliation definition that lists most companies" loop
  ranged over a map — so for a login with several names, or several equally
  long affiliation definitions of the same source priority, every import
  stored a different choice (real `github_users.json`: 215 such names, 397
  such affiliation ties, 393 of them between different companies; ~100
  `gha_actors` rows and those companies flipped on every daily import). Both
  picks are now the smallest candidate (byte order), and acquisition regexps
  are tried in `companies.yaml` order (first match wins) instead of map
  order; Rust already behaved like this, the compat tests now compare
  `gha_actors.name` and the `updated actors` counters exactly.
* `calc_metric` single-threaded path: `from` later than `to` produced an empty
  range and `dta[0]` panicked with `index out of range [0] with length 0`
  (exit 2, no `gha_last_computed`); now nothing is computed (`All done.`,
  exit 0) like the multi-threaded path.
* `calc_metric` printed the options with `%+v` of `&cfg` — a pointer to a
  pointer, so only an address was shown; now the struct is printed.
* `lib` `ArtificialPREvent` (`sync_issues`, `ghapi2db`): the PR's milestone was
  inserted through `ghMilestone`, which read the *issue's* milestone
  (`ic.GhIssue.Milestone` / `ic.MilestoneID`) — a nil-pointer crash (exit 2)
  whenever the PR payload carried a milestone the separately fetched issue
  payload lacked. `ghMilestone` now takes the milestone to insert.
* `ghapi2db` `syncEvents`/`syncCommits`: one `*github.ListOptions` was shared by
  all goroutines (`opt.Page = response.NextPage` on a common pointer; the
  "deep copy" of `copt` was a pointer copy) — a repo whose first request ran
  after another goroutine advanced the shared page started at page N > 1 and
  silently missed its most recent events (a data race under `-race`). Now
  every goroutine owns its `ListOptions`.
* `ghapi2db` `syncLicenses`/`syncLangs`: a repository whose license / languages
  endpoint keeps answering 403 (GitHub's permanent "Repository access blocked"
  403s, not only abuse limits) was retried forever, hanging the hourly sync.
  The per-repo 403 retry is now bounded by `GHA2DB_MAX_GHAPI_RETRY`; then the
  repo is skipped with `Licenses abuse detected on o/r, giving up after N
  retries` (`Languages …`).
* `ghapi2db` stars restore (bug 50, Rust only, found by the first Rust `cii`
  sync on the test cluster): GitHub answers an empty stargazers page with
  `"startCursor": null` (and `"data": null` next to top-level errors); Go's
  `encoding/json` leaves such fields at their zero value, serde rejected them
  (`invalid type: null, expected a string`) so every repository of the stars
  restore was skipped. Every field of the GraphQL response is now
  null-tolerant like Go.
* Rust performance parity (item 52): every GitHub `Client` had its own small
  HTTP/1.1 connection pool and each GraphQL POST opened a new TLS connection,
  so the `ghapi2db` API phases ran 30–70 % slower than Go (whose clients all
  share `http.DefaultTransport`'s HTTP/2 connection). All clients and
  `raw_post` now share one process-wide pool (100 idle connections, 60 s
  idle age).
* Stale keep-alive connections (item 53, Rust only, follow-up of item 52 seen
  live as `GetRateLimit(21): Get "…/rate_limit": peer disconnected`): a pooled
  connection the server closed in the meantime made the next call fail
  immediately; Go's `http.Transport` replays such a request transparently
  (`shouldRetryRequest`: GET/HEAD/OPTIONS/TRACE always, a re-sendable body only
  when nothing was written yet). `github.rs` now does the same
  (`run_replaying`, two retries) and reports the exhausted case with Go's
  `EOF` wording instead of ureq's `peer disconnected`.
* `import_affs` (bug 51, both sides): `gha_actors.name` is stored as
  `maybeHide(TruncToBytes(name, 120))` but a re-import compared the raw name
  with it, so the 9 live actors with names longer than 120 bytes (and any
  hidden name) were counted as `updated actors` and rewritten with the same
  value on every daily import. The stored form is compared now.
* `gha2db` `eventExistsCollision`: the existing event's `created_at` (a
  `timestamp` column without a zone) was compared with the new event's time as
  an instant (`eD.Equal(createdAt)`), so re-running old-format hours (2012–2014,
  whose stamps carry `-07:00`/`-08:00` offsets and are stored as wall clocks)
  reported a bogus `event id collision: …` for every event. Both sides now
  compare the `YYYY-MM-DD HH:MM:SS` wall clocks (compat test
  `old_format_2014_rerun`).
* `api` `apiRepos`: the only handler without `defer c.Close()` after
  `getContextAndDB` — every `Repos` request leaked a connection pool for the
  server's lifetime. Rust drops the connection at scope end.
* `api` `ensureManualData`: the "approves/reviews mode only allowed for
  kubernetes project" error was assigned and immediately overwritten by the
  following `rows, err := …` (the `return` had been glued into the message
  string as `projectreturn`), so a non-`gha` project asking for `Approves` /
  `Reviews` with a manual range ran `calc_metric` on `hist_approvers.sql` /
  `hist_reviewers.sql` that do not exist for it. Both sides now return
  `ensureManualData: approves mode only allowed for kubernetes project (…)`.
* `api` cache-expiry log lines named the wrong TTL constant
  (`GithubIDContributions` printed the `CumulativeCounts` TTL 43200 although
  it expires after 86400; `SiteStats` printed the `CumulativeCounts` constant).
* `api` `apiSiteStats`: the four query goroutines sent on an unbuffered
  channel and the receiver returned on the first error, leaking the remaining
  goroutines and their connections on every failed request (buffered channel
  now; Rust uses an mpsc channel).
* `GetRateLimits` (bug 54, both sides, found 2026-09-12 while explaining the
  multi-hour `kubeflow` syncs): the rate limits of **all** configured tokens
  were polled sequentially (one HTTPS `/rate_limit` round trip each) before
  **every** GitHub API call of `ghapi2db`/`sync_issues`. With the 49
  production tokens that is ~105 ms × 49 ≈ 5.25 s of pure overhead per API
  call (measured from a prod pod), so a repo with thousands of PR events took
  hours (kubeflow: 15 min syncs until 2026-09-09, 2.5–4.5 h afterwards under
  Go and Rust alike; the `API points: [...]` progress lines showed ~5000 unused
  points the whole time). Now polled concurrently and cached
  (`GHA2DB_GHAPI_RATE_LIMITS_CACHE`, see the `ghapi2db` notes above).

* Stale Go tests found by the parity audit (2026-09-13; the code was right,
  the tests were not): `series_test.go` `TestProcessAnnotations` filtered the
  `now`-dependent rows with `skipI` indices that were off by one since the
  `y100` quick range was added (2024-06-28) — 6/15 cases failed under Go;
  fixed (`{11}`→`{12}`, `{11, 13}`→`{12, 14}`) and mirrored in
  `tests/series_db.rs`. The sibling `devstats/metrics_test.go` did not compile
  since the Elasticsearch removal (5-arg `ProcessTag`) — fixed; with it
  compiling, 15 of its 70 `tests.yaml` cases fail identically under Go and
  Rust because the fixtures predate the current metric SQL (`gha_repo_groups`,
  `trepo_groups`/`tsig_mentions_labels` tag tables, changed `prs_state` /
  `reviews_per_user` sources) — kept as `KNOWN_STALE` in `tests/metrics_yaml.rs`
  by decision, not modernized. The Go tests Docker image never generated the
  `en_US.UTF-8` locale, so its `create database … lc_collate = 'en_US.UTF-8'`
  step could not succeed (`locale-gen` added to
  `devstats-docker-images/images/Dockerfile.tests`).
* Bug 60 (`ghapi2db` commits pass, Go and Rust alike, found 2026-09-14 by
  `devstats/devel/mega_health_check.sh` as "dangling" `gha_actors_emails` /
  `gha_actors_names` rows in the shared `affiliations` DB: 60/66 on prod,
  2819/3055 on test): `processCommit` recorded the author's and committer's
  email and name (origin 1) for **every** commit returned by the GitHub API,
  but inserted the actor row only for commits already present in
  `gha_commits` (`sha != ""` guard). Commits the GHA archives never delivered
  (pushes with more than 20 commits — the `linux` project on test — or
  events lost between hourly archives) therefore produced identity rows that
  referenced a missing actor, which nothing can join. The `sha` condition was
  removed from both actor inserts (author/committer) in Go and Rust, so an
  actor now exists for every identity row written; compat tests
  `commits_unknown_sha_ensures_actors[_in_shared_affiliations_db]`. The
  existing dangling rows were repaired in place (the GitHub user looked up by
  id and inserted exactly like `InsertActorTx` would have), not deleted.

### Rust-only bugs found after go-live (Go was correct)

* Bug 55 (`goregex`, Rust only, found 2026-09-13 04:44 UTC by the overnight
  log check, one day after the switch to the `-rust` images): a `-` that
  follows a class escape, a POSIX class or a completed range inside a bracket
  expression is a **literal** in Go (`[\w-+\d.]` = word chars, `-`, `+`,
  digits, `.`), but the adapter expanded `\w` to `0-9A-Za-z_` and left the `-`
  alone, producing the range `_-+` which the `regex` crate rejects
  (`invalid character class range, the start must be <= the end`). The only
  real pattern with this construct is containerd's
  `annotation_regexp: '^v?\d+\.\d+\.\d+(-[\w-+\d.]+)?$'`, so the 2026-09-13
  00:18 UTC containerd sync failed at `annotations` (exit 2, sync aborted
  before metrics; the Job itself is `Complete` because `devstats` logs the
  error and exits 0) — the first Rust run that reached the once-a-day
  annotations step. Fixed in `goregex::emit_class` (a `-` after such an item
  is emitted as `\-`; `\pL`/`\PN` one-letter classes are consumed as one item),
  covered by unit tests mirroring `regexp.MatchString` results from Go for
  `[\w-+\d.]`, `[a-c-e]`, `[\d-x]`, `[[:alpha:]-x]`, `[\pL-x]`, `[\w--]`,
  `[\w-\-]`, `[a\-z]`, `[a-c-]`, `[--x]`, `[\.-z]`, `[^\w-+]`, plus a test that
  compiles every `annotation_regexp` of the sibling `devstats/projects.yaml`
  when it is available.
* Bug 59 (`threads::num_cpu`, Rust only, found 2026-09-13 by comparing
  per-step durations in prod `gha_logs` after the switchover): Go's
  `runtime.NumCPU()` is the popcount of the process' `sched_getaffinity` mask
  and ignores cgroup CPU quotas, but the port used
  `std::thread::available_parallelism()`, which also caps the count to the
  cgroup v2 `cpu.max` bandwidth limit. Every DevStats CronJob sets
  `GHA2DB_NCPUS=8` with a pod CPU limit of 6 or 10, so `GetThreadsNum`
  returned 8 under Go but the `NCPUs > NumCPU` clamp gave **6** under Rust on
  the limit-6 pods (`Running (6 CPUs)` vs `Running (8 CPUs)` banners). The
  visible symptom was `gha2db` taking ~28 s instead of ~21 s per hourly sync:
  a 6-hour window plus the current hour is 7 archives, Go fetched all 7 in
  parallel, Rust only 6, so the current hour (which always ends in the
  `No data yet` retry cycle with `sleep((1+intn(3))*trials)`) started ~6 s
  later. `num_cpu()` now uses `libc::sched_getaffinity` + `CPU_COUNT` on
  Linux/Android and falls back to `available_parallelism()` elsewhere; unit
  tests check `num_cpu() >= available_parallelism()` and that it equals the
  popcount of `Cpus_allowed_list` from `/proc/self/status` (both verified
  inside `docker run --cpus=6`, where the affinity count is the host's 16 and
  the std answer is 6).
