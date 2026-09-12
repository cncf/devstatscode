# merge_dbs fixtures

- `schema.sql` — the tables `merge_dbs` copies (reduced, type-diverse
  columns; see the header of the file). The compat tests create every input
  and output database of both sides from it and seed the input databases from
  generated SQL (`seed_sql` in `rust/cmd/merge_dbs/tests/compat.rs`); the
  `projects.yaml` of the `GHA2DB_INPUT_DBS=-all-` cases is embedded in the
  test as well.
