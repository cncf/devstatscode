//! `columns` — Rust port of `cmd/columns/columns.go`.
//!
//! Reads `metrics/<project>/columns.yaml` (`GHA2DB_COLUMNS_YAML`) and makes
//! sure that every TSDB series table matching a config's `table_regexp` has
//! one `double precision` (or `hll`) column per value of the given tag
//! (`select "<column>" from "<tag>"`): stale columns are dropped, missing
//! ones added, and the newly added ones are then mass-updated to their
//! default (`0.0` / `hll_empty()`) and made `not null` with that default.
//! Configs and then tables are processed concurrently by up to
//! `GetThreadsNum` workers (`GHA2DB_ST`, `GHA2DB_NCPUS`). Environment,
//! output and exit codes are those of the Go program.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{mpsc, Mutex};
use std::thread;
use std::time::Instant;

use serde::Deserialize;

use devstatscode::pg::api::AddedColumns;
use devstatscode::yamlv2::de as yde;
use devstatscode::{fatal_on_error, gofmt, io, pg, printf, signal, threads, time as gotime, Ctx};

/// Go `columns`: list of columns that must be present on certain series.
#[derive(Debug, Default, Clone, Deserialize, PartialEq)]
#[serde(default)]
struct Columns {
    #[serde(deserialize_with = "yde::seq")]
    columns: Vec<Column>,
}

/// Go `column`: configuration of the columns needed on specific series
/// (yaml.v2 decoding rules).
#[derive(Debug, Default, Clone, Deserialize, PartialEq)]
#[serde(default)]
struct Column {
    #[serde(rename = "table_regexp", deserialize_with = "yde::string")]
    table_regexp: String,
    #[serde(deserialize_with = "yde::string")]
    tag: String,
    #[serde(deserialize_with = "yde::string")]
    column: String,
    #[serde(deserialize_with = "yde::boolean")]
    hll: bool,
}

/// Go `%+v` of a `*column`: `&{TableRegexp:… Tag:… Column:… HLL:false}`.
impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "&{{TableRegexp:{} Tag:{} Column:{} HLL:{}}}",
            self.table_regexp, self.tag, self.column, self.hll
        )
    }
}

/// Result of one column config worker: parallel lists of
/// (table, added column, "y"/"n" for HLL).
type Added = (Vec<String>, Vec<String>, Vec<String>);

const MAX_TRIALS: usize = 3;

/// Phase 1 for one column config (the Go per-config goroutine): make sure
/// every matching table has exactly the tag's columns; returns the columns
/// added by this worker.
fn ensure_column_config(
    con: &pg::PgConn,
    ctx: &Ctx,
    col: &Column,
    added_cols: &AddedColumns,
) -> Added {
    let mut tables = Vec::new();
    let mut cols = Vec::new();
    let mut hlls = Vec::new();
    if ctx.debug >= 0 {
        printf!("Ensure column config: {}\n", col);
    }
    let mut crows = pg::api::query_sql_with_err(
        con,
        ctx,
        &format!(
            "select \"{}\" from \"{}\" order by time asc",
            col.column, col.tag
        ),
        &[],
    );
    let col_type = if col.hll { "hll" } else { "double precision" };
    let mut col_name = String::new();
    let mut col_names: Vec<String> = Vec::new();
    while crows.next() {
        if let Err(e) = crows.scan(&mut [&mut col_name]) {
            fatal_on_error(e);
        }
        col_names.push(col_name.clone());
    }
    if let Err(e) = crows.err() {
        fatal_on_error(e);
    }
    if let Err(e) = crows.close() {
        fatal_on_error(e);
    }
    if col_names.is_empty() {
        printf!("Warning: no tag values for ({}, {})\n", col.column, col.tag);
        return (tables, cols, hlls);
    }
    if ctx.debug > 0 {
        printf!(
            "Ensure columns({}): {} --> {}\n",
            col_names.len(),
            col,
            gofmt::slice(&col_names)
        );
    } else {
        printf!("Ensure {} columns in '{}'\n", col_names.len(), col);
    }
    let mut rows = pg::api::query_sql_with_err(
        con,
        ctx,
        &format!(
            "select tablename from pg_catalog.pg_tables where schemaname = 'public' and tablename ~ {} order by tablename",
            pg::api::n_value(1)
        ),
        &[pg::SqlArg::from(col.table_regexp.as_str())],
    );
    let mut table = String::new();
    let mut num_tables = 0usize;
    while rows.next() {
        if let Err(e) = rows.scan(&mut [&mut table]) {
            fatal_on_error(e);
        }
        let (mut curr_cols, curr_cols_map) =
            match pg::api::get_current_table_columns(con, ctx, &table) {
                Ok(v) => v,
                Err(e) => fatal_on_error(e),
            };
        if ctx.debug > 0 {
            curr_cols.sort();
            printf!(
                "Current columns({}): {} --> {}\n",
                curr_cols.len(),
                table,
                gofmt::slice(&curr_cols)
            );
        } else {
            printf!("Currently {} columns in '{}'\n", curr_cols.len(), table);
        }
        let mut cols_to_del = pg::api::identify_columns_to_delete(&curr_cols, &col_names);
        if !cols_to_del.is_empty() {
            cols_to_del.sort();
            printf!(
                "Need to delete {} columns: {} from '{}' table\n",
                cols_to_del.len(),
                gofmt::slice(&cols_to_del),
                table
            );
        }
        for col_name in &cols_to_del {
            if let Err(e) = pg::api::exec_sql(
                con,
                ctx,
                &format!(
                    "alter table \"{}\" drop column if exists \"{}\"",
                    table, col_name
                ),
                &[],
            ) {
                fatal_on_error(e);
            }
            printf!("Deleted column \"{}\" from '{}' table\n", col_name, table);
        }
        for col_name in &col_names {
            let mut trials = 0usize;
            loop {
                match pg::api::exec_sql(
                    con,
                    ctx,
                    &format!(
                        "alter table \"{}\" add column if not exists \"{}\" {}",
                        table, col_name, col_type
                    ),
                    &[],
                ) {
                    Ok(_) => {
                        if !curr_cols_map.contains(col_name) {
                            {
                                let mut added =
                                    added_cols.lock().unwrap_or_else(|e| e.into_inner());
                                added
                                    .entry(table.clone())
                                    .or_default()
                                    .insert(col_name.clone());
                            }
                            printf!("Added column \"{}\" to '{}' table\n", col_name, table);
                            tables.push(table.clone());
                            cols.push(col_name.clone());
                            hlls.push(if col.hll { "y" } else { "n" }.to_string());
                        }
                        break;
                    }
                    Err(err) => {
                        let info = format!("add column {}/{}", col_name, col_type);
                        let rtry = pg::api::handle_row_is_too_big(
                            con,
                            ctx,
                            &table,
                            &info,
                            Some(added_cols),
                            Some(&err),
                        );
                        if rtry {
                            trials += 1;
                            if trials < MAX_TRIALS {
                                continue;
                            }
                            printf!("Give up '{}' after {} trials\n", info, MAX_TRIALS);
                        }
                        break;
                    }
                }
            }
        }
        num_tables += 1;
    }
    if let Err(e) = rows.err() {
        fatal_on_error(e);
    }
    if let Err(e) = rows.close() {
        fatal_on_error(e);
    }
    if num_tables == 0 {
        printf!("Warning: '{}': no table hits\n", col);
    }
    (tables, cols, hlls)
}

/// Phase 2 for one table (the Go per-table goroutine): set the added
/// columns to their default value and make them `not null` with that
/// default. `cols` maps column → "y"/"n" (HLL or not).
fn finalize_table(
    con: &pg::PgConn,
    ctx: &Ctx,
    tab: &str,
    cols: &BTreeMap<String, String>,
    added_cols: &AddedColumns,
) {
    let def = |hll: &str| -> &'static str {
        if hll == "y" {
            "hll_empty()"
        } else {
            "0.0"
        }
    };
    let n_cols = cols.len();
    let mut trials = 0usize;
    // Go `retry:` label: a "row is too big" failure of either statement
    // restarts from the update.
    loop {
        let sets: Vec<String> = cols
            .iter()
            .map(|(col, hll)| format!("\"{}\" = {}", col, def(hll)))
            .collect();
        let s = format!("update \"{}\" set {}", tab, sets.join(", "));
        let dt_start = Instant::now();
        let res = pg::api::exec_sql(con, ctx, &s, &[]);
        let took = dt_start.elapsed();
        match res {
            Ok(_) => {
                printf!(
                    "Mass updated \"{}\", columns: {}, took: {}\n",
                    tab,
                    n_cols,
                    gotime::format_go_duration(took)
                );
            }
            Err(err) => {
                let rtry = pg::api::handle_row_is_too_big(
                    con,
                    ctx,
                    tab,
                    "mass add columns",
                    Some(added_cols),
                    Some(&err),
                );
                if rtry {
                    trials += 1;
                    if trials < MAX_TRIALS {
                        continue;
                    }
                    printf!("Give up 'mass add columns' after {} trials\n", MAX_TRIALS);
                }
            }
        }
        let alters: Vec<String> = cols
            .iter()
            .map(|(col, hll)| {
                format!(
                    "alter column \"{}\" set not null, alter column \"{}\" set default {}",
                    col,
                    col,
                    def(hll)
                )
            })
            .collect();
        let s = format!("alter table \"{}\" {}", tab, alters.join(", "));
        let dt_start = Instant::now();
        let res = pg::api::exec_sql(con, ctx, &s, &[]);
        let took = dt_start.elapsed();
        match res {
            Ok(_) => {
                printf!(
                    "Altered \"{}\" defaults and restrictions, columns: {}, took: {}\n",
                    tab,
                    n_cols,
                    gotime::format_go_duration(took)
                );
            }
            Err(err) => {
                let rtry = pg::api::handle_row_is_too_big(
                    con,
                    ctx,
                    tab,
                    "mass alter defaults",
                    Some(added_cols),
                    Some(&err),
                );
                if rtry {
                    trials += 1;
                    if trials < MAX_TRIALS {
                        continue;
                    }
                    printf!(
                        "Give up 'mass alter defaults' after {} trials\n",
                        MAX_TRIALS
                    );
                }
            }
        }
        break;
    }
}

/// Ensure that specific TSDB series have all needed columns (Go
/// `ensureColumns`).
fn ensure_columns() {
    // Environment context parse
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);

    // If skip TSDB or only ES output - nothing to do
    if ctx.skip_tsdb {
        return;
    }

    // Connect to Postgres DB
    let con = pg::pg_conn(&ctx);

    // Local or cron mode?
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    // Read columns config
    let path = format!("{data_prefix}{}", ctx.columns_yaml);
    let data = match io::read_file(&ctx, &path) {
        Ok(d) => d,
        Err(e) => fatal_on_error(e),
    };
    let all_columns: Columns = match yde::unmarshal(&data) {
        Ok(c) => c,
        Err(e) => fatal_on_error(e),
    };
    if ctx.debug > 0 {
        printf!(
            "Read {} columns configs from '{}'\n",
            all_columns.columns.len(),
            path
        );
    }

    let thr_n = threads::get_threads_num(&mut ctx);
    let ctx = &ctx;
    let con = &con;
    let added_cols: AddedColumns = Mutex::new(HashMap::new());
    let added_cols = &added_cols;
    let mut all_tables: Vec<String> = Vec::new();
    let mut all_cols: Vec<String> = Vec::new();
    let mut all_hlls: Vec<String> = Vec::new();
    // One worker per column config, at most `thr_n` running at a time,
    // synchronised through an unbuffered channel like the Go goroutines.
    thread::scope(|s| {
        let (tx, rx) = mpsc::sync_channel::<Added>(0);
        let mut n_threads = 0usize;
        let mut collect = |data: Added| {
            let (tables, cols, hlls) = data;
            for (i, table) in tables.into_iter().enumerate() {
                all_tables.push(table);
                all_cols.push(cols[i].clone());
                all_hlls.push(hlls[i].clone());
            }
        };
        for col in &all_columns.columns {
            let tx = tx.clone();
            s.spawn(move || {
                let data = ensure_column_config(con, ctx, col, added_cols);
                // Synchronize go routine
                let _ = tx.send(data);
            });
            n_threads += 1;
            if n_threads >= thr_n {
                if let Ok(data) = rx.recv() {
                    collect(data);
                }
                n_threads -= 1;
            }
        }
        // Usually all work happens on '<-ch'
        while n_threads > 0 {
            if let Ok(data) = rx.recv() {
                collect(data);
            }
            n_threads -= 1;
        }
    });
    if ctx.debug > 1 {
        printf!("Tables: {}\n", gofmt::slice(&all_tables));
        printf!("Columns: {}\n", gofmt::slice(&all_cols));
        printf!("HLLs: {}\n", gofmt::slice(&all_hlls));
    }
    // Go `map[string]map[string]string`; sorted here (Go's map order is
    // random, its `%+v` output sorted).
    let mut cfg: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for (i, table) in all_tables.iter().enumerate() {
        cfg.entry(table.clone())
            .or_default()
            .insert(all_cols[i].clone(), all_hlls[i].clone());
    }
    if ctx.debug > 0 {
        let rendered: BTreeMap<&String, String> =
            cfg.iter().map(|(t, m)| (t, gofmt::map(m))).collect();
        printf!("Cfg: {}\n", gofmt::map(&rendered));
    }

    // process separate tables in parallel
    thread::scope(|s| {
        let (tx, rx) = mpsc::sync_channel::<(String, String)>(0);
        let mut n_threads = 0usize;
        for (table, columns) in &cfg {
            let tx = tx.clone();
            s.spawn(move || {
                finalize_table(con, ctx, table, columns, added_cols);
                let _ = tx.send((table.clone(), "ok".to_string()));
            });
            n_threads += 1;
            if n_threads >= thr_n {
                let _ = rx.recv();
                n_threads -= 1;
            }
        }
        while n_threads > 0 {
            let _ = rx.recv();
            n_threads -= 1;
        }
    });
    // `defer func() { lib.FatalOnError(con.Close()) }()` — closing the pool
    // cannot fail here.
    con.close();
}

fn main() {
    devstatscode::error::exit_on_panic();
    let dt_start = Instant::now();
    ensure_columns();
    printf!("Time: {}\n", gotime::format_go_duration(dt_start.elapsed()));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn yaml_decoding_follows_yaml_v2() {
        let y = b"---\ncolumns:\n  - table_regexp: '^s(act|commits)$'\n    tag: trepo_groups\n    column: repo_group_name\n  - table_regexp: '^sprjcntr$'\n    tag: trepo_groups\n    column: repo_group_name\n    hll: true\n  - table_regexp: 7\n    hll: yes\n";
        let c: Columns = yde::unmarshal(y).unwrap();
        assert_eq!(c.columns.len(), 3);
        assert_eq!(c.columns[0].table_regexp, "^s(act|commits)$");
        assert_eq!(c.columns[0].tag, "trepo_groups");
        assert_eq!(c.columns[0].column, "repo_group_name");
        assert!(!c.columns[0].hll);
        assert!(c.columns[1].hll);
        assert_eq!(c.columns[2].table_regexp, "7");
        assert_eq!(c.columns[2].tag, "");
        assert!(c.columns[2].hll);
        let empty: Columns = yde::unmarshal(b"---\n").unwrap();
        assert!(empty.columns.is_empty());
        let no_list: Columns = yde::unmarshal(b"---\ncolumns:\n").unwrap();
        assert!(no_list.columns.is_empty());
    }

    #[test]
    fn column_formats_like_go_plus_v() {
        let c = Column {
            table_regexp: "^s(act|commits|grp_pr_merg)$".into(),
            tag: "trepo_groups".into(),
            column: "repo_group_name".into(),
            hll: false,
        };
        assert_eq!(
            c.to_string(),
            "&{TableRegexp:^s(act|commits|grp_pr_merg)$ Tag:trepo_groups Column:repo_group_name HLL:false}"
        );
        assert_eq!(
            Column::default().to_string(),
            "&{TableRegexp: Tag: Column: HLL:false}"
        );
    }
}
