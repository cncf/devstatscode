//! `vars` — Rust port of `cmd/vars/vars.go`.
//!
//! Reads `metrics/<project>/vars.yaml` (`GHA2DB_VARS_YAML`, `GHA2DB_VARS_FN_YAML`)
//! and upserts every variable into the project database's `gha_vars` table.
//! A variable is either a literal `value` or the trimmed combined output of a
//! `command` (with `{{datadir}}`/`{{project}}` expanded), optionally
//! post-processed by `loops`, `queries` (SQL results addressable as
//! `name:column:value:row:col`) and `replaces` (`[[from]]` → an earlier
//! variable, a `$ENV` value or a `:literal`). Environment (`GHA2DB_LOCAL`,
//! `GHA2DB_DATADIR`, `GHA2DB_EXCLUDE_VARS`, `GHA2DB_ONLY_VARS`, `GHA2DB_SKIPPDB`,
//! `GHA2DB_DEBUG`, `PG_*`), output and exit codes are those of the Go program.

use std::collections::BTreeMap;
use std::fmt::Display;
use std::process;
use std::time::Instant;

use serde::Deserialize;

use devstatscode::pg::{PgConn, ScanDest, SqlArg};
use devstatscode::yamlv2::de as yde;
use devstatscode::{
    exec, fatal_on_error, fatalf, gofmt, io, pg, printf, signal, time as gotime, Ctx,
};

/// Go `pvars`: list of Postgres variables to set (`vars.yaml`).
#[derive(Debug, Default, Clone, Deserialize, PartialEq)]
#[serde(default)]
struct PVars {
    #[serde(deserialize_with = "yde::seq")]
    vars: Vec<PVar>,
}

/// Go `pvar`: one variable definition (yaml.v2 decoding rules).
#[derive(Debug, Default, Clone, Deserialize, PartialEq)]
#[serde(default)]
struct PVar {
    #[serde(deserialize_with = "yde::string")]
    name: String,
    #[serde(rename = "type", deserialize_with = "yde::string")]
    typ: String,
    #[serde(deserialize_with = "yde::string")]
    value: String,
    #[serde(deserialize_with = "str_seq")]
    command: Vec<String>,
    #[serde(deserialize_with = "str_seq2")]
    replaces: Vec<Vec<String>>,
    #[serde(deserialize_with = "yde::boolean")]
    disabled: bool,
    #[serde(rename = "no_write", deserialize_with = "yde::boolean")]
    no_write: bool,
    #[serde(deserialize_with = "str_seq2")]
    queries: Vec<Vec<String>>,
    #[serde(deserialize_with = "int_seq2")]
    loops: Vec<Vec<i64>>,
    #[serde(rename = "queries_before", deserialize_with = "yde::boolean")]
    queries_before: bool,
    #[serde(rename = "queries_after", deserialize_with = "yde::boolean")]
    queries_after: bool,
    #[serde(rename = "loops_before", deserialize_with = "yde::boolean")]
    loops_before: bool,
    #[serde(rename = "loops_after", deserialize_with = "yde::boolean")]
    loops_after: bool,
}

/// Go `[]string` field.
fn str_seq<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    yde::seq::<D, yde::Str>(d).map(|v| v.into_iter().map(String::from).collect())
}

/// Go `[][]string` field.
fn str_seq2<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<Vec<String>>, D::Error> {
    yde::seq::<D, Option<Vec<yde::Str>>>(d).map(|v| {
        v.into_iter()
            .map(|inner| {
                inner
                    .unwrap_or_default()
                    .into_iter()
                    .map(String::from)
                    .collect()
            })
            .collect()
    })
}

/// Go `[][]int` field.
fn int_seq2<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<Vec<i64>>, D::Error> {
    yde::seq::<D, Option<Vec<yde::Int>>>(d).map(|v| {
        v.into_iter()
            .map(|inner| {
                inner
                    .unwrap_or_default()
                    .into_iter()
                    .map(i64::from)
                    .collect()
            })
            .collect()
    })
}

/// Go `%v` of a slice of slices: `[[a b] [c d]]`.
fn go_slice2<T: Display>(v: &[Vec<T>]) -> String {
    let inner: Vec<String> = v.iter().map(|x| gofmt::slice(x)).collect();
    format!("[{}]", inner.join(" "))
}

/// Query results: name → `column:value` → rows (all columns as strings).
type Queries = BTreeMap<String, BTreeMap<String, Vec<Vec<String>>>>;

/// Go `processLoops`: expand every `loop:N:start … loop:N:end` block,
/// repeating its body for `i` in `from..to` step `inc` with `loop:N:i`
/// replaced by the counter.
fn process_loops(mut s: String, loops: &[Vec<i64>]) -> String {
    for lp in loops {
        if lp.len() != 4 {
            fatalf!(
                "Loop definition should be array with 4 elements [n, from, to, inc], got: {}",
                gofmt::slice(lp)
            );
        }
        if lp[3] <= 0 {
            fatalf!("Loop increment must be positive, got: {}", gofmt::slice(lp));
        }
        let (loop_n, from, to, inc) = (lp[0], lp[1], lp[2], lp[3]);
        let start = format!("loop:{loop_n}:start");
        let end = format!("loop:{loop_n}:end");
        let rep = format!("loop:{loop_n}:i");
        while let (Some(i_start), Some(i_end)) = (s.find(&start), s.find(&end)) {
            // Go slices bytes here: `str[iStart+lStart : iEnd]` panics when
            // the end marker precedes the start marker.
            let body_start = i_start + start.len();
            if i_end < body_start {
                panic!("runtime error: slice bounds out of range [{body_start}:{i_end}]");
            }
            let before = &s[..i_start];
            let body = &s[body_start..i_end];
            let after = &s[i_end + end.len()..];
            let mut out = String::from(before);
            let mut i = from;
            while i < to {
                out.push_str(&body.replace(&rep, &i.to_string()));
                i += inc;
            }
            out.push_str(after);
            s = out;
        }
    }
    s
}

/// Go `processQueries`: replace `name:column:value:row:col` with the query
/// result cell (Go iterates its maps in random order; sorted here).
fn process_queries(mut s: String, queries: &Queries) -> String {
    for (name, query) in queries {
        for (mp, values) in query {
            let pref = format!("{name}:{mp}");
            for (r, columns) in values.iter().enumerate() {
                for (c, value) in columns.iter().enumerate() {
                    let rep = format!("{pref}:{r}:{c}");
                    s = s.replace(&rep, value);
                }
            }
        }
    }
    s
}

/// Go `handleQuery`: run `queryData[1]` and store its rows under
/// `queryData[0]`, indexed by every `queryData[2:]` column's value.
fn handle_query(c: &PgConn, ctx: &Ctx, queries: &mut Queries, query_data: &[String]) {
    if query_data.len() < 2 {
        fatalf!(
            "Query definition should be array with at least 2 elements [name, sql, columns...], got: {}",
            gofmt::slice(query_data)
        );
    }
    // Name to store query results
    let name = &query_data[0];
    if queries.contains_key(name) {
        fatalf!("query '{}' already defined", name);
    }

    // Execute SQL
    let sql = &query_data[1];
    let mut rows = pg::query_sql_with_err(c, ctx, sql, &[]);

    // Columns metadata
    let columns = rows.column_names();
    let mut columns_map: BTreeMap<&str, usize> = BTreeMap::new();
    for (i, col) in columns.iter().enumerate() {
        columns_map.insert(col.as_str(), i);
    }
    let mut results_map: BTreeMap<&str, usize> = BTreeMap::new();
    for mp in &query_data[2..] {
        match columns_map.get(mp.as_str()) {
            Some(i) => {
                results_map.insert(mp.as_str(), *i);
            }
            None => fatalf!(
                "column '{}' not found in query results: {}",
                mp,
                gofmt::slice(&columns)
            ),
        }
    }

    let n_columns = columns.len();
    let mut result: BTreeMap<String, Vec<Vec<String>>> = BTreeMap::new();
    // Values: every column is read as raw bytes (Go scans into `*[]byte`)
    while rows.next() {
        let mut vals: Vec<Vec<u8>> = vec![Vec::new(); n_columns];
        {
            let mut dest: Vec<&mut dyn ScanDest> =
                vals.iter_mut().map(|v| v as &mut dyn ScanDest).collect();
            if let Err(e) = rows.scan(&mut dest) {
                fatal_on_error(e);
            }
        }
        let svals: Vec<String> = vals
            .iter()
            .map(|v| String::from_utf8_lossy(v).into_owned())
            .collect();
        for (mp, i) in &results_map {
            let key = format!("{mp}:{}", svals[*i]);
            result.entry(key).or_default().push(svals.clone());
        }
    }
    if let Err(e) = rows.err() {
        fatal_on_error(e);
    }
    if let Err(e) = rows.close() {
        fatal_on_error(e);
    }
    queries.insert(name.clone(), result);
}

/// Go `pdbVars`: insert Postgres vars.
fn pdb_vars() {
    // Environment context parse
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);

    // Connect to Postgres DB
    let c = pg::pg_conn(&ctx);

    // Local or cron mode?
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    // Read vars to generate
    let data = match io::read_file(&ctx, &format!("{data_prefix}{}", ctx.vars_yaml)) {
        Ok(d) => d,
        Err(e) => fatal_on_error(e),
    };
    let all_vars: PVars = match yde::unmarshal(&data) {
        Ok(v) => v,
        Err(e) => fatal_on_error(e),
    };

    // All key name - values are stored in map
    // So next keys can replace strings using previous key values
    let mut replaces: BTreeMap<String, String> = BTreeMap::new();
    // Also make environment variables available too
    for (k, v) in std::env::vars_os() {
        replaces.insert(
            format!("${}", k.to_string_lossy()),
            v.to_string_lossy().into_owned(),
        );
    }
    // Queries
    let mut queries: Queries = BTreeMap::new();
    // Iterate vars
    for va in all_vars.vars {
        let mut va = va;
        // If given variable name is in the exclude list, skip it
        let skip = ctx.exclude_vars.contains_key(&va.name);
        if ctx.debug > 0 {
            printf!(
                "Variable Name '{}', Value '{}', Type '{}', Command {}, Replaces {}, Queries: {}, Loops: {}, Disabled: {}, Skip: {}, NoWrite: {}\n",
                va.name,
                va.value,
                va.typ,
                gofmt::slice(&va.command),
                go_slice2(&va.replaces),
                go_slice2(&va.queries),
                go_slice2(&va.loops),
                va.disabled,
                skip,
                va.no_write
            );
        }
        if skip || va.disabled {
            continue;
        }
        if va.typ.is_empty() || va.name.is_empty() || (va.value.is_empty() && va.command.is_empty())
        {
            printf!("Incorrect variable configuration, skipping\n");
            continue;
        }

        // Handle queries
        for query_data in &va.queries {
            handle_query(&c, &ctx, &mut queries, query_data);
        }

        if !va.command.is_empty() {
            for arg in va.command.iter_mut() {
                *arg = arg.replace("{{datadir}}", &data_prefix);
            }
            for arg in va.command.iter_mut() {
                *arg = arg.replace("{{project}}", &ctx.project);
            }
            let cmd_bytes = match exec::combined_output(&va.command) {
                Ok(out) => out,
                Err((_, err)) => {
                    printf!(
                        "Failed command: {} {}\n",
                        va.command[0],
                        gofmt::slice(&va.command[1..])
                    );
                    fatal_on_error(err);
                }
            };
            let mut out_string = String::from_utf8_lossy(&cmd_bytes).trim().to_string();
            if !out_string.is_empty() {
                // Process queries and loops (first pass)
                if va.loops_before {
                    out_string = process_loops(out_string, &va.loops);
                }
                if va.queries_before {
                    out_string = process_queries(out_string, &queries);
                }

                // Handle replacements using variables defined so far
                for repl in &va.replaces {
                    if repl.len() != 2 {
                        fatalf!(
                            "Replacement definition should be array with 2 elements, got: {}",
                            gofmt::slice(repl)
                        );
                    }
                    // Handle direct string replacements
                    let repl_to: String = if repl[1].starts_with(':') {
                        repl[1][1..].to_string()
                    } else {
                        match replaces.get(&repl[1]) {
                            Some(v) => v.clone(),
                            None => fatalf!(
                                "Variable '{}' requests replacing '{}', but not such variable is defined, defined: {}",
                                va.name,
                                repl[1],
                                gofmt::map(&replaces)
                            ),
                        }
                    };
                    // If 'replace from' starts with ':' then do not use [[ and ]] when replacing.
                    // That means you can replace non-template parts
                    if repl[0].len() > 1 && repl[0].starts_with(':') {
                        out_string = out_string.replace(&repl[0][1..], &repl_to);
                    } else {
                        out_string = out_string.replace(&format!("[[{}]]", repl[0]), &repl_to);
                        // Make replacements results available as variables too
                        if repl[0] != repl[1] {
                            replaces.insert(repl[0].clone(), repl_to);
                        }
                    }
                }
                // Process queries and loops (second pass after variables/replacements processing)
                if va.loops_after {
                    out_string = process_loops(out_string, &va.loops);
                }
                if va.queries_after {
                    out_string = process_queries(out_string, &queries);
                }
                va.value = out_string;
                if ctx.debug > 0 {
                    printf!(
                        "Name '{}', New Value '{}', Type '{}'\n",
                        va.name,
                        va.value,
                        va.typ
                    );
                }
            }
        }
        replaces.insert(va.name.clone(), va.value.clone());

        let mut write = !va.no_write;
        // If only selected variables mode is on, the check if we want to include this variable
        if write && !ctx.only_vars.is_empty() {
            write = ctx.only_vars.contains_key(&va.name);
        }

        if !ctx.skip_pdb && write {
            pg::exec_sql_with_err(
                &c,
                &ctx,
                &format!(
                    "insert into gha_vars(name, value_{}) {} on conflict(name) do update set value_{} = {} where gha_vars.name = {}",
                    va.typ,
                    pg::n_values(2),
                    va.typ,
                    pg::n_value(3),
                    pg::n_value(4)
                ),
                &[
                    SqlArg::from(&va.name),
                    SqlArg::from(&va.value),
                    SqlArg::from(&va.value),
                    SqlArg::from(&va.name),
                ],
            );
        } else if ctx.debug > 0 {
            printf!("Skipping postgres vars write\n");
        }
    }
    c.close();
}

fn main() {
    devstatscode::error::exit_on_panic();
    let dt_start = Instant::now();
    pdb_vars();
    printf!("Time: {}\n", gotime::format_go_duration(dt_start.elapsed()));
    process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loops_expand_bodies() {
        let s = "a loop:0:start[loop:0:i]loop:0:end b loop:1:start<loop:1:i>loop:1:end".to_string();
        let out = process_loops(s, &[vec![0, 0, 3, 1], vec![1, 2, 8, 3]]);
        assert_eq!(out, "a [0][1][2] b <2><5>");
        // several blocks of the same loop, nested markers of another loop
        let s = "loop:0:start(loop:0:i,loop:1:start-loop:1:i-loop:1:end)loop:0:end|loop:0:startxloop:0:end"
            .to_string();
        let out = process_loops(s, &[vec![0, 0, 2, 1], vec![1, 0, 2, 1]]);
        assert_eq!(out, "(0,-0--1-)(1,-0--1-)|xx");
        // empty range, missing markers, from >= to
        assert_eq!(
            process_loops("loop:0:start x loop:0:end".to_string(), &[vec![0, 5, 5, 1]]),
            ""
        );
        assert_eq!(
            process_loops("loop:0:start x".to_string(), &[vec![0, 0, 2, 1]]),
            "loop:0:start x"
        );
        assert_eq!(
            process_loops("no loops here".to_string(), &[vec![7, 0, 2, 1]]),
            "no loops here"
        );
    }

    #[test]
    fn queries_replace_cells() {
        let mut q: Queries = BTreeMap::new();
        let mut m: BTreeMap<String, Vec<Vec<String>>> = BTreeMap::new();
        m.insert(
            "series:s1".to_string(),
            vec![
                vec!["s1".into(), "n1".into(), "v1".into()],
                vec!["s1".into(), "n2".into(), "v2".into()],
            ],
        );
        m.insert(
            "series:s2".to_string(),
            vec![vec!["s2".into(), "n3".into(), "".into()]],
        );
        q.insert("metrics".to_string(), m);
        let s = "metrics:series:s1:0:2|metrics:series:s1:1:1|metrics:series:s2:0:2|metrics:series:s3:0:0|metrics:series:s1:2:0"
            .to_string();
        assert_eq!(
            process_queries(s, &q),
            "v1|n2||metrics:series:s3:0:0|metrics:series:s1:2:0"
        );
    }

    #[test]
    fn yaml_decoding_follows_yaml_v2() {
        let y = br#"---
vars:
  - name: a
    type: s
    value: 1
    command: [hostname]
    replaces:
      - [hostname, os_hostname]
      - [':x', ':y']
    queries_before: yes
    loops:
      - [0,0,253,1] # comment
    no_write: true
    unknown_field: ignored
  - name: b
    type: i
    value: "12"
"#;
        let v: PVars = yde::unmarshal(y).unwrap();
        assert_eq!(v.vars.len(), 2);
        let a = &v.vars[0];
        assert_eq!(a.name, "a");
        assert_eq!(a.value, "1");
        assert_eq!(a.command, vec!["hostname".to_string()]);
        assert_eq!(a.replaces.len(), 2);
        assert_eq!(a.replaces[1], vec![":x".to_string(), ":y".to_string()]);
        assert!(a.queries_before);
        assert!(!a.queries_after);
        assert_eq!(a.loops, vec![vec![0, 0, 253, 1]]);
        assert!(a.no_write);
        assert!(!a.disabled);
        assert_eq!(v.vars[1].typ, "i");
        assert_eq!(v.vars[1].value, "12");
        let empty: PVars = yde::unmarshal(b"---\n").unwrap();
        assert!(empty.vars.is_empty());
        assert_eq!(go_slice2(&a.loops), "[[0 0 253 1]]");
        assert_eq!(go_slice2(&a.replaces), "[[hostname os_hostname] [:x :y]]");
        assert_eq!(go_slice2::<String>(&[]), "[]");
    }
}
