//! Port of `tags.go`: TSDB tag series — the configuration read from
//! `tags.yaml` and `ProcessTag`, which runs one tag SQL and writes the tag
//! values as a `t<series_name>` time series (used by the `tags` tool and by
//! `gha2db_sync`).

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::context::Ctx;
use crate::error::fatal_on_error;
use crate::gofmt;
use crate::pg::{
    self, exec_sql_tx, fatal_on_pg_err, fatal_on_pg_error, query_sql_with_err, table_exists,
    write_ts_points, PgConn,
};
use crate::ts_points::{add_ts_point, new_ts_point, TSPoints};
use crate::unicode::normalize_name;
use crate::yamlv2::de as yde;
use crate::{fatalf, printf};

/// Go `Tags`: list of TSDB tags (`tags.yaml`).
#[derive(Debug, Default, Clone, Deserialize, PartialEq)]
#[serde(default)]
pub struct Tags {
    #[serde(deserialize_with = "yde::seq")]
    pub tags: Vec<Tag>,
}

/// Go `Tag`: one TSDB tag definition (yaml.v2 decoding rules).
#[derive(Debug, Default, Clone, Deserialize, PartialEq)]
#[serde(default)]
pub struct Tag {
    #[serde(deserialize_with = "yde::string")]
    pub name: String,
    #[serde(rename = "sql", deserialize_with = "yde::string")]
    pub sql_file: String,
    #[serde(rename = "series_name", deserialize_with = "yde::string")]
    pub series_name: String,
    #[serde(rename = "name_tag", deserialize_with = "yde::string")]
    pub name_tag: String,
    #[serde(rename = "value_tag", deserialize_with = "yde::string")]
    pub value_tag: String,
    /// `other_tags: {tag_name: [column, normalize?]}`.
    #[serde(rename = "other_tags", deserialize_with = "yde::str_array_map::<_, 2>")]
    pub other_tags: BTreeMap<String, [String; 2]>,
    #[serde(deserialize_with = "yde::int")]
    pub limit: i64,
    #[serde(deserialize_with = "yde::boolean")]
    pub disabled: bool,
}

impl Tag {
    /// Go `%+v` of a `Tag` (used by the "have no values" warning).
    pub fn go_string(&self) -> String {
        let other: BTreeMap<&str, String> = self
            .other_tags
            .iter()
            .map(|(k, v)| (k.as_str(), format!("[{} {}]", v[0], v[1])))
            .collect();
        format!(
            "{{Name:{} SQLFile:{} SeriesName:{} NameTag:{} ValueTag:{} OtherTags:{} Limit:{} Disabled:{}}}",
            self.name,
            self.sql_file,
            self.series_name,
            self.name_tag,
            self.value_tag,
            gofmt::map(&other),
            self.limit,
            self.disabled
        )
    }
}

/// `tx, err := con.Begin(); FatalOnError(err)` — after one of the retryable
/// conditions (`FatalOnError` returned instead of exiting) Go would go on with
/// a nil transaction and crash; here the begin is simply retried.
fn begin_tx(con: &PgConn) -> pg::PgTx<'_> {
    loop {
        match con.begin() {
            Ok(tx) => return tx,
            Err(e) => {
                fatal_on_pg_error(&e);
            }
        }
    }
}

/// `_, err = ExecSQLTx(...); FatalOnError(err)`.
fn exec_tx_fatal(tx: &mut pg::PgTx<'_>, ctx: &Ctx, query: &str) {
    if let Err(e) = exec_sql_tx(tx, ctx, query, &[]) {
        fatal_on_pg_error(&e);
    }
}

/// Go `yaml.Unmarshal` of a `tags.yaml` document.
pub fn parse_tags(data: &[u8]) -> Result<Tags, String> {
    yde::unmarshal(data)
}

/// Go `ProcessTag`: insert the given tag's values into the Postgres TSDB.
///
/// `replaces` are extra `{from, to}` text replacements applied to the SQL
/// (every entry must have exactly two elements, like the Go `[][]string`).
pub fn process_tag(con: &PgConn, ctx: &Ctx, tg: &Tag, replaces: &[Vec<String>]) {
    // Batch TS points
    let mut pts: TSPoints = Vec::new();

    // Skip disabled tags
    if tg.disabled && !ctx.test_mode {
        return;
    }

    // Local or cron mode
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    // Per project directory for SQL files
    let mut dir = crate::consts::METRICS.to_string();
    if !ctx.project.is_empty() {
        dir.push_str(&ctx.project);
        dir.push('/');
    }

    // Read SQL file
    let bytes = match crate::io::read_file(ctx, &format!("{data_prefix}{dir}{}.sql", tg.sql_file)) {
        Ok(b) => b,
        Err(e) => fatal_on_error(e),
    };
    let mut sql_query = String::from_utf8_lossy(&bytes).into_owned();

    // Handle excluding bots
    let bytes = match crate::io::read_file(ctx, &format!("{data_prefix}util_sql/exclude_bots.sql"))
    {
        Ok(b) => b,
        Err(e) => fatal_on_error(e),
    };
    let exclude_bots = String::from_utf8_lossy(&bytes).into_owned();

    // Transform SQL
    let mut limit = tg.limit;
    if limit <= 0 {
        limit = 127;
    }
    sql_query = sql_query.replace("{{lim}}", &limit.to_string());
    sql_query = sql_query.replace("{{exclude_bots}}", &exclude_bots);

    // Replaces
    for replace in replaces {
        if replace.len() != 2 {
            fatal_on_error(format!(
                "replace(s) should have length 2, invalid: {}",
                gofmt::slice(replace)
            ));
        }
        sql_query = sql_query.replace(&replace[0], &replace[1]);
    }

    // Execute SQL
    let mut rows = query_sql_with_err(con, ctx, &sql_query, &[]);

    // Drop current tags
    if !ctx.skip_tsdb {
        let table = format!("t{}", tg.series_name);
        if table_exists(con, ctx, &table) {
            let mut tx = begin_tx(con);
            exec_tx_fatal(&mut tx, ctx, "set local lock_timeout='500ms'");
            if let Err(e) = exec_sql_tx(&mut tx, ctx, &format!("truncate {table}"), &[]) {
                printf!("truncate failed for {} (warning): {}\n", table, e);
                // The failed truncate aborted the transaction: the delete
                // fallback needs a fresh one (Go bug 9, fixed in both).
                let _ = tx.rollback();
                let mut tx = begin_tx(con);
                exec_tx_fatal(&mut tx, ctx, "set local lock_timeout='500ms'");
                exec_tx_fatal(&mut tx, ctx, "set local statement_timeout='300s'");
                match exec_sql_tx(&mut tx, ctx, &format!("delete from {table}"), &[]) {
                    Err(e2) => {
                        let _ = tx.rollback();
                        printf!("delete failed for {} (warning): {}\n", table, e2);
                    }
                    Ok(_) => {
                        if let Err(e) = tx.commit() {
                            fatal_on_pg_error(&e);
                        }
                    }
                }
            } else if let Err(e) = tx.commit() {
                fatal_on_pg_error(&e);
            }
        }
    }
    let mut tm = crate::time::time_parse_any("2012-07-01");

    // Columns
    let columns: Vec<String> = rows.column_names();
    let mut col_idx: BTreeMap<&str, usize> = BTreeMap::new();
    for (i, column) in columns.iter().enumerate() {
        col_idx.insert(column.as_str(), i);
    }

    // Iterate tag values
    let mut tags: BTreeMap<String, String> = BTreeMap::new();
    let mut got = false;
    while rows.next() {
        got = true;
        // Go scans every column into a `*[]byte` (NULL → empty).
        let mut vals: Vec<Vec<u8>> = vec![Vec::new(); columns.len()];
        {
            let mut dests: Vec<&mut dyn pg::ScanDest> = vals
                .iter_mut()
                .map(|v| v as &mut dyn pg::ScanDest)
                .collect();
            fatal_on_pg_err(rows.scan(&mut dests));
        }
        let s_vals: Vec<String> = vals
            .iter()
            .map(|v| String::from_utf8_lossy(v).into_owned())
            .collect();
        let str_val = s_vals[0].clone();
        if !tg.name_tag.is_empty() {
            tags.insert(tg.name_tag.clone(), str_val.clone());
        }
        if !tg.value_tag.is_empty() {
            tags.insert(tg.value_tag.clone(), normalize_name(&str_val));
        }
        for (t_name, t_data) in &tg.other_tags {
            let t_value = &t_data[0];
            let Some(&c_idx) = col_idx.get(t_value.as_str()) else {
                fatalf!("other tag: name: {}: column {} not found", t_name, t_value);
            };
            tags.insert(t_name.clone(), s_vals[c_idx].clone());
            let t_norm = t_data[1].to_lowercase();
            if t_norm == "1" || t_norm == "t" || t_norm == "y" {
                tags.insert(format!("{t_name}_norm"), normalize_name(&s_vals[c_idx]));
            }
        }
        if ctx.debug > 0 {
            printf!("'{}': {}\n", tg.series_name, gofmt::map(&tags));
        }
        // Add batch point
        let pt = new_ts_point(ctx, &tg.series_name, "", Some(&tags), None, tm, false);
        add_ts_point(ctx, &mut pts, pt);
        tm += chrono::Duration::hours(1);
    }
    if let Err(e) = rows.err() {
        fatal_on_pg_error(&e);
    }
    if let Err(e) = rows.close() {
        fatal_on_pg_error(&e);
    }
    if !got {
        // Go formats the `*Tag` pointer with `%+v`: `&{Name:… Disabled:false}`.
        printf!("Warning: Tag '&{}' have no values\n", tg.go_string());
    }

    // Write the batch
    if !ctx.skip_tsdb {
        write_ts_points(ctx, con, &pts, "", &[], None);
    } else if ctx.debug > 0 {
        printf!("Skipping tags series write\n");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_real_tags_yaml_shape() {
        let y = b"---\ntags:\n  - name: Repos\n    sql: repos_tags\n    series_name: repos\n    name_tag: repo_name\n    limit: 200\n  - name: Bot commands\n    sql: bot_commands_tags\n    series_name: bot_commands\n    name_tag: bot_command_name\n    value_tag: bot_command_value\n    other_tags:\n      alias: [full_command, yes]\n    disabled: true\n";
        let t = parse_tags(y).unwrap();
        assert_eq!(t.tags.len(), 2);
        assert_eq!(t.tags[0].limit, 200);
        assert_eq!(t.tags[0].value_tag, "");
        assert!(t.tags[0].other_tags.is_empty());
        assert!(!t.tags[0].disabled);
        assert!(t.tags[1].disabled);
        assert_eq!(
            t.tags[1].other_tags["alias"],
            ["full_command".to_string(), "yes".to_string()]
        );
        // empty document → zero value, like yaml.Unmarshal
        assert_eq!(parse_tags(b"").unwrap(), Tags::default());
        assert_eq!(parse_tags(b"---\n").unwrap(), Tags::default());
        assert!(parse_tags(b"tags: [\n").is_err());
    }

    #[test]
    fn go_string_matches_percent_plus_v() {
        let mut tg = Tag {
            name: "Repos".into(),
            sql_file: "repos_tags".into(),
            series_name: "repos".into(),
            name_tag: "repo_name".into(),
            limit: 200,
            ..Tag::default()
        };
        assert_eq!(
            tg.go_string(),
            "{Name:Repos SQLFile:repos_tags SeriesName:repos NameTag:repo_name ValueTag: OtherTags:map[] Limit:200 Disabled:false}"
        );
        tg.other_tags.insert("z".into(), ["col".into(), "1".into()]);
        tg.other_tags
            .insert("a".into(), ["c2".into(), String::new()]);
        assert_eq!(
            tg.go_string(),
            "{Name:Repos SQLFile:repos_tags SeriesName:repos NameTag:repo_name ValueTag: OtherTags:map[a:[c2 ] z:[col 1]] Limit:200 Disabled:false}"
        );
    }
}
