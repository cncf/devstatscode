//! Port of `computed.go`: `gha_computed` markers of successful `calc_metric` runs.
//!
//! `calc_metric` writes one `(key, sync hour)` row once all points of a metric & period
//! are written (never when the run fails); `gha2db_sync` checks them (in addition to
//! `time::compute_period_at_this_date`) to rerun metrics that failed or were not
//! computed since the current period (see `time::period_class`) started.

use chrono::{DateTime, Utc};

use crate::context::Ctx;
use crate::pg::{
    exec_sql_with_err, fatal_on_pg_err, insert_ignore, n_value, n_values, query_sql_with_err,
    PgConn, SqlArg,
};
use crate::time::{hour_start, period_start_at};

/// Go `PeriodComputedKey`: `gha_computed` key of a single `calc_metric` run:
/// `"<proj>/<file>.sql <series_name_or_func> <period>"`; the SQL file path is reduced to
/// its last two components, so the key does not depend on the data directory.
pub fn period_computed_key(series_name_or_func: &str, sql_file: &str, period: &str) -> String {
    let ary: Vec<&str> = sql_file.split('/').collect();
    let file = if ary.len() > 2 {
        ary[ary.len() - 2..].join("/")
    } else {
        sql_file.to_string()
    };
    format!("{file} {series_name_or_func} {period}")
}

/// Go `IsPeriodComputed`: true when `gha_computed` has a marker of `key` written by a sync
/// between the start of the period containing `to` (see `period_start_at`, `range_start` is
/// the start of a histogram quick range, `None` otherwise) and `to`; always true for periods
/// without a calendar period.
pub fn is_period_computed(
    con: &PgConn,
    ctx: &Ctx,
    key: &str,
    period: &str,
    range_start: Option<DateTime<Utc>>,
    to: DateTime<Utc>,
) -> bool {
    let Some(dt) = period_start_at(ctx, period, range_start, to) else {
        return true;
    };
    let mut rows = query_sql_with_err(
        con,
        ctx,
        &format!(
            "select 1 from gha_computed where metric = {} and dt >= {} and dt <= {} limit 1",
            n_value(1),
            n_value(2),
            n_value(3)
        ),
        &[SqlArg::from(key), SqlArg::from(dt), SqlArg::from(to)],
    );
    let mut i: i64 = 0;
    while rows.next() {
        fatal_on_pg_err(rows.scan(&mut [&mut i]));
    }
    fatal_on_pg_err(rows.err());
    fatal_on_pg_err(rows.close());
    i > 0
}

/// Go `SetPeriodComputed`: marks `key` as successfully computed by the sync ending at `to`
/// (hour precision).
pub fn set_period_computed(con: &PgConn, ctx: &Ctx, key: &str, to: DateTime<Utc>) {
    exec_sql_with_err(
        con,
        ctx,
        &insert_ignore(&format!("into gha_computed(metric, dt) {}", n_values(2))),
        &[SqlArg::from(key), SqlArg::from(hour_start(to))],
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mirrors Go `TestPeriodComputedKey` (computed_test.go).
    #[test]
    fn period_computed_key_table() {
        let cases = [
            (
                "reviewers",
                "/etc/gha2db/metrics/kubernetes/reviewers.sql",
                "d",
                "kubernetes/reviewers.sql reviewers d",
            ),
            (
                "multi_row_multi_column",
                "./metrics/all/companies.sql",
                "w",
                "all/companies.sql multi_row_multi_column w",
            ),
            (
                "multi_row_multi_column",
                "metrics/all/companies.sql",
                "w",
                "all/companies.sql multi_row_multi_column w",
            ),
            ("f", "all/companies.sql", "d7", "all/companies.sql f d7"),
            ("f", "companies.sql", "y10", "companies.sql f y10"),
            ("f", "", "h", " f h"),
            (
                "hist_reviewers_d",
                "/a/b/c/kubernetes/hist_reviewers.sql",
                "a_0_n",
                "kubernetes/hist_reviewers.sql hist_reviewers_d a_0_n",
            ),
        ];
        for (series, file, period, expected) in cases {
            assert_eq!(
                period_computed_key(series, file, period),
                expected,
                "{series} {file} {period}"
            );
        }
    }
}
