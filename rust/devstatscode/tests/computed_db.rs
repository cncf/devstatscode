//! 1:1 port of the Go `computed_db_test.go::TestPeriodComputedMarkers` DB test:
//! `set_period_computed` / `is_period_computed` against a real `gha_computed`
//! table (created exactly like `structure.go` does).
//!
//! Like the Go test this only runs with `PG_DB=dbtest` (see `test.sh`); it
//! uses its own scratch database `dbtest_computed`.

use devstats_compat::pg::{self as tpg, TestDb};
use devstatscode::chrono::{DateTime, TimeZone, Utc};
use devstatscode::computed::{is_period_computed, period_computed_key, set_period_computed};
use devstatscode::pg::{create_table, exec_sql_with_err, PgConn, SqlArg};

/// Go `testlib.YMDHMS`: missing parts default to 1 (month, day) / 0 (h, m, s).
fn ft(parts: &[i32]) -> DateTime<Utc> {
    let g = |i: usize, def: i32| parts.get(i).copied().unwrap_or(def);
    Utc.with_ymd_and_hms(
        g(0, 2000),
        g(1, 1) as u32,
        g(2, 1) as u32,
        g(3, 0) as u32,
        g(4, 0) as u32,
        g(5, 0) as u32,
    )
    .unwrap()
}

/// Go `markerRows`: the `gha_computed` dates stored for a given key.
fn marker_rows(c: &PgConn, key: &str) -> Vec<String> {
    tpg::snapshot(
        c,
        "select dt::text from gha_computed where metric = $1 order by dt",
        &[SqlArg::from(key)],
    )
    .rows
    .into_iter()
    .map(|r| r[0].clone())
    .collect()
}

#[test]
fn period_computed_markers() {
    let Some(db) = TestDb::fresh("computed") else {
        return;
    };
    let mut ctx = db.ctx.clone();
    let c = db.conn();

    // The same 'gha_computed' definition as in structure.go
    exec_sql_with_err(
        &c,
        &ctx,
        &create_table(
            "gha_computed(metric text not null, dt {{ts}} not null, primary key(metric, dt))",
        ),
        &[],
    );

    let sql_file = "/etc/gha2db/metrics/kubernetes/reviewers.sql";
    let w_key = period_computed_key("reviewers", sql_file, "w");
    let d_key = period_computed_key("reviewers", sql_file, "d");
    let h_key = period_computed_key("reviewers", sql_file, "h");
    let other_key = period_computed_key("multi_row_multi_column", sql_file, "w");
    let hist_file = "/etc/gha2db/metrics/kubernetes/hist_reviewers.sql";
    let a_key = period_computed_key("hist_reviewers", hist_file, "a_0_1");
    let n_key = period_computed_key("hist_reviewers", hist_file, "a_3_n");

    // Nothing computed yet
    assert!(
        !is_period_computed(&c, &ctx, &w_key, "w", None, ft(&[2026, 9, 25, 10])),
        "empty table: week must not be reported as computed"
    );

    // Markers are the hours of the syncs which computed the metric
    // Weekly metric computed by the Monday 2026-09-21 09:00 sync, then again on Wednesday (twice in the same hour)
    set_period_computed(&c, &ctx, &w_key, ft(&[2026, 9, 21, 9]));
    set_period_computed(&c, &ctx, &w_key, ft(&[2026, 9, 23, 15, 30]));
    set_period_computed(&c, &ctx, &w_key, ft(&[2026, 9, 23, 15, 45]));
    // Daily metric computed by the 2026-09-25 03:04 sync, hourly by the 10:00 one
    set_period_computed(&c, &ctx, &d_key, ft(&[2026, 9, 25, 3, 4]));
    set_period_computed(&c, &ctx, &h_key, ft(&[2026, 9, 25, 10]));
    // Histogram quick ranges get markers like any other period
    set_period_computed(&c, &ctx, &a_key, ft(&[2026, 9, 25, 3, 4]));
    set_period_computed(&c, &ctx, &n_key, ft(&[2026, 9, 25, 3, 4]));

    let stored_cases: &[(&str, &[&str])] = &[
        (&w_key, &["2026-09-21 09:00:00", "2026-09-23 15:00:00"]),
        (&d_key, &["2026-09-25 03:00:00"]),
        (&h_key, &["2026-09-25 10:00:00"]),
        (&a_key, &["2026-09-25 03:00:00"]),
        (&n_key, &["2026-09-25 03:00:00"]),
        (&other_key, &[]),
    ];
    for (index, (key, expected)) in stored_cases.iter().enumerate() {
        let got = marker_rows(&c, key);
        assert_eq!(
            got,
            expected.to_vec(),
            "stored test number {}, key '{}'",
            index + 1,
            key
        );
    }

    const NO: &[i32] = &[];
    // (key, period, range_start (empty: zero), tm_offset, to, expected)
    type Row<'a> = (&'a str, &'a str, &'a [i32], i64, &'a [i32], bool);
    let test_cases: &[Row] = &[
        // the week 2026-09-21 - 2026-09-27 is covered by its markers from the Monday sync on
        (&w_key, "w", NO, 0, &[2026, 9, 21, 9], true),
        (&w_key, "w", NO, 0, &[2026, 9, 21, 10], true),
        (&w_key, "w", NO, 0, &[2026, 9, 25, 10, 58], true),
        (&w_key, "w", NO, 0, &[2026, 9, 27, 23, 59, 59], true),
        // a sync ending before the first marker of the week does not see it
        (&w_key, "w", NO, 0, &[2026, 9, 21, 8, 59], false),
        // previous and next weeks are not covered
        (&w_key, "w", NO, 0, &[2026, 9, 20, 23, 59, 59], false),
        (&w_key, "w", NO, 0, &[2026, 9, 28], false),
        // another metric using the same SQL file has its own key
        (&other_key, "w", NO, 0, &[2026, 9, 25, 10], false),
        // daily: the same day, from the marker hour on, hour precision 'to' as passed to calc_metric
        (&d_key, "d", NO, 0, &[2026, 9, 25, 3], true),
        (&d_key, "d", NO, 0, &[2026, 9, 25, 2, 59], false),
        (&d_key, "d", NO, 0, &[2026, 9, 25, 23], true),
        (&d_key, "d", NO, 0, &[2026, 9, 26], false),
        (&d_key, "d", NO, 0, &[2026, 9, 24, 23], false),
        // GHA2DB_TMOFFSET moves the day boundary: 2026-09-25 22:00 + 2h is already 09-26
        (&d_key, "d", NO, 2, &[2026, 9, 25, 21, 59], true),
        (&d_key, "d", NO, 2, &[2026, 9, 25, 22], false),
        (&d_key, "d", NO, 2, &[2026, 9, 24, 23], false),
        // hourly: the same hour only
        (&h_key, "h", NO, 0, &[2026, 9, 25, 10], true),
        (&h_key, "h", NO, 0, &[2026, 9, 25, 10, 59, 59], true),
        (&h_key, "h", NO, 0, &[2026, 9, 25, 11], false),
        (&h_key, "h", NO, 0, &[2026, 9, 25, 9, 59], false),
        // past quick range: daily, whatever its start
        (&a_key, "a_0_1", NO, 0, &[2026, 9, 25, 10], true),
        (&a_key, "a_0_1", &[2015, 1, 1], 0, &[2026, 9, 25, 10], true),
        (&a_key, "a_0_1", &[2015, 1, 1], 0, &[2026, 9, 26], false),
        // quick range ending now: by its length
        (&n_key, "a_3_n", &[2026, 9, 15], 0, &[2026, 9, 25, 10], true),
        (&n_key, "a_3_n", &[2026, 9, 15], 0, &[2026, 9, 26], false),
        (&n_key, "a_3_n", &[2026, 8, 1], 0, &[2026, 9, 26], true),
        (&n_key, "a_3_n", &[2026, 8, 1], 0, &[2026, 9, 30, 23], true),
        (&n_key, "a_3_n", &[2026, 8, 1], 0, &[2026, 10, 1], false),
        (&n_key, "a_3_n", &[2020, 3, 1], 0, &[2026, 12, 31, 23], true),
        (&n_key, "a_3_n", &[2020, 3, 1], 0, &[2027, 1, 1], false),
        (&n_key, "a_3_n", NO, 0, &[2026, 9, 26], false),
        // periods without a calendar period are always reported as computed
        (
            "unknown",
            "range:2020-01-01,2020-02-01",
            NO,
            0,
            &[2026, 9, 25, 10],
            true,
        ),
        ("unknown", "", NO, 0, &[2026, 9, 25, 10], true),
    ];
    assert_eq!(test_cases.len(), 33);
    for (index, (key, period, range_start, tm_offset, to, expected)) in
        test_cases.iter().enumerate()
    {
        ctx.tm_offset = *tm_offset;
        let range_start = if range_start.is_empty() {
            None
        } else {
            Some(ft(range_start))
        };
        let got = is_period_computed(&c, &ctx, key, period, range_start, ft(to));
        assert_eq!(
            got,
            *expected,
            "test number {}, key '{}', period '{}', range start {:?}, to {:?}, offset {}",
            index + 1,
            key,
            period,
            range_start,
            to,
            tm_offset
        );
    }
    ctx.tm_offset = 0;

    // Markers do not depend on the offset: one written with offset 14 is found with any offset
    ctx.tm_offset = 14;
    set_period_computed(&c, &ctx, &d_key, ft(&[2026, 9, 26, 10, 58]));
    assert!(
        is_period_computed(&c, &ctx, &d_key, "d", None, ft(&[2026, 9, 26, 11])),
        "offset 14: marker written at 2026-09-26 10:00 must be found for 2026-09-26 11:00"
    );
    ctx.tm_offset = 0;
    assert!(
        is_period_computed(&c, &ctx, &d_key, "d", None, ft(&[2026, 9, 26, 11])),
        "offset 0: marker written at 2026-09-26 10:00 must be found for 2026-09-26 11:00"
    );
    assert!(
        !is_period_computed(&c, &ctx, &d_key, "d", None, ft(&[2026, 9, 26, 9, 59])),
        "offset 0: 2026-09-26 09:59 is before the marker"
    );
    assert_eq!(
        marker_rows(&c, &d_key),
        vec!["2026-09-25 03:00:00", "2026-09-26 10:00:00"],
        "daily markers"
    );
}
