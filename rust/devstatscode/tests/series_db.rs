//! 1:1 port of the Go `series_test.go::TestProcessAnnotations` DB test:
//! `process_annotations` is called with an in-memory `Annotations` table and
//! the five CNCF milestone dates, then the `sannotations` and `tquick_ranges`
//! tables it wrote are read back and compared with the Go expectations
//! (volatile `now`-dependent columns filtered exactly like the Go
//! `getTSDBResultFiltered`).
//!
//! Like the Go test this only runs with `PG_DB=dbtest` (see `test.sh`); it
//! uses its own scratch database `dbtest_series`.
//!
//! Bug 56 (stale Go test, fixed on both sides): the `y100` / "Last century"
//! quick range was added in 2024 after the Go table was last updated, so the
//! `skipI` indices of the `now`-dependent rows (`a_0_n`, `c_n`) were off by
//! one and 6 of the 15 Go cases failed. The indices below (`12`, `12, 14`)
//! are the corrected ones; Go and Rust now pass all 15 cases.

use devstats_compat::pg::{self as tpg, TestDb};
use devstatscode::annotations::{process_annotations, Annotation, Annotations, MilestoneDates};
use devstatscode::chrono::{DateTime, FixedOffset, TimeZone, Utc};
use devstatscode::pg::{exec_sql_with_err, PgConn};

/// Go `testlib.YMDHMS(year, month)` (day 1, midnight UTC).
fn ft(year: i32, month: u32) -> DateTime<FixedOffset> {
    Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0)
        .unwrap()
        .fixed_offset()
}

/// Go `getTSDBResult`: every value as text (times in RFC 3339, like the Go
/// `[]byte` scan of a `time.Time`).
fn get_tsdb_result(c: &PgConn, sql: &str) -> Vec<Vec<String>> {
    tpg::snapshot(c, sql, &[]).rows
}

/// Go `getTSDBResultFiltered`: drop the first (time) column, the last (always
/// `0`) column and, for the last row plus the `skip_i` rows when
/// `additional_skip` is set, the second column whose value depends on `now`.
fn get_tsdb_result_filtered(
    c: &PgConn,
    sql: &str,
    additional_skip: bool,
    skip_i: &[usize],
) -> Vec<Vec<String>> {
    let res = get_tsdb_result(c, sql);
    if res.is_empty() || res[0].is_empty() {
        return Vec::new();
    }
    let last_i = res.len() - 1;
    let last_j = res[0].len() - 1;
    res.iter()
        .enumerate()
        .map(|(i, val)| {
            let skip_period = i == last_i || (additional_skip && skip_i.contains(&i));
            val.iter()
                .enumerate()
                .filter(|(j, _)| !(*j == 0 || *j == last_j || (*j == 1 && skip_period)))
                .map(|(_, col)| col.clone())
                .collect()
        })
        .collect()
}

struct Case {
    /// `(name, description, year, month)` of every annotation, in Go order.
    annotations: &'static [(&'static str, &'static str, i32, u32)],
    /// Years of start, join, incubating, graduated, archived (all Jan 1st).
    dates: [Option<i32>; 5],
    expected_annotations: &'static [&'static [&'static str]],
    expected_quick_ranges: &'static [&'static [&'static str]],
    additional_skip: bool,
    skip_i: &'static [usize],
}

fn to_rows(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect())
        .collect()
}

#[test]
fn test_process_annotations_go_port() {
    let Some(db) = TestDb::fresh("series") else {
        return;
    };
    let mut ctx = db.ctx.clone();
    let c = db.conn();
    let test_cases: &[Case] = &[
        Case {
            annotations: &[("release 0.0.0", "desc 0.0.0", 2017, 2)],
            dates: [Some(2014), Some(2016), None, None, None],
            expected_annotations: &[
                &[
                    "2014-01-01T00:00:00Z",
                    "2014-01-01 - project starts",
                    "Project start date",
                ],
                &[
                    "2016-01-01T00:00:00Z",
                    "2016-01-01 - joined CNCF",
                    "CNCF join date",
                ],
                &["2017-02-01T00:00:00Z", "desc 0.0.0", "release 0.0.0"],
            ],
            expected_quick_ranges: &[
                &["d;1 day;;", "Last day", "d"],
                &["w;1 week;;", "Last week", "w"],
                &["d10;10 days;;", "Last 10 days", "d10"],
                &["m;1 month;;", "Last month", "m"],
                &["q;3 months;;", "Last quarter", "q"],
                &["m6;6 months;;", "Last 6 months", "m6"],
                &["y;1 year;;", "Last year", "y"],
                &["y2;2 years;;", "Last 2 years", "y2"],
                &["y3;3 years;;", "Last 3 years", "y3"],
                &["y5;5 years;;", "Last 5 years", "y5"],
                &["y10;10 years;;", "Last decade", "y10"],
                &["y100;100 years;;", "Last century", "y100"],
                &["release 0.0.0 - now", "a_0_n"],
                &[
                    "c_b;;2014-01-01 00:00:00;2016-01-01 00:00:00",
                    "Before joining CNCF",
                    "c_b",
                ],
                &["Since joining CNCF", "c_n"],
            ],
            additional_skip: true,
            skip_i: &[12],
        },
        Case {
            annotations: &[("release 0.0.0", "desc 0.0.0", 2017, 2)],
            dates: [Some(2014), Some(2014), None, None, None],
            expected_annotations: &[&["2017-02-01T00:00:00Z", "desc 0.0.0", "release 0.0.0"]],
            expected_quick_ranges: &[
                &["d;1 day;;", "Last day", "d"],
                &["w;1 week;;", "Last week", "w"],
                &["d10;10 days;;", "Last 10 days", "d10"],
                &["m;1 month;;", "Last month", "m"],
                &["q;3 months;;", "Last quarter", "q"],
                &["m6;6 months;;", "Last 6 months", "m6"],
                &["y;1 year;;", "Last year", "y"],
                &["y2;2 years;;", "Last 2 years", "y2"],
                &["y3;3 years;;", "Last 3 years", "y3"],
                &["y5;5 years;;", "Last 5 years", "y5"],
                &["y10;10 years;;", "Last decade", "y10"],
                &["y100;100 years;;", "Last century", "y100"],
                &["release 0.0.0 - now", "a_0_n"],
            ],
            additional_skip: false,
            skip_i: &[],
        },
        Case {
            annotations: &[("release 0.0.0", "desc 0.0.0", 2017, 2)],
            dates: [Some(2016), Some(2014), None, None, None],
            expected_annotations: &[&["2017-02-01T00:00:00Z", "desc 0.0.0", "release 0.0.0"]],
            expected_quick_ranges: &[
                &["d;1 day;;", "Last day", "d"],
                &["w;1 week;;", "Last week", "w"],
                &["d10;10 days;;", "Last 10 days", "d10"],
                &["m;1 month;;", "Last month", "m"],
                &["q;3 months;;", "Last quarter", "q"],
                &["m6;6 months;;", "Last 6 months", "m6"],
                &["y;1 year;;", "Last year", "y"],
                &["y2;2 years;;", "Last 2 years", "y2"],
                &["y3;3 years;;", "Last 3 years", "y3"],
                &["y5;5 years;;", "Last 5 years", "y5"],
                &["y10;10 years;;", "Last decade", "y10"],
                &["y100;100 years;;", "Last century", "y100"],
                &["release 0.0.0 - now", "a_0_n"],
            ],
            additional_skip: false,
            skip_i: &[],
        },
        Case {
            annotations: &[("release 0.0.0", "desc 0.0.0", 2017, 2)],
            dates: [Some(2016), None, None, None, None],
            expected_annotations: &[
                &[
                    "2016-01-01T00:00:00Z",
                    "2016-01-01 - project starts",
                    "Project start date",
                ],
                &["2017-02-01T00:00:00Z", "desc 0.0.0", "release 0.0.0"],
            ],
            expected_quick_ranges: &[
                &["d;1 day;;", "Last day", "d"],
                &["w;1 week;;", "Last week", "w"],
                &["d10;10 days;;", "Last 10 days", "d10"],
                &["m;1 month;;", "Last month", "m"],
                &["q;3 months;;", "Last quarter", "q"],
                &["m6;6 months;;", "Last 6 months", "m6"],
                &["y;1 year;;", "Last year", "y"],
                &["y2;2 years;;", "Last 2 years", "y2"],
                &["y3;3 years;;", "Last 3 years", "y3"],
                &["y5;5 years;;", "Last 5 years", "y5"],
                &["y10;10 years;;", "Last decade", "y10"],
                &["y100;100 years;;", "Last century", "y100"],
                &["release 0.0.0 - now", "a_0_n"],
            ],
            additional_skip: false,
            skip_i: &[],
        },
        Case {
            annotations: &[("release 0.0.0", "desc 0.0.0", 2017, 2)],
            dates: [None, None, None, None, None],
            expected_annotations: &[&["2017-02-01T00:00:00Z", "desc 0.0.0", "release 0.0.0"]],
            expected_quick_ranges: &[
                &["d;1 day;;", "Last day", "d"],
                &["w;1 week;;", "Last week", "w"],
                &["d10;10 days;;", "Last 10 days", "d10"],
                &["m;1 month;;", "Last month", "m"],
                &["q;3 months;;", "Last quarter", "q"],
                &["m6;6 months;;", "Last 6 months", "m6"],
                &["y;1 year;;", "Last year", "y"],
                &["y2;2 years;;", "Last 2 years", "y2"],
                &["y3;3 years;;", "Last 3 years", "y3"],
                &["y5;5 years;;", "Last 5 years", "y5"],
                &["y10;10 years;;", "Last decade", "y10"],
                &["y100;100 years;;", "Last century", "y100"],
                &["release 0.0.0 - now", "a_0_n"],
            ],
            additional_skip: false,
            skip_i: &[],
        },
        Case {
            annotations: &[("release 0.0.0", "desc 0.0.0", 2017, 2)],
            dates: [None, Some(2014), None, None, None],
            expected_annotations: &[
                &[
                    "2014-01-01T00:00:00Z",
                    "2014-01-01 - joined CNCF",
                    "CNCF join date",
                ],
                &["2017-02-01T00:00:00Z", "desc 0.0.0", "release 0.0.0"],
            ],
            expected_quick_ranges: &[
                &["d;1 day;;", "Last day", "d"],
                &["w;1 week;;", "Last week", "w"],
                &["d10;10 days;;", "Last 10 days", "d10"],
                &["m;1 month;;", "Last month", "m"],
                &["q;3 months;;", "Last quarter", "q"],
                &["m6;6 months;;", "Last 6 months", "m6"],
                &["y;1 year;;", "Last year", "y"],
                &["y2;2 years;;", "Last 2 years", "y2"],
                &["y3;3 years;;", "Last 3 years", "y3"],
                &["y5;5 years;;", "Last 5 years", "y5"],
                &["y10;10 years;;", "Last decade", "y10"],
                &["y100;100 years;;", "Last century", "y100"],
                &["release 0.0.0 - now", "a_0_n"],
            ],
            additional_skip: false,
            skip_i: &[],
        },
        Case {
            annotations: &[("release 0.0.0", "desc 0.0.0", 2017, 2)],
            dates: [None, Some(2018), None, None, None],
            expected_annotations: &[
                &["2017-02-01T00:00:00Z", "desc 0.0.0", "release 0.0.0"],
                &[
                    "2018-01-01T00:00:00Z",
                    "2018-01-01 - joined CNCF",
                    "CNCF join date",
                ],
            ],
            expected_quick_ranges: &[
                &["d;1 day;;", "Last day", "d"],
                &["w;1 week;;", "Last week", "w"],
                &["d10;10 days;;", "Last 10 days", "d10"],
                &["m;1 month;;", "Last month", "m"],
                &["q;3 months;;", "Last quarter", "q"],
                &["m6;6 months;;", "Last 6 months", "m6"],
                &["y;1 year;;", "Last year", "y"],
                &["y2;2 years;;", "Last 2 years", "y2"],
                &["y3;3 years;;", "Last 3 years", "y3"],
                &["y5;5 years;;", "Last 5 years", "y5"],
                &["y10;10 years;;", "Last decade", "y10"],
                &["y100;100 years;;", "Last century", "y100"],
                &["release 0.0.0 - now", "a_0_n"],
            ],
            additional_skip: false,
            skip_i: &[],
        },
        Case {
            annotations: &[],
            dates: [None, None, None, None, None],
            expected_annotations: &[],
            expected_quick_ranges: &[
                &["d;1 day;;", "Last day", "d"],
                &["w;1 week;;", "Last week", "w"],
                &["d10;10 days;;", "Last 10 days", "d10"],
                &["m;1 month;;", "Last month", "m"],
                &["q;3 months;;", "Last quarter", "q"],
                &["m6;6 months;;", "Last 6 months", "m6"],
                &["y;1 year;;", "Last year", "y"],
                &["y2;2 years;;", "Last 2 years", "y2"],
                &["y3;3 years;;", "Last 3 years", "y3"],
                &["y5;5 years;;", "Last 5 years", "y5"],
                &["y10;10 years;;", "Last decade", "y10"],
                &["Last century", "y100"],
            ],
            additional_skip: false,
            skip_i: &[],
        },
        Case {
            annotations: &[
                ("release 4.0.0", "desc 4.0.0", 2017, 5),
                ("release 3.0.0", "desc 3.0.0", 2017, 4),
                ("release 1.0.0", "desc 1.0.0", 2017, 2),
                ("release 0.0.0", "desc 0.0.0", 2017, 1),
                ("release 2.0.0", "desc 2.0.0", 2017, 3),
            ],
            dates: [None, None, None, None, None],
            expected_annotations: &[
                &["2017-01-01T00:00:00Z", "desc 0.0.0", "release 0.0.0"],
                &["2017-02-01T00:00:00Z", "desc 1.0.0", "release 1.0.0"],
                &["2017-03-01T00:00:00Z", "desc 2.0.0", "release 2.0.0"],
                &["2017-04-01T00:00:00Z", "desc 3.0.0", "release 3.0.0"],
                &["2017-05-01T00:00:00Z", "desc 4.0.0", "release 4.0.0"],
            ],
            expected_quick_ranges: &[
                &["d;1 day;;", "Last day", "d"],
                &["w;1 week;;", "Last week", "w"],
                &["d10;10 days;;", "Last 10 days", "d10"],
                &["m;1 month;;", "Last month", "m"],
                &["q;3 months;;", "Last quarter", "q"],
                &["m6;6 months;;", "Last 6 months", "m6"],
                &["y;1 year;;", "Last year", "y"],
                &["y2;2 years;;", "Last 2 years", "y2"],
                &["y3;3 years;;", "Last 3 years", "y3"],
                &["y5;5 years;;", "Last 5 years", "y5"],
                &["y10;10 years;;", "Last decade", "y10"],
                &["y100;100 years;;", "Last century", "y100"],
                &[
                    "a_0_1;;2017-01-01 00:00:00;2017-02-01 00:00:00",
                    "release 0.0.0 - release 1.0.0",
                    "a_0_1",
                ],
                &[
                    "a_1_2;;2017-02-01 00:00:00;2017-03-01 00:00:00",
                    "release 1.0.0 - release 2.0.0",
                    "a_1_2",
                ],
                &[
                    "a_2_3;;2017-03-01 00:00:00;2017-04-01 00:00:00",
                    "release 2.0.0 - release 3.0.0",
                    "a_2_3",
                ],
                &[
                    "a_3_4;;2017-04-01 00:00:00;2017-05-01 00:00:00",
                    "release 3.0.0 - release 4.0.0",
                    "a_3_4",
                ],
                &["release 4.0.0 - now", "a_4_n"],
            ],
            additional_skip: false,
            skip_i: &[],
        },
        Case {
            annotations: &[
                ("v1.0", "desc v1.0", 2016, 1),
                ("v6.0", "desc v6.0", 2016, 6),
                ("v2.0", "desc v2.0", 2016, 2),
                ("v4.0", "desc v4.0", 2016, 4),
                ("v3.0", "desc v3.0", 2016, 3),
                ("v5.0", "desc v5.0", 2016, 5),
            ],
            dates: [None, None, None, None, None],
            expected_annotations: &[
                &["2016-01-01T00:00:00Z", "desc v1.0", "v1.0"],
                &["2016-02-01T00:00:00Z", "desc v2.0", "v2.0"],
                &["2016-03-01T00:00:00Z", "desc v3.0", "v3.0"],
                &["2016-04-01T00:00:00Z", "desc v4.0", "v4.0"],
                &["2016-05-01T00:00:00Z", "desc v5.0", "v5.0"],
                &["2016-06-01T00:00:00Z", "desc v6.0", "v6.0"],
            ],
            expected_quick_ranges: &[
                &["d;1 day;;", "Last day", "d"],
                &["w;1 week;;", "Last week", "w"],
                &["d10;10 days;;", "Last 10 days", "d10"],
                &["m;1 month;;", "Last month", "m"],
                &["q;3 months;;", "Last quarter", "q"],
                &["m6;6 months;;", "Last 6 months", "m6"],
                &["y;1 year;;", "Last year", "y"],
                &["y2;2 years;;", "Last 2 years", "y2"],
                &["y3;3 years;;", "Last 3 years", "y3"],
                &["y5;5 years;;", "Last 5 years", "y5"],
                &["y10;10 years;;", "Last decade", "y10"],
                &["y100;100 years;;", "Last century", "y100"],
                &[
                    "a_0_1;;2016-01-01 00:00:00;2016-02-01 00:00:00",
                    "v1.0 - v2.0",
                    "a_0_1",
                ],
                &[
                    "a_1_2;;2016-02-01 00:00:00;2016-03-01 00:00:00",
                    "v2.0 - v3.0",
                    "a_1_2",
                ],
                &[
                    "a_2_3;;2016-03-01 00:00:00;2016-04-01 00:00:00",
                    "v3.0 - v4.0",
                    "a_2_3",
                ],
                &[
                    "a_3_4;;2016-04-01 00:00:00;2016-05-01 00:00:00",
                    "v4.0 - v5.0",
                    "a_3_4",
                ],
                &[
                    "a_4_5;;2016-05-01 00:00:00;2016-06-01 00:00:00",
                    "v5.0 - v6.0",
                    "a_4_5",
                ],
                &["v6.0 - now", "a_5_n"],
            ],
            additional_skip: false,
            skip_i: &[],
        },
        Case {
            annotations: &[("release 0.0.0", "desc 0.0.0", 2017, 2)],
            dates: [Some(2014), Some(2015), Some(2016), Some(2017), Some(2018)],
            expected_annotations: &[
                &[
                    "2014-01-01T00:00:00Z",
                    "2014-01-01 - project starts",
                    "Project start date",
                ],
                &[
                    "2015-01-01T00:00:00Z",
                    "2015-01-01 - joined CNCF",
                    "CNCF join date",
                ],
                &[
                    "2016-01-01T00:00:00Z",
                    "2016-01-01 - project moved to incubating state",
                    "Moved to incubating state",
                ],
                &[
                    "2017-01-01T00:00:00Z",
                    "2017-01-01 - project graduated",
                    "Graduated",
                ],
                &["2017-02-01T00:00:00Z", "desc 0.0.0", "release 0.0.0"],
                &[
                    "2018-01-01T00:00:00Z",
                    "2018-01-01 - project was archived",
                    "Archived",
                ],
            ],
            expected_quick_ranges: &[
                &["d;1 day;;", "Last day", "d"],
                &["w;1 week;;", "Last week", "w"],
                &["d10;10 days;;", "Last 10 days", "d10"],
                &["m;1 month;;", "Last month", "m"],
                &["q;3 months;;", "Last quarter", "q"],
                &["m6;6 months;;", "Last 6 months", "m6"],
                &["y;1 year;;", "Last year", "y"],
                &["y2;2 years;;", "Last 2 years", "y2"],
                &["y3;3 years;;", "Last 3 years", "y3"],
                &["y5;5 years;;", "Last 5 years", "y5"],
                &["y10;10 years;;", "Last decade", "y10"],
                &["y100;100 years;;", "Last century", "y100"],
                &["release 0.0.0 - now", "a_0_n"],
                &[
                    "c_b;;2014-01-01 00:00:00;2015-01-01 00:00:00",
                    "Before joining CNCF",
                    "c_b",
                ],
                &["Since joining CNCF", "c_n"],
                &[
                    "c_j_i;;2015-01-01 00:00:00;2016-01-01 00:00:00",
                    "CNCF join date - moved to incubation",
                    "c_j_i",
                ],
                &[
                    "c_i_g;;2016-01-01 00:00:00;2017-01-01 00:00:00",
                    "Moved to incubation - graduated",
                    "c_i_g",
                ],
                &["Since graduating", "c_g_n"],
            ],
            additional_skip: true,
            skip_i: &[12, 14],
        },
        Case {
            annotations: &[("release 0.0.0", "desc 0.0.0", 2017, 2)],
            dates: [Some(2014), None, Some(2016), Some(2017), Some(2018)],
            expected_annotations: &[
                &[
                    "2014-01-01T00:00:00Z",
                    "2014-01-01 - project starts",
                    "Project start date",
                ],
                &[
                    "2016-01-01T00:00:00Z",
                    "2016-01-01 - project moved to incubating state",
                    "Moved to incubating state",
                ],
                &[
                    "2017-01-01T00:00:00Z",
                    "2017-01-01 - project graduated",
                    "Graduated",
                ],
                &["2017-02-01T00:00:00Z", "desc 0.0.0", "release 0.0.0"],
                &[
                    "2018-01-01T00:00:00Z",
                    "2018-01-01 - project was archived",
                    "Archived",
                ],
            ],
            expected_quick_ranges: &[
                &["d;1 day;;", "Last day", "d"],
                &["w;1 week;;", "Last week", "w"],
                &["d10;10 days;;", "Last 10 days", "d10"],
                &["m;1 month;;", "Last month", "m"],
                &["q;3 months;;", "Last quarter", "q"],
                &["m6;6 months;;", "Last 6 months", "m6"],
                &["y;1 year;;", "Last year", "y"],
                &["y2;2 years;;", "Last 2 years", "y2"],
                &["y3;3 years;;", "Last 3 years", "y3"],
                &["y5;5 years;;", "Last 5 years", "y5"],
                &["y10;10 years;;", "Last decade", "y10"],
                &["y100;100 years;;", "Last century", "y100"],
                &["release 0.0.0 - now", "a_0_n"],
            ],
            additional_skip: true,
            skip_i: &[12, 14],
        },
        Case {
            annotations: &[("release 0.0.0", "desc 0.0.0", 2017, 2)],
            dates: [Some(2014), Some(2015), Some(2016), None, None],
            expected_annotations: &[
                &[
                    "2014-01-01T00:00:00Z",
                    "2014-01-01 - project starts",
                    "Project start date",
                ],
                &[
                    "2015-01-01T00:00:00Z",
                    "2015-01-01 - joined CNCF",
                    "CNCF join date",
                ],
                &[
                    "2016-01-01T00:00:00Z",
                    "2016-01-01 - project moved to incubating state",
                    "Moved to incubating state",
                ],
                &["2017-02-01T00:00:00Z", "desc 0.0.0", "release 0.0.0"],
            ],
            expected_quick_ranges: &[
                &["d;1 day;;", "Last day", "d"],
                &["w;1 week;;", "Last week", "w"],
                &["d10;10 days;;", "Last 10 days", "d10"],
                &["m;1 month;;", "Last month", "m"],
                &["q;3 months;;", "Last quarter", "q"],
                &["m6;6 months;;", "Last 6 months", "m6"],
                &["y;1 year;;", "Last year", "y"],
                &["y2;2 years;;", "Last 2 years", "y2"],
                &["y3;3 years;;", "Last 3 years", "y3"],
                &["y5;5 years;;", "Last 5 years", "y5"],
                &["y10;10 years;;", "Last decade", "y10"],
                &["y100;100 years;;", "Last century", "y100"],
                &["release 0.0.0 - now", "a_0_n"],
                &[
                    "c_b;;2014-01-01 00:00:00;2015-01-01 00:00:00",
                    "Before joining CNCF",
                    "c_b",
                ],
                &["Since joining CNCF", "c_n"],
                &[
                    "c_j_i;;2015-01-01 00:00:00;2016-01-01 00:00:00",
                    "CNCF join date - moved to incubation",
                    "c_j_i",
                ],
                &["Since moving to incubating state", "c_i_n"],
            ],
            additional_skip: true,
            skip_i: &[12, 14],
        },
        Case {
            annotations: &[("release 0.0.0", "desc 0.0.0", 2017, 2)],
            dates: [Some(2014), Some(2015), None, Some(2017), None],
            expected_annotations: &[
                &[
                    "2014-01-01T00:00:00Z",
                    "2014-01-01 - project starts",
                    "Project start date",
                ],
                &[
                    "2015-01-01T00:00:00Z",
                    "2015-01-01 - joined CNCF",
                    "CNCF join date",
                ],
                &[
                    "2017-01-01T00:00:00Z",
                    "2017-01-01 - project graduated",
                    "Graduated",
                ],
                &["2017-02-01T00:00:00Z", "desc 0.0.0", "release 0.0.0"],
            ],
            expected_quick_ranges: &[
                &["d;1 day;;", "Last day", "d"],
                &["w;1 week;;", "Last week", "w"],
                &["d10;10 days;;", "Last 10 days", "d10"],
                &["m;1 month;;", "Last month", "m"],
                &["q;3 months;;", "Last quarter", "q"],
                &["m6;6 months;;", "Last 6 months", "m6"],
                &["y;1 year;;", "Last year", "y"],
                &["y2;2 years;;", "Last 2 years", "y2"],
                &["y3;3 years;;", "Last 3 years", "y3"],
                &["y5;5 years;;", "Last 5 years", "y5"],
                &["y10;10 years;;", "Last decade", "y10"],
                &["y100;100 years;;", "Last century", "y100"],
                &["release 0.0.0 - now", "a_0_n"],
                &[
                    "c_b;;2014-01-01 00:00:00;2015-01-01 00:00:00",
                    "Before joining CNCF",
                    "c_b",
                ],
                &["Since joining CNCF", "c_n"],
                &[
                    "c_j_g;;2015-01-01 00:00:00;2017-01-01 00:00:00",
                    "CNCF join date - graduated",
                    "c_j_g",
                ],
                &["Since graduating", "c_g_n"],
            ],
            additional_skip: true,
            skip_i: &[12, 14],
        },
        Case {
            annotations: &[("release 0.0.0", "desc 0.0.0", 2017, 2)],
            dates: [Some(2014), Some(2015), Some(2017), Some(2016), Some(2018)],
            expected_annotations: &[
                &[
                    "2014-01-01T00:00:00Z",
                    "2014-01-01 - project starts",
                    "Project start date",
                ],
                &[
                    "2015-01-01T00:00:00Z",
                    "2015-01-01 - joined CNCF",
                    "CNCF join date",
                ],
                &[
                    "2016-01-01T00:00:00Z",
                    "2016-01-01 - project graduated",
                    "Graduated",
                ],
                &[
                    "2017-01-01T00:00:00Z",
                    "2017-01-01 - project moved to incubating state",
                    "Moved to incubating state",
                ],
                &["2017-02-01T00:00:00Z", "desc 0.0.0", "release 0.0.0"],
                &[
                    "2018-01-01T00:00:00Z",
                    "2018-01-01 - project was archived",
                    "Archived",
                ],
            ],
            expected_quick_ranges: &[
                &["d;1 day;;", "Last day", "d"],
                &["w;1 week;;", "Last week", "w"],
                &["d10;10 days;;", "Last 10 days", "d10"],
                &["m;1 month;;", "Last month", "m"],
                &["q;3 months;;", "Last quarter", "q"],
                &["m6;6 months;;", "Last 6 months", "m6"],
                &["y;1 year;;", "Last year", "y"],
                &["y2;2 years;;", "Last 2 years", "y2"],
                &["y3;3 years;;", "Last 3 years", "y3"],
                &["y5;5 years;;", "Last 5 years", "y5"],
                &["y10;10 years;;", "Last decade", "y10"],
                &["y100;100 years;;", "Last century", "y100"],
                &["release 0.0.0 - now", "a_0_n"],
                &[
                    "c_b;;2014-01-01 00:00:00;2015-01-01 00:00:00",
                    "Before joining CNCF",
                    "c_b",
                ],
                &["Since joining CNCF", "c_n"],
            ],
            additional_skip: true,
            skip_i: &[12],
        },
    ];
    assert_eq!(test_cases.len(), 15);
    for (index, test) in test_cases.iter().enumerate() {
        let mut annotations = Annotations {
            annotations: test
                .annotations
                .iter()
                .map(|(name, description, y, m)| Annotation {
                    name: name.to_string(),
                    description: description.to_string(),
                    date: ft(*y, *m),
                })
                .collect(),
        };
        let dates: MilestoneDates = [
            test.dates[0].map(|y| ft(y, 1)),
            test.dates[1].map(|y| ft(y, 1)),
            test.dates[2].map(|y| ft(y, 1)),
            test.dates[3].map(|y| ft(y, 1)),
            test.dates[4].map(|y| ft(y, 1)),
        ];
        process_annotations(&mut ctx, &mut annotations, &dates);

        let got_annotations = get_tsdb_result(
            &c,
            "select time, description, title from \"sannotations\" order by time asc",
        );
        assert_eq!(
            to_rows(test.expected_annotations),
            got_annotations,
            "test number {}: join date: {:?}\nannotations: {:?}",
            index + 1,
            dates[1],
            annotations.annotations
        );
        exec_sql_with_err(&c, &ctx, "delete from \"sannotations\"", &[]);

        let got_quick_ranges = get_tsdb_result_filtered(
            &c,
            "select time, quick_ranges_data, quick_ranges_name, quick_ranges_suffix, 0 from \"tquick_ranges\" order by time asc",
            test.additional_skip,
            test.skip_i,
        );
        assert_eq!(
            to_rows(test.expected_quick_ranges),
            got_quick_ranges,
            "test number {}: join date: {:?}\nannotations: {:?}",
            index + 1,
            dates[1],
            annotations.annotations
        );
        exec_sql_with_err(&c, &ctx, "delete from \"tquick_ranges\"", &[]);
    }
}
