//! Annotations and quick ranges — port of `annotations.go`.
//!
//! [`get_annotations`] lists the tags of a project's main repository through
//! the `git_tags.sh` script and turns those matching the project's regexp
//! into annotations; [`process_annotations`] writes the `annotations` series
//! (and `annotations_shared` in the shared database), the CNCF milestone
//! annotations and the `quick_ranges` tag series into the TSDB.
//!
//! Times: Go builds the tag dates with `time.Unix` (local zone) and its
//! `HourStart`/`ToYMDHMSDate` use the *wall clock* components, so the dates
//! here carry their zone (`DateTime<FixedOffset>`) and are relabelled with
//! [`wall_as_utc`] wherever Go would call `HourStart`.

use std::collections::BTreeMap;
use std::time::Instant;

use chrono::{DateTime, Duration, FixedOffset, Local, TimeZone, Utc};

use crate::context::Ctx;
use crate::error::{fatal_on_err, fatal_on_error};
use crate::exec::exec_command_bytes;
use crate::string::{safe_utf8_bytes, safe_utf8_string};
use crate::time::{
    format_go_duration, hour_start, next_day_start, time_parse_any, to_ymd_date, to_ymdhms_date,
    wall_as_utc,
};
use crate::ts_points::{add_ts_point, new_ts_point, FieldValue, Fields, TSPoints, Tags};
use crate::{consts, fatalf, gofmt, goregex, pg, printf};

/// Go `Annotation`: one annotation (a tag or a milestone).
#[derive(Debug, Clone, PartialEq)]
pub struct Annotation {
    pub name: String,
    pub description: String,
    pub date: DateTime<FixedOffset>,
}

impl Annotation {
    /// Go `%v` of the struct: `{Name Description Date}`.
    pub fn go_v(&self) -> String {
        format!(
            "{{{} {} {}}}",
            self.name,
            self.description,
            gofmt::time(self.date)
        )
    }
}

/// Go `Annotations`: the list of annotations.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Annotations {
    pub annotations: Vec<Annotation>,
}

/// The CNCF milestone dates handed to [`process_annotations`]: start, join,
/// incubating, graduated, archived (Go `[]*time.Time`).
pub type MilestoneDates = [Option<DateTime<FixedOffset>>; 5];

/// Annotations before this date are ignored (GHA data starts here).
fn min_date() -> DateTime<Utc> {
    time_parse_any("2012-07-01")
}

/// Go `sort.Sort(AnnotationsByDate(...))`: by date (Rust's sort is stable,
/// Go's is not — same-second tags may come out in a different order).
fn sort_by_date(anns: &mut [Annotation]) {
    anns.sort_by_key(|a| a.date);
}

/// Go `GetFakeAnnotations`: the `startDate - joinDate` and `joinDate - now`
/// annotations of a project without a main repository.
pub fn get_fake_annotations(
    start_date: DateTime<FixedOffset>,
    join_date: DateTime<FixedOffset>,
) -> Annotations {
    let mut annotations = Annotations::default();
    let min = min_date();
    if join_date < min || start_date < min || join_date <= start_date {
        return annotations;
    }
    annotations.annotations.push(Annotation {
        name: "Project start".to_string(),
        description: format!("{} - project starts", to_ymd_date(start_date)),
        date: start_date,
    });
    annotations.annotations.push(Annotation {
        name: "First CNCF project join date".to_string(),
        description: to_ymd_date(join_date),
        date: join_date,
    });
    annotations
}

/// Go `GetAnnotations`: run `git_tags.sh` for `org_repo` and return the tags
/// matching `anno_regexp` (all tags when empty) as annotations, one per hour
/// at most.
pub fn get_annotations(ctx: &mut Ctx, org_repo: &str, anno_regexp: &str) -> Annotations {
    // Get org and repo from orgRepo
    let ary: Vec<&str> = org_repo.split('/').collect();
    if ary.len() != 2 {
        fatalf!(
            "main repository format must be 'org/repo', found '{}'",
            org_repo
        );
    }

    // Compile annotation regexp if present, if no regexp then return all tags
    let re = if anno_regexp.is_empty() {
        None
    } else {
        match goregex::compile(anno_regexp) {
            Ok(re) => Some(re),
            Err(e) => fatal_on_error(format!("regexp: Compile(`{anno_regexp}`): {e}")),
        }
    };

    // Local or cron mode?
    let cmd_prefix = if ctx.local_cmd {
        consts::LOCAL_GIT_SCRIPTS
    } else {
        ""
    };

    // We need this to capture 'git_tags.sh' output.
    ctx.exec_output = true;

    // Get tags is using shell script that does 'chdir'
    if ctx.debug > 0 {
        printf!("Getting tags for repo {}\n", org_repo);
    }
    let dt_start = Instant::now();
    let rwd = format!("{}{}", ctx.repos_dir, org_repo);
    let env: BTreeMap<String, String> = [("GIT_TERMINAL_PROMPT".to_string(), "0".to_string())]
        .into_iter()
        .collect();
    // Go strings are byte strings: the tag message is cut at 40 *bytes* and
    // invalid UTF-8 is dropped only when the point is written, so the
    // output is processed as bytes here.
    let res = exec_command_bytes(ctx, &[format!("{cmd_prefix}git_tags.sh"), rwd], &env);
    let took = dt_start.elapsed();
    let tags_bytes = fatal_on_err(res);

    let min = min_date();
    let mut anns: Vec<Annotation> = Vec::new();
    let mut n_tags = 0;
    for tag_data in tags_bytes.split(|b| *b == b'\n') {
        let data = trim_space(tag_data);
        if data.is_empty() {
            continue;
        }
        // Use '♂♀' separator to avoid any character that can appear inside tag name or description
        let tag_data_ary: Vec<&[u8]> = split_bytes(data, "♂♀".as_bytes());
        if tag_data_ary.len() != 3 {
            fatalf!(
                "invalid tagData returned for repo: {}: '{}'",
                org_repo,
                String::from_utf8_lossy(data)
            );
        }
        let tag_name = safe_utf8_bytes(tag_data_ary[0]);
        if let Some(re) = &re {
            if !re.is_match(&String::from_utf8_lossy(tag_data_ary[0])) {
                continue;
            }
        }
        if tag_data_ary[1].is_empty() {
            if ctx.debug > 0 {
                printf!(
                    "Empty time returned for repo: {}, tag: {}\n",
                    org_repo,
                    tag_name
                );
            }
            continue;
        }
        let unix_time_stamp: i64 = match parse_go_int(tag_data_ary[1]) {
            Some(v) => v,
            None => {
                printf!(
                    "Invalid time returned for repo: {}, tag: {}: '{}'\n",
                    org_repo,
                    tag_name,
                    String::from_utf8_lossy(data)
                );
                continue;
            }
        };
        // Go `time.Unix(ts, 0)`: local time
        let creator_date = match Local.timestamp_opt(unix_time_stamp, 0).single() {
            Some(dt) => dt.fixed_offset(),
            None => fatalf!(
                "invalid tag time returned for repo: {}, tag: {}: '{}'",
                org_repo,
                tag_name,
                String::from_utf8_lossy(data)
            ),
        };
        if creator_date < min {
            if ctx.debug > 0 {
                printf!(
                    "Skipping annotation {} because it is before {}\n",
                    gofmt::time(creator_date),
                    gofmt::time(min)
                );
            }
            continue;
        }
        let mut message = tag_data_ary[2];
        if message.len() > 40 {
            message = &message[..40];
        }
        let message = safe_utf8_bytes(message).replace(['\n', '\r', '\t'], " ");

        anns.push(Annotation {
            name: tag_name,
            description: message,
            date: creator_date,
        });
        n_tags += 1;
    }

    if ctx.debug > 0 {
        printf!(
            "Got {} tags for {}, took {}\n",
            n_tags,
            org_repo,
            format_go_duration(took)
        );
    }

    // Remove duplicates (annotations falling into the same hour)
    let mut annotations = Annotations::default();
    let mut prev_hour_date = min;
    sort_by_date(&mut anns);
    for ann in anns {
        let curr_hour_date = hour_start(wall_as_utc(&ann.date));
        if curr_hour_date == prev_hour_date {
            if ctx.debug > 0 {
                printf!(
                    "Skipping annotation {} because its hour date is the same as the previous one\n",
                    ann.go_v()
                );
            }
            continue;
        }
        prev_hour_date = curr_hour_date;
        annotations.annotations.push(ann);
    }
    annotations
}

/// Go `strings.TrimSpace` on a byte string: leading and trailing Unicode
/// white space (ASCII white space when the bytes are not valid UTF-8).
fn trim_space(b: &[u8]) -> &[u8] {
    match std::str::from_utf8(b) {
        Ok(s) => s.trim().as_bytes(),
        Err(_) => b.trim_ascii(),
    }
}

/// Go `strings.Split(data, sep)` on byte strings (`sep` non-empty).
fn split_bytes<'a>(data: &'a [u8], sep: &[u8]) -> Vec<&'a [u8]> {
    let mut parts = Vec::new();
    let mut rest = data;
    while let Some(pos) = rest.windows(sep.len()).position(|w| w == sep) {
        parts.push(&rest[..pos]);
        rest = &rest[pos + sep.len()..];
    }
    parts.push(rest);
    parts
}

/// Go `strconv.ParseInt(s, 10, 64)`: optional sign, decimal digits only.
fn parse_go_int(b: &[u8]) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse().ok()
}

/// `title`/`description` fields of an `annotations` point.
fn annotation_fields(title: &str, description: &str) -> Fields {
    let mut fields = Fields::new();
    fields.insert("title".to_string(), FieldValue::Str(title.to_string()));
    fields.insert(
        "description".to_string(),
        FieldValue::Str(description.to_string()),
    );
    fields
}

/// One CNCF milestone annotation (Go's repeated blocks in `ProcessAnnotations`).
fn milestone_point(
    ctx: &Ctx,
    pts: &mut TSPoints,
    debug_label: &str,
    title: &str,
    description: &str,
    date: DateTime<FixedOffset>,
) {
    let fields = annotation_fields(title, description);
    if ctx.debug > 0 {
        printf!(
            "{}: {}: '{}', '{}'\n",
            debug_label,
            to_ymd_date(date),
            title,
            description
        );
    }
    let pt = new_ts_point(
        ctx,
        "annotations",
        "",
        None,
        Some(&fields),
        wall_as_utc(&date),
        false,
    );
    add_ts_point(ctx, pts, pt);
}

/// The `quick_ranges` tag series: every point carries the same three tags.
struct QuickRanges {
    tags: Tags,
    tm: DateTime<Utc>,
}

impl QuickRanges {
    fn new() -> Self {
        QuickRanges {
            tags: Tags::new(),
            tm: time_parse_any("2012-07-01"),
        }
    }

    /// `quick_ranges_data` is `suffix;period;from;to`.
    fn add(&mut self, ctx: &Ctx, pts: &mut TSPoints, suffix: &str, name: &str, data: &str) {
        self.tags
            .insert("quick_ranges_suffix".to_string(), suffix.to_string());
        self.tags
            .insert("quick_ranges_name".to_string(), name.to_string());
        self.tags
            .insert("quick_ranges_data".to_string(), data.to_string());
        if ctx.debug > 0 {
            printf!("Series: {}: {}\n", "quick_ranges", gofmt::map(&self.tags));
        }
        let pt = new_ts_point(
            ctx,
            "quick_ranges",
            "",
            Some(&self.tags),
            None,
            self.tm,
            false,
        );
        add_ts_point(ctx, pts, pt);
        self.tm += Duration::hours(1);
    }

    /// An exact date range: `suffix;;from;to`.
    fn add_range<A: TimeZone, B: TimeZone>(
        &mut self,
        ctx: &Ctx,
        pts: &mut TSPoints,
        suffix: &str,
        name: &str,
        from: DateTime<A>,
        to: DateTime<B>,
    ) {
        let data = format!(
            "{};;{};{}",
            suffix,
            to_ymdhms_date(from),
            to_ymdhms_date(to)
        );
        self.add(ctx, pts, suffix, name, &data);
    }
}

/// Go `NextDayStart(time.Now())`: tomorrow 00:00 of the local wall clock.
fn tomorrow() -> DateTime<Utc> {
    next_day_start(wall_as_utc(&Local::now()))
}

/// Go `ProcessAnnotations`: write the annotations, the CNCF milestone
/// annotations and the quick ranges into the TSDB (and the annotations into
/// the shared database when the project has one).
pub fn process_annotations(ctx: &mut Ctx, annotations: &mut Annotations, dates: &MilestoneDates) {
    // Connect to Postgres
    let ic = pg::pg_conn(ctx);

    // CNCF milestone dates
    let start_date = dates[0];
    let join_date = dates[1];
    let incubating_date = dates[2];
    let graduated_date = dates[3];
    let archived_date = dates[4];

    // Get BatchPoints
    let mut pts: TSPoints = Vec::new();

    // Annotations must be sorted to create quick ranges
    sort_by_date(&mut annotations.annotations);

    // Iterate annotations
    for annotation in &annotations.annotations {
        let annotation_name = safe_utf8_string(&annotation.name);
        let annotation_description = safe_utf8_string(&annotation.description);
        let fields = annotation_fields(&annotation_name, &annotation_description);
        if ctx.debug > 0 {
            printf!(
                "Series: {}: Date: {}: '{}', '{}'\n",
                "annotations",
                to_ymd_date(annotation.date),
                annotation.name,
                annotation.description
            );
        }
        let pt = new_ts_point(
            ctx,
            "annotations",
            "",
            None,
            Some(&fields),
            wall_as_utc(&annotation.date),
            false,
        );
        add_ts_point(ctx, &mut pts, pt);
    }

    // If both start and join dates are present then join date must be after start date
    let start_join_ok = match (start_date, join_date) {
        (Some(s), Some(j)) => j > s,
        _ => true,
    };
    if start_join_ok {
        // Project start date (additional annotation not used in quick ranges)
        if let Some(start) = start_date {
            milestone_point(
                ctx,
                &mut pts,
                "Project start date",
                "Project start date",
                &format!("{} - project starts", to_ymd_date(start)),
                start,
            );
        }
        // Join CNCF (additional annotation not used in quick ranges)
        if let Some(join) = join_date {
            milestone_point(
                ctx,
                &mut pts,
                "CNCF join date",
                "CNCF join date",
                &format!("{} - joined CNCF", to_ymd_date(join)),
                join,
            );
        }
    }

    // Moved to Incubating
    if let Some(incubating) = incubating_date {
        milestone_point(
            ctx,
            &mut pts,
            "Project moved to incubating state",
            "Moved to incubating state",
            &format!(
                "{} - project moved to incubating state",
                to_ymd_date(incubating)
            ),
            incubating,
        );
    }

    // Graduated
    if let Some(graduated) = graduated_date {
        milestone_point(
            ctx,
            &mut pts,
            "Project graduated",
            "Graduated",
            &format!("{} - project graduated", to_ymd_date(graduated)),
            graduated,
        );
    }

    // Archived
    if let Some(archived) = archived_date {
        milestone_point(
            ctx,
            &mut pts,
            "Project was archived",
            "Archived",
            &format!("{} - project was archived", to_ymd_date(archived)),
            archived,
        );
    }

    // Special ranges
    let periods: [(&str, &str, &str); 12] = [
        ("d", "Last day", "1 day"),
        ("w", "Last week", "1 week"),
        ("d10", "Last 10 days", "10 days"),
        ("m", "Last month", "1 month"),
        ("q", "Last quarter", "3 months"),
        ("m6", "Last 6 months", "6 months"),
        ("y", "Last year", "1 year"),
        ("y2", "Last 2 years", "2 years"),
        ("y3", "Last 3 years", "3 years"),
        ("y5", "Last 5 years", "5 years"),
        ("y10", "Last decade", "10 years"),
        ("y100", "Last century", "100 years"),
    ];

    // Last "..." periods
    let mut qr = QuickRanges::new();
    for (suffix, name, period) in periods {
        qr.add(ctx, &mut pts, suffix, name, &format!("{suffix};{period};;"));
    }

    // Add '(i) - (i+1)' annotation ranges
    let anns = &annotations.annotations;
    let last_index = anns.len().wrapping_sub(1);
    for (index, annotation) in anns.iter().enumerate() {
        let annotation_name = safe_utf8_string(&annotation.name);
        if index == last_index {
            let sfx = format!("a_{index}_n");
            qr.add_range(
                ctx,
                &mut pts,
                &sfx,
                &format!("{annotation_name} - now"),
                annotation.date,
                tomorrow(),
            );
            break;
        }
        let next_annotation = &anns[index + 1];
        let sfx = format!("a_{}_{}", index, index + 1);
        let next_annotation_name = safe_utf8_string(&next_annotation.name);
        qr.add_range(
            ctx,
            &mut pts,
            &sfx,
            &format!("{annotation_name} - {next_annotation_name}"),
            annotation.date,
            next_annotation.date,
        );
    }

    // 2 special periods: before and after joining CNCF
    if let (Some(start), Some(join)) = (start_date, join_date) {
        if join > start {
            // From project start to CNCF join date
            qr.add_range(ctx, &mut pts, "c_b", "Before joining CNCF", start, join);
            // From CNCF join date till now
            qr.add_range(ctx, &mut pts, "c_n", "Since joining CNCF", join, tomorrow());

            // If we have both moved to incubating and graduation, then graduation must happen after moving to incubation
            let correct_order = match (incubating_date, graduated_date) {
                (Some(i), Some(g)) => g > i,
                _ => true,
            };

            // Moved to incubating handle
            if let Some(incubating) = incubating_date {
                if correct_order && incubating > join {
                    // From CNCF join date to incubating date
                    qr.add_range(
                        ctx,
                        &mut pts,
                        "c_j_i",
                        "CNCF join date - moved to incubation",
                        join,
                        incubating,
                    );
                    // From incubating till graduating or now
                    match graduated_date {
                        Some(graduated) => qr.add_range(
                            ctx,
                            &mut pts,
                            "c_i_g",
                            "Moved to incubation - graduated",
                            incubating,
                            graduated,
                        ),
                        None => qr.add_range(
                            ctx,
                            &mut pts,
                            "c_i_n",
                            "Since moving to incubating state",
                            incubating,
                            tomorrow(),
                        ),
                    }
                }
            }

            // Graduated handle
            if let Some(graduated) = graduated_date {
                if correct_order && graduated > join {
                    // If there was no moved to incubating date
                    if incubating_date.is_none() {
                        qr.add_range(
                            ctx,
                            &mut pts,
                            "c_j_g",
                            "CNCF join date - graduated",
                            join,
                            graduated,
                        );
                    }
                    // From graduated till now
                    qr.add_range(
                        ctx,
                        &mut pts,
                        "c_g_n",
                        "Since graduating",
                        graduated,
                        tomorrow(),
                    );
                }
            }
        }
    }

    // Write the batch
    if !ctx.skip_tsdb {
        let table = "tquick_ranges";
        let column = "quick_ranges_suffix";
        if pg::table_exists(&ic, ctx, table) && pg::table_column_exists(&ic, ctx, table, column) {
            pg::exec_sql_with_err(
                &ic,
                ctx,
                &format!("delete from \"{table}\" where \"{column}\" like '%_n'"),
                &[],
            );
        }
        pg::write_ts_points(ctx, &ic, &pts, "", &[], None);
        // Annotations from all projects into 'allprj' database
        if !ctx.skip_shared_db && !ctx.shared_db.is_empty() {
            let mut anots: TSPoints = Vec::new();
            for pt in &pts {
                if pt.name != "annotations" {
                    continue;
                }
                let mut pt = pt.clone();
                pt.name = "annotations_shared".to_string();
                if let Some(fields) = &mut pt.fields {
                    pt.period = ctx.project.clone();
                    fields.insert(
                        "repo".to_string(),
                        FieldValue::Str(ctx.project_main_repo.clone()),
                    );
                }
                anots.push(pt);
            }
            let shared_db = ctx.shared_db.clone();
            let ics = pg::pg_conn_db(ctx, &shared_db);
            pg::write_ts_points(ctx, &ics, &anots, "", &[], None);
            ics.close();
        }
    } else if ctx.debug > 0 {
        printf!("Skipping annotations series write\n");
    }
    ic.close();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fx(s: &str) -> DateTime<FixedOffset> {
        DateTime::parse_from_rfc3339(s).unwrap()
    }

    #[test]
    fn fake_annotations_need_sane_dates() {
        assert!(
            get_fake_annotations(fx("2016-03-10T00:00:00Z"), fx("2016-03-10T00:00:00Z"))
                .annotations
                .is_empty()
        );
        assert!(
            get_fake_annotations(fx("2011-01-01T00:00:00Z"), fx("2016-03-10T00:00:00Z"))
                .annotations
                .is_empty()
        );
        assert!(
            get_fake_annotations(fx("2016-03-10T00:00:00Z"), fx("2015-03-10T00:00:00Z"))
                .annotations
                .is_empty()
        );
        let a = get_fake_annotations(fx("2014-06-01T00:00:00Z"), fx("2016-03-10T00:00:00Z"));
        assert_eq!(a.annotations.len(), 2);
        assert_eq!(a.annotations[0].name, "Project start");
        assert_eq!(a.annotations[0].description, "2014-06-01 - project starts");
        assert_eq!(a.annotations[1].name, "First CNCF project join date");
        assert_eq!(a.annotations[1].description, "2016-03-10");
    }

    #[test]
    fn byte_helpers_follow_go_strings() {
        assert_eq!(trim_space(b"  a b \t\r"), b"a b");
        assert_eq!(trim_space("\u{a0}x\u{a0}".as_bytes()), b"x");
        assert_eq!(trim_space(b" \xff a "), b"\xff a");
        assert_eq!(
            split_bytes("v1♂♀12♂♀m".as_bytes(), "♂♀".as_bytes()),
            vec![&b"v1"[..], &b"12"[..], &b"m"[..]]
        );
        assert_eq!(split_bytes(b"abc", b"x"), vec![&b"abc"[..]]);
        assert_eq!(split_bytes(b"", b"x"), vec![&b""[..]]);
        assert_eq!(parse_go_int(b"1441102500"), Some(1441102500));
        assert_eq!(parse_go_int(b"-5"), Some(-5));
        assert_eq!(parse_go_int(b"+5"), Some(5));
        assert_eq!(parse_go_int(b"1.5"), None);
        assert_eq!(parse_go_int(b" 1"), None);
        assert_eq!(parse_go_int(b"0x10"), None);
        // Go `message[0:40]` followed by `SafeUTF8String`: a multi-byte
        // character cut in half is dropped.
        let s = "123456789012345678901234567890123456789é";
        assert_eq!(safe_utf8_bytes(&s.as_bytes()[..40]), &s[..39]);
    }

    #[test]
    fn go_struct_rendering() {
        let a = Annotation {
            name: "v1.0".to_string(),
            description: "release".to_string(),
            date: fx("2015-08-01T12:00:00Z"),
        };
        assert_eq!(a.go_v(), "{v1.0 release 2015-08-01 12:00:00 +0000 UTC}");
    }

    #[test]
    fn wall_clock_relabelled_as_utc() {
        let dt = fx("2015-08-01T23:30:00+02:00");
        assert_eq!(
            hour_start(wall_as_utc(&dt)),
            Utc.with_ymd_and_hms(2015, 8, 1, 23, 0, 0).unwrap()
        );
    }
}
