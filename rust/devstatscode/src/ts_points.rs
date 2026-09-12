//! Time-series points — port of `ts_points.go`.
//!
//! A [`TSPoint`] is one row of a `s<series>` (fields) / `t<series>` (tags)
//! table; batches are written by [`crate::pg::write_ts_points`]. Tags and
//! fields are kept in `BTreeMap`s, so every iteration order is the sorted key
//! order — one of the (random) orders the Go maps could produce.

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, FixedOffset, Local, Utc};

use crate::context::Ctx;
use crate::gofmt;
use crate::printf;
use crate::time::{hour_start, to_ymdh_date};

/// A field value (Go `interface{}` restricted to the types `WriteTSPoints`
/// accepts: `float64`, `time.Time`, `string`, `[]uint8` for HLL sketches).
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Float(f64),
    Time(DateTime<Utc>),
    Str(String),
    Hll(Vec<u8>),
}

impl FieldValue {
    /// Type tag used by `WriteTSPointsBatch` to pick the column type
    /// (0 float, 1 time, 2 string, 3 HLL).
    pub fn type_id(&self) -> i32 {
        match self {
            FieldValue::Float(_) => 0,
            FieldValue::Time(_) => 1,
            FieldValue::Str(_) => 2,
            FieldValue::Hll(_) => 3,
        }
    }

    /// Go `%v`/`%+v` rendering.
    pub fn go_string(&self) -> String {
        match self {
            FieldValue::Float(f) => gofmt::float(*f),
            FieldValue::Time(t) => gofmt::time(*t),
            FieldValue::Str(s) => s.clone(),
            FieldValue::Hll(b) => {
                let parts: Vec<String> = b.iter().map(|x| x.to_string()).collect();
                format!("[{}]", parts.join(" "))
            }
        }
    }

    /// Go `%T`.
    pub fn go_type_name(&self) -> &'static str {
        match self {
            FieldValue::Float(_) => "float64",
            FieldValue::Time(_) => "time.Time",
            FieldValue::Str(_) => "string",
            FieldValue::Hll(_) => "[]uint8",
        }
    }
}

impl From<f64> for FieldValue {
    fn from(v: f64) -> Self {
        FieldValue::Float(v)
    }
}
impl From<i64> for FieldValue {
    fn from(v: i64) -> Self {
        FieldValue::Float(v as f64)
    }
}
impl From<DateTime<Utc>> for FieldValue {
    fn from(v: DateTime<Utc>) -> Self {
        FieldValue::Time(v)
    }
}
impl From<&str> for FieldValue {
    fn from(v: &str) -> Self {
        FieldValue::Str(v.to_string())
    }
}
impl From<String> for FieldValue {
    fn from(v: String) -> Self {
        FieldValue::Str(v)
    }
}
impl From<Vec<u8>> for FieldValue {
    fn from(v: Vec<u8>) -> Self {
        FieldValue::Hll(v)
    }
}

/// Tag set (`map[string]string`).
pub type Tags = BTreeMap<String, String>;
/// Field set (`map[string]interface{}`).
pub type Fields = BTreeMap<String, FieldValue>;

/// Single time series point (Go `TSPoint`).
#[derive(Debug, Clone, PartialEq)]
pub struct TSPoint {
    pub t: DateTime<Utc>,
    /// Creation time (Go `time.Now()`: the local zone, shown by `str()`).
    pub added: DateTime<FixedOffset>,
    pub period: String,
    pub name: String,
    /// `None` = Go nil map (no tags table row for this point).
    pub tags: Option<Tags>,
    /// `None` = Go nil map (no fields table row for this point).
    pub fields: Option<Fields>,
}

/// Batch of points (Go `TSPoints`).
pub type TSPoints = Vec<TSPoint>;

fn go_map_string<V: std::fmt::Display>(m: &Option<BTreeMap<String, V>>) -> String {
    match m {
        None => "map[]".to_string(),
        Some(m) => gofmt::map(m),
    }
}

impl std::fmt::Display for FieldValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.go_string())
    }
}

impl TSPoint {
    /// Go `TSPoint.Str()`:
    /// `<t> <added> <name> period: <period> tags: map[..] fields: map[..]`.
    pub fn str(&self) -> String {
        format!(
            "{} {} {} period: {} tags: {} fields: {}",
            to_ymdh_date(self.t),
            to_ymdh_date(self.added),
            self.name,
            self.period,
            go_map_string(&self.tags),
            go_map_string(&self.fields),
        )
    }
}

/// Go `TSPoints.Str()`: numbered points, one per line.
pub fn ts_points_str(pts: &[TSPoint]) -> String {
    let mut s = String::new();
    for (i, p) in pts.iter().enumerate() {
        s.push_str(&format!("#{} {}\n", i + 1, p.str()));
    }
    s
}

/// Go `NewTSPoint`: the point's time is `t` when `exact`, else the hour start.
pub fn new_ts_point(
    ctx: &Ctx,
    name: &str,
    period: &str,
    tags: Option<&Tags>,
    fields: Option<&Fields>,
    t: DateTime<Utc>,
    exact: bool,
) -> TSPoint {
    let pt = if exact { t } else { hour_start(t) };
    let p = TSPoint {
        t: pt,
        added: Local::now().fixed_offset(),
        name: name.to_string(),
        period: period.to_string(),
        tags: tags.cloned(),
        fields: fields.cloned(),
    };
    if ctx.debug > 0 {
        printf!("NewTSPoint: {}\n", p.str());
    }
    p
}

/// Go `AddTSPoint`.
pub fn add_ts_point(ctx: &Ctx, pts: &mut TSPoints, pt: TSPoint) {
    if ctx.debug > 0 {
        printf!("AddTSPoint: {}\n", pt.str());
    }
    pts.push(pt);
    if ctx.debug > 0 {
        printf!("AddTSPoint: point added, now {} points\n", pts.len());
    }
}

/// Sort key of a point: name, period and the sorted tags/fields
/// (Go `tsPointKey`).
pub fn ts_point_key(pt: &TSPoint) -> String {
    let mut s = format!("{}\0{}", pt.name, pt.period);
    if let Some(tags) = &pt.tags {
        for (k, v) in tags {
            s.push_str(&format!("\0{}={}", k, v));
        }
    }
    if let Some(fields) = &pt.fields {
        for (k, v) in fields {
            s.push_str(&format!("\0{}={}", k, v.go_string()));
        }
    }
    s
}

/// Go `MakeTSPointsUniqueTimes`: points sharing `(time, name, period)` get
/// consecutive microseconds added (in `ts_point_key` order) so that they no
/// longer collide on the `(time, period)` primary key.
pub fn make_ts_points_unique_times(ctx: &Ctx, pts: &mut TSPoints) {
    let mut keys: HashMap<(DateTime<Utc>, String, String), Vec<usize>> = HashMap::new();
    for (idx, pt) in pts.iter().enumerate() {
        keys.entry((pt.t, pt.name.clone(), pt.period.clone()))
            .or_default()
            .push(idx);
    }
    // Deterministic order of groups (Go iterates the map randomly; groups are
    // independent so the result is the same).
    let mut groups: Vec<_> = keys.into_iter().collect();
    groups.sort_by(|a, b| a.0.cmp(&b.0));
    for ((t, name, period), mut idxs) in groups {
        let n = idxs.len();
        if n < 2 {
            continue;
        }
        if n > 1_000_000 {
            crate::fatalf(format_args!(
                "MakeTSPointsUniqueTimes: too many points for {} {} {}: {}",
                gofmt::time(t),
                name,
                period,
                n
            ));
        }
        idxs.sort_by_cached_key(|i| ts_point_key(&pts[*i]));
        for (idx, pt_idx) in idxs.iter().enumerate() {
            pts[*pt_idx].t += chrono::Duration::microseconds(idx as i64);
        }
        if ctx.debug > 0 {
            printf!(
                "MakeTSPointsUniqueTimes: adjusted {} points for {} {} {}\n",
                n,
                gofmt::time(t),
                name,
                period
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ctx() -> Ctx {
        Ctx {
            debug: 0,
            ..Ctx::default()
        }
    }

    fn tags(pairs: &[(&str, &str)]) -> Tags {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn point_string_and_key() {
        let t = Utc.with_ymd_and_hms(2020, 3, 4, 5, 6, 7).unwrap();
        let mut fields = Fields::new();
        fields.insert("value".into(), FieldValue::Float(1.5));
        fields.insert("name".into(), FieldValue::Str("x".into()));
        fields.insert("dt".into(), FieldValue::Time(t));
        fields.insert("hll".into(), FieldValue::Hll(vec![1, 2]));
        let p = new_ts_point(
            &ctx(),
            "series",
            "d",
            Some(&tags(&[("b", "2"), ("a", "1")])),
            Some(&fields),
            t,
            false,
        );
        assert_eq!(p.t, Utc.with_ymd_and_hms(2020, 3, 4, 5, 0, 0).unwrap());
        // Go `ToYMDHDate` prints the hour with `%d` (no zero padding).
        assert!(p.str().starts_with("2020-03-04 5 "));
        assert!(p.str().ends_with(
            " series period: d tags: map[a:1 b:2] fields: map[dt:2020-03-04 05:06:07 +0000 UTC hll:[1 2] name:x value:1.5]"
        ));
        assert_eq!(
            ts_point_key(&p),
            "series\0d\0a=1\0b=2\0dt=2020-03-04 05:06:07 +0000 UTC\0hll=[1 2]\0name=x\0value=1.5"
        );
        let exact = new_ts_point(&ctx(), "s", "", None, None, t, true);
        assert_eq!(exact.t, t);
        assert!(exact
            .str()
            .ends_with(" s period:  tags: map[] fields: map[]"));
        assert_eq!(ts_point_key(&exact), "s\0");
        let mut pts = TSPoints::new();
        add_ts_point(&ctx(), &mut pts, exact.clone());
        add_ts_point(&ctx(), &mut pts, p.clone());
        let s = ts_points_str(&pts);
        assert!(s.starts_with("#1 "));
        assert!(s.contains("\n#2 "));
        assert_eq!(s.lines().count(), 2);
    }

    #[test]
    fn unique_times() {
        let t = Utc.with_ymd_and_hms(2020, 3, 4, 5, 0, 0).unwrap();
        let mk = |name: &str, period: &str, val: f64| {
            let mut f = Fields::new();
            f.insert("v".into(), FieldValue::Float(val));
            new_ts_point(&ctx(), name, period, None, Some(&f), t, true)
        };
        // Three colliding points (same time/name/period), one distinct series
        // and one distinct period.
        let mut pts = vec![
            mk("s", "d", 3.0),
            mk("s", "d", 1.0),
            mk("other", "d", 9.0),
            mk("s", "d", 2.0),
            mk("s", "w", 5.0),
        ];
        make_ts_points_unique_times(&ctx(), &mut pts);
        // Sorted by key "s\0d\0v=<val>": 1.0 → +0µs, 2.0 → +1µs, 3.0 → +2µs.
        assert_eq!(pts[0].t, t + chrono::Duration::microseconds(2));
        assert_eq!(pts[1].t, t);
        assert_eq!(pts[3].t, t + chrono::Duration::microseconds(1));
        assert_eq!(pts[2].t, t);
        assert_eq!(pts[4].t, t);
        // Idempotent once unique.
        let before = pts.clone();
        make_ts_points_unique_times(&ctx(), &mut pts);
        assert_eq!(pts, before);
    }

    #[test]
    fn field_values() {
        assert_eq!(FieldValue::from(2i64), FieldValue::Float(2.0));
        assert_eq!(FieldValue::Float(1e21).go_string(), "1e+21");
        assert_eq!(FieldValue::Float(100.0).go_string(), "100");
        assert_eq!(FieldValue::Str("a".into()).type_id(), 2);
        assert_eq!(FieldValue::Hll(vec![]).type_id(), 3);
        assert_eq!(FieldValue::Hll(vec![]).go_string(), "[]");
        assert_eq!(FieldValue::Float(0.0).go_type_name(), "float64");
    }
}
