//! `import_affs` — Rust port of `cmd/import_affs/import_affs.go`.
//!
//! Imports the cncf/gitdm developer affiliations (`github_users.json`, plus
//! the company acquisitions mapping `companies.yaml`) into the `gha_actors`,
//! `gha_actors_emails`, `gha_actors_names`, `gha_companies` and
//! `gha_actors_affiliations` tables of a DevStats PostgreSQL database.
//! Environment variables, messages, SQL statements and exit codes (0 imported,
//! 2 dry run / fatal, 3 already imported) are those of the Go program.
//!
//! Deliberate differences (all Go-random situations made deterministic):
//! maps are iterated in sorted order (Go: random map order), so the name
//! picked for a login with several names, the affiliation picked among ties,
//! the order of the `Mapped to`/`Used mapping` summary lines and which of
//! several acquisition-mapping violations is reported first are sorted
//! choices; error wording of the JSON/YAML/regexp parsers differs.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::Instant;

use devstatscode::chrono::{DateTime, TimeZone, Utc};
use devstatscode::pg::{
    self, exec_sql_with_err, fatal_on_pg_err, insert_ignore, n_value, n_values, query_sql_with_err,
    trunc_to_bytes, PgConn, SqlArg,
};
use devstatscode::yamlv2::de as yde;
use devstatscode::{
    consts, fatal_on_err, fatal_on_error, fatalf, goregex, hash, io, printf, signal,
    string as gostring, threads, time as gotime, unicode, Ctx,
};
use serde::de::{self, Deserializer, IgnoredAny, MapAccess, Visitor};
use serde::Deserialize;
use sha2::{Digest, Sha256};

/// Go `gitHubUser`: a single entry of cncf/gitdm `github_users.json`.
#[derive(Debug, Default, Clone, PartialEq)]
struct GitHubUser {
    login: String,
    email: String,
    affiliation: String,
    source: String,
    name: String,
    country_id: Option<String>,
    sex: Option<String>,
    tz: Option<String>,
    sex_prob: Option<f64>,
    age: Option<i64>,
}

/// jsoniter (`ConfigDefault`) struct decoding: keys match the Go field tags
/// ASCII case-insensitively, the last duplicate wins, unknown keys are
/// skipped, `null` is the zero value (`""` / nil pointer) and a `null` element
/// leaves the whole struct zeroed; wrong value types are errors.
impl<'de> Deserialize<'de> for GitHubUser {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = GitHubUser;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a JSON object")
            }

            fn visit_unit<E: de::Error>(self) -> Result<GitHubUser, E> {
                Ok(GitHubUser::default())
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<GitHubUser, A::Error> {
                let mut u = GitHubUser::default();
                while let Some(key) = map.next_key::<String>()? {
                    match key.to_ascii_lowercase().as_str() {
                        "login" => {
                            u.login = map.next_value::<Option<String>>()?.unwrap_or_default()
                        }
                        "email" => {
                            u.email = map.next_value::<Option<String>>()?.unwrap_or_default()
                        }
                        "affiliation" => {
                            u.affiliation = map.next_value::<Option<String>>()?.unwrap_or_default()
                        }
                        "source" => {
                            u.source = map.next_value::<Option<String>>()?.unwrap_or_default()
                        }
                        "name" => u.name = map.next_value::<Option<String>>()?.unwrap_or_default(),
                        "country_id" => u.country_id = map.next_value()?,
                        "sex" => u.sex = map.next_value()?,
                        "tz" => u.tz = map.next_value()?,
                        "sex_prob" => u.sex_prob = map.next_value()?,
                        "age" => u.age = map.next_value()?,
                        _ => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                Ok(u)
            }
        }
        d.deserialize_any(V)
    }
}

/// Go `allAcquisitions`: `acquisitions: [[regexp, company], ...]`.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct AllAcquisitions {
    #[serde(deserialize_with = "yde::seq")]
    acquisitions: Vec<yde::StrArray<2>>,
}

impl AllAcquisitions {
    /// Go `%+v`: `{Acquisitions:[[re1 com1] [re2 com2]]}`.
    fn go_string(&self) -> String {
        let items: Vec<String> = self.acquisitions.iter().map(acq_string).collect();
        format!("{{Acquisitions:[{}]}}", items.join(" "))
    }
}

/// Go `%+v` / `%s` of a `[2]string`: `[a b]`.
fn acq_string(acq: &yde::StrArray<2>) -> String {
    format!("[{} {}]", acq.0[0], acq.0[1])
}

/// Go `affData`: a single affiliation period of a login.
#[derive(Debug, Clone)]
struct AffData {
    login: String,
    company: String,
    source: String,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

/// Go `csData`: country_id, tz, tz_offset, sex, sex_prob, age of a user.
#[derive(Debug, Default, Clone)]
struct CsData {
    country_id: Option<String>,
    sex: Option<String>,
    tz: Option<String>,
    sex_prob: Option<f64>,
    tz_offset: Option<i64>,
    age: Option<i64>,
}

impl CsData {
    /// The `lib.Compare*Ptr` chain of `processLoginCSData` (floats compared
    /// with a `1e-10` tolerance).
    fn same_as(&self, o: &CsData) -> bool {
        self.country_id == o.country_id
            && self.sex == o.sex
            && self.tz == o.tz
            && self.tz_offset == o.tz_offset
            && self.age == o.age
            && match (self.sex_prob, o.sex_prob) {
                (None, None) => true,
                (Some(a), Some(b)) => (a - b).abs() < 1e-10,
                _ => false,
            }
    }
}

/// Go `lib.Actor` (only the fields `import_affs` uses).
#[derive(Debug, Default, Clone)]
struct Actor {
    id: i64,
    #[allow(dead_code)]
    login: String,
    name: String,
}

/// Go `stringSet`.
type StringSet = BTreeSet<String>;
/// Go `mapIntSet`: source priority → affiliations.
type MapIntSet = BTreeMap<i64, StringSet>;
/// Go `mapStringIntSet`: login → priority → affiliations.
type MapStringIntSet = BTreeMap<String, MapIntSet>;
/// Go `cacheActIDs` + `cacheActLogins`: login → correlated actor ids / logins.
type Caches = (HashMap<String, Vec<i64>>, HashMap<String, Vec<String>>);

/// Go `emailDecode`: `user!domain` → `user@domain`.
fn email_decode(line: &str) -> String {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| goregex::compile(r"([^\s!]+)!([^\s!]+)").expect("valid regex"));
    re.replace_all(line, "$1@$2").into_owned()
}

/// Go `tzOffset`: timezone offset in minutes for a tz name (cached), `None`
/// for no/empty/unknown zone.
fn tz_offset(
    con: &PgConn,
    ctx: &Ctx,
    ptz: Option<&str>,
    cache: &mut HashMap<String, Option<i64>>,
) -> Option<i64> {
    let tz = ptz?;
    if tz.is_empty() {
        return None;
    }
    if let Some(off) = cache.get(tz) {
        return *off;
    }
    // PostgreSQL 14+ returns numeric from extract(), which database/sql cannot
    // scan into *int ("120.0000000000000000": invalid syntax) - cast to int
    let mut rows = query_sql_with_err(
        con,
        ctx,
        &format!(
            "select (extract(epoch from utc_offset) / 60)::int from pg_timezone_names where name = {} union select null order by 1 limit 1",
            n_value(1)
        ),
        &[SqlArg::from(tz)],
    );
    let mut offset: Option<i64> = None;
    while rows.next() {
        fatal_on_pg_err(rows.scan(&mut [&mut offset]));
    }
    fatal_on_pg_err(rows.err());
    fatal_on_pg_err(rows.close());
    cache.insert(tz.to_string(), offset);
    offset
}

/// Go `findActor`: the actor with the maximum id for a login (exact or
/// case-insensitive match), with his/her country/sex/tz data.
fn find_actor(
    con: &PgConn,
    ctx: &Ctx,
    login: &str,
    maybe_hide: &(dyn Fn(&str) -> String + Sync),
) -> Option<(Actor, CsData)> {
    let login = maybe_hide(login);
    let mut rows = query_sql_with_err(
        con,
        ctx,
        &format!(
            "select id, name, country_id, tz, tz_offset, sex, sex_prob, age from gha_actors where login={} \
             union select id, name, country_id, tz, tz_offset, sex, sex_prob, age from gha_actors where lower(login)={} \
             order by id desc limit 1",
            n_value(1),
            n_value(2),
        ),
        &[SqlArg::from(login.as_str()), SqlArg::from(login.to_lowercase())],
    );
    let mut found: Option<(Actor, CsData)> = None;
    while rows.next() {
        let mut actor = Actor::default();
        let mut csd = CsData::default();
        let mut name: Option<String> = None;
        fatal_on_pg_err(rows.scan(&mut [
            &mut actor.id,
            &mut name,
            &mut csd.country_id,
            &mut csd.tz,
            &mut csd.tz_offset,
            &mut csd.sex,
            &mut csd.sex_prob,
            &mut csd.age,
        ]));
        actor.login = login.clone();
        if let Some(n) = name {
            actor.name = n;
        }
        found = Some((actor, csd));
    }
    fatal_on_pg_err(rows.err());
    fatal_on_pg_err(rows.close());
    found
}

/// Go `%+v` of the `logins` (`map[string]struct{}`) and `ids`
/// (`map[int]struct{}`) maps: `map[a:{} b:{}]` with sorted keys.
fn go_set_string<T: fmt::Display>(keys: impl Iterator<Item = T>) -> String {
    let parts: Vec<String> = keys.map(|k| format!("{k}:{{}}")).collect();
    format!("map[{}]", parts.join(" "))
}

/// Go `findActors`: all actor ids correlated with a login — transitively via
/// shared ids and case-insensitive logins (at most 10 rounds) — and all the
/// logins reached (the given one first). Ids and the other logins are sorted.
fn find_actors(
    con: &PgConn,
    ctx: &Ctx,
    login: &str,
    maybe_hide: &(dyn Fn(&str) -> String + Sync),
) -> (Vec<i64>, Vec<String>) {
    let login = maybe_hide(login);
    let mut ids: BTreeSet<i64> = BTreeSet::new();
    let mut logins: BTreeSet<String> = BTreeSet::new();
    logins.insert(login.clone());
    let mut prev_ids = String::new();
    let mut prev_logins = login.clone();
    let mut depth = 0;
    loop {
        let args: Vec<SqlArg> = logins
            .iter()
            .map(|l| SqlArg::from(l.to_lowercase()))
            .collect();
        let placeholders: Vec<String> = (1..=args.len()).map(n_value).collect();
        let query = format!(
            "select id from gha_actors where lower(login) in ({})",
            placeholders.join(",")
        );
        let mut rows = query_sql_with_err(con, ctx, &query, &args);
        while rows.next() {
            let mut aid: i64 = 0;
            fatal_on_pg_err(rows.scan(&mut [&mut aid]));
            ids.insert(aid);
        }
        if ids.is_empty() {
            return (Vec::new(), vec![login]);
        }
        fatal_on_pg_err(rows.err());
        fatal_on_pg_err(rows.close());
        let args: Vec<SqlArg> = ids.iter().map(|i| SqlArg::from(*i)).collect();
        let placeholders: Vec<String> = (1..=args.len()).map(n_value).collect();
        let query = format!(
            "select login from gha_actors where id in ({})",
            placeholders.join(",")
        );
        let mut rows = query_sql_with_err(con, ctx, &query, &args);
        while rows.next() {
            let mut alogin = String::new();
            fatal_on_pg_err(rows.scan(&mut [&mut alogin]));
            logins.insert(maybe_hide(&alogin));
        }
        fatal_on_pg_err(rows.err());
        fatal_on_pg_err(rows.close());
        let curr_logins = logins.iter().cloned().collect::<Vec<_>>().join(",");
        let mut curr_ids_ary: Vec<String> = ids.iter().map(|i| i.to_string()).collect();
        curr_ids_ary.sort();
        let curr_ids = curr_ids_ary.join(",");
        depth += 1;
        if prev_logins == curr_logins && prev_ids == curr_ids {
            break;
        }
        if depth >= 10 {
            printf!(
                "Error (non fatal): gone too deep: logins map: {}, ids map: {}\n",
                go_set_string(logins.iter()),
                go_set_string(ids.iter())
            );
            printf!(
                "Error (non fatal): gone too deep: Logins: '{}'=='{}', IDs: '{}'=='{}'\n",
                prev_logins,
                curr_logins,
                prev_ids,
                curr_ids
            );
            break;
        }
        prev_logins = curr_logins;
        prev_ids = curr_ids;
    }
    let act_ids: Vec<i64> = ids.into_iter().collect();
    let mut act_logins = vec![login.clone()];
    act_logins.extend(logins.into_iter().filter(|l| *l != login));
    (act_ids, act_logins)
}

/// Go `firstKey`: the first (here: smallest) element of a set.
fn first_key(set: &StringSet) -> String {
    set.iter().next().cloned().unwrap_or_default()
}

/// Go `addActor`: insert a not-yet-existing actor with an artificial
/// (hash-based, negative) id; returns that id.
#[allow(clippy::too_many_arguments)]
fn add_actor(
    con: &PgConn,
    ctx: &Ctx,
    login: &str,
    name: &str,
    csd: &CsData,
    maybe_hide: &(dyn Fn(&str) -> String + Sync),
) -> i64 {
    let hlogin = maybe_hide(login);
    let name = maybe_hide(name);
    let aid = hash::hash_strings(&[login]);
    exec_sql_with_err(
        con,
        ctx,
        &format!(
            "insert into gha_actors(id, login, name, country_id, sex, tz, sex_prob, tz_offset, age) {}",
            n_values(9)
        ),
        &[
            SqlArg::from(aid),
            SqlArg::from(hlogin),
            SqlArg::from(trunc_to_bytes(&name, 120)),
            SqlArg::from(csd.country_id.as_deref()),
            SqlArg::from(csd.sex.as_deref()),
            SqlArg::from(csd.tz.as_deref()),
            SqlArg::from(csd.sex_prob),
            SqlArg::from(csd.tz_offset),
            SqlArg::from(csd.age),
        ],
    );
    aid
}

/// Company acquisitions mapping state (Go `acqMap`, `comMap`, `stat`).
#[derive(Default)]
struct AcqState {
    /// `[(regexp, index in the yaml, mapped company)]` in yaml order.
    acq_map: Vec<(regex::Regex, String)>,
    /// company → (mapped company, `"m"` mapped / `"u"` unmapped).
    com_map: BTreeMap<String, (String, &'static str)>,
    /// `---` (unmapped) or mapped company → `[regexp matches, cache hits]`.
    stat: BTreeMap<String, [i64; 2]>,
}

impl AcqState {
    /// Go `mapCompanyName`: map a company to the acquiring company (cached,
    /// with statistics).
    fn map_company_name(&mut self, company: &str) -> String {
        if let Some((res, kind)) = self.com_map.get(company).cloned() {
            let key = if kind == "m" { res.as_str() } else { "---" };
            self.stat.entry(key.to_string()).or_default()[1] += 1;
            return res;
        }
        for (re, res) in &self.acq_map {
            if re.is_match(company) {
                self.com_map.insert(company.to_string(), (res.clone(), "m"));
                self.stat.entry(res.clone()).or_default()[0] += 1;
                return res.clone();
            }
        }
        self.com_map
            .insert(company.to_string(), (company.to_string(), "u"));
        self.stat.entry("---".to_string()).or_default()[0] += 1;
        company.to_string()
    }
}

/// Go `alreadyImported`: SHA-256 of a file and whether it is recorded in
/// `gha_imported_shas`.
fn already_imported(con: &PgConn, ctx: &Ctx, fname: &str) -> (bool, String) {
    let data = match io::read_file(ctx, fname) {
        Ok(d) => d,
        Err(e) => fatal_on_error(e),
    };
    let sha: String = Sha256::digest(&data)
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect();
    let mut rows = query_sql_with_err(
        con,
        ctx,
        &format!("select sha from gha_imported_shas where sha={}", n_value(1)),
        &[SqlArg::from(sha.as_str())],
    );
    let mut sha2 = String::new();
    while rows.next() {
        fatal_on_pg_err(rows.scan(&mut [&mut sha2]));
    }
    fatal_on_pg_err(rows.err());
    fatal_on_pg_err(rows.close());
    (sha2 == sha, sha)
}

/// Go `setImportedSHA`.
fn set_imported_sha(con: &PgConn, ctx: &Ctx, sha: &str) {
    exec_sql_with_err(
        con,
        ctx,
        &format!(
            "insert into gha_imported_shas(sha) select {} on conflict do nothing",
            n_value(1)
        ),
        &[SqlArg::from(sha)],
    );
}

/// Go `scoreCSD`: how much country/sex/tz/age information a record carries.
fn score_csd(csd: &CsData) -> f64 {
    let mut score = 0.0;
    if csd.country_id.as_deref().is_some_and(|c| !c.is_empty()) {
        score += 2.0;
    }
    if csd.tz.as_deref().is_some_and(|t| !t.is_empty()) {
        score += 1.0;
    }
    if csd.tz_offset.is_some() {
        score += 1.0;
    }
    if matches!(csd.sex.as_deref(), Some("m") | Some("f") | Some("b")) {
        score += 1.0;
    }
    if let Some(p) = csd.sex_prob {
        score += p;
    }
    if csd.age.is_some() {
        score += 0.5;
    }
    score
}

/// Run `f` over `items` with at most `thr_n` concurrent workers (the Go
/// goroutine/channel throttling), sequentially when `thr_n <= 1`.
fn run_pool<T: Sync>(thr_n: usize, items: &[T], f: impl Fn(&T) + Sync) {
    if thr_n <= 1 || items.len() <= 1 {
        for it in items {
            f(it);
        }
        return;
    }
    let next = AtomicUsize::new(0);
    let f = &f;
    thread::scope(|s| {
        for _ in 0..thr_n.min(items.len()) {
            s.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::SeqCst);
                if i >= items.len() {
                    break;
                }
                f(&items[i]);
            });
        }
    });
}

/// Go `company[:32] + company[l-31:]` (byte slicing, so a multi-byte
/// character can be cut; the invalid remainders decode to U+FFFD which
/// `StripUnicode` drops, exactly like Go's `RuneError` handling).
fn shorten_company(company: &str) -> String {
    let b = company.as_bytes();
    let l = b.len();
    let joined = [&b[..32], &b[l - 31..]].concat();
    unicode::strip_unicode(&String::from_utf8_lossy(&joined))
}

/// Go `importAffs`: import the given JSON file (default
/// `<data_prefix><GHA2DB_AFFILIATIONS_JSON>`); returns the exit code.
fn import_affs(json_fn: &str) -> i32 {
    // Environment context parse
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);

    // Files path
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    // Handle default file name
    let json_fn = if json_fn.is_empty() {
        // Local or cron mode?
        format!("{data_prefix}{}", ctx.affiliations_json)
    } else {
        json_fn.to_string()
    };
    printf!("Importing {}\n", json_fn);

    // Connect to Postgres DB
    let con = pg::pg_conn(&ctx);

    // Check if given file was already imported
    let mut current_sha = String::new();
    let mut current_sha2 = String::new();
    if ctx.check_imported_sha {
        let (imported, sha) = already_imported(&con, &ctx, &json_fn);
        if imported {
            if ctx.skip_company_acq {
                printf!(
                    "{} (SHA: {}) was already imported and skip company acquisitions mode is set, exiting\n",
                    json_fn,
                    sha
                );
                return 3;
            }
            printf!(
                "{} (SHA: {}) was already imported, checking company acquisitions file import status\n",
                json_fn,
                sha
            );
        }
        current_sha = sha;
        let fname = format!("{data_prefix}{}", ctx.company_acq_yaml);
        let (imported2, sha) = already_imported(&con, &ctx, &fname);
        if imported2 {
            if imported {
                printf!(
                    "{} (SHA: {}) was already imported, {} (SHA: {}) also imported, exiting\n",
                    fname,
                    sha,
                    json_fn,
                    current_sha
                );
                return 3;
            }
            printf!(
                "{} (SHA: {}) was already imported, but {} (SHA: {}) wasn't, continuying\n",
                fname,
                sha,
                json_fn,
                current_sha
            );
        }
        current_sha2 = sha;
        if ctx.only_check_imported_sha {
            printf!("Returining not-imported state\n");
            return 0;
        }
    }

    // Read company acquisitions mapping
    let mut acq = AcqState::default();
    if !ctx.skip_company_acq {
        let mut acqs = AllAcquisitions::default();
        match io::read_file(&ctx, &format!("{data_prefix}{}", ctx.company_acq_yaml)) {
            Err(e) => {
                printf!(
                    "Cannot read company acquisitions mapping '{}', continuying without\n",
                    e
                );
            }
            Ok(data) => {
                acqs = fatal_on_err(yde::unmarshal(&data));
                if ctx.debug > 0 {
                    printf!("Acquisitions: {}\n", acqs.go_string());
                }
            }
        }
        let mut src_map: BTreeMap<String, String> = BTreeMap::new();
        let mut res_map: BTreeSet<String> = BTreeSet::new();
        for (idx, a) in acqs.acquisitions.iter().enumerate() {
            let re = match goregex::compile(&a.0[0]) {
                Ok(re) => re,
                // regexp.MustCompile panics on an invalid pattern
                Err(e) => panic!("regexp: Compile(`{}`): {}", a.0[0], e),
            };
            if let Some(res) = src_map.get(&a.0[0]) {
                fatalf(format_args!(
                    "Acquisition number {} '{}' is already present in the mapping and maps into '{}'",
                    idx,
                    acq_string(a),
                    res
                ));
            }
            src_map.insert(a.0[0].clone(), a.0[1].clone());
            if res_map.contains(&a.0[1]) {
                fatalf(format_args!(
                    "Acquisition number {} '{}': some other acquisition already maps into '{}', merge them",
                    idx,
                    acq_string(a),
                    a.0[1]
                ));
            }
            res_map.insert(a.0[1].clone());
            acq.acq_map.push((re, a.0[1].clone()));
        }
        for (i, (re, res)) in acq.acq_map.iter().enumerate() {
            for (idx, a) in acqs.acquisitions.iter().enumerate() {
                if re.is_match(&a.0[1]) && i != idx {
                    fatalf(format_args!(
                        "Acquisition's number {} '{}' result '{}' matches other acquisition number {} '{}' which maps to '{}', simplify it: '{}' -> '{}'",
                        idx, a.0[0], a.0[1], i, acqs.acquisitions[i].0[0], res, a.0[0], res
                    ));
                }
                if re.is_match(&a.0[0]) && *res != a.0[1] {
                    fatalf(format_args!(
                        "Acquisition's number {} '{}' regexp '{}' matches other acquisition number {} '{}' which maps to '{}': result is different '{}'",
                        idx,
                        acq_string(a),
                        a.0[0],
                        i,
                        acqs.acquisitions[i].0[0],
                        res,
                        a.0[1]
                    ));
                }
            }
        }
    }

    // Parse github_users.json
    let data = match io::read_file(&ctx, &json_fn) {
        Ok(d) => d,
        Err(e) => fatal_on_error(e),
    };
    let users: Vec<GitHubUser> = match serde_json::from_slice::<Option<Vec<GitHubUser>>>(&data) {
        Ok(u) => u.unwrap_or_default(),
        Err(e) => fatal_on_error(e),
    };

    // Process users affiliations
    let mut login_emails: BTreeMap<String, StringSet> = BTreeMap::new();
    let mut login_names: BTreeMap<String, StringSet> = BTreeMap::new();
    let mut login_affs: MapStringIntSet = BTreeMap::new();
    let mut login_cs_data: BTreeMap<String, CsData> = BTreeMap::new();
    let mut tz_cache: HashMap<String, Option<i64>> = HashMap::new();
    let source_to_prio: HashMap<&str, i64> = HashMap::from([
        ("notfound", -20),
        ("domain", -10),
        ("", 0),
        ("config", 10),
        ("manual", 20),
        ("user_manual", 30),
        ("user", 40),
    ]);
    let prio_to_source: HashMap<i64, &str> = HashMap::from([
        (-20, "notfound"),
        (-10, "domain"),
        (0, ""),
        (10, "config"),
        (20, "manual"),
        (30, "user_manual"),
        (40, "user"),
    ]);
    let (mut e_names, mut e_emails, mut e_affs) = (0, 0, 0);
    printf!("Processing {} JSON entries\n", users.len());
    for user in &users {
        // Email decode ! --> @
        let email = email_decode(&user.email).to_lowercase();
        let login = user.login.to_lowercase();

        // Affiliation source
        let source = user.source.to_lowercase();
        let source_prio = source_to_prio.get(source.as_str()).copied().unwrap_or(0);

        // Email
        if !email.is_empty() {
            login_emails.entry(login.clone()).or_default().insert(email);
        } else {
            e_emails += 1;
        }

        // Name
        if !user.name.is_empty() {
            login_names
                .entry(login.clone())
                .or_default()
                .insert(user.name.clone());
        } else {
            e_names += 1;
        }

        // Affiliation
        let aff = user.affiliation.as_str();
        if aff != "NotFound" && aff != "(Unknown)" && aff != "?" && aff != "-" && !aff.is_empty() {
            let aff = aff.replace('"', "");
            login_affs
                .entry(login.clone())
                .or_default()
                .entry(source_prio)
                .or_default()
                .insert(aff);
        } else {
            e_affs += 1;
        }

        // Country & sex data
        let new_csd = CsData {
            country_id: user.country_id.clone(),
            sex: user.sex.clone(),
            tz: user.tz.clone(),
            sex_prob: user.sex_prob,
            tz_offset: tz_offset(&con, &ctx, user.tz.as_deref(), &mut tz_cache),
            age: user.age,
        };
        match login_cs_data.get(&login) {
            Some(csd) => {
                if score_csd(&new_csd) > score_csd(csd) {
                    login_cs_data.insert(login, new_csd);
                }
            }
            None => {
                login_cs_data.insert(login, new_csd);
            }
        }
    }
    printf!(
        "Processing non-empty: {} name lists, {} email lists, {} affiliations lists, {} objects\n",
        login_names.len(),
        login_emails.len(),
        login_affs.len(),
        login_cs_data.len()
    );
    printf!(
        "Empty/Not found: names: {}, emails: {}, affiliations: {}\n",
        e_names,
        e_emails,
        e_affs
    );

    if ctx.dry_run {
        printf!("Exiting due to dry-run mode.\n");
        return 2;
    }

    // Threads
    let mut thr_n = threads::get_threads_num(&mut ctx);
    if thr_n > 10 {
        thr_n = 10;
    }
    let maybe_hide = gostring::maybe_hide_func(gostring::get_hidden(&ctx, consts::HIDE_CFG_FILE));
    let maybe_hide: &(dyn Fn(&str) -> String + Sync) = &maybe_hide;
    let ctx = &ctx;
    let con = &con;

    // Login - Names should be 1:1 (also handle records without name set)
    let added = AtomicI64::new(0);
    let (updated, no_name, mul_names, not_changed) = (
        AtomicI64::new(0),
        AtomicI64::new(0),
        AtomicI64::new(0),
        AtomicI64::new(0),
    );
    let process_login_cs_data = |login: &str, cs_d: &CsData| {
        let mut name = String::new();
        let found_name = match login_names.get(login) {
            Some(names) => {
                // Other option would be to join all names via ", " - but it's better to query gha_actors_names then
                name = first_key(names);
                if names.len() > 1 {
                    mul_names.fetch_add(1, Ordering::SeqCst);
                }
                true
            }
            None => {
                no_name.fetch_add(1, Ordering::SeqCst);
                false
            }
        };
        // Try to find actor by login
        match find_actor(con, ctx, login, maybe_hide) {
            None => {
                // If no such actor, add with artificial ID (just like data from pre-2015)
                add_actor(con, ctx, login, &name, cs_d, maybe_hide);
                added.fetch_add(1, Ordering::SeqCst);
            }
            Some((actor, csd)) => {
                if (found_name && name != actor.name) || !csd.same_as(cs_d) {
                    let lower_login = maybe_hide(login).to_lowercase();
                    if found_name {
                        // If actor found, but with different name (actually with name == "" after standard GHA import), update name
                        // Because there can be the same actor (by id) with different IDs (pre-2015 and post 2015), update His/Her name
                        // for all records with this login
                        exec_sql_with_err(
                            con,
                            ctx,
                            &format!(
                                "update gha_actors set name={}, country_id={}, sex={}, tz={}, sex_prob={}, tz_offset={}, age={} where lower(login)={}",
                                n_value(1), n_value(2), n_value(3), n_value(4), n_value(5), n_value(6), n_value(7), n_value(8)
                            ),
                            &[
                                SqlArg::from(maybe_hide(&trunc_to_bytes(&name, 120))),
                                SqlArg::from(cs_d.country_id.as_deref()),
                                SqlArg::from(cs_d.sex.as_deref()),
                                SqlArg::from(cs_d.tz.as_deref()),
                                SqlArg::from(cs_d.sex_prob),
                                SqlArg::from(cs_d.tz_offset),
                                SqlArg::from(cs_d.age),
                                SqlArg::from(lower_login),
                            ],
                        );
                    } else {
                        exec_sql_with_err(
                            con,
                            ctx,
                            &format!(
                                "update gha_actors set country_id={}, sex={}, tz={}, sex_prob={}, tz_offset={}, age={} where lower(login)={}",
                                n_value(1), n_value(2), n_value(3), n_value(4), n_value(5), n_value(6), n_value(7)
                            ),
                            &[
                                SqlArg::from(cs_d.country_id.as_deref()),
                                SqlArg::from(cs_d.sex.as_deref()),
                                SqlArg::from(cs_d.tz.as_deref()),
                                SqlArg::from(cs_d.sex_prob),
                                SqlArg::from(cs_d.tz_offset),
                                SqlArg::from(cs_d.age),
                                SqlArg::from(lower_login),
                            ],
                        );
                    }
                    updated.fetch_add(1, Ordering::SeqCst);
                } else {
                    not_changed.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    };
    let cs_items: Vec<(&String, &CsData)> = login_cs_data.iter().collect();
    if thr_n > 1 {
        printf!("Processing using MT{} version\n", thr_n);
        run_pool(thr_n, &cs_items, |(login, cs_d)| {
            process_login_cs_data(login, cs_d)
        });
        printf!("Final threads join\n");
    } else {
        printf!("Processing using ST version\n");
        run_pool(1, &cs_items, |(login, cs_d)| {
            process_login_cs_data(login, cs_d)
        });
    }
    printf!(
        "Added actors: {}, updated actors: {}, empty names: {}, non-unique names: {}, non-changed: {}\n",
        added.load(Ordering::SeqCst),
        updated.load(Ordering::SeqCst),
        no_name.load(Ordering::SeqCst),
        mul_names.load(Ordering::SeqCst),
        not_changed.load(Ordering::SeqCst)
    );

    // Main caches
    let caches: Mutex<Caches> = Mutex::new((HashMap::new(), HashMap::new()));
    let cache = |act_ids: &Vec<i64>, act_logins: &Vec<String>| {
        let mut c = caches.lock().unwrap();
        for a_login in act_logins {
            c.0.insert(a_login.clone(), act_ids.clone());
            c.1.insert(a_login.clone(), act_logins.clone());
        }
    };
    let login_csd = |login: &str| login_cs_data.get(login).cloned().unwrap_or_default();

    // Login - Possible multiple logins, possibly multiple affs
    added.store(0, Ordering::SeqCst);
    let aff_logins: Vec<&String> = login_affs.keys().collect();
    run_pool(thr_n, &aff_logins, |login| {
        let (mut act_ids, act_logins) = find_actors(con, ctx, login, maybe_hide);
        if act_ids.is_empty() {
            let a_id = add_actor(con, ctx, login, "", &login_csd(login), maybe_hide);
            act_ids.push(a_id);
            added.fetch_add(1, Ordering::SeqCst);
        }
        // Store given login's actor IDs in the case
        cache(&act_ids, &act_logins);
    });
    if added.load(Ordering::SeqCst) > 0 {
        printf!(
            "Unexpected: added actors: {} while caching affiliations\n",
            added.load(Ordering::SeqCst)
        );
    }

    // Handle GitHub login changes
    // Propagate until nothing changes (deterministic fixpoint over a sorted
    // snapshot of the logins; the loop adds new logins to login_affs)
    let (mut new_logins, mut copied_affs, mut other_prios) = (0, 0, 0);
    loop {
        let mut changed = false;
        let logins: Vec<String> = login_affs.keys().cloned().collect();
        for login in &logins {
            let prios = login_affs[login].clone();
            let act_logins = match caches.lock().unwrap().1.get(login) {
                Some(l) => l.clone(),
                None => continue,
            };
            for other_login in &act_logins {
                if other_login == login {
                    continue;
                }
                let other = match login_affs.get_mut(other_login) {
                    Some(o) => o,
                    None => {
                        new_logins += 1;
                        changed = true;
                        login_affs.entry(other_login.clone()).or_default()
                    }
                };
                for (prio, affs) in &prios {
                    let set = match other.get_mut(prio) {
                        Some(s) => s,
                        None => {
                            other_prios += 1;
                            changed = true;
                            other.entry(*prio).or_default()
                        }
                    };
                    for aff in affs {
                        if set.insert(aff.clone()) {
                            copied_affs += 1;
                            changed = true;
                        }
                    }
                }
            }
        }
        if !changed {
            break;
        }
    }
    printf!(
        "{} new logins added by correlations, copied affiliations: {} ({} different priority)\n",
        new_logins,
        copied_affs,
        other_prios
    );

    // Login - Email(s) 1:N
    added.store(0, Ordering::SeqCst);
    let all_emails = AtomicI64::new(0);
    let email_items: Vec<(&String, &StringSet)> = login_emails.iter().collect();
    run_pool(thr_n, &email_items, |(login, emails)| {
        let (mut act_ids, act_logins) = find_actors(con, ctx, login, maybe_hide);
        if act_ids.is_empty() {
            // Should not happen
            let a_id = add_actor(con, ctx, login, "", &login_csd(login), maybe_hide);
            act_ids.push(a_id);
            added.fetch_add(1, Ordering::SeqCst);
        }
        cache(&act_ids, &act_logins);
        for email in emails.iter() {
            // One actor can have multiple emails but...
            // One email can also belong to multiple actors
            // This happens when actor was first defined in pre-2015 era (so He/She have negative ID then)
            // And then in new API era 2015+ that actor was active too (so He/She will
            // have entry with valid GitHub actor_id > 0)
            for aid in &act_ids {
                exec_sql_with_err(
                    con,
                    ctx,
                    &insert_ignore(&format!(
                        "into gha_actors_emails(actor_id, email) {}",
                        n_values(2)
                    )),
                    &[
                        SqlArg::from(*aid),
                        SqlArg::from(maybe_hide(&trunc_to_bytes(email, 120))),
                    ],
                );
                all_emails.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    if added.load(Ordering::SeqCst) > 0 {
        printf!(
            "Unexpected: added {} actors while processing emails\n",
            added.load(Ordering::SeqCst)
        );
    }
    printf!(
        "Added up to {} actors emails\n",
        all_emails.load(Ordering::SeqCst)
    );

    // Login - Names(s) 1:N
    let all_names = AtomicI64::new(0);
    let name_items: Vec<(&String, &StringSet)> = login_names.iter().collect();
    run_pool(thr_n, &name_items, |(login, names)| {
        let (act_ids, act_logins) = find_actors(con, ctx, login, maybe_hide);
        if act_ids.is_empty() {
            fatalf(format_args!("actor login not found {}", login));
        }
        // Store given login's actor IDs in the case
        cache(&act_ids, &act_logins);
        for name in names.iter() {
            // One actor can have multiple names but...
            // One name can also belong to multiple actors
            for aid in &act_ids {
                exec_sql_with_err(
                    con,
                    ctx,
                    &insert_ignore(&format!(
                        "into gha_actors_names(actor_id, name) {}",
                        n_values(2)
                    )),
                    &[
                        SqlArg::from(*aid),
                        SqlArg::from(maybe_hide(&trunc_to_bytes(name, 120))),
                    ],
                );
                all_names.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    printf!(
        "Added up to {} actors names\n",
        all_names.load(Ordering::SeqCst)
    );

    // Login - Affiliation should be 1:1, but it is sometimes 1:2 or 1:3
    // There are some ambigous affiliations in github_users.json
    // For such cases we're picking up the one with top source priority
    // If there are still multiple such we're taking one with most entries
    // And then if more than 1 with the same number of entries, then pick up first
    let (unique, non_unique, all_affs, non_unique_prio) = (
        AtomicI64::new(0),
        AtomicI64::new(0),
        AtomicI64::new(0),
        AtomicI64::new(0),
    );
    let default_start_date = Utc.with_ymd_and_hms(1900, 1, 1, 0, 0, 0).unwrap();
    let default_end_date = Utc.with_ymd_and_hms(2100, 1, 1, 0, 0, 0).unwrap();
    let companies_affs: Mutex<(StringSet, Vec<AffData>)> =
        Mutex::new((BTreeSet::new(), Vec::new()));
    let aff_items: Vec<(&String, &MapIntSet)> = login_affs.iter().collect();
    run_pool(thr_n, &aff_items, |(login, prios)| {
        let a_prios: Vec<i64> = prios.keys().copied().collect();
        if a_prios.len() > 1 {
            non_unique_prio.fetch_add(1, Ordering::SeqCst);
        }
        let max_prio = *a_prios.last().unwrap();
        let source = prio_to_source.get(&max_prio).copied().unwrap_or("");
        let affs = &prios[&max_prio];
        let affs_ary: Vec<String> = if affs.len() > 1 {
            // This login has different affiliations definitions in the input JSON
            // Look for an affiliation that list most companies
            let max_num = affs
                .iter()
                .map(|aff| aff.split(',').count())
                .max()
                .unwrap_or(1)
                .max(1);
            // maxNum holds max number of companies listed in any of affiliations
            // Just pick first affiliation definition that lists most companies
            let picked = affs
                .iter()
                .map(|aff| aff.split(',').map(str::to_string).collect::<Vec<_>>())
                .find(|ary| ary.len() == max_num)
                .unwrap_or_default();
            // Count this as non-unique
            non_unique.fetch_add(1, Ordering::SeqCst);
            picked
        } else {
            // This is a good definition, only one list of companies affiliation for this GitHub user login
            unique.fetch_add(1, Ordering::SeqCst);
            first_key(affs).split(',').map(str::to_string).collect()
        };
        // Affiliation has a form "com1 < dt1, com2 < dt2, ..., com(N-1) < dt(N-1), comN"
        // We have array of companies affiliation with eventual end date: array item is:
        // "company name" or "company name < date", lets iterate and parse it
        let mut prev_date = default_start_date;
        for aff in &affs_ary {
            let aff = aff.trim();
            let mut ary = aff.split('<');
            let company = ary.next().unwrap_or("").trim().to_string();
            let dt_from = prev_date;
            let dt_to = match ary.next() {
                // "company < date" form
                Some(dt) => gotime::time_parse_any(dt.trim()),
                // "company" form
                None => default_end_date,
            };
            if company.is_empty() {
                continue;
            }
            let mut ca = companies_affs.lock().unwrap();
            ca.0.insert(company.clone());
            ca.1.push(AffData {
                login: login.to_string(),
                company,
                source: source.to_string(),
                from: dt_from,
                to: dt_to,
            });
            prev_date = dt_to;
            all_affs.fetch_add(1, Ordering::SeqCst);
        }
    });
    printf!(
        "Affiliations unique: {}, non-unique: {}, with multiple priorities: {}, all user-company connections: {}\n",
        unique.load(Ordering::SeqCst),
        non_unique.load(Ordering::SeqCst),
        non_unique_prio.load(Ordering::SeqCst),
        all_affs.load(Ordering::SeqCst)
    );
    let (companies, aff_list) = companies_affs.into_inner().unwrap();

    // Add companies
    let acq = Mutex::new(acq);
    let company_items: Vec<&String> = companies.iter().filter(|c| !c.is_empty()).collect();
    run_pool(thr_n, &company_items, |company| {
        let mut company = company.to_string();
        if company.len() > 63 {
            company = shorten_company(&company);
        }
        exec_sql_with_err(
            con,
            ctx,
            &insert_ignore(&format!("into gha_companies(name) {}", n_values(1))),
            &[SqlArg::from(maybe_hide(&trunc_to_bytes(&company, 160)))],
        );
        let mapped_company = acq.lock().unwrap().map_company_name(&company);
        if mapped_company != company {
            exec_sql_with_err(
                con,
                ctx,
                &insert_ignore(&format!("into gha_companies(name) {}", n_values(1))),
                &[SqlArg::from(maybe_hide(&trunc_to_bytes(
                    &mapped_company,
                    160,
                )))],
            );
        }
    });
    printf!("Processed {} companies\n", companies.len());

    // Add affiliations
    added.store(0, Ordering::SeqCst);
    let (non_cached, added_affs) = (AtomicI64::new(0), AtomicI64::new(0));
    run_pool(thr_n, &aff_list, |aff| {
        let login = &aff.login;
        // Check if we have that actor IDs cached
        let cached = {
            let c = caches.lock().unwrap();
            match (c.1.get(login), c.0.get(login)) {
                (Some(l), Some(i)) => Some((i.clone(), l.clone())),
                _ => None,
            }
        };
        let act_ids = match cached {
            Some((ids, _)) => ids,
            None => {
                let (mut act_ids, act_logins) = find_actors(con, ctx, login, maybe_hide);
                if act_ids.is_empty() {
                    // Should not happen
                    let a_id = add_actor(con, ctx, login, "", &login_csd(login), maybe_hide);
                    act_ids.push(a_id);
                    added.fetch_add(1, Ordering::SeqCst);
                }
                cache(&act_ids, &act_logins);
                non_cached.fetch_add(1, Ordering::SeqCst);
                act_ids
            }
        };
        let company = &aff.company;
        if company.is_empty() {
            return;
        }
        let mapped_company = acq.lock().unwrap().map_company_name(company);
        let source = trunc_to_bytes(&aff.source, 30);
        for aid in &act_ids {
            exec_sql_with_err(
                con,
                ctx,
                &insert_ignore(&format!(
                    "into gha_actors_affiliations(actor_id, company_name, original_company_name, dt_from, dt_to, source) {}",
                    n_values(6)
                )),
                &[
                    SqlArg::from(*aid),
                    SqlArg::from(maybe_hide(&trunc_to_bytes(&mapped_company, 160))),
                    SqlArg::from(maybe_hide(&trunc_to_bytes(company, 160))),
                    SqlArg::from(aff.from),
                    SqlArg::from(aff.to),
                    SqlArg::from(source.as_str()),
                ],
            );
            added_affs.fetch_add(1, Ordering::SeqCst);
        }
    });
    if added.load(Ordering::SeqCst) > 0 {
        printf!(
            "Unexpected: added {} actors while processing affiliations\n",
            added.load(Ordering::SeqCst)
        );
    }
    if non_cached.load(Ordering::SeqCst) > 0 {
        printf!(
            "Unexpected: {} cache misses\n",
            non_cached.load(Ordering::SeqCst)
        );
    }
    printf!(
        "Affiliations added up to: {}\n",
        added_affs.load(Ordering::SeqCst)
    );
    let acq = acq.into_inner().unwrap();
    for (company, data) in &acq.stat {
        if company == "---" {
            printf!(
                "Non-acquired companies: checked all regexp: {}, cache hit: {}\n",
                data[0],
                data[1]
            );
        } else {
            printf!(
                "Mapped to '{}': checked regexp: {}, cache hit: {}\n",
                company,
                data[0],
                data[1]
            );
        }
    }
    for (company, (mapped, kind)) in &acq.com_map {
        if *kind == "u" {
            continue;
        }
        printf!("Used mapping '{}' --> '{}'\n", company, mapped);
    }

    // If check imported flag is set, then mark imported file
    if ctx.check_imported_sha {
        set_imported_sha(con, ctx, &current_sha);
        if !ctx.skip_company_acq {
            set_imported_sha(con, ctx, &current_sha2);
        }
    }
    0
}

fn main() {
    devstatscode::error::exit_on_panic();
    let dt_start = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let ret = if args.len() < 2 {
        import_affs("")
    } else {
        import_affs(&args[1])
    };
    printf!("Time: {}\n", gotime::format_go_duration(dt_start.elapsed()));
    std::process::exit(ret);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn email_decode_like_go() {
        assert_eq!(email_decode("lgryglicki!o2.pl"), "lgryglicki@o2.pl");
        assert_eq!(email_decode("a@b.cd"), "a@b.cd");
        assert_eq!(email_decode("a!b!c"), "a@b!c");
        assert_eq!(email_decode("!b"), "!b");
        assert_eq!(email_decode("a! b"), "a! b");
        assert_eq!(email_decode(""), "");
    }

    #[test]
    fn score_csd_like_go() {
        let empty = CsData::default();
        assert_eq!(score_csd(&empty), 0.0);
        let full = CsData {
            country_id: Some("pl".into()),
            sex: Some("m".into()),
            tz: Some("Europe/Warsaw".into()),
            sex_prob: Some(0.75),
            tz_offset: Some(60),
            age: Some(30),
        };
        assert_eq!(score_csd(&full), 2.0 + 1.0 + 1.0 + 1.0 + 0.75 + 0.5);
        let weird = CsData {
            country_id: Some(String::new()),
            sex: Some("x".into()),
            tz: Some(String::new()),
            sex_prob: Some(0.0),
            tz_offset: None,
            age: None,
        };
        assert_eq!(score_csd(&weird), 0.0);
        assert!(full.same_as(&full.clone()));
        let mut other = full.clone();
        other.sex_prob = Some(0.75 + 1e-12);
        assert!(full.same_as(&other));
        other.sex_prob = Some(0.76);
        assert!(!full.same_as(&other));
        other.sex_prob = None;
        assert!(!full.same_as(&other));
    }

    #[test]
    fn map_company_name_stats() {
        let mut st = AcqState::default();
        st.acq_map.push((
            goregex::compile("(?i)^kismatic$").unwrap(),
            "Apprenda Inc.".into(),
        ));
        assert_eq!(st.map_company_name("Kismatic"), "Apprenda Inc.");
        assert_eq!(st.map_company_name("Kismatic"), "Apprenda Inc.");
        assert_eq!(st.map_company_name("ACME"), "ACME");
        assert_eq!(st.map_company_name("ACME"), "ACME");
        assert_eq!(st.map_company_name("ACME"), "ACME");
        assert_eq!(st.stat["Apprenda Inc."], [1, 1]);
        assert_eq!(st.stat["---"], [1, 2]);
        assert_eq!(st.com_map["Kismatic"], ("Apprenda Inc.".to_string(), "m"));
        assert_eq!(st.com_map["ACME"], ("ACME".to_string(), "u"));
    }

    #[test]
    fn json_decoding_like_jsoniter() {
        let users: Option<Vec<GitHubUser>> = serde_json::from_str(
            r#"[{"Login":"A","EMAIL":"e","affiliation":"x","source":"user","name":"n","country_id":"pl","sex":"m","tz":"UTC","sex_prob":1,"age":30,"commits":5,"location":{"a":[1,2]}},
                null,
                {"login":null,"country_id":null,"age":null,"sex_prob":null,"login":"dup"}]"#,
        )
        .unwrap();
        let users = users.unwrap();
        assert_eq!(users.len(), 3);
        assert_eq!(users[0].login, "A");
        assert_eq!(users[0].email, "e");
        assert_eq!(users[0].sex_prob, Some(1.0));
        assert_eq!(users[0].age, Some(30));
        assert_eq!(users[0].country_id.as_deref(), Some("pl"));
        assert_eq!(users[1], GitHubUser::default());
        assert_eq!(users[2].login, "dup");
        assert_eq!(users[2].country_id, None);
        assert_eq!(users[2].age, None);
        let none: Option<Vec<GitHubUser>> = serde_json::from_str("null").unwrap();
        assert!(none.is_none());
        for bad in [
            r#"[{"age":1.5}]"#,
            r#"[{"age":"1"}]"#,
            r#"[{"login":1}]"#,
            r#"[{"sex_prob":"0.5"}]"#,
            r#"[1]"#,
            r#"{}"#,
            r#"[] x"#,
            "",
        ] {
            assert!(
                serde_json::from_str::<Option<Vec<GitHubUser>>>(bad).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn acquisitions_yaml_and_go_string() {
        let a: AllAcquisitions = yde::unmarshal(
            b"---\nacquisitions:\n  - ['(?i)^kismatic$', 'Apprenda Inc.']\n  - [x, y]\n",
        )
        .unwrap();
        assert_eq!(a.acquisitions.len(), 2);
        assert_eq!(
            a.go_string(),
            "{Acquisitions:[[(?i)^kismatic$ Apprenda Inc.] [x y]]}"
        );
        let empty: AllAcquisitions = yde::unmarshal(b"---\n").unwrap();
        assert_eq!(empty.go_string(), "{Acquisitions:[]}");
        assert!(yde::unmarshal::<AllAcquisitions>(b"acquisitions:\n  - [a, b, c]\n").is_err());
    }

    #[test]
    fn shorten_company_bytes() {
        let long = "A".repeat(70);
        assert_eq!(shorten_company(&long), "A".repeat(63));
        // 'ą' is 2 bytes: 0xC4 0x85 - byte 32 splits one, the partial bytes vanish
        let s = format!("{}ą{}", "x".repeat(31), "y".repeat(40));
        assert_eq!(s.len(), 73);
        assert_eq!(
            shorten_company(&s),
            format!("{}{}", "x".repeat(31), "y".repeat(31))
        );
        assert_eq!(go_set_string(["b", "a"].iter()), "map[b:{} a:{}]");
        assert_eq!(go_set_string([1, 2].iter()), "map[1:{} 2:{}]");
    }
}
