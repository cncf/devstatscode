//! `projects.yaml` — the list of DevStats projects (Go `AllProjects` /
//! `Project` from `gha.go`, `GetProjectsList` and `IsProjectDisabled`).

use std::collections::BTreeMap;

use chrono::{DateTime, FixedOffset};
use serde::Deserialize;

use crate::yamlv2::de as yde;
use crate::Ctx;

/// Go `AllProjects`: all projects data.
#[derive(Debug, Default, Clone, Deserialize, PartialEq)]
#[serde(default)]
pub struct AllProjects {
    #[serde(deserialize_with = "yde::str_key_map")]
    pub projects: BTreeMap<String, Project>,
}

/// Go `Project`: mapping from a project name to its command line used to sync
/// it (yaml.v2 decoding rules; `*time.Time` / `*float64` fields are `Option`s).
#[derive(Debug, Default, Clone, Deserialize, PartialEq)]
#[serde(default)]
pub struct Project {
    #[serde(rename = "command_line", deserialize_with = "yde::str_seq")]
    pub command_line: Vec<String>,
    #[serde(rename = "start_date", deserialize_with = "yde::opt_time")]
    pub start_date: Option<DateTime<FixedOffset>>,
    #[serde(rename = "psql_db", deserialize_with = "yde::string")]
    pub pdb: String,
    #[serde(deserialize_with = "yde::boolean")]
    pub disabled: bool,
    #[serde(rename = "main_repo", deserialize_with = "yde::string")]
    pub main_repo: String,
    #[serde(rename = "annotation_regexp", deserialize_with = "yde::string")]
    pub annotation_regexp: String,
    #[serde(deserialize_with = "yde::int")]
    pub order: i64,
    #[serde(rename = "join_date", deserialize_with = "yde::opt_time")]
    pub join_date: Option<DateTime<FixedOffset>>,
    #[serde(rename = "files_skip_pattern", deserialize_with = "yde::string")]
    pub files_skip_pattern: String,
    #[serde(deserialize_with = "yde::str_map")]
    pub env: BTreeMap<String, String>,
    #[serde(rename = "name", deserialize_with = "yde::string")]
    pub full_name: String,
    #[serde(deserialize_with = "yde::string")]
    pub status: String,
    #[serde(rename = "shared_db", deserialize_with = "yde::string")]
    pub shared_db: String,
    #[serde(rename = "incubating_date", deserialize_with = "yde::opt_time")]
    pub incubating_date: Option<DateTime<FixedOffset>>,
    #[serde(rename = "graduated_date", deserialize_with = "yde::opt_time")]
    pub graduated_date: Option<DateTime<FixedOffset>>,
    #[serde(rename = "archived_date", deserialize_with = "yde::opt_time")]
    pub archived_date: Option<DateTime<FixedOffset>>,
    // sic: the yaml key is misspelled in projects.yaml and in the Go struct tag
    #[serde(rename = "sync_probabilty", deserialize_with = "yde::opt_float")]
    pub sync_probability: Option<f64>,
    #[serde(rename = "project_scale", deserialize_with = "yde::opt_float")]
    pub project_scale: Option<f64>,
}

/// Go `IsProjectDisabled`: the yaml `disabled` flag unless
/// `GHA2DB_PROJECTS_OVERRIDE` (`+pro1,-pro2`) names the project — then `+`
/// (override `true`) means enabled and `-` means disabled.
pub fn is_project_disabled(ctx: &Ctx, proj: &str, yaml_disabled: bool) -> bool {
    match ctx.projects_override.get(proj) {
        None => yaml_disabled,
        Some(override_) => !*override_,
    }
}

/// Go `ExcludedForProject`: is a metric with the `project: <metric_projects>`
/// setting excluded for `current_project`? `metric_projects` is a comma
/// separated list of the projects the metric is for, or — prefixed with `!` —
/// of the projects it is not for. Empty values never exclude.
pub fn excluded_for_project(current_project: &str, metric_projects: &str) -> bool {
    if metric_projects.is_empty() || current_project.is_empty() {
        return false;
    }
    let (exclude_mode, list) = match metric_projects.strip_prefix('!') {
        Some(rest) => (true, rest),
        None => (false, metric_projects),
    };
    for metric_project in list.split(',') {
        if exclude_mode {
            if current_project == metric_project {
                return true;
            }
            continue;
        }
        if current_project == metric_project {
            return false;
        }
    }
    !exclude_mode
}

/// Go `GetProjectsList`: the enabled projects (see [`is_project_disabled`])
/// sorted by `order` (then by name) and filtered by `ONLY="proj1 proj2 …"`;
/// returns the names and the matching project entries.
///
/// Projects sharing the same `order` value are all kept (with a warning);
/// the original Go code kept only one of them — a random one — and listed it
/// twice (bug 19).
pub fn get_projects_list(ctx: &Ctx, projects: &AllProjects) -> (Vec<String>, Vec<Project>) {
    let mut ordered: Vec<(i64, &str)> = projects
        .projects
        .iter()
        .filter(|(name, proj)| !is_project_disabled(ctx, name, proj.disabled))
        .map(|(name, proj)| (proj.order, name.as_str()))
        .collect();
    ordered.sort_unstable();
    for pair in ordered.windows(2) {
        if pair[0].0 == pair[1].0 {
            crate::printf!(
                "Warning: projects '{}' and '{}' have the same order {}\n",
                pair[0].1,
                pair[1].1,
                pair[1].0
            );
        }
    }

    let only_s = std::env::var("ONLY").unwrap_or_default();
    let only: Option<std::collections::BTreeSet<&str>> = if only_s.is_empty() {
        None
    } else {
        Some(only_s.split(' ').filter(|s| !s.is_empty()).collect())
    };

    let mut names = Vec::new();
    let mut projs = Vec::new();
    for (_, name) in ordered {
        if let Some(only) = &only {
            if !only.contains(name) {
                continue;
            }
        }
        names.push(name.to_string());
        projs.push(projects.projects[name].clone());
    }
    (names, projs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn excluded_for_project_follows_go() {
        // the Go TestExcludedForProject table: (current project, metric project, expected)
        let cases = [
            ("", "", false),
            ("X", "", false),
            ("", "X", false),
            ("X", "X", false),
            ("X", "!X", true),
            ("Y", "X", true),
            ("Y", "!X", false),
            ("", "!X", false),
            ("", "!", false),
            // more: lists
            ("X", "A,X,B", false),
            ("Y", "A,X,B", true),
            ("X", "!A,X,B", true),
            ("Y", "!A,X,B", false),
            ("X", "!", false),
            ("X", "!,X", true),
        ];
        for (i, (cur, metric, expected)) in cases.iter().enumerate() {
            assert_eq!(
                excluded_for_project(cur, metric),
                *expected,
                "test number {}: {cur:?} {metric:?}",
                i + 1
            );
        }
    }

    #[test]
    fn is_project_disabled_follows_go() {
        // (override of pro1, yaml disabled, expected) — the Go TestIsProjectDisabled table
        let cases: [(Option<bool>, bool, bool); 6] = [
            (None, false, false),
            (None, true, true),
            (Some(true), true, false),
            (Some(false), true, true),
            (Some(true), false, false),
            (Some(false), false, true),
        ];
        for (i, (override_, yaml_disabled, expected)) in cases.into_iter().enumerate() {
            let ctx = Ctx {
                projects_override: override_
                    .into_iter()
                    .map(|v| ("pro1".to_string(), v))
                    .collect(),
                ..Ctx::default()
            };
            assert_eq!(
                is_project_disabled(&ctx, "pro1", yaml_disabled),
                expected,
                "test number {i}"
            );
        }
    }

    const YAML: &[u8] = br#"---
projects:
  kubernetes:
    order: 1
    name: Kubernetes
    status: Graduated
    command_line:
      - "kubernetes,kubernetes-client,\
         kubernetes-sigs"
    start_date: 2014-06-01T00:00:00Z
    join_date: 2016-03-10T00:00:00Z
    incubating_date: 2016-03-11T00:00:00Z
    graduated_date: 2018-03-06T00:00:00Z
    psql_db: gha
    shared_db: allprj
    main_repo: kubernetes/kubernetes
    annotation_regexp: '^v((0\.\d+)|(\d+\.\d+\.0))$'
    files_skip_pattern: '(^|/)_?(vendor|Godeps|_workspace)/'
    sync_probabilty: 0.99
    project_scale: 3.0
    env:
      GHA2DB_EXCLUDE_REPOS: "kubernetes/api,kubernetes/apimachinery"
      GHA2DB_NCPUS: 4
  opentracing:
    order: 3
    name: OpenTracing
    status: Archived
    command_line:
      - opentracing
    start_date: 2015-11-26
    archived_date: 2022-01-31T00:00:00+02:00
    psql_db: opentracing
    disabled: yes
  prometheus:
    order: 2
    name: Prometheus
    command_line:
      - prometheus
    psql_db: prometheus
  all:
    order: 100
    name: All CNCF
    psql_db: allprj
    project_scale: 5
"#;

    #[test]
    fn decodes_projects_yaml_like_yaml_v2() {
        let all: AllProjects = yde::unmarshal(YAML).unwrap();
        assert_eq!(all.projects.len(), 4);
        let k = &all.projects["kubernetes"];
        assert_eq!(k.order, 1);
        assert_eq!(k.full_name, "Kubernetes");
        assert_eq!(k.status, "Graduated");
        assert_eq!(
            k.command_line,
            ["kubernetes,kubernetes-client,kubernetes-sigs"]
        );
        assert_eq!(
            k.start_date.map(yde::to_utc),
            Some(Utc.with_ymd_and_hms(2014, 6, 1, 0, 0, 0).unwrap())
        );
        assert_eq!(
            k.graduated_date.map(yde::to_utc),
            Some(Utc.with_ymd_and_hms(2018, 3, 6, 0, 0, 0).unwrap())
        );
        assert_eq!(k.archived_date, None);
        assert_eq!(k.pdb, "gha");
        assert_eq!(k.shared_db, "allprj");
        assert_eq!(k.main_repo, "kubernetes/kubernetes");
        assert_eq!(k.annotation_regexp, r"^v((0\.\d+)|(\d+\.\d+\.0))$");
        assert_eq!(k.files_skip_pattern, r"(^|/)_?(vendor|Godeps|_workspace)/");
        assert_eq!(k.sync_probability, Some(0.99));
        assert_eq!(k.project_scale, Some(3.0));
        assert_eq!(
            k.env["GHA2DB_EXCLUDE_REPOS"],
            "kubernetes/api,kubernetes/apimachinery"
        );
        assert_eq!(k.env["GHA2DB_NCPUS"], "4");
        assert!(!k.disabled);
        let o = &all.projects["opentracing"];
        assert!(o.disabled);
        assert_eq!(
            o.start_date.map(yde::to_utc),
            Some(Utc.with_ymd_and_hms(2015, 11, 26, 0, 0, 0).unwrap())
        );
        let archived = o.archived_date.unwrap();
        assert_eq!(archived.offset().local_minus_utc(), 7200);
        assert_eq!(o.sync_probability, None);
        let p = &all.projects["prometheus"];
        assert!(p.env.is_empty());
        assert_eq!(p.status, "");
        assert_eq!(all.projects["all"].project_scale, Some(5.0));
        assert_eq!(all.projects["all"].command_line, Vec::<String>::new());
        let empty: AllProjects = yde::unmarshal(b"---\n").unwrap();
        assert!(empty.projects.is_empty());
        let none: AllProjects = yde::unmarshal(b"projects:\n").unwrap();
        assert!(none.projects.is_empty());
    }

    #[test]
    fn projects_list_is_ordered_filtered_and_overridable() {
        let _lock = crate::context::test_support::env_lock();
        let all: AllProjects = yde::unmarshal(YAML).unwrap();
        std::env::remove_var("ONLY");
        let ctx = Ctx::default();
        let (names, projs) = get_projects_list(&ctx, &all);
        assert_eq!(names, ["kubernetes", "prometheus", "all"]);
        assert_eq!(
            projs.iter().map(|p| p.pdb.as_str()).collect::<Vec<_>>(),
            ["gha", "prometheus", "allprj"]
        );
        // overrides: enable the disabled one, disable an enabled one
        let mut ctx2 = Ctx::default();
        ctx2.projects_override
            .insert("opentracing".to_string(), true);
        ctx2.projects_override
            .insert("prometheus".to_string(), false);
        let (names, _) = get_projects_list(&ctx2, &all);
        assert_eq!(names, ["kubernetes", "opentracing", "all"]);
        // ONLY: subset, extra spaces and unknown names ignored
        std::env::set_var("ONLY", " all  kubernetes nosuch ");
        let (names, _) = get_projects_list(&ctx, &all);
        assert_eq!(names, ["kubernetes", "all"]);
        std::env::set_var("ONLY", "opentracing");
        let (names, _) = get_projects_list(&ctx, &all);
        assert!(names.is_empty());
        std::env::remove_var("ONLY");
    }

    #[test]
    fn projects_sharing_an_order_are_all_kept() {
        let _lock = crate::context::test_support::env_lock();
        std::env::remove_var("ONLY");
        let yaml = b"projects:\n  zeta:\n    order: 2\n    psql_db: z\n  beta:\n    order: 2\n    psql_db: b\n  alpha:\n    order: 1\n    psql_db: a\n  gamma:\n    order: 3\n    psql_db: g\n";
        let all: AllProjects = yde::unmarshal(yaml).unwrap();
        let (names, projs) = get_projects_list(&Ctx::default(), &all);
        assert_eq!(names, ["alpha", "beta", "zeta", "gamma"]);
        assert_eq!(
            projs.iter().map(|p| p.pdb.as_str()).collect::<Vec<_>>(),
            ["a", "b", "z", "g"]
        );
    }
}
