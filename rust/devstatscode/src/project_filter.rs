//! The ingestion rules of a project: what its own gha2db accepts (Go
//! `project_filter.go`). Used by the tools that re-check events against the
//! project's org/repo/actor rules (reconcile_dbs, the ghapi2db repository
//! events feed pass).

use std::collections::BTreeSet;

use crate::context::GoRegex;
use crate::error::{fatal_on_err, fatal_on_error};
use crate::gha::{actor_hit, repo_hit};
use crate::map::{strings_map_to_array, strings_map_to_set};
use crate::projects::{is_project_disabled, AllProjects, Project};
use crate::yamlv2::de as yde;
use crate::{io, Ctx};

/// The ingestion rules of a project: what its own gha2db accepts (see
/// [`repo_hit`], [`actor_hit`]). An empty `name` means "no project": nothing
/// is filtered (Go `ProjectFilter`).
#[derive(Debug, Default)]
pub struct ProjectFilter {
    /// Project name, "" - no project: nothing is filtered.
    pub name: String,
    /// Why nothing is filtered (when `name` is "").
    pub detail: String,
    /// Historical mode: the project's `hist_command_line` rules were requested.
    pub hist: bool,
    /// Historical mode and the project defines `hist_command_line` (else
    /// `command_line` is used).
    pub hist_rules: bool,
    pub forg: BTreeSet<String>,
    pub frepo: BTreeSet<String>,
    pub org_re: Option<GoRegex>,
    pub repo_re: Option<GoRegex>,
    /// Context initialized with the project's `env` applied (exclude list,
    /// exact mode, actor filters).
    pub ctx: Option<Box<Ctx>>,
}

impl ProjectFilter {
    /// Go `ProjectFilter.Active`: is there a project whose rules are applied?
    pub fn active(&self) -> bool {
        !self.name.is_empty()
    }

    /// Go `ProjectFilter.Hit`: would the project's gha2db ingest an event of
    /// this repository by this actor?
    pub fn hit(&self, repo_name: &str, actor_login: &str) -> bool {
        self.repo_hit(repo_name) && self.actor_hit(actor_login)
    }

    /// Go `ProjectFilter.RepoHit`: does this repository (full name) pass the
    /// project's org/repo rules?
    pub fn repo_hit(&self, repo_name: &str) -> bool {
        let Some(ctx) = self.rules_ctx() else {
            return true;
        };
        repo_hit(
            ctx,
            repo_name,
            &self.forg,
            &self.frepo,
            self.org_re.as_ref(),
            self.repo_re.as_ref(),
        )
    }

    /// Go `ProjectFilter.ActorHit`: does this actor pass the project's actor
    /// rules (`GHA2DB_ACTORS_FILTER/ALLOW/FORBID`)?
    pub fn actor_hit(&self, actor_login: &str) -> bool {
        let Some(ctx) = self.rules_ctx() else {
            return true;
        };
        actor_hit(ctx, actor_login)
    }

    /// The project's context when there is a project (else nothing is filtered).
    fn rules_ctx(&self) -> Option<&Ctx> {
        if self.name.is_empty() {
            return None;
        }
        self.ctx.as_deref()
    }

    /// Go `ProjectFilter.Info`: human readable filter description.
    pub fn info(&self) -> String {
        let Some(ctx) = self.rules_ctx() else {
            return format!("none ({})", self.detail);
        };
        let mode = if self.hist_rules {
            " (historical rules)"
        } else if self.hist {
            " (historical rules: none, using command_line)"
        } else {
            ""
        };
        format!(
            "project '{}'{}: {}, {}, {} excluded repo(s), exact {}, actors filter {}",
            self.name,
            mode,
            names_info("org", &self.forg, self.org_re.as_ref()),
            names_info("repo", &self.frepo, self.repo_re.as_ref()),
            ctx.exclude_repos.len(),
            ctx.exact,
            ctx.actors_filter
        )
    }
}

/// Go `ProjectsPath`: path of projects.yaml: `GHA2DB_PROJECTS_YAML` in the
/// data directory (or ./ in local mode).
pub fn projects_path(ctx: &Ctx) -> String {
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };
    format!("{data_prefix}{}", ctx.projects_yaml)
}

/// Go `ReadProjects`: projects.yaml (`GHA2DB_PROJECTS_YAML`) from the data
/// directory (or ./ in local mode); returns the projects and the path read.
pub fn read_projects(ctx: &Ctx) -> (AllProjects, String) {
    let path = projects_path(ctx);
    // `ioutil.ReadFile` — no `/shared/` fallback.
    let data = fatal_on_err(io::read_file_raw(&path));
    let all: AllProjects = match yde::unmarshal(&data) {
        Ok(p) => p,
        Err(e) => fatal_on_error(e),
    };
    (all, path)
}

/// Go `ReadProjectsIfPresent`: like [`read_projects`], but a missing file is
/// not an error (`None` projects).
pub fn read_projects_if_present(ctx: &Ctx) -> (Option<AllProjects>, String) {
    let path = projects_path(ctx);
    if let Err(e) = std::fs::metadata(&path) {
        if e.kind() == std::io::ErrorKind::NotFound {
            return (None, path);
        }
        fatal_on_error(format!("stat {path}: {e}"));
    }
    let (all, path) = read_projects(ctx);
    (Some(all), path)
}

/// Go `ParseFilterArg`: one `command_line` item of a project the way gha2db
/// gets it from gha2db_sync (comma split, trimmed, re-joined) and parses it:
/// `regexp:` prefix - a regexp, else a set of names (empty - no restriction).
pub fn parse_filter_arg(arg: &str) -> (BTreeSet<String>, Option<GoRegex>) {
    let strip = |x: &str| x.trim().to_string();
    let joined =
        strings_map_to_array(strip, arg.split(',').map(str::to_string).collect()).join(",");
    if let Some(re) = joined.strip_prefix("regexp:") {
        return (BTreeSet::new(), Some(GoRegex::must(re)));
    }
    (
        strings_map_to_set(strip, joined.split(',').map(str::to_string).collect()),
        None,
    )
}

/// Go `ProjectForDB`: the project of the target database: `GHA2DB_PROJECT`
/// when it names an enabled project with that database, else the first
/// (order, name) enabled project with `psql_db` = target.
pub fn project_for_db<'a>(
    ctx: &Ctx,
    all: &'a AllProjects,
    target: &str,
) -> Option<(&'a str, &'a Project)> {
    if !ctx.project.is_empty() {
        if let Some((name, proj)) = all.projects.get_key_value(&ctx.project) {
            if !is_project_disabled(ctx, name, proj.disabled) && proj.pdb.trim() == target {
                return Some((name.as_str(), proj));
            }
        }
    }
    let mut best: Option<(&'a str, &'a Project)> = None;
    for (name, proj) in &all.projects {
        if is_project_disabled(ctx, name, proj.disabled) {
            continue;
        }
        if proj.pdb.trim() != target {
            continue;
        }
        let better = match best {
            None => true,
            Some((best_name, best_proj)) => {
                proj.order < best_proj.order
                    || (proj.order == best_proj.order && name.as_str() < best_name)
            }
        };
        if better {
            best = Some((name.as_str(), proj));
        }
    }
    best
}

/// Go `NewProjectFilter`: the ingestion rules of the target database's
/// project (none when `all` is `None` or no enabled project uses the
/// database); the project's `env` is applied like gha2db_sync does (unless
/// `ENV_SET`). `hist` - historical mode: use the project's
/// `hist_command_line` (the biggest scope it ever had) when defined, else its
/// `command_line` like the daily runs do.
pub fn new_project_filter(
    ctx: &Ctx,
    all: Option<&AllProjects>,
    target: &str,
    path: &str,
    hist: bool,
) -> ProjectFilter {
    let Some(all) = all else {
        return ProjectFilter {
            detail: format!("no {path}"),
            hist,
            ..ProjectFilter::default()
        };
    };
    let Some((name, proj)) = project_for_db(ctx, all, target) else {
        return ProjectFilter {
            detail: format!("no enabled project uses this database in {path}"),
            hist,
            ..ProjectFilter::default()
        };
    };
    if std::env::var("ENV_SET").unwrap_or_default().is_empty() {
        for (env_k, env_v) in &proj.env {
            setenv(env_k, env_v);
        }
    }
    let mut fctx = Ctx::default();
    fctx.init();
    let mut filter = ProjectFilter {
        name: name.to_string(),
        hist,
        ctx: Some(Box::new(fctx)),
        ..ProjectFilter::default()
    };
    let mut command_line = &proj.command_line;
    if hist && !proj.hist_command_line.is_empty() {
        command_line = &proj.hist_command_line;
        filter.hist_rules = true;
    }
    let org_arg = command_line.first().map(String::as_str).unwrap_or("");
    let repo_arg = command_line.get(1).map(String::as_str).unwrap_or("");
    (filter.forg, filter.org_re) = parse_filter_arg(org_arg);
    (filter.frepo, filter.repo_re) = parse_filter_arg(repo_arg);
    filter
}

/// Go `os.Setenv` failure conditions (`setenv: invalid argument`).
fn setenv(key: &str, value: &str) {
    if key.is_empty() || key.contains('=') || key.contains('\0') || value.contains('\0') {
        fatal_on_error("setenv: invalid argument");
    }
    crate::env::set_var(key, value);
}

/// Go `namesInfo`: "any org" / "3 org(s)" / "org regexp '...'".
fn names_info(kind: &str, names: &BTreeSet<String>, re: Option<&GoRegex>) -> String {
    if let Some(re) = re {
        return format!("{kind} regexp '{}'", re.as_str());
    }
    if names.is_empty() {
        return format!("any {kind}");
    }
    format!("{} {kind}(s)", names.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::test_support::{env_lock, set_or_unset};

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    fn test_projects() -> AllProjects {
        let yaml = r#"
projects:
  zeta: {psql_db: zeta, shared_db: allprj, order: 5}
  alpha: {psql_db: alpha, shared_db: allprj, order: 5}
  beta: {psql_db: beta, shared_db: allprj, order: 1}
  gamma: {psql_db: gamma, shared_db: allprj, order: 3, disabled: true}
  delta: {psql_db: beta, shared_db: allprj, order: 0}
  all: {psql_db: allprj, order: 2}
  kube: {psql_db: gha, order: 0}
  other: {psql_db: other, shared_db: otherprj, order: 9}
  selfref: {psql_db: allprj, shared_db: allprj, order: 9}
  noshared: {psql_db: solo, order: 9}
"#;
        yde::unmarshal(yaml.as_bytes()).expect("test projects.yaml")
    }

    /// The filter test projects (Go `filterTestProjects`).
    fn filter_projects() -> AllProjects {
        let yaml = r#"
projects:
  kcp:
    psql_db: kcp
    shared_db: allprj
    command_line: ['kcp-dev']
    hist_command_line: ['kcp-dev,kubestellar', 'regexp:^(?:kcp|edge-mc|kubestellar|kcp-dev/.*)$']
  spire: {psql_db: spire, shared_db: allprj, command_line: ['regexp:(?i)^spiffe\/spire.*$']}
  kube:
    psql_db: gha
    command_line: ['kubernetes,kubernetes-client, kubernetes-sigs']
    env: {GHA2DB_EXCLUDE_REPOS: 'kubernetes/api,kubernetes/apimachinery'}
  oci:
    psql_db: oci
    command_line: ['opencontainers', 'runc,image-spec']
    hist_command_line: ['opencontainers', 'runc,image-spec,runtime-spec,distribution-spec']
  velero:
    psql_db: velero
    command_line: ['vmware-tanzu', 'velero,velero-plugin-for-aws']
    hist_command_line: ["regexp:(?:^(?:heptio|vmware-tanzu)/(?:ark|velero.*)$)|\
       ^(?:vmware-tanzu/velero|vmware-tanzu/velero-plugin-for-aws)$"]
  linkerd:
    psql_db: linkerd
    command_line: ['linkerd']
    env: {GHA2DB_EXCLUDE_REPOS: 'linkerd/website'}
  all: {psql_db: allprj, command_line: ['kcp-dev,kubestellar,kubernetes']}
  off: {psql_db: off, disabled: true, command_line: ['x']}
  exact:
    psql_db: exact
    command_line: ['cncf/devstats,cncf/devstatscode']
    env: {GHA2DB_EXACT: '1'}
"#;
        yde::unmarshal(yaml.as_bytes()).expect("filter test projects.yaml")
    }

    /// Saves the environment variables the filter tests touch and restores them on drop.
    struct EnvGuard {
        saved: Vec<(&'static str, Option<String>)>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl EnvGuard {
        fn new() -> EnvGuard {
            let lock = env_lock();
            let names = ["ENV_SET", "GHA2DB_EXCLUDE_REPOS", "GHA2DB_EXACT"];
            let saved = names
                .iter()
                .map(|n| (*n, std::env::var(n).ok()))
                .collect::<Vec<_>>();
            for n in names {
                crate::env::remove_var(n);
            }
            EnvGuard { saved, _lock: lock }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (n, v) in &self.saved {
                set_or_unset(n, v.as_deref());
            }
        }
    }

    #[test]
    fn project_for_db_cases() {
        let mut ctx = Ctx::default();
        let all = test_projects();
        let (name, proj) = project_for_db(&ctx, &all, "beta").expect("delta");
        assert_eq!((name, proj.pdb.as_str()), ("delta", "beta"));
        ctx.project = "beta".to_string();
        assert_eq!(
            project_for_db(&ctx, &all, "beta").map(|(n, _)| n),
            Some("beta")
        );
        ctx.project = "alpha".to_string();
        assert_eq!(
            project_for_db(&ctx, &all, "beta").map(|(n, _)| n),
            Some("delta")
        );
        ctx.project = String::new();
        assert!(project_for_db(&ctx, &all, "gamma").is_none());
        ctx.projects_override = [("gamma".to_string(), true)].into_iter().collect();
        let (name, proj) = project_for_db(&ctx, &all, "gamma").expect("gamma");
        assert_eq!((name, proj.shared_db.as_str()), ("gamma", "allprj"));
        assert!(project_for_db(&ctx, &all, "nosuchdb").is_none());
        let (name, proj) = project_for_db(&ctx, &all, "allprj").expect("all");
        assert_eq!((name, proj.shared_db.as_str()), ("all", ""));
    }

    #[test]
    fn parse_filter_arg_cases() {
        let (names, re) = parse_filter_arg("");
        assert!(names.is_empty() && re.is_none(), "empty - no restriction");
        let (names, re) = parse_filter_arg(" kcp-dev ");
        assert_eq!(names.iter().cloned().collect::<Vec<_>>(), s(&["kcp-dev"]));
        assert!(re.is_none());
        let (names, re) = parse_filter_arg("a, b ,a,c");
        assert_eq!(
            names.iter().cloned().collect::<Vec<_>>(),
            s(&["a", "b", "c"])
        );
        assert!(re.is_none());
        let (names, re) = parse_filter_arg("regexp:(?i)^spiffe\\/spire.*$");
        let re = re.expect("regexp");
        assert!(names.is_empty());
        assert_eq!(re.as_str(), "(?i)^spiffe\\/spire.*$");
        assert!(re.is_match("spiffe/spire-api-sdk") && !re.is_match("kcp-dev/kcp"));
        // gha2db_sync splits by comma and trims before passing the argument on
        let (_, re) = parse_filter_arg("regexp:^(a|b)$ , ^c$");
        assert_eq!(re.expect("regexp").as_str(), "^(a|b)$,^c$");
    }

    #[test]
    fn names_info_cases() {
        assert_eq!(names_info("org", &BTreeSet::new(), None), "any org");
        let two: BTreeSet<String> = s(&["a", "b"]).into_iter().collect();
        assert_eq!(names_info("repo", &two, None), "2 repo(s)");
        let re = GoRegex::must("^a$");
        assert_eq!(names_info("org", &two, Some(&re)), "org regexp '^a$'");
        // ... through info()
        let ctx = Ctx::default();
        let all = filter_projects();
        let _guard = EnvGuard::new();
        let f = new_project_filter(&ctx, Some(&all), "spire", "p.yaml", false);
        assert!(f
            .info()
            .contains("org regexp '(?i)^spiffe\\/spire.*$', any repo"));
    }

    #[test]
    fn project_filter_cases() {
        let _guard = EnvGuard::new();
        let all = filter_projects();
        let mut ctx = Ctx::default();

        // No projects.yaml / no project: nothing is filtered
        let f = new_project_filter(&ctx, None, "kcp", "./projects.yaml", false);
        assert!(!f.active() && f.name.is_empty() && f.hit("other/repo", "bot"));
        assert!(f.repo_hit("") && f.actor_hit(""));
        assert_eq!(f.info(), "none (no ./projects.yaml)");
        let f = new_project_filter(&ctx, Some(&all), "off", "p.yaml", false);
        assert!(!f.active() && f.hit("x/y", "a"));
        assert_eq!(
            f.info(),
            "none (no enabled project uses this database in p.yaml)"
        );

        // Org list
        let f = new_project_filter(&ctx, Some(&all), "kcp", "p.yaml", false);
        assert!(f.active());
        assert_eq!(f.name, "kcp");
        assert_eq!(
            f.info(),
            "project 'kcp': 1 org(s), any repo, 0 excluded repo(s), exact false, actors filter false"
        );
        assert!(f.hit("kcp-dev/kcp", "alice") && f.hit("kcp-dev/edge-mc", "alice"));
        assert!(
            !f.hit("kubestellar/kubestellar", "alice")
                && !f.hit("", "alice")
                && !f.hit("kcp", "alice")
        );
        assert!(f.repo_hit("kcp-dev/kcp") && !f.repo_hit("kubestellar/kubestellar"));

        // Regexp on the full name
        let f = new_project_filter(&ctx, Some(&all), "spire", "p.yaml", false);
        assert_eq!(
            f.info(),
            "project 'spire': org regexp '(?i)^spiffe\\/spire.*$', any repo, 0 excluded repo(s), exact false, actors filter false"
        );
        assert!(f.hit("spiffe/spire", "a") && f.hit("SPIFFE/spire-tutorials", "a"));
        assert!(!f.hit("spiffe/go-spiffe", "a"));

        // Org and repo lists
        let f = new_project_filter(&ctx, Some(&all), "oci", "p.yaml", false);
        assert_eq!(
            f.info(),
            "project 'oci': 1 org(s), 2 repo(s), 0 excluded repo(s), exact false, actors filter false"
        );
        assert!(f.hit("opencontainers/runc", "a"));
        assert!(!f.hit("opencontainers/distribution-spec", "a") && !f.hit("other/runc", "a"));

        // The project's env (exclude list) is applied like gha2db_sync does
        let f = new_project_filter(&ctx, Some(&all), "gha", "p.yaml", false);
        assert_eq!(
            f.info(),
            "project 'kube': 3 org(s), any repo, 2 excluded repo(s), exact false, actors filter false"
        );
        assert!(f.hit("kubernetes/kubernetes", "a") && f.hit("kubernetes-sigs/kind", "a"));
        assert!(!f.hit("kubernetes/api", "a") && !f.hit("kubernetes/apimachinery", "a"));
        assert_eq!(
            std::env::var("GHA2DB_EXCLUDE_REPOS").unwrap_or_default(),
            "kubernetes/api,kubernetes/apimachinery"
        );
        // ... unless ENV_SET says the environment is already prepared
        crate::env::remove_var("GHA2DB_EXCLUDE_REPOS");
        crate::env::set_var("ENV_SET", "1");
        let f = new_project_filter(&ctx, Some(&all), "gha", "p.yaml", false);
        assert!(f.ctx.as_ref().expect("ctx").exclude_repos.is_empty());
        assert!(f.hit("kubernetes/api", "a"));
        crate::env::remove_var("ENV_SET");

        // Exact mode (the project's env): full repository names, no org matching
        let f = new_project_filter(&ctx, Some(&all), "exact", "p.yaml", false);
        assert_eq!(
            f.info(),
            "project 'exact': 2 org(s), any repo, 0 excluded repo(s), exact true, actors filter false"
        );
        assert!(f.hit("cncf/devstats", "a") && !f.hit("cncf/other", "a") && !f.hit("cncf", "a"));
        crate::env::remove_var("GHA2DB_EXACT");

        // GHA2DB_PROJECT selects the project of a shared database
        ctx.project = "all".to_string();
        let f = new_project_filter(&ctx, Some(&all), "allprj", "p.yaml", false);
        assert_eq!(f.name, "all");
        assert!(f.hit("kubestellar/kubestellar", "a") && !f.hit("cncf/devstats", "a"));
        // ... and when GHA2DB_PROJECT names a project of another database, the
        // shared database's own project still decides (the shared side is never
        // filtered by a child's rules)
        ctx.project = "kcp".to_string();
        let f = new_project_filter(&ctx, Some(&all), "allprj", "p.yaml", false);
        assert_eq!(f.name, "all");
        assert!(f.hit("kubernetes/kubernetes", "a") && !f.hit("cncf/devstats", "a"));
        ctx.project = "all".to_string();
        let mut f = new_project_filter(&ctx, Some(&all), "allprj", "p.yaml", false);
        // Actor rules
        {
            let fctx = f.ctx.as_mut().expect("ctx");
            fctx.actors_filter = true;
            fctx.actors_forbid = Some(GoRegex::must("(?i)bot$"));
        }
        assert!(f.hit("kubernetes/kubernetes", "alice"));
        assert!(!f.hit("kubernetes/kubernetes", "k8s-ci-robot-bot"));
        assert!(f.repo_hit("kubernetes/kubernetes") && !f.actor_hit("k8s-ci-robot-bot"));
        assert_eq!(
            f.info(),
            "project 'all': 3 org(s), any repo, 0 excluded repo(s), exact false, actors filter true"
        );
    }

    #[test]
    fn project_filter_hist_cases() {
        let _guard = EnvGuard::new();
        let all = filter_projects();
        let ctx = Ctx::default();

        // Daily mode ignores hist_command_line
        let f = new_project_filter(&ctx, Some(&all), "kcp", "p.yaml", false);
        assert!(!f.hist && !f.hist_rules);
        assert!(!f.hit("kubestellar/kubestellar", "a"));

        // Historical mode: the biggest scope the project ever had
        let f = new_project_filter(&ctx, Some(&all), "kcp", "p.yaml", true);
        assert!(f.active() && f.hist && f.hist_rules);
        assert_eq!(
            f.info(),
            "project 'kcp' (historical rules): 2 org(s), repo regexp '^(?:kcp|edge-mc|kubestellar|kcp-dev/.*)$', 0 excluded repo(s), exact false, actors filter false"
        );
        assert!(
            f.hit("kubestellar/kubestellar", "a")
                && f.hit("kcp-dev/kcp", "a")
                && f.hit("kcp-dev/edge-mc", "a")
        );
        assert!(!f.hit("kcp-dev/old", "a") && !f.hit("other/repo", "a"));

        // Folded double-quoted regexp (kubernetes style line continuation)
        let f = new_project_filter(&ctx, Some(&all), "velero", "p.yaml", true);
        assert!(f.hist_rules);
        assert_eq!(
            f.org_re.as_ref().expect("org regexp").as_str(),
            "(?:^(?:heptio|vmware-tanzu)/(?:ark|velero.*)$)|^(?:vmware-tanzu/velero|vmware-tanzu/velero-plugin-for-aws)$"
        );
        assert!(f.hit("heptio/ark", "a") && f.hit("vmware-tanzu/velero-plugin-for-gcp", "a"));
        assert!(!f.hit("vmware-tanzu/other", "a"));
        let daily = new_project_filter(&ctx, Some(&all), "velero", "p.yaml", false);
        assert!(
            !daily.hit("heptio/ark", "a") && !daily.hit("vmware-tanzu/velero-plugin-for-gcp", "a")
        );

        // Two element form
        let f = new_project_filter(&ctx, Some(&all), "oci", "p.yaml", true);
        assert_eq!(
            f.info(),
            "project 'oci' (historical rules): 1 org(s), 4 repo(s), 0 excluded repo(s), exact false, actors filter false"
        );
        assert!(
            f.hit("opencontainers/distribution-spec", "a") && !f.hit("opencontainers/tob", "a")
        );

        // No hist_command_line: command_line is used (and said so); the project's env still applies
        let f = new_project_filter(&ctx, Some(&all), "linkerd", "p.yaml", true);
        assert!(f.active() && f.hist && !f.hist_rules);
        assert_eq!(
            f.info(),
            "project 'linkerd' (historical rules: none, using command_line): 1 org(s), any repo, 1 excluded repo(s), exact false, actors filter false"
        );
        assert!(f.hit("linkerd/linkerd2", "a") && !f.hit("linkerd/website", "a"));
        crate::env::remove_var("GHA2DB_EXCLUDE_REPOS");

        // Inactive filters keep the mode flag
        let f = new_project_filter(&ctx, None, "kcp", "./projects.yaml", true);
        assert!(f.hist && !f.hist_rules && !f.active());
        assert_eq!(f.info(), "none (no ./projects.yaml)");
    }

    #[test]
    fn read_projects_if_present_cases() {
        let dir = std::env::temp_dir().join(format!(
            "project_filter_projects_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let dir_s = dir.to_str().expect("utf8").to_string();
        let mut ctx = Ctx {
            data_dir: format!("{dir_s}/"),
            projects_yaml: "projects.yaml".to_string(),
            ..Ctx::default()
        };
        assert_eq!(projects_path(&ctx), format!("{dir_s}/projects.yaml"));
        ctx.local = true;
        assert_eq!(projects_path(&ctx), "./projects.yaml");
        ctx.local = false;
        ctx.projects_yaml = "custom.yaml".to_string();
        let (projects, path) = read_projects_if_present(&ctx);
        assert!(projects.is_none());
        assert_eq!(path, format!("{dir_s}/custom.yaml"));
        // hist_command_line in both projects.yaml styles: a single-quoted one-liner and a
        // double-quoted scalar folded with escaped line breaks (the way long lists and regexps
        // are wrapped) - the same fixture as the Go test
        let yaml_data = concat!(
            "---\nprojects:\n  kcp:\n    name: KCP\n    psql_db: kcp\n    shared_db: allprj\n    command_line: ['kcp-dev']\n",
            "    hist_command_line:\n      - 'kcp-dev,kubestellar'\n    env:\n      GHA2DB_EXCLUDE_REPOS: kcp-dev/old\n    order: 1\n",
            "  velero:\n    psql_db: velero\n    command_line:\n      - 'regexp:(?i)^(velero-io\\/.*|vmware-tanzu\\/.*velero.*)$'\n",
            "    hist_command_line:\n      - \"regexp:(?:(?i)^(velero-io/.*|vmware-tanzu/.*velero.*)$)|\\\n",
            "         (?:(?i)^(velero-io/.*|vmware-tanzu/.*velero.*|heptio/(ark|\\\n",
            "         .*velero.*))$)\"\n",
            "  oci:\n    psql_db: oci\n    command_line:\n      - opencontainers\n      - 'runc,image-spec'\n",
            "    hist_command_line:\n      - opencontainers\n      - \"runc,image-spec,\\\n         ocitools,specs\"\n",
            "  linkerd:\n    psql_db: linkerd\n    command_line: ['linkerd']\n",
        );
        std::fs::write(dir.join("custom.yaml"), yaml_data).expect("write yaml");
        let (projects, path) = read_projects_if_present(&ctx);
        assert_eq!(path, format!("{dir_s}/custom.yaml"));
        let projects = projects.expect("parsed projects");
        let kcp = projects.projects.get("kcp").expect("kcp");
        assert_eq!(kcp.pdb, "kcp");
        assert_eq!(kcp.shared_db, "allprj");
        assert_eq!(kcp.command_line, vec!["kcp-dev".to_string()]);
        assert_eq!(kcp.hist_command_line, s(&["kcp-dev,kubestellar"]));
        assert_eq!(
            kcp.env.get("GHA2DB_EXCLUDE_REPOS").map(String::as_str),
            Some("kcp-dev/old")
        );
        assert_eq!(kcp.order, 1);
        let velero = projects.projects.get("velero").expect("velero");
        assert_eq!(
            velero.command_line,
            s(&["regexp:(?i)^(velero-io\\/.*|vmware-tanzu\\/.*velero.*)$"])
        );
        assert_eq!(
            velero.hist_command_line,
            s(&["regexp:(?:(?i)^(velero-io/.*|vmware-tanzu/.*velero.*)$)|(?:(?i)^(velero-io/.*|vmware-tanzu/.*velero.*|heptio/(ark|.*velero.*))$)"])
        );
        let oci = projects.projects.get("oci").expect("oci");
        assert_eq!(oci.command_line, s(&["opencontainers", "runc,image-spec"]));
        assert_eq!(
            oci.hist_command_line,
            s(&["opencontainers", "runc,image-spec,ocitools,specs"])
        );
        let linkerd = projects.projects.get("linkerd").expect("linkerd");
        assert!(linkerd.hist_command_line.is_empty());
        // read_projects reads the same file
        let (projects2, path2) = read_projects(&ctx);
        assert_eq!(path2, path);
        assert_eq!(projects2.projects.len(), projects.projects.len());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
