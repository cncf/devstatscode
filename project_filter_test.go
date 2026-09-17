package devstatscode

import (
	"os"
	"reflect"
	"regexp"
	"testing"

	lib "github.com/cncf/devstatscode"
)

func filterTestProjects() *lib.AllProjects {
	return &lib.AllProjects{
		Projects: map[string]lib.Project{
			"zeta":     {PDB: "zeta", SharedDB: "allprj", Order: 5},
			"alpha":    {PDB: "alpha", SharedDB: "allprj", Order: 5},
			"beta":     {PDB: "beta", SharedDB: "allprj", Order: 1},
			"gamma":    {PDB: "gamma", SharedDB: "allprj", Order: 3, Disabled: true},
			"delta":    {PDB: "beta", SharedDB: "allprj", Order: 0},
			"all":      {PDB: "allprj", Order: 2},
			"kube":     {PDB: "gha", Order: 0},
			"other":    {PDB: "other", SharedDB: "otherprj", Order: 9},
			"selfref":  {PDB: "allprj", SharedDB: "allprj", Order: 9},
			"noshared": {PDB: "solo", Order: 9},
		},
	}
}

// preserveEnv - restores the given environment variables when the test ends (and unsets them now)
func preserveEnv(t *testing.T, names ...string) {
	for _, name := range names {
		value, had := os.LookupEnv(name)
		t.Cleanup(func() {
			if had {
				_ = os.Setenv(name, value)
			} else {
				_ = os.Unsetenv(name)
			}
		})
		_ = os.Unsetenv(name)
	}
}

func TestProjectForDB(t *testing.T) {
	var ctx lib.Ctx
	projects := filterTestProjects()
	name, project := lib.ProjectForDB(&ctx, projects, "beta")
	if name != "delta" || project == nil || project.PDB != "beta" {
		t.Errorf("ProjectForDB(beta) = %s, expected delta (lowest order)", name)
	}
	ctx.Project = "beta"
	name, project = lib.ProjectForDB(&ctx, projects, "beta")
	if name != "beta" || project == nil {
		t.Errorf("ProjectForDB(beta, GHA2DB_PROJECT=beta) = %s, expected beta", name)
	}
	ctx.Project = "alpha"
	name, _ = lib.ProjectForDB(&ctx, projects, "beta")
	if name != "delta" {
		t.Errorf("ProjectForDB(beta, GHA2DB_PROJECT=alpha) = %s, expected delta", name)
	}
	ctx.Project = ""
	name, project = lib.ProjectForDB(&ctx, projects, "gamma")
	if name != "" || project != nil {
		t.Errorf("ProjectForDB(gamma disabled) = %s, expected none", name)
	}
	ctx.ProjectsOverride = map[string]bool{"gamma": true}
	name, project = lib.ProjectForDB(&ctx, projects, "gamma")
	if name != "gamma" || project == nil || project.SharedDB != "allprj" {
		t.Errorf("ProjectForDB(gamma enabled by override) = %s, expected gamma", name)
	}
	name, project = lib.ProjectForDB(&ctx, projects, "nosuchdb")
	if name != "" || project != nil {
		t.Errorf("ProjectForDB(nosuchdb) = %s, expected none", name)
	}
	name, project = lib.ProjectForDB(&ctx, projects, "allprj")
	if name != "all" || project == nil || project.SharedDB != "" {
		t.Errorf("ProjectForDB(allprj) = %s, expected all", name)
	}
}

func TestParseFilterArg(t *testing.T) {
	names, re := lib.ParseFilterArg("")
	if len(names) != 0 || re != nil {
		t.Errorf("ParseFilterArg('') = %v, %v, expected an empty set (no restriction)", names, re)
	}
	names, re = lib.ParseFilterArg(" kcp-dev ")
	if !reflect.DeepEqual(names, map[string]struct{}{"kcp-dev": {}}) || re != nil {
		t.Errorf("ParseFilterArg(' kcp-dev ') = %v, %v", names, re)
	}
	names, re = lib.ParseFilterArg("a, b ,a,c")
	if !reflect.DeepEqual(names, map[string]struct{}{"a": {}, "b": {}, "c": {}}) || re != nil {
		t.Errorf("ParseFilterArg('a, b ,a,c') = %v, %v", names, re)
	}
	names, re = lib.ParseFilterArg("regexp:(?i)^spiffe\\/spire.*$")
	if names != nil || re == nil || re.String() != "(?i)^spiffe\\/spire.*$" {
		t.Errorf("ParseFilterArg(regexp) = %v, %v", names, re)
	}
	if !re.MatchString("spiffe/spire-api-sdk") || re.MatchString("kcp-dev/kcp") {
		t.Errorf("ParseFilterArg(regexp): unexpected matches")
	}
	// gha2db_sync splits by comma and trims before passing the argument on
	_, re = lib.ParseFilterArg("regexp:^(a|b)$ , ^c$")
	if re == nil || re.String() != "^(a|b)$,^c$" {
		t.Errorf("ParseFilterArg(regexp with comma) = %v", re)
	}
}

func TestProjectFilter(t *testing.T) {
	preserveEnv(t, "ENV_SET", "GHA2DB_EXCLUDE_REPOS", "GHA2DB_EXACT")
	var ctx lib.Ctx
	projects := &lib.AllProjects{
		Projects: map[string]lib.Project{
			"kcp":   {PDB: "kcp", SharedDB: "allprj", CommandLine: []string{"kcp-dev"}},
			"spire": {PDB: "spire", SharedDB: "allprj", CommandLine: []string{"regexp:(?i)^spiffe\\/spire.*$"}},
			"kube": {
				PDB: "gha", CommandLine: []string{"kubernetes,kubernetes-client, kubernetes-sigs"},
				Env: map[string]string{"GHA2DB_EXCLUDE_REPOS": "kubernetes/api,kubernetes/apimachinery"},
			},
			"oci": {PDB: "oci", CommandLine: []string{"opencontainers", "runc,image-spec"}},
			"all": {PDB: "allprj", CommandLine: []string{"kcp-dev,kubestellar,kubernetes"}},
			"off": {PDB: "off", Disabled: true, CommandLine: []string{"x"}},
			"exact": {
				PDB: "exact", CommandLine: []string{"cncf/devstats,cncf/devstatscode"},
				Env: map[string]string{"GHA2DB_EXACT": "1"},
			},
		},
	}

	// No projects.yaml / no project: nothing is filtered
	f := lib.NewProjectFilter(&ctx, nil, "kcp", "./projects.yaml", false)
	if f.Active() || !f.Hit("other/repo", "bot") || !f.RepoHit("x") || !f.ActorHit("y") || f.Info() != "none (no ./projects.yaml)" {
		t.Errorf("filter without projects: %+v, info %s", f, f.Info())
	}
	f = lib.NewProjectFilter(&ctx, projects, "off", "p.yaml", false)
	if f.Active() || !f.Hit("x/y", "a") || f.Info() != "none (no enabled project uses this database in p.yaml)" {
		t.Errorf("filter for a disabled project's database: %+v, info %s", f, f.Info())
	}

	// Org list
	f = lib.NewProjectFilter(&ctx, projects, "kcp", "p.yaml", false)
	if !f.Active() || f.Name != "kcp" || f.Info() != "project 'kcp': 1 org(s), any repo, 0 excluded repo(s), exact false, actors filter false" {
		t.Errorf("kcp filter: %+v, info %s", f, f.Info())
	}
	if !f.Hit("kcp-dev/kcp", "alice") || !f.Hit("kcp-dev/edge-mc", "alice") || !f.RepoHit("kcp-dev/kcp") {
		t.Errorf("kcp filter should accept kcp-dev repositories")
	}
	if f.Hit("kubestellar/kubestellar", "alice") || f.Hit("", "alice") || f.Hit("kcp", "alice") || f.RepoHit("kubestellar/kubestellar") {
		t.Errorf("kcp filter should reject other orgs, empty and org-less names")
	}

	// Regexp on the full name
	f = lib.NewProjectFilter(&ctx, projects, "spire", "p.yaml", false)
	if f.Info() != "project 'spire': org regexp '(?i)^spiffe\\/spire.*$', any repo, 0 excluded repo(s), exact false, actors filter false" {
		t.Errorf("spire filter info: %s", f.Info())
	}
	if !f.Hit("spiffe/spire", "a") || !f.Hit("SPIFFE/spire-tutorials", "a") || f.Hit("spiffe/go-spiffe", "a") {
		t.Errorf("spire regexp filter mismatch")
	}

	// Org and repo lists
	f = lib.NewProjectFilter(&ctx, projects, "oci", "p.yaml", false)
	if f.Info() != "project 'oci': 1 org(s), 2 repo(s), 0 excluded repo(s), exact false, actors filter false" {
		t.Errorf("oci filter info: %s", f.Info())
	}
	if !f.Hit("opencontainers/runc", "a") || f.Hit("opencontainers/distribution-spec", "a") || f.Hit("other/runc", "a") {
		t.Errorf("oci org+repo filter mismatch")
	}

	// The project's env (exclude list) is applied like gha2db_sync does
	f = lib.NewProjectFilter(&ctx, projects, "gha", "p.yaml", false)
	if f.Info() != "project 'kube': 3 org(s), any repo, 2 excluded repo(s), exact false, actors filter false" {
		t.Errorf("kube filter info: %s", f.Info())
	}
	if !f.Hit("kubernetes/kubernetes", "a") || !f.Hit("kubernetes-sigs/kind", "a") || f.Hit("kubernetes/api", "a") || f.Hit("kubernetes/apimachinery", "a") {
		t.Errorf("kube exclude list not applied")
	}
	if os.Getenv("GHA2DB_EXCLUDE_REPOS") != "kubernetes/api,kubernetes/apimachinery" {
		t.Errorf("project env not exported")
	}
	// ... unless ENV_SET says the environment is already prepared
	_ = os.Unsetenv("GHA2DB_EXCLUDE_REPOS")
	_ = os.Setenv("ENV_SET", "1")
	f = lib.NewProjectFilter(&ctx, projects, "gha", "p.yaml", false)
	if len(f.Ctx.ExcludeRepos) != 0 || !f.Hit("kubernetes/api", "a") {
		t.Errorf("project env applied despite ENV_SET")
	}
	_ = os.Unsetenv("ENV_SET")

	// Exact mode (the project's env): full repository names, no org matching
	f = lib.NewProjectFilter(&ctx, projects, "exact", "p.yaml", false)
	if f.Info() != "project 'exact': 2 org(s), any repo, 0 excluded repo(s), exact true, actors filter false" {
		t.Errorf("exact filter info: %s", f.Info())
	}
	if !f.Hit("cncf/devstats", "a") || f.Hit("cncf/other", "a") || f.Hit("cncf", "a") {
		t.Errorf("exact filter mismatch")
	}
	_ = os.Unsetenv("GHA2DB_EXACT")

	// GHA2DB_PROJECT selects the project of a shared database
	ctx.Project = "all"
	f = lib.NewProjectFilter(&ctx, projects, "allprj", "p.yaml", false)
	if f.Name != "all" || !f.Hit("kubestellar/kubestellar", "a") || f.Hit("cncf/devstats", "a") {
		t.Errorf("all filter mismatch: %+v", f)
	}
	// ... and when GHA2DB_PROJECT names a project of another database, the shared database's own
	// project still decides (the shared side is never filtered by a child's rules)
	ctx.Project = "kcp"
	f = lib.NewProjectFilter(&ctx, projects, "allprj", "p.yaml", false)
	if f.Name != "all" || !f.Hit("kubernetes/kubernetes", "a") || f.Hit("cncf/devstats", "a") {
		t.Errorf("all filter (GHA2DB_PROJECT=kcp) mismatch: %+v", f)
	}
	ctx.Project = "all"
	f = lib.NewProjectFilter(&ctx, projects, "allprj", "p.yaml", false)
	// Actor rules
	f.Ctx.ActorsFilter = true
	f.Ctx.ActorsForbid = regexp.MustCompile(`(?i)bot$`)
	if !f.Hit("kubernetes/kubernetes", "alice") || f.Hit("kubernetes/kubernetes", "k8s-ci-robot-bot") {
		t.Errorf("actor filter not applied")
	}
	if !f.ActorHit("alice") || f.ActorHit("k8s-ci-robot-bot") || !f.RepoHit("kubernetes/kubernetes") {
		t.Errorf("RepoHit/ActorHit mismatch")
	}
	if f.Info() != "project 'all': 3 org(s), any repo, 0 excluded repo(s), exact false, actors filter true" {
		t.Errorf("all filter info: %s", f.Info())
	}
}

func TestProjectFilterHist(t *testing.T) {
	preserveEnv(t, "ENV_SET", "GHA2DB_EXCLUDE_REPOS", "GHA2DB_EXACT")
	var ctx lib.Ctx
	projects := &lib.AllProjects{
		Projects: map[string]lib.Project{
			// a repository moved to another org: the current rules track the new org only
			"kcp": {
				PDB: "kcp", SharedDB: "allprj", CommandLine: []string{"kcp-dev"},
				HistCommandLine: []string{"kcp-dev,kubestellar,kcp-dev/edge-mc"},
				Env:             map[string]string{"GHA2DB_EXCLUDE_REPOS": "kcp-dev/old"},
			},
			"velero": {
				PDB: "velero", SharedDB: "allprj", CommandLine: []string{"regexp:(?i)^(velero-io\\/.*|vmware-tanzu\\/.*velero.*)$"},
				HistCommandLine: []string{"regexp:(?:(?i)^(velero-io/.*|vmware-tanzu/.*velero.*)$)|(?:(?i)^(heptio/(ark|.*velero.*))$)"},
			},
			"oci": {
				PDB: "oci", CommandLine: []string{"opencontainers", "runc,image-spec"},
				HistCommandLine: []string{"opencontainers", "runc,image-spec,ocitools,specs"},
			},
			// no historical rules: the daily ones are used
			"linkerd": {PDB: "linkerd", SharedDB: "allprj", CommandLine: []string{"linkerd"}},
		},
	}

	// Daily mode ignores hist_command_line
	f := lib.NewProjectFilter(&ctx, projects, "kcp", "p.yaml", false)
	if f.Hist || f.HistRules || f.Hit("kubestellar/kubestellar", "a") || !f.Hit("kcp-dev/kcp", "a") {
		t.Errorf("daily kcp filter uses hist rules: %+v", f)
	}
	if f.Info() != "project 'kcp': 1 org(s), any repo, 1 excluded repo(s), exact false, actors filter false" {
		t.Errorf("daily kcp filter info: %s", f.Info())
	}

	// Historical mode: the biggest scope the project ever had, the project's env still applies
	f = lib.NewProjectFilter(&ctx, projects, "kcp", "p.yaml", true)
	if !f.Hist || !f.HistRules || !f.Active() || f.Name != "kcp" {
		t.Errorf("hist kcp filter: %+v", f)
	}
	if f.Info() != "project 'kcp' (historical rules): 3 org(s), any repo, 1 excluded repo(s), exact false, actors filter false" {
		t.Errorf("hist kcp filter info: %s", f.Info())
	}
	if !f.Hit("kubestellar/kubestellar", "a") || !f.Hit("kcp-dev/kcp", "a") || !f.Hit("kcp-dev/edge-mc", "a") || f.Hit("kcp-dev/old", "a") || f.Hit("other/repo", "a") {
		t.Errorf("hist kcp filter mismatch")
	}
	// (the project's env was exported like gha2db_sync does - one project per process)
	_ = os.Unsetenv("GHA2DB_EXCLUDE_REPOS")

	// Regexp union
	f = lib.NewProjectFilter(&ctx, projects, "velero", "p.yaml", true)
	if !f.HistRules || !f.Hit("heptio/ark", "a") || !f.Hit("heptio/velero-plugin-for-aws", "a") || !f.Hit("vmware-tanzu/velero", "a") || f.Hit("heptio/other", "a") {
		t.Errorf("hist velero filter mismatch: %+v", f)
	}
	f = lib.NewProjectFilter(&ctx, projects, "velero", "p.yaml", false)
	if f.HistRules || f.Hit("heptio/ark", "a") || !f.Hit("velero-io/velero", "a") {
		t.Errorf("daily velero filter mismatch: %+v", f)
	}

	// Two elements form (orgs, repos)
	f = lib.NewProjectFilter(&ctx, projects, "oci", "p.yaml", true)
	if f.Info() != "project 'oci' (historical rules): 1 org(s), 4 repo(s), 0 excluded repo(s), exact false, actors filter false" {
		t.Errorf("hist oci filter info: %s", f.Info())
	}
	if !f.Hit("opencontainers/ocitools", "a") || !f.Hit("opencontainers/runc", "a") || f.Hit("opencontainers/distribution-spec", "a") {
		t.Errorf("hist oci filter mismatch")
	}

	// No hist_command_line: falls back to command_line (compat mode), says so
	f = lib.NewProjectFilter(&ctx, projects, "linkerd", "p.yaml", true)
	if !f.Hist || f.HistRules || !f.Hit("linkerd/linkerd2", "a") || f.Hit("runconduit/conduit", "a") {
		t.Errorf("hist linkerd filter (no hist rules): %+v", f)
	}
	if f.Info() != "project 'linkerd' (historical rules: none, using command_line): 1 org(s), any repo, 0 excluded repo(s), exact false, actors filter false" {
		t.Errorf("hist linkerd filter info: %s", f.Info())
	}

	// Inactive filters report the requested mode too
	f = lib.NewProjectFilter(&ctx, nil, "kcp", "p.yaml", true)
	if !f.Hist || f.HistRules || f.Active() || f.Info() != "none (no p.yaml)" {
		t.Errorf("hist filter without projects: %+v, info %s", f, f.Info())
	}
}

func TestNamesInfo(t *testing.T) {
	var ctx lib.Ctx
	projects := &lib.AllProjects{
		Projects: map[string]lib.Project{
			"any":  {PDB: "any", CommandLine: []string{}},
			"two":  {PDB: "two", CommandLine: []string{"a", "x,y"}},
			"rex":  {PDB: "rex", CommandLine: []string{"regexp:^a$", "regexp:^b$"}},
			"none": {PDB: "none"},
		},
	}
	preserveEnv(t, "ENV_SET", "GHA2DB_EXCLUDE_REPOS", "GHA2DB_EXACT")
	f := lib.NewProjectFilter(&ctx, projects, "any", "p.yaml", false)
	if f.Info() != "project 'any': any org, any repo, 0 excluded repo(s), exact false, actors filter false" {
		t.Errorf("any filter info: %s", f.Info())
	}
	f = lib.NewProjectFilter(&ctx, projects, "two", "p.yaml", false)
	if f.Info() != "project 'two': 1 org(s), 2 repo(s), 0 excluded repo(s), exact false, actors filter false" {
		t.Errorf("two filter info: %s", f.Info())
	}
	f = lib.NewProjectFilter(&ctx, projects, "rex", "p.yaml", false)
	if f.Info() != "project 'rex': org regexp '^a$', repo regexp '^b$', 0 excluded repo(s), exact false, actors filter false" {
		t.Errorf("rex filter info: %s", f.Info())
	}
	f = lib.NewProjectFilter(&ctx, projects, "none", "p.yaml", false)
	if f.Info() != "project 'none': any org, any repo, 0 excluded repo(s), exact false, actors filter false" || !f.Hit("a/b", "c") {
		t.Errorf("none filter: %s", f.Info())
	}
}

func TestReadProjectsIfPresent(t *testing.T) {
	dir := t.TempDir()
	var ctx lib.Ctx
	ctx.DataDir = dir + "/"
	ctx.ProjectsYaml = "projects.yaml"
	if p := lib.ProjectsPath(&ctx); p != dir+"/projects.yaml" {
		t.Errorf("ProjectsPath = %s", p)
	}
	ctx.Local = true
	if p := lib.ProjectsPath(&ctx); p != "./projects.yaml" {
		t.Errorf("ProjectsPath (local) = %s", p)
	}
	ctx.Local = false
	ctx.ProjectsYaml = "custom.yaml"
	projects, path := lib.ReadProjectsIfPresent(&ctx)
	if projects != nil || path != dir+"/custom.yaml" {
		t.Errorf("ReadProjectsIfPresent(missing) = %+v, %s", projects, path)
	}
	// hist_command_line in both projects.yaml styles: a single-quoted one-liner and a double-quoted scalar
	// folded with escaped line breaks (the way long lists and regexps are wrapped)
	yamlData := "---\nprojects:\n  kcp:\n    name: KCP\n    psql_db: kcp\n    shared_db: allprj\n    command_line: ['kcp-dev']\n" +
		"    hist_command_line:\n      - 'kcp-dev,kubestellar'\n    env:\n      GHA2DB_EXCLUDE_REPOS: kcp-dev/old\n    order: 1\n" +
		"  velero:\n    psql_db: velero\n    command_line:\n      - 'regexp:(?i)^(velero-io\\/.*|vmware-tanzu\\/.*velero.*)$'\n" +
		"    hist_command_line:\n      - \"regexp:(?:(?i)^(velero-io/.*|vmware-tanzu/.*velero.*)$)|\\\n" +
		"         (?:(?i)^(velero-io/.*|vmware-tanzu/.*velero.*|heptio/(ark|\\\n" +
		"         .*velero.*))$)\"\n" +
		"  oci:\n    psql_db: oci\n    command_line:\n      - opencontainers\n      - 'runc,image-spec'\n" +
		"    hist_command_line:\n      - opencontainers\n      - \"runc,image-spec,\\\n         ocitools,specs\"\n" +
		"  linkerd:\n    psql_db: linkerd\n    command_line: ['linkerd']\n"
	if err := os.WriteFile(dir+"/custom.yaml", []byte(yamlData), 0644); err != nil {
		t.Fatal(err)
	}
	projects, path = lib.ReadProjectsIfPresent(&ctx)
	if projects == nil || path != dir+"/custom.yaml" {
		t.Fatalf("ReadProjectsIfPresent(present) = %+v, %s", projects, path)
	}
	kcp, ok := projects.Projects["kcp"]
	if !ok || kcp.PDB != "kcp" || kcp.SharedDB != "allprj" || !reflect.DeepEqual(kcp.CommandLine, []string{"kcp-dev"}) || kcp.Env["GHA2DB_EXCLUDE_REPOS"] != "kcp-dev/old" || kcp.Order != 1 {
		t.Errorf("parsed project: %+v", kcp)
	}
	if !reflect.DeepEqual(kcp.HistCommandLine, []string{"kcp-dev,kubestellar"}) {
		t.Errorf("parsed hist_command_line: %+v", kcp.HistCommandLine)
	}
	velero := projects.Projects["velero"]
	expected := []string{"regexp:(?:(?i)^(velero-io/.*|vmware-tanzu/.*velero.*)$)|(?:(?i)^(velero-io/.*|vmware-tanzu/.*velero.*|heptio/(ark|.*velero.*))$)"}
	if !reflect.DeepEqual(velero.HistCommandLine, expected) {
		t.Errorf("parsed folded hist_command_line: %q, expected %q", velero.HistCommandLine, expected)
	}
	oci := projects.Projects["oci"]
	if !reflect.DeepEqual(oci.HistCommandLine, []string{"opencontainers", "runc,image-spec,ocitools,specs"}) || !reflect.DeepEqual(oci.CommandLine, []string{"opencontainers", "runc,image-spec"}) {
		t.Errorf("parsed two elements hist_command_line: %q", oci.HistCommandLine)
	}
	if linkerd := projects.Projects["linkerd"]; len(linkerd.HistCommandLine) != 0 {
		t.Errorf("hist_command_line should be absent: %q", linkerd.HistCommandLine)
	}
	// ReadProjects reads the same file
	projects2, path2 := lib.ReadProjects(&ctx)
	if path2 != path || !reflect.DeepEqual(projects2.Projects["velero"].HistCommandLine, expected) {
		t.Errorf("ReadProjects mismatch: %s", path2)
	}
}
