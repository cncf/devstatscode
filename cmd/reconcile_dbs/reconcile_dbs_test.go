package main

import (
	"errors"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"

	lib "github.com/cncf/devstatscode"
	"github.com/lib/pq"
)

func TestEnvFlag(t *testing.T) {
	const name = "GHA2DB_RECONCILE_TEST_FLAG"
	defer os.Unsetenv(name)
	cases := map[string]bool{
		"":      false,
		"0":     false,
		"false": false,
		"no":    false,
		"1":     true,
		" 1 ":   true,
		"t":     true,
		"TRUE":  true,
		"yes":   true,
		"Y":     true,
	}
	for value, expected := range cases {
		lib.FatalOnError(os.Setenv(name, value))
		if got := envFlag(name); got != expected {
			t.Errorf("envFlag(%q) = %v, expected %v", value, got, expected)
		}
	}
}

func TestParseDBList(t *testing.T) {
	cases := []struct {
		value    string
		expected []string
	}{
		{"", []string{}},
		{" , ,", []string{}},
		{"a", []string{"a"}},
		{"a,b", []string{"a", "b"}},
		{" b , a ,b,,a", []string{"b", "a"}},
	}
	for _, c := range cases {
		if got := parseDBList(c.value); !reflect.DeepEqual(got, c.expected) {
			t.Errorf("parseDBList(%q) = %v, expected %v", c.value, got, c.expected)
		}
	}
}

func TestClassOf(t *testing.T) {
	cases := map[int64]string{
		-1:                                   "orphan",
		-9223372036854775808:                 "orphan",
		0:                                    "zero",
		1:                                    "native",
		lib.ArtificialIDBase - 1:             "native",
		lib.ArtificialIDBase:                 "artificial",
		lib.ArtificialIDBase + 4000000000000: "artificial",
		9223372036854775807:                  "artificial",
	}
	for id, expected := range cases {
		if got := classOf(id); got != expected {
			t.Errorf("classOf(%d) = %s, expected %s", id, got, expected)
		}
	}
}

func TestArrayLiterals(t *testing.T) {
	if got, expected := int64ArrayLiteral([]int64{}), "'{}'::bigint[]"; got != expected {
		t.Errorf("int64ArrayLiteral(empty) = %s, expected %s", got, expected)
	}
	if got, expected := int64ArrayLiteral([]int64{-5, 0, 7, lib.ArtificialIDBase}), "'{-5,0,7,281474976710656}'::bigint[]"; got != expected {
		t.Errorf("int64ArrayLiteral = %s, expected %s", got, expected)
	}
	if got, expected := textArrayLiteral([]string{}), "'{}'::text[]"; got != expected {
		t.Errorf("textArrayLiteral(empty) = %s, expected %s", got, expected)
	}
	got := textArrayLiteral([]string{"abc", `a"b`, `a\b`, "a'b", "a,b", "a b", "żółw"})
	expected := `'{"abc","a\"b","a\\b","a''b","a,b","a b","żółw"}'::text[]`
	if got != expected {
		t.Errorf("textArrayLiteral = %s, expected %s", got, expected)
	}
	if got, expected := dateArrayLiteral([]string{"2026-01-02", "2026-01-03"}), "'{2026-01-02,2026-01-03}'::date[]"; got != expected {
		t.Errorf("dateArrayLiteral = %s, expected %s", got, expected)
	}
}

func TestBatchValues(t *testing.T) {
	cases := []struct {
		rows, cols int
		expected   string
	}{
		{1, 1, "values ($1)"},
		{1, 3, "values ($1,$2,$3)"},
		{2, 2, "values ($1,$2),($3,$4)"},
		{3, 1, "values ($1),($2),($3)"},
	}
	for _, c := range cases {
		if got := batchValues(c.rows, c.cols); got != c.expected {
			t.Errorf("batchValues(%d, %d) = %s, expected %s", c.rows, c.cols, got, c.expected)
		}
	}
}

func TestSortedSetOps(t *testing.T) {
	cases := []struct {
		a, b      []int64
		intersect []int64
		diff      []int64
	}{
		{[]int64{}, []int64{}, []int64{}, []int64{}},
		{[]int64{1, 2, 3}, []int64{}, []int64{}, []int64{1, 2, 3}},
		{[]int64{}, []int64{1, 2, 3}, []int64{}, []int64{}},
		{[]int64{1, 2, 3}, []int64{2, 3, 4}, []int64{2, 3}, []int64{1}},
		{[]int64{-3, -1, 5, 9}, []int64{-1, 9, 10}, []int64{-1, 9}, []int64{-3, 5}},
		{[]int64{1, 1, 2, 2, 3}, []int64{2, 2}, []int64{2}, []int64{1, 3}},
		{[]int64{1, 2, 3}, []int64{1, 2, 3}, []int64{1, 2, 3}, []int64{}},
	}
	for _, c := range cases {
		if got := intersectSorted(c.a, c.b); !reflect.DeepEqual(got, c.intersect) {
			t.Errorf("intersectSorted(%v, %v) = %v, expected %v", c.a, c.b, got, c.intersect)
		}
		if got := diffSorted(c.a, c.b); !reflect.DeepEqual(got, c.diff) {
			t.Errorf("diffSorted(%v, %v) = %v, expected %v", c.a, c.b, got, c.diff)
		}
	}
}

func TestSortedInt64Keys(t *testing.T) {
	set := map[int64]struct{}{5: {}, -2: {}, 0: {}, 3: {}}
	if got, expected := sortedInt64Keys(set), []int64{-2, 0, 3, 5}; !reflect.DeepEqual(got, expected) {
		t.Errorf("sortedInt64Keys = %v, expected %v", got, expected)
	}
	if got := sortedInt64Keys(map[int64]struct{}{}); len(got) != 0 {
		t.Errorf("sortedInt64Keys(empty) = %v, expected empty", got)
	}
}

func TestDifferingBuckets(t *testing.T) {
	source := map[bucket]digest{
		{2, "2026-01-02"}: {3, "30"},
		{2, "2026-01-01"}: {2, "20"},
		{1, "2026-01-03"}: {1, "10"},
		{1, "2026-01-01"}: {5, "50"},
		{3, "2026-01-01"}: {1, "-7"},
	}
	target := map[bucket]digest{
		{2, "2026-01-02"}: {3, "30"},
		{2, "2026-01-01"}: {2, "21"},
		{1, "2026-01-03"}: {2, "10"},
		{3, "2026-01-01"}: {1, "-7"},
		{4, "2026-01-01"}: {9, "99"},
	}
	got := differingBuckets(source, target)
	expected := []bucket{{1, "2026-01-01"}, {1, "2026-01-03"}, {2, "2026-01-01"}}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("differingBuckets = %v, expected %v", got, expected)
	}
	if got := differingBuckets(map[bucket]digest{}, target); len(got) != 0 {
		t.Errorf("differingBuckets(empty source) = %v, expected empty", got)
	}
	if got := differingBuckets(source, source); len(got) != 0 {
		t.Errorf("differingBuckets(same) = %v, expected empty", got)
	}
}

func TestChunks(t *testing.T) {
	if got := chunkInt64([]int64{}, 2); len(got) != 0 {
		t.Errorf("chunkInt64(empty) = %v, expected empty", got)
	}
	got := chunkInt64([]int64{1, 2, 3, 4, 5}, 2)
	expected := [][]int64{{1, 2}, {3, 4}, {5}}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("chunkInt64 = %v, expected %v", got, expected)
	}
	if got := chunkInt64([]int64{1, 2}, 5); !reflect.DeepEqual(got, [][]int64{{1, 2}}) {
		t.Errorf("chunkInt64(small) = %v, expected [[1 2]]", got)
	}
	gotS := chunkStrings([]string{"a", "b", "c"}, 2)
	expectedS := [][]string{{"a", "b"}, {"c"}}
	if !reflect.DeepEqual(gotS, expectedS) {
		t.Errorf("chunkStrings = %v, expected %v", gotS, expectedS)
	}
	if got := chunkStrings([]string{}, 2); len(got) != 0 {
		t.Errorf("chunkStrings(empty) = %v, expected empty", got)
	}
}

func TestClassConditionAndInfo(t *testing.T) {
	if got, expected := classCondition(false), " and id < 281474976710656"; got != expected {
		t.Errorf("classCondition(false) = %q, expected %q", got, expected)
	}
	if got := classCondition(true); got != "" {
		t.Errorf("classCondition(true) = %q, expected empty", got)
	}
	if got, expected := classesInfo(false), "native, orphan"; got != expected {
		t.Errorf("classesInfo(false) = %q, expected %q", got, expected)
	}
	if got, expected := classesInfo(true), "native, orphan, artificial"; got != expected {
		t.Errorf("classesInfo(true) = %q, expected %q", got, expected)
	}
}

func TestIsNoDBError(t *testing.T) {
	if !isNoDBError(&pq.Error{Code: "3D000", Message: "database \"x\" does not exist"}) {
		t.Errorf("isNoDBError(pq 3D000) = false, expected true")
	}
	if isNoDBError(&pq.Error{Code: "42P01", Message: "relation \"gha_events\" does not exist"}) {
		t.Errorf("isNoDBError(pq 42P01) = true, expected false")
	}
	if !isNoDBError(errors.New("pq: database \"nodb\" does not exist")) {
		t.Errorf("isNoDBError(text) = false, expected true")
	}
	if isNoDBError(errors.New("connection refused")) {
		t.Errorf("isNoDBError(connection refused) = true, expected false")
	}
}

func TestSQLBuilders(t *testing.T) {
	got := digestsSQL([]int64{1, 2}, classCondition(false))
	expected := "select repo_id, date_trunc('day', created_at)::date::text, count(*), sum(id)::text from gha_events " +
		"where created_at >= $1::timestamp and repo_id = any('{1,2}'::bigint[]) and id < 281474976710656 group by 1, 2"
	if got != expected {
		t.Errorf("digestsSQL = %s, expected %s", got, expected)
	}
	got = eventIDsSQL(7, []string{"2026-01-01", "2026-01-05"}, "")
	expected = "select id from gha_events where repo_id = 7 and created_at >= $1::timestamp and " +
		"date_trunc('day', created_at)::date = any('{2026-01-01,2026-01-05}'::date[]) order by id"
	if got != expected {
		t.Errorf("eventIDsSQL = %s, expected %s", got, expected)
	}
	got = isDistinctSQL("'{1}'::bigint[]")
	for _, part := range []string{"update gha_commits c set is_distinct = (", "not exists (select 1 from gha_commits o where o.sha = c.sha and o.event_id <> c.event_id and not (o.event_id = any('{1}'::bigint[])))", "min(n.event_id)", ") where c.event_id = any('{1}'::bigint[])"} {
		if !strings.Contains(got, part) {
			t.Errorf("isDistinctSQL = %s, expected to contain %s", got, part)
		}
	}
}

func TestDimensionSelects(t *testing.T) {
	var ctx lib.Ctx
	ids := "'{1,2}'::bigint[]"
	ctx.AffiliationsDB = ""
	if got := dimensionSelect(&ctx, "gha_actors", ids); got != "from gha_actors where id in (select actor_id from gha_events where id = any('{1,2}'::bigint[]))" {
		t.Errorf("dimensionSelect(gha_actors, legacy) = %s", got)
	}
	ctx.AffiliationsDB = "affiliations"
	if got := dimensionSelect(&ctx, "gha_actors", ids); got != "" {
		t.Errorf("dimensionSelect(gha_actors, shared affiliations) = %s, expected empty", got)
	}
	if got := dimensionSelect(&ctx, "gha_repos", ids); got != "from gha_repos where id in (select repo_id from gha_events where id = any('{1,2}'::bigint[]))" {
		t.Errorf("dimensionSelect(gha_repos) = %s", got)
	}
	if got := dimensionSelect(&ctx, "gha_orgs", ids); got != "from gha_orgs where id in (select org_id from gha_events where id = any('{1,2}'::bigint[]) and org_id is not null)" {
		t.Errorf("dimensionSelect(gha_orgs) = %s", got)
	}
	if got := dimensionSelect(&ctx, "gha_labels", ids); got != "from gha_labels where id in (select label_id from gha_issues_labels where event_id = any('{1,2}'::bigint[]))" {
		t.Errorf("dimensionSelect(gha_labels) = %s", got)
	}
	if got := dimensionSelect(&ctx, "gha_texts", ids); got != "" {
		t.Errorf("dimensionSelect(other) = %s, expected empty", got)
	}
	if got := dimensionColumns("gha_repos"); got != "id, name, org_id, org_login" {
		t.Errorf("dimensionColumns(gha_repos) = %s", got)
	}
	if got := dimensionColumns("gha_orgs"); got != "*" {
		t.Errorf("dimensionColumns(gha_orgs) = %s", got)
	}
}

func TestEventTablesOrder(t *testing.T) {
	if eventTables[0].name != "gha_events" || eventTables[0].key != "id" {
		t.Errorf("first copied table must be gha_events(id), got %+v", eventTables[0])
	}
	seen := map[string]struct{}{}
	for i, et := range eventTables {
		if i > 0 && et.key != "event_id" {
			t.Errorf("table %s must be keyed by event_id, got %s", et.name, et.key)
		}
		if _, ok := seen[et.name]; ok {
			t.Errorf("duplicate table %s", et.name)
		}
		seen[et.name] = struct{}{}
	}
	if len(eventTables) != 21 {
		t.Errorf("expected 21 event tables, got %d", len(eventTables))
	}
	if !reflect.DeepEqual(dimTables, []string{"gha_repos", "gha_orgs", "gha_labels", "gha_actors"}) {
		t.Errorf("unexpected dimension tables %v", dimTables)
	}
}

func testProjects() *lib.AllProjects {
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

func TestSharedDBSources(t *testing.T) {
	var ctx lib.Ctx
	projects := testProjects()
	got := sharedDBSources(&ctx, projects, "allprj")
	// delta (order 0, db beta), beta (1, beta - dup), gamma disabled, alpha (5), zeta (5)
	expected := []string{"beta", "alpha", "zeta"}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("sharedDBSources(allprj) = %v, expected %v", got, expected)
	}
	// Override enables gamma and disables zeta
	ctx.ProjectsOverride = map[string]bool{"gamma": true, "zeta": false}
	got = sharedDBSources(&ctx, projects, "allprj")
	expected = []string{"beta", "gamma", "alpha"}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("sharedDBSources(allprj, override) = %v, expected %v", got, expected)
	}
	if got := sharedDBSources(&ctx, projects, "gha"); len(got) != 0 {
		t.Errorf("sharedDBSources(gha) = %v, expected empty", got)
	}
	if got := sharedDBSources(&ctx, projects, "otherprj"); !reflect.DeepEqual(got, []string{"other"}) {
		t.Errorf("sharedDBSources(otherprj) = %v, expected [other]", got)
	}
}

func TestProjectForDB(t *testing.T) {
	var ctx lib.Ctx
	projects := testProjects()
	name, project := projectForDB(&ctx, projects, "beta")
	if name != "delta" || project == nil || project.PDB != "beta" {
		t.Errorf("projectForDB(beta) = %s, expected delta (lowest order)", name)
	}
	ctx.Project = "beta"
	name, project = projectForDB(&ctx, projects, "beta")
	if name != "beta" || project == nil {
		t.Errorf("projectForDB(beta, GHA2DB_PROJECT=beta) = %s, expected beta", name)
	}
	ctx.Project = "alpha"
	name, _ = projectForDB(&ctx, projects, "beta")
	if name != "delta" {
		t.Errorf("projectForDB(beta, GHA2DB_PROJECT=alpha) = %s, expected delta", name)
	}
	ctx.Project = ""
	name, project = projectForDB(&ctx, projects, "gamma")
	if name != "" || project != nil {
		t.Errorf("projectForDB(gamma disabled) = %s, expected none", name)
	}
	ctx.ProjectsOverride = map[string]bool{"gamma": true}
	name, project = projectForDB(&ctx, projects, "gamma")
	if name != "gamma" || project == nil || project.SharedDB != "allprj" {
		t.Errorf("projectForDB(gamma enabled by override) = %s, expected gamma", name)
	}
	name, project = projectForDB(&ctx, projects, "nosuchdb")
	if name != "" || project != nil {
		t.Errorf("projectForDB(nosuchdb) = %s, expected none", name)
	}
	name, project = projectForDB(&ctx, projects, "allprj")
	if name != "all" || project == nil || project.SharedDB != "" {
		t.Errorf("projectForDB(allprj) = %s, expected all", name)
	}
}

func TestParseFilterArg(t *testing.T) {
	names, re := parseFilterArg("")
	if len(names) != 0 || re != nil {
		t.Errorf("parseFilterArg('') = %v, %v, expected an empty set (no restriction)", names, re)
	}
	names, re = parseFilterArg(" kcp-dev ")
	if !reflect.DeepEqual(names, map[string]struct{}{"kcp-dev": {}}) || re != nil {
		t.Errorf("parseFilterArg(' kcp-dev ') = %v, %v", names, re)
	}
	names, re = parseFilterArg("a, b ,a,c")
	if !reflect.DeepEqual(names, map[string]struct{}{"a": {}, "b": {}, "c": {}}) || re != nil {
		t.Errorf("parseFilterArg('a, b ,a,c') = %v, %v", names, re)
	}
	names, re = parseFilterArg("regexp:(?i)^spiffe\\/spire.*$")
	if names != nil || re == nil || re.String() != "(?i)^spiffe\\/spire.*$" {
		t.Errorf("parseFilterArg(regexp) = %v, %v", names, re)
	}
	if !re.MatchString("spiffe/spire-api-sdk") || re.MatchString("kcp-dev/kcp") {
		t.Errorf("parseFilterArg(regexp): unexpected matches")
	}
	// gha2db_sync splits by comma and trims before passing the argument on
	_, re = parseFilterArg("regexp:^(a|b)$ , ^c$")
	if re == nil || re.String() != "^(a|b)$,^c$" {
		t.Errorf("parseFilterArg(regexp with comma) = %v", re)
	}
}

func TestProjectFilter(t *testing.T) {
	envSet, hadEnvSet := os.LookupEnv("ENV_SET")
	exclude, hadExclude := os.LookupEnv("GHA2DB_EXCLUDE_REPOS")
	defer func() {
		if hadEnvSet {
			_ = os.Setenv("ENV_SET", envSet)
		} else {
			_ = os.Unsetenv("ENV_SET")
		}
		if hadExclude {
			_ = os.Setenv("GHA2DB_EXCLUDE_REPOS", exclude)
		} else {
			_ = os.Unsetenv("GHA2DB_EXCLUDE_REPOS")
		}
	}()
	_ = os.Unsetenv("ENV_SET")
	_ = os.Unsetenv("GHA2DB_EXCLUDE_REPOS")
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
	exact, hadExact := os.LookupEnv("GHA2DB_EXACT")
	defer func() {
		if hadExact {
			_ = os.Setenv("GHA2DB_EXACT", exact)
		} else {
			_ = os.Unsetenv("GHA2DB_EXACT")
		}
	}()
	_ = os.Unsetenv("GHA2DB_EXACT")

	// No projects.yaml / no project: nothing is filtered
	f := newProjectFilter(&ctx, nil, "kcp", "./projects.yaml")
	if f.name != "" || !f.hit("other/repo", "bot") || f.info() != "none (no ./projects.yaml)" {
		t.Errorf("filter without projects: %+v, info %s", f, f.info())
	}
	f = newProjectFilter(&ctx, projects, "off", "p.yaml")
	if f.name != "" || !f.hit("x/y", "a") || f.info() != "none (no enabled project uses this database in p.yaml)" {
		t.Errorf("filter for a disabled project's database: %+v, info %s", f, f.info())
	}

	// Org list
	f = newProjectFilter(&ctx, projects, "kcp", "p.yaml")
	if f.name != "kcp" || f.info() != "project 'kcp': 1 org(s), any repo, 0 excluded repo(s), exact false, actors filter false" {
		t.Errorf("kcp filter: %+v, info %s", f, f.info())
	}
	if !f.hit("kcp-dev/kcp", "alice") || !f.hit("kcp-dev/edge-mc", "alice") {
		t.Errorf("kcp filter should accept kcp-dev repositories")
	}
	if f.hit("kubestellar/kubestellar", "alice") || f.hit("", "alice") || f.hit("kcp", "alice") {
		t.Errorf("kcp filter should reject other orgs, empty and org-less names")
	}

	// Regexp on the full name
	f = newProjectFilter(&ctx, projects, "spire", "p.yaml")
	if f.info() != "project 'spire': org regexp '(?i)^spiffe\\/spire.*$', any repo, 0 excluded repo(s), exact false, actors filter false" {
		t.Errorf("spire filter info: %s", f.info())
	}
	if !f.hit("spiffe/spire", "a") || !f.hit("SPIFFE/spire-tutorials", "a") || f.hit("spiffe/go-spiffe", "a") {
		t.Errorf("spire regexp filter mismatch")
	}

	// Org and repo lists
	f = newProjectFilter(&ctx, projects, "oci", "p.yaml")
	if f.info() != "project 'oci': 1 org(s), 2 repo(s), 0 excluded repo(s), exact false, actors filter false" {
		t.Errorf("oci filter info: %s", f.info())
	}
	if !f.hit("opencontainers/runc", "a") || f.hit("opencontainers/distribution-spec", "a") || f.hit("other/runc", "a") {
		t.Errorf("oci org+repo filter mismatch")
	}

	// The project's env (exclude list) is applied like gha2db_sync does
	f = newProjectFilter(&ctx, projects, "gha", "p.yaml")
	if f.info() != "project 'kube': 3 org(s), any repo, 2 excluded repo(s), exact false, actors filter false" {
		t.Errorf("kube filter info: %s", f.info())
	}
	if !f.hit("kubernetes/kubernetes", "a") || !f.hit("kubernetes-sigs/kind", "a") || f.hit("kubernetes/api", "a") || f.hit("kubernetes/apimachinery", "a") {
		t.Errorf("kube exclude list not applied")
	}
	if os.Getenv("GHA2DB_EXCLUDE_REPOS") != "kubernetes/api,kubernetes/apimachinery" {
		t.Errorf("project env not exported")
	}
	// ... unless ENV_SET says the environment is already prepared
	_ = os.Unsetenv("GHA2DB_EXCLUDE_REPOS")
	_ = os.Setenv("ENV_SET", "1")
	f = newProjectFilter(&ctx, projects, "gha", "p.yaml")
	if len(f.ctx.ExcludeRepos) != 0 || !f.hit("kubernetes/api", "a") {
		t.Errorf("project env applied despite ENV_SET")
	}
	_ = os.Unsetenv("ENV_SET")

	// Exact mode (the project's env): full repository names, no org matching
	f = newProjectFilter(&ctx, projects, "exact", "p.yaml")
	if f.info() != "project 'exact': 2 org(s), any repo, 0 excluded repo(s), exact true, actors filter false" {
		t.Errorf("exact filter info: %s", f.info())
	}
	if !f.hit("cncf/devstats", "a") || f.hit("cncf/other", "a") || f.hit("cncf", "a") {
		t.Errorf("exact filter mismatch")
	}
	_ = os.Unsetenv("GHA2DB_EXACT")

	// GHA2DB_PROJECT selects the project of a shared database
	ctx.Project = "all"
	f = newProjectFilter(&ctx, projects, "allprj", "p.yaml")
	if f.name != "all" || !f.hit("kubestellar/kubestellar", "a") || f.hit("cncf/devstats", "a") {
		t.Errorf("all filter mismatch: %+v", f)
	}
	// ... and when GHA2DB_PROJECT names a project of another database, the shared database's own
	// project still decides (the shared side is never filtered by a child's rules)
	ctx.Project = "kcp"
	f = newProjectFilter(&ctx, projects, "allprj", "p.yaml")
	if f.name != "all" || !f.hit("kubernetes/kubernetes", "a") || f.hit("cncf/devstats", "a") {
		t.Errorf("all filter (GHA2DB_PROJECT=kcp) mismatch: %+v", f)
	}
	ctx.Project = "all"
	f = newProjectFilter(&ctx, projects, "allprj", "p.yaml")
	// Actor rules
	f.ctx.ActorsFilter = true
	f.ctx.ActorsForbid = regexp.MustCompile(`(?i)bot$`)
	if !f.hit("kubernetes/kubernetes", "alice") || f.hit("kubernetes/kubernetes", "k8s-ci-robot-bot") {
		t.Errorf("actor filter not applied")
	}
	if f.info() != "project 'all': 3 org(s), any repo, 0 excluded repo(s), exact false, actors filter true" {
		t.Errorf("all filter info: %s", f.info())
	}
}

func TestNamesInfo(t *testing.T) {
	if s := namesInfo("org", nil, nil); s != "any org" {
		t.Errorf("namesInfo(nil) = %s", s)
	}
	if s := namesInfo("repo", map[string]struct{}{"a": {}, "b": {}}, nil); s != "2 repo(s)" {
		t.Errorf("namesInfo(2) = %s", s)
	}
	if s := namesInfo("org", nil, regexp.MustCompile("^a$")); s != "org regexp '^a$'" {
		t.Errorf("namesInfo(re) = %s", s)
	}
}

func TestReadProjectsIfPresent(t *testing.T) {
	dir := t.TempDir()
	var ctx lib.Ctx
	ctx.DataDir = dir + "/"
	ctx.ProjectsYaml = "projects.yaml"
	if p := projectsPath(&ctx); p != dir+"/projects.yaml" {
		t.Errorf("projectsPath = %s", p)
	}
	ctx.Local = true
	if p := projectsPath(&ctx); p != "./projects.yaml" {
		t.Errorf("projectsPath (local) = %s", p)
	}
	ctx.Local = false
	ctx.ProjectsYaml = "custom.yaml"
	projects, path := readProjectsIfPresent(&ctx)
	if projects != nil || path != dir+"/custom.yaml" {
		t.Errorf("readProjectsIfPresent(missing) = %+v, %s", projects, path)
	}
	yamlData := "---\nprojects:\n  kcp:\n    name: KCP\n    psql_db: kcp\n    shared_db: allprj\n    command_line: ['kcp-dev']\n    env:\n      GHA2DB_EXCLUDE_REPOS: kcp-dev/old\n    order: 1\n"
	if err := os.WriteFile(dir+"/custom.yaml", []byte(yamlData), 0644); err != nil {
		t.Fatal(err)
	}
	projects, path = readProjectsIfPresent(&ctx)
	if projects == nil || path != dir+"/custom.yaml" {
		t.Fatalf("readProjectsIfPresent(present) = %+v, %s", projects, path)
	}
	kcp, ok := projects.Projects["kcp"]
	if !ok || kcp.PDB != "kcp" || kcp.SharedDB != "allprj" || !reflect.DeepEqual(kcp.CommandLine, []string{"kcp-dev"}) || kcp.Env["GHA2DB_EXCLUDE_REPOS"] != "kcp-dev/old" || kcp.Order != 1 {
		t.Errorf("parsed project: %+v", kcp)
	}
}
