package main

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	lib "github.com/cncf/devstatscode"
)

// testRules - project rules like getProjectRules builds them: `command_line` and `hist_command_line` filters
func testRules(t *testing.T, project lib.Project) *projectRules {
	for _, name := range []string{"ENV_SET", "GHA2DB_EXCLUDE_REPOS", "GHA2DB_EXACT", "GHA2DB_PROJECT"} {
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
	var ctx lib.Ctx
	project.PDB = "proj"
	projects := &lib.AllProjects{Projects: map[string]lib.Project{"proj": project}}
	return &projectRules{
		filter: lib.NewProjectFilter(&ctx, projects, "proj", "p.yaml", false),
		hist:   lib.NewProjectFilter(&ctx, projects, "proj", "p.yaml", true),
	}
}

func TestDecodeHeartbeatOwner(t *testing.T) {
	body := []byte(`{"data": {
		"r0": {"databaseId": 1, "nameWithOwner": "neworg/repo", "owner": {"__typename": "Organization", "login": "neworg", "databaseId": 11}},
		"r1": {"databaseId": 2, "nameWithOwner": "someone/repo", "owner": {"__typename": "User", "login": "someone", "databaseId": 12}},
		"r2": {"databaseId": 3, "nameWithOwner": "plain/repo", "owner": {"login": "plain", "databaseId": 13}},
		"r3": {"databaseId": 4, "nameWithOwner": "none/repo"},
		"r4": null
	}}`)
	hbs, err := decodeHeartbeat(body, []string{"a/a", "b/b", "c/c", "d/d", "e/e"}, 0, 1)
	if err != nil || len(hbs) != 5 {
		t.Fatalf("decodeHeartbeat: %v, %d heartbeats", err, len(hbs))
	}
	expected := []struct {
		found bool
		id    int64
		login string
		isOrg bool
	}{
		{true, 11, "neworg", true},
		{true, 12, "someone", false},
		{true, 13, "plain", false},
		{true, 0, "", false},
		{false, 0, "", false},
	}
	for i, exp := range expected {
		hb := hbs[i]
		if hb.found != exp.found || hb.ownerID != exp.id || hb.ownerLogin != exp.login || hb.ownerIsOrg != exp.isOrg || hb.excluded {
			t.Errorf("heartbeat %d: found %v, owner (%d, %s, org %v), excluded %v; expected %+v", i, hb.found, hb.ownerID, hb.ownerLogin, hb.ownerIsOrg, hb.excluded, exp)
		}
	}
}

func TestHeartbeatQueryAsksTheOwnerType(t *testing.T) {
	query := string(heartbeatQuery([]string{"org/repo"}))
	// the owner type decides whether the gha_repos row of a renamed repository gets an organization
	for _, part := range []string{
		"owner { __typename login ... on User { databaseId } ... on Organization { databaseId } }",
		`r0: repository(owner: \"org\", name: \"repo\") { ...F }`,
	} {
		if !strings.Contains(query, part) {
			t.Errorf("heartbeat query lacks %q: %s", part, query)
		}
	}
}

func TestExcludedHeartbeatIsNeverActive(t *testing.T) {
	now := time.Now()
	recent := now.Add(-time.Hour)
	hb := &repoHeartbeat{found: true, pushedAt: &now, issueAt: &now, prAt: &now, releaseAt: &now, forkAt: &now, stargazerCount: 5}
	passes := []apiPass{passEvents, passCommits, passComments, passReviews, passForks, passReleases, passStars, passRepoStats, passRepoEvents, passIssuesPRs}
	for _, pass := range passes {
		if !hb.active(pass, recent) {
			t.Errorf("%s: an active repository must be processed", pass.label())
		}
	}
	hb.excluded = true
	for _, pass := range passes {
		if hb.active(pass, recent) {
			t.Errorf("%s: an excluded repository must not be processed", pass.label())
		}
	}
	if hb.counters() == nil {
		t.Errorf("an excluded repository still has counters (no pass asks for them)")
	}
}

func TestSwapName(t *testing.T) {
	hbA, hbC, hbZ := &repoHeartbeat{databaseID: 1}, &repoHeartbeat{databaseID: 3}, &repoHeartbeat{databaseID: 26}
	s := &repoScope{
		repos:     []string{"a/old", "c/keep", "z/old"},
		ids:       map[string][]int64{"a/old": {1, 7}, "c/keep": {3}, "z/old": {26}},
		heartbeat: map[string]*repoHeartbeat{"a/old": hbA, "c/keep": hbC, "z/old": hbZ},
	}
	// the new name sorts last
	s.swapName("a/old", "x/new")
	if !reflect.DeepEqual(s.repos, []string{"c/keep", "x/new", "z/old"}) {
		t.Errorf("repos after the first swap: %v", s.repos)
	}
	if !reflect.DeepEqual(s.ids, map[string][]int64{"x/new": {1, 7}, "c/keep": {3}, "z/old": {26}}) {
		t.Errorf("ids after the first swap: %v", s.ids)
	}
	if s.heartbeat["x/new"] != hbA || s.heartbeat["a/old"] != nil || len(s.heartbeat) != 3 {
		t.Errorf("heartbeat after the first swap: %v", s.heartbeat)
	}
	// the new name sorts first
	s.swapName("z/old", "b/new")
	if !reflect.DeepEqual(s.repos, []string{"b/new", "c/keep", "x/new"}) {
		t.Errorf("repos after the second swap: %v", s.repos)
	}
	if s.heartbeat["b/new"] != hbZ || s.heartbeat["c/keep"] != hbC || !s.tracks("b/new", 26) || s.tracks("z/old", 26) || s.trackedID("x/new") != 7 {
		t.Errorf("scope after the second swap: ids %v, heartbeat %v", s.ids, s.heartbeat)
	}
	if !reflect.DeepEqual(s.dbNames, map[string]string{"x/new": "a/old", "b/new": "z/old"}) {
		t.Errorf("database names: %v", s.dbNames)
	}
	// the accessors the passes use
	gScopeMtx.Lock()
	saved := gScope
	gScope = s
	gScopeMtx.Unlock()
	defer func() {
		gScopeMtx.Lock()
		gScope = saved
		gScopeMtx.Unlock()
	}()
	s.untracked = map[string]struct{}{"b/new": {}}
	if scopeDBName("x/new") != "a/old" || scopeDBName("b/new") != "z/old" || scopeDBName("c/keep") != "c/keep" || scopeDBName("not/there") != "not/there" {
		t.Errorf("scopeDBName mismatch")
	}
	if !scopeUntracked("b/new") || scopeUntracked("x/new") || scopeUntracked("c/keep") || scopeUntracked("z/old") {
		t.Errorf("scopeUntracked mismatch")
	}
	gScopeMtx.Lock()
	gScope = nil
	gScopeMtx.Unlock()
	if scopeDBName("x/new") != "x/new" || scopeUntracked("b/new") {
		t.Errorf("no scope: the name itself, nothing untracked")
	}
}

func TestProjectRulesFormerName(t *testing.T) {
	// today's rules: the new organization; historical rules: also the names the repositories had before
	rules := testRules(t, lib.Project{
		CommandLine:     []string{"neworg"},
		HistCommandLine: []string{"neworg,oldorg/repo,oldorg/other"},
		Env:             map[string]string{"GHA2DB_EXCLUDE_REPOS": "neworg/excluded,oldorg/other"},
	})
	if !rules.filter.Active() || !rules.hist.Active() || !rules.hist.HistRules {
		t.Fatalf("rules not active: %+v", rules)
	}
	covers := map[string]bool{
		"neworg/repo":     true,  // today's rules
		"oldorg/repo":     true,  // historical rules only
		"oldorg/unknown":  false, // neither
		"elsewhere/repo":  false,
		"neworg/excluded": false, // excluded names are never tracked
		"oldorg/other":    false,
		"":                false,
	}
	for name, exp := range covers {
		if got := rules.covers(name); got != exp {
			t.Errorf("covers(%q) = %v, expected %v", name, got, exp)
		}
	}
	cases := []struct {
		what, orgRepo, current string
		names                  []string
		expected               string
	}{
		{"renamed now, the database name is tracked", "oldorg/repo", "elsewhere/repo", []string{"oldorg/repo"}, "oldorg/repo"},
		{"renamed now, the database name is tracked (no gha_repos names needed)", "neworg/repo", "elsewhere/repo", nil, "neworg/repo"},
		{"renamed now, another former name is tracked", "third/repo", "elsewhere/repo", []string{"third/repo", "oldorg/repo", "elsewhere/repo"}, "oldorg/repo"},
		{"renamed earlier (same name now), a former name is tracked", "elsewhere/repo", "elsewhere/repo", []string{"elsewhere/repo", "oldorg/repo"}, "oldorg/repo"},
		{"several tracked former names: the alphabetically first", "elsewhere/repo", "elsewhere/repo", []string{"oldorg/repo", "neworg/zzz", "neworg/aaa"}, "neworg/aaa"},
		{"no tracked name at all", "third/repo", "elsewhere/repo", []string{"third/repo", "fourth/repo"}, ""},
		{"only an excluded former name", "oldorg/other", "elsewhere/repo", []string{"oldorg/other", "neworg/excluded"}, ""},
		{"the current name does not count as a former name", "neworg/repo", "neworg/repo", []string{"neworg/repo"}, ""},
		{"no names", "elsewhere/repo", "elsewhere/repo", nil, ""},
	}
	for _, c := range cases {
		if got := rules.formerName(c.orgRepo, c.current, c.names); got != c.expected {
			t.Errorf("%s: formerName(%s, %s, %v) = %q, expected %q", c.what, c.orgRepo, c.current, c.names, got, c.expected)
		}
	}

	// without `hist_command_line` the historical rules are today's rules
	rules = testRules(t, lib.Project{CommandLine: []string{"neworg"}})
	if rules.hist.HistRules || !rules.covers("neworg/repo") || rules.covers("oldorg/repo") {
		t.Errorf("rules without hist_command_line: %+v", rules)
	}
	if got := rules.formerName("oldorg/repo", "elsewhere/repo", []string{"oldorg/repo"}); got != "" {
		t.Errorf("a name outside the only rules is not tracked, got %q", got)
	}

	// no project: nothing is filtered, nothing is excluded (the warning is never evaluated then)
	var ctx lib.Ctx
	none := &projectRules{
		filter: lib.NewProjectFilter(&ctx, nil, "proj", "p.yaml", false),
		hist:   lib.NewProjectFilter(&ctx, nil, "proj", "p.yaml", true),
	}
	if none.filter.Active() || none.filter.RepoExcluded("any/repo") || !none.filter.RepoHit("any/repo") {
		t.Errorf("rules without a project: %+v", none)
	}
}

func TestRepoOwner(t *testing.T) {
	cases := map[string]string{"org/repo": "org", "org/repo/more": "org", "norepo": "norepo", "": "", "/repo": ""}
	for name, exp := range cases {
		if got := repoOwner(name); got != exp {
			t.Errorf("repoOwner(%q) = %q, expected %q", name, got, exp)
		}
	}
}
