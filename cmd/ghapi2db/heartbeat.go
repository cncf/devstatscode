package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	lib "github.com/cncf/devstatscode"
	"github.com/google/go-github/v38/github"
)

// apiPass - the ghapi2db passes that work on a list of repositories
type apiPass int

const (
	passEvents apiPass = iota
	passCommits
	passComments
	passReviews
	passForks
	passReleases
	passStars
	passRepoStats
	passRepoEvents
	passIssuesPRs
)

// label - pass name used in the log lines (the restore passes use the same names)
func (p apiPass) label() string {
	switch p {
	case passEvents:
		return "ghapi2db events"
	case passCommits:
		return "ghapi2db commits"
	case passComments:
		return "ghapi2db comments restore"
	case passReviews:
		return "ghapi2db reviews restore"
	case passForks:
		return "ghapi2db forks restore"
	case passReleases:
		return "ghapi2db releases restore"
	case passStars:
		return "ghapi2db stars restore"
	case passRepoStats:
		return "ghapi2db repo stats"
	case passRepoEvents:
		return "ghapi2db repo events"
	case passIssuesPRs:
		return "ghapi2db issues prs"
	}
	return "ghapi2db"
}

// gate - what the heartbeat must show since the recent date for the pass to process a repository
func (p apiPass) gate() string {
	switch p {
	case passEvents:
		return "issue or PR updates"
	case passCommits:
		return "pushes"
	case passComments:
		return "issue or PR updates or pushes"
	case passReviews:
		return "PR updates"
	case passForks:
		return "forks"
	case passReleases:
		return "releases"
	case passStars:
		return "star changes"
	case passRepoStats:
		return "repository data"
	case passRepoEvents:
		return "activity"
	case passIssuesPRs:
		return "repository data"
	}
	return "activity"
}

// heartbeatBatch - repositories per GraphQL heartbeat query: 100 exceeds GitHub's per-query node
// limit for this selection (RESOURCE_LIMITS_EXCEEDED), 50 costs 2 rate-limit points
const heartbeatBatch = 50

const heartbeatFragment = "fragment F on Repository { databaseId nameWithOwner isArchived pushedAt stargazerCount forkCount watchers { totalCount } " +
	"owner { login ... on User { databaseId } ... on Organization { databaseId } } " +
	"openIssues: issues(states: OPEN) { totalCount } openPRs: pullRequests(states: OPEN) { totalCount } " +
	"issues(last: 1, orderBy: {field: UPDATED_AT, direction: ASC}) { nodes { updatedAt } } " +
	"pullRequests(last: 1, orderBy: {field: UPDATED_AT, direction: ASC}) { nodes { updatedAt } } " +
	"releases(last: 1, orderBy: {field: CREATED_AT, direction: ASC}) { nodes { createdAt publishedAt } } " +
	"forks(last: 1, orderBy: {field: CREATED_AT, direction: ASC}) { nodes { createdAt } } }"

// errGraphQLNoRetry - wrapped by handlers when trying another token cannot help (the query itself is rejected)
var errGraphQLNoRetry = errors.New("graphql query rejected")

// repoHeartbeat - what one GraphQL heartbeat query told about a repository
type repoHeartbeat struct {
	found          bool
	notFound       bool
	moved          bool // resolves to a different repository id than the tracked one
	unknown        bool // heartbeat failed: treated as active in every pass
	archived       bool
	reason         string // not found: the GraphQL error type (NOT_FOUND, FORBIDDEN, ...)
	databaseID     int64
	nameWithOwner  string
	ownerID        int64
	pushedAt       *time.Time
	issueAt        *time.Time
	prAt           *time.Time
	releaseAt      *time.Time
	forkAt         *time.Time
	stargazerCount int64
	forkCount      int64
	watchers       int64
	openIssues     int64
	openPRs        int64
	starSnapshot   *int64 // newest gha_forkees.stargazers_count at or before the recent date
}

func since(t *time.Time, dt time.Time) bool {
	return t != nil && !t.Before(dt)
}

// active - should the pass process the repository
func (h *repoHeartbeat) active(pass apiPass, recentDt time.Time) bool {
	if h == nil || h.unknown {
		return true
	}
	if h.notFound || h.moved {
		return false
	}
	switch pass {
	case passEvents:
		return since(h.issueAt, recentDt) || since(h.prAt, recentDt)
	case passCommits:
		return since(h.pushedAt, recentDt)
	case passComments:
		return since(h.issueAt, recentDt) || since(h.prAt, recentDt) || since(h.pushedAt, recentDt)
	case passReviews:
		return since(h.prAt, recentDt)
	case passForks:
		return since(h.forkAt, recentDt)
	case passReleases:
		return since(h.releaseAt, recentDt)
	case passStars:
		return h.stargazerCount > 0 && (h.starSnapshot == nil || *h.starSnapshot != h.stargazerCount)
	case passRepoStats:
		return true
	case passRepoEvents:
		// the events feed carries every event type: any signal the heartbeat has
		return since(h.issueAt, recentDt) || since(h.prAt, recentDt) || since(h.pushedAt, recentDt) ||
			since(h.forkAt, recentDt) || since(h.releaseAt, recentDt) || h.active(passStars, recentDt)
	case passIssuesPRs:
		// the stub sweep is database-driven (a repository without stub rows costs one query),
		// the listing part gates itself on issue or PR updates
		return true
	}
	return true
}

// repoScope - the repositories ghapi2db works on (computed once per process)
type repoScope struct {
	repos      []string           // current names, sorted
	ids        map[string][]int64 // tracked ids per current name, ascending
	historical []string           // names no longer current for any id
	nIDs       int
	heartbeat  map[string]*repoHeartbeat // nil until the first gated pass
	recentDt   time.Time                 // recent date the heartbeat was evaluated against
}

var (
	gScope    *repoScope
	gScopeMtx sync.Mutex
)

// trackedID - the id a name is reported under when it resolves elsewhere (the newest one)
func (s *repoScope) trackedID(name string) int64 {
	ids := s.ids[name]
	if len(ids) == 0 {
		return 0
	}
	return ids[len(ids)-1]
}

func (s *repoScope) tracks(name string, id int64) bool {
	for _, tid := range s.ids[name] {
		if tid == id {
			return true
		}
	}
	return false
}

// getRepoScope - all tracked repositories (one current name per id), memoized
func getRepoScope(c *sql.DB, ctx *lib.Ctx) *repoScope {
	gScopeMtx.Lock()
	defer gScopeMtx.Unlock()
	if gScope != nil {
		return gScope
	}
	repos, ids, historical := lib.GetTrackedRepos(c, ctx)
	nIDs := make(map[int64]struct{})
	for _, rids := range ids {
		for _, rid := range rids {
			nIDs[rid] = struct{}{}
		}
	}
	gScope = &repoScope{repos: repos, ids: ids, historical: historical, nIDs: len(nIDs)}
	lib.Printf("ghapi2db scope: %d repos from gha_repos (%d ids), %d historical names skipped\n", len(repos), len(nIDs), len(historical))
	if ctx.Debug > 0 {
		lib.Printf("Repos to process (all tracked): %v\n", repos)
		lib.Printf("Historical names skipped: %v\n", historical)
	}
	return gScope
}

// wellFormed - "org/repo" with the characters GitHub allows, safe to embed in a GraphQL query
func wellFormed(orgRepo string) (org, repo string, ok bool) {
	ary := strings.Split(orgRepo, "/")
	if len(ary) != 2 || ary[0] == "" || ary[1] == "" {
		return
	}
	for _, part := range ary {
		for _, r := range part {
			if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
				return
			}
		}
	}
	return ary[0], ary[1], true
}

// heartbeatQuery - one GraphQL query for a batch of repositories aliased r0..rN
func heartbeatQuery(batch []string) []byte {
	var b strings.Builder
	b.WriteString("query { rateLimit { cost remaining }")
	for i, orgRepo := range batch {
		org, repo, _ := wellFormed(orgRepo)
		fmt.Fprintf(&b, " r%d: repository(owner: \"%s\", name: \"%s\") { ...F }", i, org, repo)
	}
	b.WriteString(" } ")
	b.WriteString(heartbeatFragment)
	payload, err := json.Marshal(map[string]interface{}{"query": b.String()})
	lib.FatalOnError(err)
	return payload
}

type heartbeatNode struct {
	DatabaseID     int64      `json:"databaseId"`
	NameWithOwner  string     `json:"nameWithOwner"`
	IsArchived     bool       `json:"isArchived"`
	PushedAt       *time.Time `json:"pushedAt"`
	StargazerCount int64      `json:"stargazerCount"`
	ForkCount      int64      `json:"forkCount"`
	Watchers       struct {
		TotalCount int64 `json:"totalCount"`
	} `json:"watchers"`
	Owner struct {
		Login      string `json:"login"`
		DatabaseID int64  `json:"databaseId"`
	} `json:"owner"`
	OpenIssues struct {
		TotalCount int64 `json:"totalCount"`
	} `json:"openIssues"`
	OpenPRs struct {
		TotalCount int64 `json:"totalCount"`
	} `json:"openPRs"`
	Issues struct {
		Nodes []struct {
			UpdatedAt *time.Time `json:"updatedAt"`
		} `json:"nodes"`
	} `json:"issues"`
	PullRequests struct {
		Nodes []struct {
			UpdatedAt *time.Time `json:"updatedAt"`
		} `json:"nodes"`
	} `json:"pullRequests"`
	Releases struct {
		Nodes []struct {
			CreatedAt   *time.Time `json:"createdAt"`
			PublishedAt *time.Time `json:"publishedAt"`
		} `json:"nodes"`
	} `json:"releases"`
	Forks struct {
		Nodes []struct {
			CreatedAt *time.Time `json:"createdAt"`
		} `json:"nodes"`
	} `json:"forks"`
}

type heartbeatResponse struct {
	Data   map[string]json.RawMessage `json:"data"`
	Errors []struct {
		Type    string        `json:"type"`
		Message string        `json:"message"`
		Path    []interface{} `json:"path"`
	} `json:"errors"`
}

// decodeHeartbeat - the heartbeat of every repository in the batch (nil: batch failed, retry with another token)
func decodeHeartbeat(body []byte, batch []string, tokenIdx, nTokens int) (hbs []*repoHeartbeat, err error) {
	var out heartbeatResponse
	if err = json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	perAlias := make(map[string]string)
	for _, e := range out.Errors {
		if e.Type == "RESOURCE_LIMITS_EXCEEDED" || strings.Contains(e.Message, "exceeds the maximum node limit") {
			return nil, fmt.Errorf("%w: %s", errGraphQLNoRetry, e.Message)
		}
		if len(e.Path) == 1 {
			if alias, ok := e.Path[0].(string); ok {
				if e.Type == "" {
					perAlias[alias] = e.Message
				} else {
					perAlias[alias] = e.Type
				}
				continue
			}
		}
		return nil, fmt.Errorf("graphql (token %d/%d): %s", tokenIdx+1, nTokens, e.Message)
	}
	if out.Data == nil {
		return nil, fmt.Errorf("graphql (token %d/%d): no data", tokenIdx+1, nTokens)
	}
	for i := range batch {
		alias := fmt.Sprintf("r%d", i)
		hb := &repoHeartbeat{}
		raw, ok := out.Data[alias]
		if !ok || string(raw) == "null" {
			hb.notFound = true
			hb.reason = perAlias[alias]
			if hb.reason == "" {
				hb.reason = "null"
			}
			hbs = append(hbs, hb)
			continue
		}
		var node heartbeatNode
		if err = json.Unmarshal(raw, &node); err != nil {
			return nil, err
		}
		hb.found = true
		hb.databaseID = node.DatabaseID
		hb.nameWithOwner = node.NameWithOwner
		hb.ownerID = node.Owner.DatabaseID
		hb.archived = node.IsArchived
		hb.pushedAt = node.PushedAt
		hb.stargazerCount = node.StargazerCount
		hb.forkCount = node.ForkCount
		hb.watchers = node.Watchers.TotalCount
		hb.openIssues = node.OpenIssues.TotalCount
		hb.openPRs = node.OpenPRs.TotalCount
		if len(node.Issues.Nodes) > 0 {
			hb.issueAt = node.Issues.Nodes[0].UpdatedAt
		}
		if len(node.PullRequests.Nodes) > 0 {
			hb.prAt = node.PullRequests.Nodes[0].UpdatedAt
		}
		if len(node.Releases.Nodes) > 0 {
			rel := node.Releases.Nodes[0]
			hb.releaseAt = rel.CreatedAt
			if rel.PublishedAt != nil && (hb.releaseAt == nil || rel.PublishedAt.After(*hb.releaseAt)) {
				hb.releaseAt = rel.PublishedAt
			}
		}
		if len(node.Forks.Nodes) > 0 {
			hb.forkAt = node.Forks.Nodes[0].CreatedAt
		}
		hbs = append(hbs, hb)
	}
	return hbs, nil
}

// heartbeatBatchQuery - heartbeat of one batch, halving it when GitHub rejects the query size
// Returns the number of GraphQL queries made and nil heartbeats for repositories GitHub could not tell about
func heartbeatBatchQuery(gctx context.Context, ctx *lib.Ctx, tokens []string, batch []string, start int, warn func(string)) (hbs []*repoHeartbeat, nQueries int) {
	if len(batch) == 0 {
		return
	}
	var got []*repoHeartbeat
	err := ghGraphQLPost(gctx, ctx, tokens, start, "ghapi2db heartbeat", heartbeatQuery(batch), func(body []byte, tokenIdx int) error {
		var herr error
		got, herr = decodeHeartbeat(body, batch, tokenIdx, len(tokens))
		return herr
	})
	nQueries = 1
	if err == nil {
		return got, nQueries
	}
	if errors.Is(err, errGraphQLNoRetry) && len(batch) > 1 {
		half := len(batch) / 2
		left, nl := heartbeatBatchQuery(gctx, ctx, tokens, batch[:half], start, warn)
		right, nr := heartbeatBatchQuery(gctx, ctx, tokens, batch[half:], start+1, warn)
		return append(left, right...), nQueries + nl + nr
	}
	warn(fmt.Sprintf("WARNING: ghapi2db heartbeat: %d repos unknown (%+v), processing them in every pass\n", len(batch), err))
	return make([]*repoHeartbeat, len(batch)), nQueries
}

// starSnapshot - newest gha_forkees.stargazers_count of the repository at or before the recent date
// The snapshot time is updated_at (the repository data time, what watchers_by_alias.sql uses too):
// the counters-less rows GH Archive writes since 2024-09 (updated_at 0001-01-01) sort last
func starSnapshot(c *sql.DB, ctx *lib.Ctx, repoID int64, recentDt time.Time) *int64 {
	rows := lib.QuerySQLWithErr(
		c,
		ctx,
		fmt.Sprintf(
			"select stargazers_count from gha_forkees where id = %s and updated_at <= %s "+
				"order by updated_at desc, event_id desc limit 1",
			lib.NValue(1),
			lib.NValue(2),
		),
		repoID,
		recentDt,
	)
	defer func() { lib.FatalOnError(rows.Close()) }()
	var snapshot *int64
	for rows.Next() {
		var cnt sql.NullInt64
		lib.FatalOnError(rows.Scan(&cnt))
		if cnt.Valid {
			v := cnt.Int64
			snapshot = &v
		}
	}
	lib.FatalOnError(rows.Err())
	return snapshot
}

// runHeartbeat - ask GitHub (GraphQL, batches of repositories) what happened in every tracked repository
// since the recent date, memoized: evaluated once per process by the first gated pass
func (s *repoScope) runHeartbeat(gctx context.Context, ctx *lib.Ctx, c *sql.DB, recentDt time.Time) {
	if s.heartbeat != nil {
		return
	}
	s.heartbeat = make(map[string]*repoHeartbeat)
	s.recentDt = recentDt
	tokens := ghTokens(ctx)
	var queried, malformed []string
	for _, orgRepo := range s.repos {
		if _, _, ok := wellFormed(orgRepo); ok {
			queried = append(queried, orgRepo)
		} else {
			malformed = append(malformed, orgRepo)
			s.heartbeat[orgRepo] = &repoHeartbeat{notFound: true, reason: "malformed name"}
		}
	}
	if len(tokens) == 0 {
		lib.Printf("WARNING: ghapi2db heartbeat needs GHA2DB_GITHUB_OAUTH token(s), processing every repo in every pass\n")
		for _, orgRepo := range queried {
			s.heartbeat[orgRepo] = &repoHeartbeat{unknown: true}
		}
		return
	}
	var batches [][]string
	for i := 0; i < len(queried); i += heartbeatBatch {
		end := i + heartbeatBatch
		if end > len(queried) {
			end = len(queried)
		}
		batches = append(batches, queried[i:end])
	}
	mtx := &sync.Mutex{}
	var warnings []string
	warn := func(msg string) {
		mtx.Lock()
		warnings = append(warnings, msg)
		mtx.Unlock()
	}
	nQueries := 0
	results := make([][]*repoHeartbeat, len(batches))
	processBatch := func(ch chan struct{}, i int) {
		hbs, n := heartbeatBatchQuery(gctx, ctx, tokens, batches[i], i, warn)
		mtx.Lock()
		results[i] = hbs
		nQueries += n
		mtx.Unlock()
		if ch != nil {
			ch <- struct{}{}
		}
	}
	thrN := lib.GetThreadsNum(ctx)
	if thrN > 1 {
		ch := make(chan struct{})
		nThreads := 0
		for i := range batches {
			go processBatch(ch, i)
			nThreads++
			if nThreads >= thrN {
				<-ch
				nThreads--
			}
		}
		for nThreads > 0 {
			<-ch
			nThreads--
		}
	} else {
		for i := range batches {
			processBatch(nil, i)
		}
	}
	sort.Strings(warnings)
	for _, msg := range warnings {
		lib.Printf("%s", msg)
	}
	var (
		found, notFound, moved, unknown, archived   int
		pushes, issues, prs, forks, releases, stars int
		lines                                       []string
	)
	notFound += len(malformed)
	if ctx.Debug > 0 {
		for _, orgRepo := range malformed {
			lines = append(lines, fmt.Sprintf("%s: not found on GitHub (%s), skipping\n", orgRepo, s.heartbeat[orgRepo].reason))
		}
	}
	for i, batch := range batches {
		for j, orgRepo := range batch {
			hb := results[i][j]
			if hb == nil {
				hb = &repoHeartbeat{unknown: true}
			}
			s.heartbeat[orgRepo] = hb
			switch {
			case hb.unknown:
				unknown++
			case hb.notFound:
				notFound++
				if ctx.Debug > 0 {
					lines = append(lines, fmt.Sprintf("%s: not found on GitHub (%s), skipping\n", orgRepo, hb.reason))
				}
			case !s.tracks(orgRepo, hb.databaseID):
				hb.moved = true
				moved++
				lines = append(lines, fmt.Sprintf("WARNING: %s: resolves to %s (id %d) but is tracked as id %d, skipping\n", orgRepo, hb.nameWithOwner, hb.databaseID, s.trackedID(orgRepo)))
			default:
				found++
				if hb.archived {
					archived++
				}
				if hb.nameWithOwner != orgRepo && ctx.Debug > 0 {
					lines = append(lines, fmt.Sprintf("%s: renamed to %s on GitHub\n", orgRepo, hb.nameWithOwner))
				}
				if since(hb.pushedAt, recentDt) {
					pushes++
				}
				if since(hb.issueAt, recentDt) {
					issues++
				}
				if since(hb.prAt, recentDt) {
					prs++
				}
				if since(hb.forkAt, recentDt) {
					forks++
				}
				if since(hb.releaseAt, recentDt) {
					releases++
				}
				if hb.stargazerCount > 0 {
					hb.starSnapshot = starSnapshot(c, ctx, hb.databaseID, recentDt)
				}
				if hb.active(passStars, recentDt) {
					stars++
				}
			}
		}
	}
	sort.Strings(lines)
	for _, line := range lines {
		lib.Printf("%s", line)
	}
	lib.Printf(
		"ghapi2db heartbeat: %d repos in %d GraphQL queries: %d found, %d not found, %d moved, %d unknown, %d archived; active since %v: pushes %d, issues %d, PRs %d, forks %d, releases %d, stars %d\n",
		len(s.repos), nQueries, found, notFound, moved, unknown, archived, recentDt, pushes, issues, prs, forks, releases, stars,
	)
}

// scopeRepos - the repositories a pass should process and how many the heartbeat skipped (-1: not gated)
// Gates are bypassed in single repository mode (REPO) and in date range mode (DTFROM/DTTO, events and commits passes)
func scopeRepos(gctx context.Context, ctx *lib.Ctx, c *sql.DB, pass apiPass, recentDt time.Time) (repos []string, skipped int) {
	s := getRepoScope(c, ctx)
	gated := os.Getenv("REPO") == ""
	if gated && (pass == passEvents || pass == passCommits) && (os.Getenv("DTFROM") != "" || os.Getenv("DTTO") != "") {
		gated = false
	}
	if !gated {
		return s.repos, -1
	}
	gScopeMtx.Lock()
	s.runHeartbeat(gctx, ctx, c, recentDt)
	gScopeMtx.Unlock()
	for _, orgRepo := range s.repos {
		hb := s.heartbeat[orgRepo]
		if hb.active(pass, s.recentDt) {
			repos = append(repos, orgRepo)
			continue
		}
		skipped++
		if ctx.Debug > 0 && hb.found && !hb.moved {
			lib.Printf("%s: %s: skipped by heartbeat (no %s since %v)\n", pass.label(), orgRepo, pass.gate(), s.recentDt)
		}
	}
	return
}

// scopeSuffix - " (heartbeat: N skipped)" for gated passes, "" otherwise
func scopeSuffix(skipped int) string {
	if skipped < 0 {
		return ""
	}
	return fmt.Sprintf(" (heartbeat: %d skipped)", skipped)
}

// Repository counters pass (sync_repo_stats): one gha_forkees snapshot per repository per run.
// GH Archive used to deliver the repository counters with every PR event (base/head repository objects),
// since 2024-09 those objects carry only ids and names and since 2025-10 nothing at all, so
// watchers_by_alias.sql ("Community stats": stars, forks, open issues per repository group) starves.
// The snapshot is attached to the newest gha_events row of the repository (gha_forkees is keyed
// (id, event_id)) like the PR events' snapshots were, with GH Archive semantics: watchers = stargazers,
// open issues include PRs, updated_at = the snapshot time, dup_* = the event's columns.
// Counters come from the heartbeat (no extra API call); GET /repos/{owner}/{repo} is used when the
// heartbeat did not tell about the repository (single repository mode, legacy scope, heartbeat failure).

// repoCounters - what the snapshot row carries
type repoCounters struct {
	id         int64
	name       string
	fullName   string
	ownerID    int64
	stars      int64
	forks      int64
	openIssues int64
	source     string // heartbeat or API
}

// counters - the snapshot counters from the heartbeat (nil: the heartbeat did not tell about the repository)
func (h *repoHeartbeat) counters() *repoCounters {
	if h == nil || !h.found || h.moved {
		return nil
	}
	return &repoCounters{
		id:         h.databaseID,
		name:       shortRepoName(h.nameWithOwner),
		fullName:   h.nameWithOwner,
		ownerID:    h.ownerID,
		stars:      h.stargazerCount,
		forks:      h.forkCount,
		openIssues: h.openIssues + h.openPRs,
		source:     "heartbeat",
	}
}

func shortRepoName(fullName string) string {
	if i := strings.LastIndex(fullName, "/"); i >= 0 {
		return fullName[i+1:]
	}
	return fullName
}

// apiCounters - the snapshot counters from GET /repos/{owner}/{repo} (REST reports open issues including PRs)
func apiCounters(repo *github.Repository) *repoCounters {
	if repo == nil || repo.ID == nil {
		return nil
	}
	return &repoCounters{
		id:         repo.GetID(),
		name:       repo.GetName(),
		fullName:   repo.GetFullName(),
		ownerID:    repo.GetOwner().GetID(),
		stars:      int64(repo.GetStargazersCount()),
		forks:      int64(repo.GetForksCount()),
		openIssues: int64(repo.GetOpenIssuesCount()),
		source:     "API",
	}
}

// heartbeatOf - the heartbeat of a repository, nil when no heartbeat was evaluated (single repository mode, legacy scope)
func heartbeatOf(orgRepo string) *repoHeartbeat {
	gScopeMtx.Lock()
	defer gScopeMtx.Unlock()
	if gScope == nil || gScope.heartbeat == nil {
		return nil
	}
	return gScope.heartbeat[orgRepo]
}

// trackedRepo - is the repository id the one tracked under the name (scope ids, else the events/gha_repos id)
func trackedRepo(c *sql.DB, ctx *lib.Ctx, orgRepo string, id int64) bool {
	gScopeMtx.Lock()
	s := gScope
	gScopeMtx.Unlock()
	if s != nil {
		return s.tracks(orgRepo, id)
	}
	rid, _ := repoIDs(c, ctx, orgRepo)
	return rid == id
}

// lastEvent - the newest gha_events row of the repository: under its current name, else under any name
func lastEvent(c *sql.DB, ctx *lib.Ctx, repoID int64, orgRepo string) (eventID int64, createdAt time.Time, actorID int64, ok bool) {
	queries := []struct {
		sql  string
		args []interface{}
	}{
		{
			"select id, created_at, actor_id from gha_events where repo_id = " + lib.NValue(1) + " and dup_repo_name = " + lib.NValue(2) +
				" order by created_at desc, id desc limit 1",
			[]interface{}{repoID, orgRepo},
		},
		{
			"select id, created_at, actor_id from gha_events where repo_id = " + lib.NValue(1) + " order by created_at desc, id desc limit 1",
			[]interface{}{repoID},
		},
	}
	for _, q := range queries {
		rows := lib.QuerySQLWithErr(c, ctx, q.sql, q.args...)
		for rows.Next() {
			lib.FatalOnError(rows.Scan(&eventID, &createdAt, &actorID))
			ok = true
		}
		lib.FatalOnError(rows.Err())
		lib.FatalOnError(rows.Close())
		if ok {
			return
		}
	}
	return
}

// writeRepoStats - upserts the snapshot row, true when inserted (false: the (id, event_id) row existed and was refreshed)
func writeRepoStats(c *sql.DB, ctx *lib.Ctx, cnt *repoCounters, orgRepo string, eventID int64, createdAt time.Time, actorID int64, now time.Time) bool {
	rows := lib.QuerySQLWithErr(
		c,
		ctx,
		fmt.Sprintf(
			"insert into gha_forkees(id, event_id, name, full_name, owner_id, updated_at, stargazers_count, forks, open_issues, watchers, "+
				"dup_actor_id, dup_repo_id, dup_repo_name, dup_created_at) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "+
				"on conflict (id, event_id) do update set name = excluded.name, full_name = excluded.full_name, owner_id = excluded.owner_id, "+
				"updated_at = excluded.updated_at, stargazers_count = excluded.stargazers_count, forks = excluded.forks, "+
				"open_issues = excluded.open_issues, watchers = excluded.watchers returning xmax = 0",
			lib.NValue(1), lib.NValue(2), lib.NValue(3), lib.NValue(4), lib.NValue(5), lib.NValue(6), lib.NValue(7),
			lib.NValue(8), lib.NValue(9), lib.NValue(10), lib.NValue(11), lib.NValue(12), lib.NValue(13), lib.NValue(14),
		),
		cnt.id,
		eventID,
		lib.TruncToBytes(cnt.name, 80),
		lib.TruncToBytes(cnt.fullName, 200),
		cnt.ownerID,
		now,
		cnt.stars,
		cnt.forks,
		cnt.openIssues,
		cnt.stars,
		actorID,
		cnt.id,
		orgRepo,
		createdAt,
	)
	inserted := false
	for rows.Next() {
		lib.FatalOnError(rows.Scan(&inserted))
	}
	lib.FatalOnError(rows.Err())
	lib.FatalOnError(rows.Close())
	return inserted
}

// repoStatsCounts - the pass summary counters
type repoStatsCounts struct {
	inserted, updated, noEvents, unavailable, apiCalls int
}

// syncRepoStats - write the repository counters snapshots (sync_repo_stats)
func syncRepoStats(ctx *lib.Ctx) {
	name := passRepoStats.label()
	repos, skipped, isSingleRepo, singleRepo, gctx, gcs, c, _ := getAPIParams(ctx, passRepoStats)
	defer func() { lib.FatalOnError(c.Close()) }()
	nRepos := len(repos)
	lib.Printf("%s: processing %d repos%s\n", name, nRepos, scopeSuffix(skipped))
	now := time.Now().Truncate(time.Second)
	thrN := lib.GetThreadsNum(ctx)
	mtx := &sync.Mutex{}
	counts := repoStatsCounts{}
	hint := -1
	processed := 0
	lastTime := time.Now()
	dtStart := lastTime
	freq := time.Duration(30) * time.Second
	// client - the GitHub client to use, rate limits (re)checked every 20 API calls
	client := func() *github.Client {
		mtx.Lock()
		defer mtx.Unlock()
		if hint < 0 || counts.apiCalls%20 == 0 {
			h, _, r, w := lib.GetRateLimits(gctx, ctx, gcs, true)
			if r[h] <= ctx.MinGHAPIPoints {
				if w[h].Seconds() <= float64(ctx.MaxGHAPIWaitSeconds) {
					lib.Printf("%s: API limit reached, waiting %v\n", name, w[h])
					time.Sleep(w[h])
				} else if ctx.GHAPIErrorIsFatal {
					lib.Fatalf("%s: API limit reached, don't want to wait %v", name, w[h])
				} else {
					lib.Printf("%s: API limit reached, don't want to wait %v\n", name, w[h])
				}
				h, _, _, _ = lib.GetRateLimits(gctx, ctx, gcs, true)
			}
			hint = h
		}
		counts.apiCalls++
		return gcs[hint]
	}
	fetchCounters := func(orgRepo string) (cnt *repoCounters) {
		org, repo, ok := wellFormed(orgRepo)
		if !ok {
			lib.Printf("WARNING: %s: malformed repo name: '%s'\n", name, orgRepo)
			return
		}
		cl := client()
		apiPage(ctx, orgRepo+" repository", func() (*github.Response, bool, error) {
			r, resp, err := cl.Repositories.Get(gctx, org, repo)
			if err != nil || resp == nil || resp.StatusCode >= 400 {
				return resp, false, err
			}
			cnt = apiCounters(r)
			return resp, false, nil
		})
		return
	}
	processRepo := func(ch chan struct{}, orgRepo string) {
		defer func() {
			if ch != nil {
				ch <- struct{}{}
			}
		}()
		hb := heartbeatOf(orgRepo)
		cnt := hb.counters()
		if cnt == nil {
			if hb != nil && !hb.unknown {
				return
			}
			cnt = fetchCounters(orgRepo)
			if cnt == nil {
				mtx.Lock()
				counts.unavailable++
				mtx.Unlock()
				return
			}
			if !trackedRepo(c, ctx, orgRepo, cnt.id) {
				lib.Printf("WARNING: %s: %s: resolves to %s (id %d) which is not tracked, skipping\n", name, orgRepo, cnt.fullName, cnt.id)
				mtx.Lock()
				counts.unavailable++
				mtx.Unlock()
				return
			}
		}
		eventID, createdAt, actorID, ok := lastEvent(c, ctx, cnt.id, orgRepo)
		if !ok {
			if ctx.Debug > 0 {
				lib.Printf("%s: %s: no events, skipping\n", name, orgRepo)
			}
			mtx.Lock()
			counts.noEvents++
			mtx.Unlock()
			return
		}
		verb := "refreshed"
		inserted := writeRepoStats(c, ctx, cnt, orgRepo, eventID, createdAt, actorID, now)
		mtx.Lock()
		if inserted {
			counts.inserted++
			verb = "inserted"
		} else {
			counts.updated++
		}
		mtx.Unlock()
		if ctx.Debug > 0 {
			lib.Printf("%s: %s: %d stars, %d forks, %d open issues from the %s, snapshot %s (event %d)\n", name, orgRepo, cnt.stars, cnt.forks, cnt.openIssues, cnt.source, verb, eventID)
		}
	}
	iter := func() {
		processed++
		mtx.Lock()
		msg := fmt.Sprintf("%s: API calls: %d", name, counts.apiCalls)
		mtx.Unlock()
		lib.ProgressInfo(processed, nRepos, dtStart, &lastTime, freq, msg)
	}
	if thrN > 1 {
		ch := make(chan struct{})
		nThreads := 0
		for _, repo := range repos {
			if isSingleRepo && repo != singleRepo {
				continue
			}
			go processRepo(ch, repo)
			nThreads++
			for nThreads >= thrN {
				<-ch
				nThreads--
				iter()
			}
		}
		for nThreads > 0 {
			<-ch
			nThreads--
			iter()
		}
	} else {
		for _, repo := range repos {
			if isSingleRepo && repo != singleRepo {
				continue
			}
			processRepo(nil, repo)
			iter()
		}
	}
	lib.Printf(
		"%s: processed %d repos, snapshots: %d inserted, %d refreshed; skipped: %d without events, %d unavailable; GH API calls: %d\n",
		name, processed, counts.inserted, counts.updated, counts.noEvents, counts.unavailable, counts.apiCalls,
	)
}
