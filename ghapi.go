package devstatscode

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/go-github/v38/github"
	"golang.org/x/oauth2"
)

// IssueConfig - holds issue data
type IssueConfig struct {
	Repo         string
	Number       int
	IssueID      int64
	Pr           bool
	MilestoneID  *int64
	Labels       string
	LabelsMap    map[int64]string
	GhIssue      *github.Issue
	CreatedAt    time.Time
	EventID      int64
	EventType    string
	GhEvent      *github.IssueEvent
	AssigneeID   *int64
	Assignees    string
	AssigneesMap map[int64]string
}

func (ic IssueConfig) String() string {
	var (
		milestoneID int64
		assigneeID  int64
	)
	if ic.MilestoneID != nil {
		milestoneID = *ic.MilestoneID
	}
	if ic.AssigneeID != nil {
		assigneeID = *ic.AssigneeID
	}
	return fmt.Sprintf(
		"{Repo: %s, Number: %d, IssueID: %d, EventID: %d, EventType: %s, Pr: %v, MilestoneID: %d, AssigneeID: %d, CreatedAt: %s, Labels: %s, LabelsMap: %+v, Assignees: %s, AssigneesMap: %+v}",
		ic.Repo,
		ic.Number,
		ic.IssueID,
		ic.EventID,
		ic.EventType,
		ic.Pr,
		milestoneID,
		assigneeID,
		ToYMDHMSDate(ic.CreatedAt),
		ic.Labels,
		ic.LabelsMap,
		ic.Assignees,
		ic.AssigneesMap,
	)
}

func (ic IssueConfig) configStr() string {
	var (
		milestoneID int64
		assigneeID  int64
	)
	if ic.MilestoneID != nil {
		milestoneID = *ic.MilestoneID
	}
	if ic.AssigneeID != nil {
		assigneeID = *ic.AssigneeID
	}
	return fmt.Sprintf(
		"{Repo: %s, Number: %d, IssueID: %d, MilestoneID: %d, AssigneeID: %d, Labels: %s, Assignees: %s}",
		ic.Repo,
		ic.Number,
		ic.IssueID,
		milestoneID,
		assigneeID,
		ic.Labels,
		ic.Assignees,
	)
}

// outputIssuesInfo: display summary of issues data to process
func outputIssuesInfo(issues map[int64]IssueConfigAry, info string) {
	Printf("%s:\n", info)
	eids := make(map[int64][2]int64)
	data := make(map[string][]string)
	for _, cfgAry := range issues {
		for _, cfg := range cfgAry {
			eid := cfg.EventID
			_, o := eids[eid]
			if o {
				eids[eid] = [2]int64{*cfg.GhIssue.ID, eids[eid][1] + 1}
			} else {
				eids[eid] = [2]int64{*cfg.GhIssue.ID, 1}
			}
			key := fmt.Sprintf("%s %d", cfg.Repo, cfg.Number)
			val := fmt.Sprintf("%s %s", ToYMDHMSDate(cfg.CreatedAt), cfg.EventType)
			_, ok := data[key]
			if ok {
				data[key] = append(data[key], val)
			} else {
				data[key] = []string{val}
			}
		}
	}
	keys := []string{}
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		values := data[key]
		svalues := []string{}
		for _, value := range values {
			svalues = append(svalues, value)
		}
		sort.Strings(svalues)
		Printf("%s: [%s]\n", key, strings.Join(svalues, ", "))
	}
	for eid, na := range eids {
		if na[1] > 1 {
			Printf("Warning: Duplicate event %d(%d): %v\n", eid, na[1], issues[na[0]])
		}
	}
	for _, cfgAry := range issues {
		l := len(cfgAry)
		for i := 0; i < l; i++ {
			for j := i + 1; j < l; j++ {
				stateA := cfgAry[i].configStr()
				stateB := cfgAry[j].configStr()
				if stateA != stateB {
					Printf("StateA: %v\n", stateA)
					Printf("StateB: %v\n\n", stateB)
				}
			}
		}
	}
}

// outputPRsInfo: display summary of PRs data to process
func outputPRsInfo(prs map[int64]github.PullRequest, info string) {
	Printf("%s:\n", info)
	infos := []string{}
	for prid, pr := range prs {
		if pr.Number != nil && pr.Base != nil && pr.Base.Repo != nil && pr.Base.Repo.FullName != nil {
			infos = append(infos, fmt.Sprintf("%s %d", *pr.Base.Repo.FullName, *pr.Number))
		} else {
			infos = append(infos, fmt.Sprintf("<%d>", prid))
		}
	}
	sort.Strings(infos)
	Printf("PRs: %s\n", strings.Join(infos, ", "))
}

// outputInfo: displays messages gathered in the map
func outputInfo(infos map[string][]string, info string) {
	Printf("%s:\n", info)
	keys := []string{}
	for key := range infos {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		msgs := infos[key]
		sort.Strings(msgs)
		Printf("%s:\n\t%s\n", key, strings.Join(msgs, "\n\t"))
	}
}

// IssueConfigAry - allows sorting IssueConfig array by IssueID annd then event creation date
type IssueConfigAry []IssueConfig

func (ic IssueConfigAry) Len() int      { return len(ic) }
func (ic IssueConfigAry) Swap(i, j int) { ic[i], ic[j] = ic[j], ic[i] }
func (ic IssueConfigAry) Less(i, j int) bool {
	if ic[i].IssueID != ic[j].IssueID {
		return ic[i].IssueID < ic[j].IssueID
	}
	if ic[i].CreatedAt != ic[j].CreatedAt {
		return ic[i].CreatedAt.Before(ic[j].CreatedAt)
	}
	return ic[i].EventID < ic[j].EventID
}

// rateLimitsCacheEntry - GetRateLimits result cached for `GHA2DB_GHAPI_RATE_LIMITS_CACHE` seconds
type rateLimitsCacheEntry struct {
	at         time.Time
	nClients   int
	limits     []int
	remainings []int
	durations  []time.Duration
}

var (
	// rateLimitsCache - cached GetRateLimits results, key: core flag (true: Core limits, false: Search limits)
	rateLimitsCache = map[bool]*rateLimitsCacheEntry{}
	// rateLimitsCacheMutex - protects rateLimitsCache, it is held while polling GitHub too, so concurrent
	// callers wait for one poll instead of all polling at once
	rateLimitsCacheMutex = &sync.Mutex{}
)

// InvalidateRateLimitsCache - drops cached GetRateLimits results, so the next call polls GitHub again
// called when a rate limit/abuse error is detected, so exhausted tokens are re-checked immediately
func InvalidateRateLimitsCache() {
	rateLimitsCacheMutex.Lock()
	rateLimitsCache = map[bool]*rateLimitsCacheEntry{}
	rateLimitsCacheMutex.Unlock()
}

// rateLimitsHint - returns index of the client with most remaining API points
// ties are won by the client whose limit resets sooner
func rateLimitsHint(remainings []int, durations []time.Duration) int {
	hint := 0
	for idx := range remainings {
		if remainings[idx] > remainings[hint] {
			hint = idx
		} else if idx != hint && remainings[idx] == remainings[hint] && durations[idx] < durations[hint] {
			hint = idx
		}
	}
	return hint
}

// pollRateLimits - asks all clients for their rate limits (concurrently, one /rate_limit call per client)
// error messages are printed in clients order after all calls finish, all durations are computed
// against the same "now", so equally loaded clients tie exactly (and the first one wins the hint)
func pollRateLimits(gctx context.Context, gcs []*github.Client, core bool) (limits, remainings []int, durations []time.Duration) {
	n := len(gcs)
	limits = make([]int, n)
	remainings = make([]int, n)
	durations = make([]time.Duration, n)
	msgs := make([]string, n)
	resets := make([]*time.Time, n)
	wg := &sync.WaitGroup{}
	for idx, gc := range gcs {
		wg.Add(1)
		go func(idx int, gc *github.Client) {
			defer wg.Done()
			rl, _, err := gc.RateLimits(gctx)
			if err != nil {
				rem, ok := PeriodParse(err.Error())
				if ok {
					msgs[idx] = fmt.Sprintf("Parsed wait time from error message: %v\n", rem)
					limits[idx], remainings[idx], durations[idx] = -1, -1, rem
					return
				}
				msgs[idx] = fmt.Sprintf("GetRateLimit(%d): %v\n", idx, err)
			}
			if rl == nil {
				limits[idx], remainings[idx], durations[idx] = -1, -1, time.Duration(5)*time.Second
				return
			}
			rate := rl.Core
			if !core {
				rate = rl.Search
			}
			limits[idx] = rate.Limit
			remainings[idx] = rate.Remaining
			reset := rate.Reset.Time
			resets[idx] = &reset
		}(idx, gc)
	}
	wg.Wait()
	now := time.Now()
	for idx, reset := range resets {
		if reset != nil {
			durations[idx] = reset.Sub(now) + time.Duration(1)*time.Second
		}
	}
	for _, msg := range msgs {
		if msg != "" {
			Printf("%s", msg)
		}
	}
	return
}

// GetRateLimits - returns all and remaining API points and duration to wait for reset
// when core=true - returns Core limits, when core=false returns Search limits
// Results are cached for ctx.GHAPIRateLimitsCache seconds (GHA2DB_GHAPI_RATE_LIMITS_CACHE, 0 disables the cache):
// every call is assumed to be followed by one API call using the hinted client, so cached remaining points
// of that client are decreased by one, cached durations are shortened by the elapsed time.
// Cache is not used (GitHub is polled) when it says that the best client has ctx.MinGHAPIPoints or less
// points left or that its limit was already reset, so waiting for the reset/aborting is always decided
// using fresh data.
func GetRateLimits(gctx context.Context, ctx *Ctx, gcs []*github.Client, core bool) (int, []int, []int, []time.Duration) {
	var (
		limits     []int
		remainings []int
		durations  []time.Duration
	)
	ttl := time.Duration(ctx.GHAPIRateLimitsCache) * time.Second
	if ttl > 0 {
		rateLimitsCacheMutex.Lock()
		defer rateLimitsCacheMutex.Unlock()
		cached := false
		entry, ok := rateLimitsCache[core]
		if ok && entry.nClients == len(gcs) && len(gcs) > 0 {
			elapsed := time.Since(entry.at)
			hint := rateLimitsHint(entry.remainings, entry.durations)
			if elapsed < ttl && entry.remainings[hint] > ctx.MinGHAPIPoints && entry.durations[hint] > elapsed {
				limits = append(limits, entry.limits...)
				remainings = append(remainings, entry.remainings...)
				for _, d := range entry.durations {
					durations = append(durations, d-elapsed)
				}
				entry.remainings[hint]--
				cached = true
			}
		}
		if !cached {
			limits, remainings, durations = pollRateLimits(gctx, gcs, core)
			entry := &rateLimitsCacheEntry{
				at:         time.Now(),
				nClients:   len(gcs),
				limits:     append([]int{}, limits...),
				remainings: append([]int{}, remainings...),
				durations:  append([]time.Duration{}, durations...),
			}
			if len(gcs) > 0 {
				// This call is followed by an API call using the hinted client too
				entry.remainings[rateLimitsHint(remainings, durations)]--
			}
			rateLimitsCache[core] = entry
		}
	} else {
		limits, remainings, durations = pollRateLimits(gctx, gcs, core)
	}
	hint := rateLimitsHint(remainings, durations)
	if ctx.GitHubDebug > 0 {
		Printf("GetRateLimits: hint: %d, limits: %+v, remaining: %+v, reset: %+v\n", hint, limits, remainings, durations)
	}
	return hint, limits, remainings, durations
}

// GHClient - get GitHub client
func GHClient(ctx *Ctx) (ghCtx context.Context, clients []*github.Client) {
	// Get GitHub OAuth from env or from file
	oAuth := ctx.GitHubOAuth
	if strings.Contains(ctx.GitHubOAuth, "/") {
		bytes, err := ReadFile(ctx, ctx.GitHubOAuth)
		FatalOnError(err)
		oAuth = strings.TrimSpace(string(bytes))
	}

	// GitHub authentication or use public access
	ghCtx = context.Background()
	if oAuth == "-" {
		client := github.NewClient(nil)
		clients = append(clients, client)
	} else {
		oAuths := strings.Split(oAuth, ",")
		for _, auth := range oAuths {
			ts := oauth2.StaticTokenSource(
				&oauth2.Token{AccessToken: auth},
			)
			tc := oauth2.NewClient(ghCtx, ts)
			client := github.NewClient(tc)
			clients = append(clients, client)
		}
	}
	// Optional API base URL override (GitHub Enterprise / testing)
	if ctx.GitHubAPIURL != "" {
		baseURL, err := url.Parse(ctx.GitHubAPIURL)
		FatalOnError(err)
		for _, client := range clients {
			client.BaseURL = baseURL
		}
	}
	return
}

// HandlePossibleError - display error specific message, detect rate limit and abuse
func HandlePossibleError(err error, cfg, info string) string {
	if err != nil {
		_, rate := err.(*github.RateLimitError)
		_, abuse := err.(*github.AbuseRateLimitError)
		if abuse || rate {
			// Cached rate limits are stale now, re-poll GitHub before the next API call
			InvalidateRateLimitsCache()
			if rate {
				Printf("Rate limit (%s) for %v\n", info, cfg)
				return "rate"
			}
			if abuse {
				Printf("Abuse detected (%s) for %v\n", info, cfg)
				return Abuse
			}
		}
		errStr := err.Error()
		if strings.Contains(errStr, "410 This issue was deleted") {
			Printf("Issue was deleted (%s) for %v: %v\n", info, cfg, err)
			return IssueIsDeleted
		} else if strings.Contains(errStr, "404 Not Found") {
			Printf("Not found (%s) for %v: %v\n", info, cfg, err)
			return NotFound
		} else if strings.Contains(errStr, "502 Server Error") {
			Printf("Server Error (%s) for %v: %v\n", info, cfg, err)
			return "server_error"
		} else if strings.Contains(errStr, "409 Git Repository is empty") {
			Printf("Git repository empty (%s) for %v: %v\n", info, cfg, err)
			return NotFound
		} else if strings.Contains(errStr, "301") {
			Printf("Moved Permanently (%s) for %v: %v\n", info, cfg, err)
			return MovedPermanently
		}
		//FatalOnError(err)
		Printf("%s error: %T:%v, non fatal, exiting 0 status\n", os.Args[0], err, err)
		os.Exit(0)
	}
	return ""
}

func ghActorIDOrNil(actPtr *github.User) interface{} {
	if actPtr == nil {
		return nil
	}
	return actPtr.ID
}

func ghActorLoginOrNil(actPtr *github.User, maybeHide func(string) string) interface{} {
	if actPtr == nil {
		return nil
	}
	if actPtr.Login == nil {
		return nil
	}
	return maybeHide(*actPtr.Login)
}

func ghMilestoneIDOrNil(milPtr *github.Milestone) interface{} {
	if milPtr == nil {
		return nil
	}
	return milPtr.ID
}

// ghEnsureEventActor - GitHub API can return an issue event without an actor (or with an
// actor missing id/login) when the account that performed the event was deleted.
// Such events cannot be stored (actor related columns are NOT NULL), so reassign them
// to the canonical GitHub placeholder actor 'ghost' (id 10137) - just like GitHub does.
func ghEnsureEventActor(cfg *IssueConfig) {
	event := cfg.GhEvent
	if event == nil {
		return
	}
	actor := event.Actor
	if actor != nil && actor.ID != nil && actor.Login != nil {
		return
	}
	ghostID := GhostActorID
	ghostLogin := GhostActorLogin
	Printf(
		"Warning: event %d for %s #%d (%s, %v) has no actor (deleted account?), reassigning to '%s' (id %d)\n",
		cfg.EventID, cfg.Repo, cfg.Number, cfg.EventType, ToYMDHMSDate(cfg.CreatedAt), ghostLogin, ghostID,
	)
	event.Actor = &github.User{ID: &ghostID, Login: &ghostLogin}
}

// Inserts single GitHub User
func ghActor(con *sql.Tx, ctx *Ctx, actor *github.User, maybeHide func(string) string) {
	if actor == nil || actor.Login == nil {
		return
	}
	InsertActorTx(con, ctx, actor.ID, maybeHide(*actor.Login), "")
}

// Insert single GitHub milestone
// milestone: the milestone to insert - the issue's one for artificial issue events,
// the PR's one for artificial PR events (they can differ: the two API payloads are
// fetched separately and the PR payload can carry a milestone the issue payload lacks).
func ghMilestone(con *sql.Tx, ctx *Ctx, eid int64, ic *IssueConfig, repoID int64, milestone *github.Milestone, maybeHide func(string) string) {
	// Defensive no-op for current callers: ArtificialEvent/ArtificialPREvent already skipped
	// (GHA2DB_GHAPIALLOWINSERTFAIL) or ghost-reassigned actor-less events before calling here.
	// Kept because the code below dereferences ev.Actor directly - protects any future caller.
	ghEnsureEventActor(ic)
	ev := ic.GhEvent
	// gha_milestones
	ExecSQLTxWithErr(
		con,
		ctx,
		InsertIgnore(
			fmt.Sprintf(
				"into gha_milestones("+
					"id, event_id, closed_at, closed_issues, created_at, creator_id, "+
					"description, due_on, number, open_issues, state, title, updated_at, "+
					"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
					"dupn_creator_login) values("+
					"%s, %s, %s, %s, %s, %s, "+
					"%s, %s, %s, %s, %s, %s, %s, "+
					"%s, %s, %s, %s, %s, %s, "+
					"%s)",
				NValue(1),
				NValue(2),
				NValue(3),
				NValue(4),
				NValue(5),
				NValue(6),
				NValue(7),
				NValue(8),
				NValue(9),
				NValue(10),
				NValue(11),
				NValue(12),
				NValue(13),
				NValue(14),
				NValue(15),
				NValue(16),
				NValue(17),
				NValue(18),
				NValue(19),
				NValue(20),
			),
		),
		AnyArray{
			milestone.ID,
			eid,
			milestone.ClosedAt,
			milestone.ClosedIssues,
			milestone.CreatedAt,
			ghActorIDOrNil(milestone.Creator),
			TruncStringOrNil(milestone.Description, 0xffff),
			milestone.DueOn,
			milestone.Number,
			milestone.OpenIssues,
			milestone.State,
			TruncStringOrNil(milestone.Title, 200),
			milestone.UpdatedAt,
			ev.Actor.ID,
			maybeHide(*ev.Actor.Login),
			repoID,
			ic.Repo,
			ic.EventType,
			ic.CreatedAt,
			ghActorLoginOrNil(milestone.Creator, maybeHide),
		}...,
	)
}

// GetRecentRepos - get list of repos active last day
func GetRecentRepos(c *sql.DB, ctx *Ctx, dtFrom time.Time) (repos []string, rids []int64) {
	rows := QuerySQLWithErr(
		c,
		ctx,
		fmt.Sprintf(
			"select distinct repo_id, dup_repo_name from gha_events "+
				"where created_at > %s",
			NValue(1),
		),
		dtFrom,
	)
	defer func() { FatalOnError(rows.Close()) }()
	var (
		repo string
		rid  int64
	)
	for rows.Next() {
		FatalOnError(rows.Scan(&rid, &repo))
		repos = append(repos, repo)
		rids = append(rids, rid)
	}
	FatalOnError(rows.Err())
	return
}

// GetTrackedRepos - every repository of gha_repos, one current name per repository id.
// A repository id can be listed under several names (renames); the current name is the one
// with the newest native GH Archive event (0 < id < 2^48), the other names are historical.
// Ids without any native event keep their alphabetically first name.
// Returns the current names (sorted), their ids (a name can be tracked under several ids,
// sorted ascending) and the sorted historical names (those that are current for no id).
func GetTrackedRepos(c *sql.DB, ctx *Ctx) (repos []string, ids map[string][]int64, historical []string) {
	type named struct {
		name      string
		createdAt time.Time
		eventID   int64
		valid     bool
	}
	rows := QuerySQLWithErr(
		c,
		ctx,
		"select r.id, r.name, n.created_at, n.id from gha_repos r left join lateral ("+
			"select e.created_at, e.id from gha_events e where e.repo_id = r.id and e.dup_repo_name = r.name "+
			"and e.id > 0 and e.id < 281474976710656 order by e.created_at desc, e.id desc limit 1) n on true",
	)
	defer func() { FatalOnError(rows.Close()) }()
	byID := make(map[int64][]named)
	for rows.Next() {
		var (
			rid       int64
			name      string
			createdAt sql.NullTime
			eventID   sql.NullInt64
		)
		FatalOnError(rows.Scan(&rid, &name, &createdAt, &eventID))
		byID[rid] = append(byID[rid], named{name: name, createdAt: createdAt.Time, eventID: eventID.Int64, valid: createdAt.Valid})
	}
	FatalOnError(rows.Err())
	ids = make(map[string][]int64)
	old := make(map[string]struct{})
	for rid, names := range byID {
		best := 0
		for i := 1; i < len(names); i++ {
			n, b := names[i], names[best]
			switch {
			case n.valid && !b.valid:
				best = i
			case n.valid && b.valid && (n.createdAt.After(b.createdAt) || (n.createdAt.Equal(b.createdAt) && n.eventID > b.eventID)):
				best = i
			case !n.valid && !b.valid && n.name < b.name:
				best = i
			}
		}
		for i, n := range names {
			if i == best {
				ids[n.name] = append(ids[n.name], rid)
			} else {
				old[n.name] = struct{}{}
			}
		}
	}
	for name, rids := range ids {
		sort.Slice(rids, func(i, j int) bool { return rids[i] < rids[j] })
		repos = append(repos, name)
	}
	sort.Strings(repos)
	for name := range old {
		if _, ok := ids[name]; !ok {
			historical = append(historical, name)
		}
	}
	sort.Strings(historical)
	return
}

// repoIDByNameCache - CurrentRepoID answers per process (the data does not change under a run)
var repoIDByNameCache sync.Map

// CurrentRepoID - the repository id (and organization id, nil when none) behind a repository name.
// GH Archive lists some names under several ids (a placeholder repository created before a transfer,
// a fork that took over a name, id-less rows): the current id is the one whose newest native event
// (0 < id < 2^48) under that name is the newest - the same rule GetTrackedRepos uses for the current
// name of an id; ids without native events come after those with, ties go to the highest id.
// native=false: the id comes from gha_repos alone (no native event under the name yet).
// Names without a gha_repos row fall back to the events (highest repo_id). Returns ok=false when the
// name is unknown to both. Bug 68: `max(repo_id)` picked the placeholder id 40511817 for
// kubernetes/kubernetes (one CreateEvent from 2015) over the real 20580498, and once an artificial
// event carried it, `max` kept returning it - 2M artificial events ended up under the placeholder.
func CurrentRepoID(c *sql.DB, ctx *Ctx, name string) (repoID int64, orgID *int64, native, ok bool) {
	type cached struct {
		repoID int64
		orgID  *int64
		native bool
		ok     bool
	}
	if v, hit := repoIDByNameCache.Load(name); hit {
		cv := v.(cached)
		return cv.repoID, cv.orgID, cv.native, cv.ok
	}
	// The lateral subquery walks events_repo_name_created_at_idx backwards: cheap unless the id
	// carries millions of artificial events (the bug 68 placeholder before its repair), and then
	// once per process per name
	queries := []string{
		"select r.id, coalesce(n.org_id, r.org_id), n.created_at is not null from gha_repos r left join lateral (" +
			"select e.created_at, e.org_id from gha_events e where e.repo_id = r.id and e.dup_repo_name = r.name " +
			"and e.id > 0 and e.id < 281474976710656 order by e.created_at desc limit 1) n on true " +
			"where r.name = " + NValue(1) + " order by n.created_at desc nulls last, r.id desc limit 1",
		"select max(repo_id), max(org_id), true from gha_events where dup_repo_name = " + NValue(1),
	}
	for _, query := range queries {
		rows := QuerySQLWithErr(c, ctx, query, name)
		var (
			rid sql.NullInt64
			oid sql.NullInt64
			nat bool
		)
		for rows.Next() {
			FatalOnError(rows.Scan(&rid, &oid, &nat))
		}
		FatalOnError(rows.Err())
		FatalOnError(rows.Close())
		if !rid.Valid {
			continue
		}
		repoID, native, ok = rid.Int64, nat, true
		if oid.Valid {
			o := oid.Int64
			orgID = &o
		}
		break
	}
	if ok && orgID == nil {
		rows := QuerySQLWithErr(c, ctx, "select max(org_id) from gha_events where dup_repo_name = "+NValue(1), name)
		var oid sql.NullInt64
		for rows.Next() {
			FatalOnError(rows.Scan(&oid))
		}
		FatalOnError(rows.Err())
		FatalOnError(rows.Close())
		if oid.Valid {
			o := oid.Int64
			orgID = &o
		}
	}
	repoIDByNameCache.Store(name, cached{repoID: repoID, orgID: orgID, native: native, ok: ok})
	return
}

// artificialRepoIDs - repo id (-1 when unknown, the legacy marker) and org id (nil when none)
// for the artificial events of a repository name
func artificialRepoIDs(c *sql.DB, ctx *Ctx, name string) (int64, interface{}) {
	repoID, orgID, _, ok := CurrentRepoID(c, ctx, name)
	if !ok {
		return -1, nil
	}
	if orgID == nil {
		return repoID, nil
	}
	return repoID, *orgID
}

// DeleteArtificialPREvent - create artificial API event (but from the past)
func DeleteArtificialPREvent(c *sql.DB, ctx *Ctx, cfg *IssueConfig) (err error) {
	if ctx.SkipPDB {
		if ctx.Debug > 0 {
			Printf("No DB write: Delete PR '%v'\n", *cfg)
		}
		return nil
	}
	eid := 281474976710656 + cfg.EventID
	condition := fmt.Sprintf(" where event_id = %d", eid)
	deletes := []string{
		"delete from gha_pull_requests" + condition,
		"delete from gha_pull_requests_assignees" + condition,
		"delete from gha_pull_requests_requested_reviewers" + condition,
	}
	// Start transaction
	tc, err := c.Begin()
	FatalOnError(err)

	for _, del := range deletes {
		ExecSQLTxWithErr(tc, ctx, del)
	}

	// Final commit
	FatalOnError(tc.Commit())
	//FatalOnError(tc.Rollback())
	return
}

// ArtificialPREvent - create artificial API event (PR state for now())
func ArtificialPREvent(c *sql.DB, ctx *Ctx, cfg *IssueConfig, pr *github.PullRequest) (err error) {
	if ctx.SkipPDB {
		if ctx.Debug > 0 {
			Printf("No DB write: PR '%v'\n", *cfg)
		}
		return nil
	}
	// To handle GDPR
	maybeHide := MaybeHideFunc(GetHidden(ctx, HideCfgFile))

	eventID := 281474976710656 + cfg.EventID
	eType := cfg.EventType
	eCreatedAt := cfg.CreatedAt
	event := cfg.GhEvent
	issue := cfg.GhIssue
	iid := *issue.ID
	// Bad GH API data: events performed by deleted GitHub accounts can have no actor.
	// When GHA2DB_GHAPIALLOWINSERTFAIL is set: report and skip such events.
	// Otherwise (default): reassign them to the 'ghost' placeholder actor instead of failing NOT NULL inserts.
	if ctx.AllowGHAPIInsertFail && (event.Actor == nil || event.Actor.ID == nil || event.Actor.Login == nil) {
		Printf("Warning: GHA2DB_GHAPIALLOWINSERTFAIL: skipped artificial PR event for %s %d (%s, %v): event has no actor\n", cfg.Repo, cfg.Number, cfg.EventType, ToYMDHMSDate(cfg.CreatedAt))
		return nil
	}
	ghEnsureEventActor(cfg)
	actor := event.Actor

	// Repository id (bug 68: the current id of the name, not max(repo_id)) and organization id
	repoID, orgID := artificialRepoIDs(c, ctx, cfg.Repo)

	// Start transaction
	tc, err := c.Begin()
	FatalOnError(err)

	// Event actor & user
	ghActor(tc, ctx, actor, maybeHide)
	ghActor(tc, ctx, pr.User, maybeHide)

	baseSHA := ""
	headSHA := ""
	if pr.Base != nil && pr.Base.SHA != nil {
		baseSHA = *pr.Base.SHA
	}
	if pr.Head != nil && pr.Head.SHA != nil {
		headSHA = *pr.Head.SHA
	}

	if pr.MergedBy != nil {
		ghActor(tc, ctx, pr.MergedBy, maybeHide)
	}

	if pr.Assignee != nil {
		ghActor(tc, ctx, pr.Assignee, maybeHide)
	}

	if pr.Milestone != nil {
		ghMilestone(tc, ctx, eventID, cfg, repoID, pr.Milestone, maybeHide)
	}

	prid := *pr.ID
	ExecSQLTxWithErr(
		tc,
		ctx,
		InsertIgnore(
			fmt.Sprintf(
				"into gha_pull_requests("+
					"id, event_id, user_id, base_sha, head_sha, merged_by_id, assignee_id, milestone_id, "+
					"number, state, title, body, created_at, updated_at, closed_at, merged_at, "+
					"merge_commit_sha, merged, mergeable, mergeable_state, comments, "+
					"maintainer_can_modify, commits, additions, deletions, changed_files, "+
					"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
					// "dup_user_login, dupn_assignee_login, dupn_merged_by_login) values("+
					"dup_user_login, dupn_merged_by_login) values("+
					"%s, %s, %s, %s, %s, %s, %s, %s, "+
					"%s, %s, %s, %s, %s, %s, %s, %s, "+
					"%s, %s, %s, %s, %s, "+
					"%s, %s, %s, %s, %s, "+
					"%s, %s, %s, %s, %s, %s, "+
					// "%s, %s, %s)",
					"%s, %s)",
				NValue(1),
				NValue(2),
				NValue(3),
				NValue(4),
				NValue(5),
				NValue(6),
				NValue(7),
				NValue(8),
				NValue(9),
				NValue(10),
				NValue(11),
				NValue(12),
				NValue(13),
				NValue(14),
				NValue(15),
				NValue(16),
				NValue(17),
				NValue(18),
				NValue(19),
				NValue(20),
				NValue(21),
				NValue(22),
				NValue(23),
				NValue(24),
				NValue(25),
				NValue(26),
				NValue(27),
				NValue(28),
				NValue(29),
				NValue(30),
				NValue(31),
				NValue(32),
				NValue(33),
				NValue(34),
				// NValue(35),
			),
		),
		AnyArray{
			prid,
			eventID,
			ghActorIDOrNil(pr.User),
			baseSHA,
			headSHA,
			ghActorIDOrNil(pr.MergedBy),
			ghActorIDOrNil(pr.Assignee),
			ghMilestoneIDOrNil(pr.Milestone),
			pr.Number,
			pr.State,
			pr.Title,
			TruncStringOrNil(pr.Body, 0xffff),
			pr.CreatedAt,
			pr.UpdatedAt,
			TimeOrNil(pr.ClosedAt),
			TimeOrNil(pr.MergedAt),
			StringOrNil(pr.MergeCommitSHA),
			BoolOrNil(pr.Merged),
			BoolOrNil(pr.Mergeable),
			StringOrNil(pr.MergeableState),
			IntOrNil(pr.Comments),
			BoolOrNil(pr.MaintainerCanModify),
			IntOrNil(pr.Commits),
			IntOrNil(pr.Additions),
			IntOrNil(pr.Deletions),
			IntOrNil(pr.ChangedFiles),
			actor.ID,
			ghActorLoginOrNil(actor, maybeHide),
			repoID,
			cfg.Repo,
			eType,
			eCreatedAt,
			ghActorLoginOrNil(pr.User, maybeHide),
			// ghActorLoginOrNil(pr.Assignee, maybeHide),
			ghActorLoginOrNil(pr.MergedBy, maybeHide),
		}...,
	)

	// Create artificial event
	ExecSQLTxWithErr(
		tc,
		ctx,
		InsertIgnore(
			fmt.Sprintf(
				"into gha_events("+
					// "id, type, actor_id, repo_id, public, created_at, "+
					"id, type, actor_id, repo_id, created_at, "+
					// "dup_actor_login, dup_repo_name, org_id, forkee_id) "+
					"dup_actor_login, dup_repo_name, org_id) "+
					// "values(%s, %s, %s, %s, true, %s, "+
					"values(%s, %s, %s, %s, %s, "+
					// "%s, %s, %s, null)",
					"%s, %s, %s)",
				NValue(1),
				NValue(2),
				NValue(3),
				NValue(4),
				NValue(5),
				NValue(6),
				NValue(7),
				NValue(8),
			),
		),
		AnyArray{
			eventID,
			cfg.EventType,
			ghActorIDOrNil(event.Actor),
			repoID,
			eCreatedAt,
			ghActorLoginOrNil(event.Actor, maybeHide),
			cfg.Repo,
			orgID,
		}...,
	)

	// Create artificial event's payload
	ExecSQLTxWithErr(
		tc,
		ctx,
		InsertIgnore(
			fmt.Sprintf(
				"into gha_payloads("+
					"event_id, push_id, size, ref, head, befor, action, "+
					// "issue_id, pull_request_id, comment_id, ref_type, master_branch, commit, "+
					"issue_id, pull_request_id, comment_id, commit, "+
					// "description, number, forkee_id, release_id, member_id, "+
					"number, forkee_id, release_id, member_id, "+
					// "dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at) "+
					"dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at) "+
					"values(%s, null, null, null, null, null, %s, "+
					// "%s, %s, null, null, null, null, "+
					"%s, %s, null, null, "+
					// "null, %s, null, null, null, "+
					"%s, null, null, null, "+
					// "%s, %s, %s, %s, %s, %s)",
					"%s, %s, %s, %s, %s)",
				NValue(1),
				NValue(2),
				NValue(3),
				NValue(4),
				NValue(5),
				NValue(6),
				NValue(7),
				NValue(8),
				NValue(9),
				NValue(10),
				// NValue(11),
			),
		),
		AnyArray{
			eventID,
			cfg.EventType,
			iid,
			prid,
			issue.Number,
			// ghActorIDOrNil(event.Actor),
			ghActorLoginOrNil(event.Actor, maybeHide),
			repoID,
			cfg.Repo,
			cfg.EventType,
			eCreatedAt,
		}...,
	)

	// If such payload already existed, we need to set PR ID on it
	ExecSQLTxWithErr(
		tc,
		ctx,
		fmt.Sprintf(
			"update gha_payloads set pull_request_id = %s where issue_id = %s and event_id = %s",
			NValue(1),
			NValue(2),
			NValue(3),
		),
		AnyArray{prid, iid, eventID}...,
	)

	// Arrays: actors: assignees, requested_reviewers
	// assignees

	if pr.Assignees != nil {
		for _, assignee := range pr.Assignees {
			if assignee == nil {
				continue
			}
			// assignee
			ghActor(tc, ctx, assignee, maybeHide)

			ExecSQLTxWithErr(
				tc,
				ctx,
				InsertIgnore(
					"into gha_pull_requests_assignees(pull_request_id, event_id, assignee_id) "+NValues(3),
				),
				AnyArray{prid, eventID, assignee.ID}...,
			)
		}
	}

	// requested_reviewers
	if pr.RequestedReviewers != nil {
		for _, reviewer := range pr.RequestedReviewers {
			if reviewer == nil {
				continue
			}
			// reviewer
			ghActor(tc, ctx, reviewer, maybeHide)

			// pull_request-requested_reviewer connection
			ExecSQLTxWithErr(
				tc,
				ctx,
				InsertIgnore(
					"into gha_pull_requests_requested_reviewers(pull_request_id, event_id, requested_reviewer_id) "+NValues(3),
				),
				AnyArray{prid, eventID, reviewer.ID}...,
			)
		}
	}
	// Final commit
	FatalOnError(tc.Commit())
	//FatalOnError(tc.Rollback())
	return
}

// DeleteArtificialEvent - create artificial API event (but from the past)
func DeleteArtificialEvent(c *sql.DB, ctx *Ctx, cfg *IssueConfig) (err error) {
	if ctx.SkipPDB {
		if ctx.Debug > 0 {
			Printf("No DB write: Delete Issue '%v'\n", *cfg)
		}
		return nil
	}
	eid := 281474976710656 + cfg.EventID
	condition := fmt.Sprintf(" where event_id = %d", eid)
	deletes := []string{
		"delete from gha_issues_labels" + condition,
		"delete from gha_issues_assignees" + condition,
		"delete from gha_issues" + condition,
		"delete from gha_milestones" + condition,
		"delete from gha_payloads" + condition,
		"delete from gha_pull_requests" + condition,
		"delete from gha_pull_requests_assignees" + condition,
		"delete from gha_pull_requests_requested_reviewers" + condition,
		fmt.Sprintf("delete from gha_events where id = %d", eid),
	}
	// Start transaction
	tc, err := c.Begin()
	FatalOnError(err)

	for _, del := range deletes {
		ExecSQLTxWithErr(tc, ctx, del)
	}

	// Final commit
	FatalOnError(tc.Commit())
	//FatalOnError(tc.Rollback())
	return
}

// ArtificialEvent - create artificial API event (but from the past)
func ArtificialEvent(c *sql.DB, ctx *Ctx, cfg *IssueConfig) (err error) {
	// github.com/google/go-github/github/issues_events.go
	if ctx.SkipPDB {
		if ctx.Debug > 0 {
			Printf("No DB write: Issue '%v'\n", *cfg)
		}
		return nil
	}
	// Create artificial event, add 2^48 to eid
	eid := cfg.EventID
	iid := cfg.IssueID
	issue := cfg.GhIssue
	event := cfg.GhEvent
	eventID := 281474976710656 + eid
	now := cfg.CreatedAt

	// To handle GDPR
	maybeHide := MaybeHideFunc(GetHidden(ctx, HideCfgFile))

	// Bad GH API data: events performed by deleted GitHub accounts can have no actor.
	// When GHA2DB_GHAPIALLOWINSERTFAIL is set: report and skip such events.
	// Otherwise (default): reassign them to the 'ghost' placeholder actor instead of failing NOT NULL inserts.
	if ctx.AllowGHAPIInsertFail && (event.Actor == nil || event.Actor.ID == nil || event.Actor.Login == nil) {
		Printf("Warning: GHA2DB_GHAPIALLOWINSERTFAIL: skipped artificial event for %s %d (%s, %v): event has no actor\n", cfg.Repo, cfg.Number, cfg.EventType, ToYMDHMSDate(cfg.CreatedAt))
		return nil
	}
	ghEnsureEventActor(cfg)

	// Repository id (bug 68: the current id of the name, not max(repo_id)) and organization id
	repoID, orgID := artificialRepoIDs(c, ctx, cfg.Repo)

	// Start transaction
	tc, err := c.Begin()
	FatalOnError(err)

	// Actors
	ghActor(tc, ctx, event.Actor, maybeHide)
	ghActor(tc, ctx, issue.Assignee, maybeHide)
	ghActor(tc, ctx, issue.User, maybeHide)
	for _, assignee := range issue.Assignees {
		ghActor(tc, ctx, assignee, maybeHide)
	}
	if issue.Milestone != nil {
		ghActor(tc, ctx, issue.Milestone.Creator, maybeHide)
	}

	// Create new issue state
	ExecSQLTxWithErr(
		tc,
		ctx,
		InsertIgnore(
			fmt.Sprintf(
				"into gha_issues("+
					"id, event_id, assignee_id, body, closed_at, comments, created_at, "+
					"locked, milestone_id, number, state, title, updated_at, user_id, "+
					"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
					// "dup_user_login, dupn_assignee_login, is_pull_request) "+
					"dup_user_login, is_pull_request) "+
					"values(%s, %s, %s, %s, %s, %s, %s, "+
					"%s, %s, %s, %s, %s, %s, %s, "+
					"%s, %s, %s, %s, %s, %s, "+
					// "%s, %s, %s) ",
					"%s, %s) ",
				NValue(1),
				NValue(2),
				NValue(3),
				NValue(4),
				NValue(5),
				NValue(6),
				NValue(7),
				NValue(8),
				NValue(9),
				NValue(10),
				NValue(11),
				NValue(12),
				NValue(13),
				NValue(14),
				NValue(15),
				NValue(16),
				NValue(17),
				NValue(18),
				NValue(19),
				NValue(20),
				NValue(21),
				NValue(22),
				// NValue(23),
			),
		),
		AnyArray{
			iid,
			eventID,
			ghActorIDOrNil(issue.Assignee),
			TruncStringOrNil(issue.Body, 0xffff),
			TimeOrNil(issue.ClosedAt),
			IntOrNil(issue.Comments),
			issue.CreatedAt,
			BoolOrNil(issue.Locked),
			ghMilestoneIDOrNil(issue.Milestone),
			issue.Number,
			issue.State,
			issue.Title,
			now,
			ghActorIDOrNil(issue.User),
			ghActorIDOrNil(event.Actor),
			ghActorLoginOrNil(event.Actor, maybeHide),
			repoID,
			cfg.Repo,
			cfg.EventType,
			now,
			ghActorLoginOrNil(issue.User, maybeHide),
			// ghActorLoginOrNil(issue.Assignee, maybeHide),
			issue.IsPullRequest(),
		}...,
	)

	// Create Milestone if new event and milestone non-null
	if issue.Milestone != nil {
		ghMilestone(tc, ctx, eventID, cfg, repoID, issue.Milestone, maybeHide)
	}

	// Create artificial event
	ExecSQLTxWithErr(
		tc,
		ctx,
		InsertIgnore(
			fmt.Sprintf(
				"into gha_events("+
					// "id, type, actor_id, repo_id, public, created_at, "+
					"id, type, actor_id, repo_id, created_at, "+
					// "dup_actor_login, dup_repo_name, org_id, forkee_id) "+
					"dup_actor_login, dup_repo_name, org_id) "+
					// "values(%s, %s, %s, %s, true, %s, "+
					"values(%s, %s, %s, %s, %s, "+
					// "%s, %s, %s, null)",
					"%s, %s, %s)",
				NValue(1),
				NValue(2),
				NValue(3),
				NValue(4),
				NValue(5),
				NValue(6),
				NValue(7),
				NValue(8),
			),
		),
		AnyArray{
			eventID,
			cfg.EventType,
			ghActorIDOrNil(event.Actor),
			repoID,
			now,
			ghActorLoginOrNil(event.Actor, maybeHide),
			cfg.Repo,
			orgID,
		}...,
	)

	// Create artificial event's payload
	ExecSQLTxWithErr(
		tc,
		ctx,
		InsertIgnore(
			fmt.Sprintf(
				"into gha_payloads("+
					"event_id, push_id, size, ref, head, befor, action, "+
					// "issue_id, pull_request_id, comment_id, ref_type, master_branch, commit, "+
					"issue_id, pull_request_id, comment_id, commit, "+
					// "description, number, forkee_id, release_id, member_id, "+
					"number, forkee_id, release_id, member_id, "+
					// "dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at) "+
					"dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at) "+
					"values(%s, null, null, null, null, null, %s, "+
					// "%s, null, null, null, null, null, "+
					"%s, null, null, null, "+
					// "null, %s, null, null, null, "+
					"%s, null, null, null, "+
					// "%s, %s, %s, %s, %s, %s)",
					"%s, %s, %s, %s, %s)",
				NValue(1),
				NValue(2),
				NValue(3),
				NValue(4),
				NValue(5),
				NValue(6),
				NValue(7),
				NValue(8),
				NValue(9),
				// NValue(10),
			),
		),
		AnyArray{
			eventID,
			cfg.EventType,
			iid,
			issue.Number,
			// ghActorIDOrNil(event.Actor),
			ghActorLoginOrNil(event.Actor, maybeHide),
			repoID,
			cfg.Repo,
			cfg.EventType,
			now,
		}...,
	)

	// Add issue labels
	for labelID, labelName := range cfg.LabelsMap {
		ExecSQLTxWithErr(
			tc,
			ctx,
			InsertIgnore(
				fmt.Sprintf(
					"into gha_issues_labels(issue_id, event_id, label_id, "+
						"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, "+
						"dup_type, dup_created_at, dup_issue_number, dup_label_name) "+
						"values(%s, %s, %s, "+
						"%s, %s, %s, %s, "+
						"%s, %s, %s, %s)",
					NValue(1),
					NValue(2),
					NValue(3),
					NValue(4),
					NValue(5),
					NValue(6),
					NValue(7),
					NValue(8),
					NValue(9),
					NValue(10),
					NValue(11),
				),
			),
			AnyArray{
				iid,
				eventID,
				labelID,
				ghActorIDOrNil(event.Actor),
				ghActorLoginOrNil(event.Actor, maybeHide),
				repoID,
				cfg.Repo,
				cfg.EventType,
				now,
				issue.Number,
				labelName,
			}...,
		)
	}

	// Add issue assignees
	for assigneeID := range cfg.AssigneesMap {
		ExecSQLTxWithErr(
			tc,
			ctx,
			InsertIgnore(
				fmt.Sprintf(
					"into gha_issues_assignees(issue_id, event_id, assignee_id) "+
						"values(%s, %s, %s)",
					NValue(1),
					NValue(2),
					NValue(3),
				),
			),
			AnyArray{
				iid,
				eventID,
				assigneeID,
			}...,
		)
	}

	// Final commit
	FatalOnError(tc.Commit())
	//FatalOnError(tc.Rollback())
	return
}

// SyncIssuesState synchonizes issues states
// manual:
//
//	false: normal devstats sync cron mode using 'ghapi2db' tool
//	true: manual sync using 'sync_issues' tool
func SyncIssuesState(gctx context.Context, gc []*github.Client, ctx *Ctx, c *sql.DB, issues map[int64]IssueConfigAry, prs map[int64]github.PullRequest, manual bool) {
	nIssuesBefore := 0
	for _, issueConfig := range issues {
		nIssuesBefore += len(issueConfig)
	}

	// Sort issues to by their state changes in time
	for issueID := range issues {
		sort.Sort(issues[issueID])
		if ctx.Debug > 1 {
			Printf("Sorted: %+v\n", issues[issueID])
		}
	}

	// Output data info
	outputIssuesInfo(issues, "Issues to process")

	// Get number of CPUs available
	thrN := GetThreadsNum(ctx)
	prc := 0

	var issuesMutex = &sync.RWMutex{}
	// Now iterate all issues/PR in MT mode
	ch := make(chan bool)
	nThreads := 0
	dtStart := time.Now()
	lastTime := dtStart
	nIssues := 0
	for _, issueConfig := range issues {
		nIssues += len(issueConfig)
	}
	nPRs := len(prs)
	checked := 0
	var updatesMutex = &sync.Mutex{}
	updates := []int{0, 0, 0}
	// updates (non-manual mode):
	// 0: no such event --> new
	// 1: event exists and the same state --> no new
	// 2: event exists with different state --> warning
	// updates (manual mode)
	// 0 - no such issue --> new
	// 1: previous issue state exists, no new
	// 2: previous issue state exists, new needed
	infos := make(map[string][]string)

	Printf("ghapi2db.go: Processing %d PRs, %d issues (%d with date collisions), manual mode: %v - GHA part\n", nPRs, nIssues, nIssuesBefore, manual)
	// Use map key to pass to the closure
	for key, issueConfig := range issues {
		for idx := range issueConfig {
			go func(ch chan bool, iid int64, idx int) {
				why := ""
				what := ""
				// Refer to current tag using index passed to anonymous function
				issuesMutex.RLock()
				cfg := issues[iid][idx]
				issuesMutex.RUnlock()
				if ctx.SkipAPIIssues || (ctx.SkipAPIPRs && cfg.Pr) {
					ch <- false
					return
				}
				if ctx.Debug > 1 {
					Printf("GHA Issue ID '%d' --> '%v'\n", iid, cfg)
				}
				var (
					ghaMilestoneID *int64
					ghaEventID     int64
					ghaClosedAt    *time.Time
					ghaState       string
					ghaTitle       string
					ghaLocked      bool
					ghaAssigneeID  *int64
				)

				// Process current milestone (given issue and second)
				apiMilestoneID := cfg.MilestoneID
				apiClosedAt := cfg.GhIssue.ClosedAt
				apiState := *cfg.GhIssue.State
				apiTitle := *cfg.GhIssue.Title
				apiLocked := *cfg.GhIssue.Locked
				apiAssigneeID := cfg.AssigneeID
				eventID := 281474976710656 + cfg.EventID

				// Get eventual current state
				var rowsM *sql.Rows
				if manual {
					rowsM = QuerySQLWithErr(
						c,
						ctx,
						fmt.Sprintf(
							"select milestone_id, event_id, closed_at, state, title, locked, assignee_id "+
								"from gha_issues where id = %s "+
								"order by updated_at desc, event_id desc limit 1",
							NValue(1),
						),
						cfg.IssueID,
					)
				} else {
					rowsM = QuerySQLWithErr(
						c,
						ctx,
						fmt.Sprintf(
							"select milestone_id, event_id, closed_at, state, title, locked, assignee_id "+
								"from gha_issues where id = %s and event_id = %s",
							NValue(1),
							NValue(2),
						),
						cfg.IssueID,
						eventID,
					)
				}
				defer func() { FatalOnError(rowsM.Close()) }()
				got := false
				for rowsM.Next() {
					FatalOnError(
						rowsM.Scan(
							&ghaMilestoneID,
							&ghaEventID,
							&ghaClosedAt,
							&ghaState,
							&ghaTitle,
							&ghaLocked,
							&ghaAssigneeID,
						),
					)
					got = true
				}
				FatalOnError(rowsM.Err())

				// Missing event
				if !got {
					if ctx.Debug > 1 {
						Printf("Adding missing (%v) event '%v'\n", cfg.CreatedAt, cfg)
					}
					FatalOnError(
						ArtificialEvent(
							c,
							ctx,
							&cfg,
						),
					)
					if manual {
						why = "no previous issue state"
						what = fmt.Sprintf("%s %d", cfg.Repo, cfg.Number)
					} else {
						why = "no issue event"
						what = fmt.Sprintf("%s %d %s %s", cfg.Repo, cfg.Number, ToYMDHMSDate(cfg.CreatedAt), cfg.EventType)
					}
					updatesMutex.Lock()
					updates[0]++
					_, ok := infos[why]
					if ok {
						infos[why] = append(infos[why], what)
					} else {
						infos[why] = []string{what}
					}
					updatesMutex.Unlock()
					ch <- true
					return
				}

				// Now have existing GHA event, but we don't know if it is a correct state event
				// Or just bot comment after which (on the same second) milestone or label(s) are updated
				// Check state change
				changedState := false
				if apiState != ghaState {
					changedState = true
					if ctx.Debug > 1 {
						Printf("Updating issue '%v' state %s -> %s\n", cfg, ghaState, apiState)
					}
					why = "changed issue state"
					if manual {
						what = fmt.Sprintf("%s %d: %s -> %s", cfg.Repo, cfg.Number, ghaState, apiState)
					} else {
						what = fmt.Sprintf("%s %d %s %s: %s -> %s", cfg.Repo, cfg.Number, ToYMDHMSDate(cfg.CreatedAt), cfg.EventType, ghaState, apiState)
					}
					updatesMutex.Lock()
					_, ok := infos[why]
					if ok {
						infos[why] = append(infos[why], what)
					} else {
						infos[why] = []string{what}
					}
					updatesMutex.Unlock()
				}

				// Check title change
				changedTitle := false
				if apiTitle != ghaTitle {
					changedTitle = true
					if ctx.Debug > 1 {
						Printf("Updating issue '%v' title %s -> %s\n", cfg, ghaTitle, apiTitle)
					}
					why = "changed issue title"
					if manual {
						what = fmt.Sprintf("%s %d: %s -> %s", cfg.Repo, cfg.Number, ghaTitle, apiTitle)
					} else {
						what = fmt.Sprintf("%s %d %s %s: %s -> %s", cfg.Repo, cfg.Number, ToYMDHMSDate(cfg.CreatedAt), cfg.EventType, ghaTitle, apiTitle)
					}
					updatesMutex.Lock()
					_, ok := infos[why]
					if ok {
						infos[why] = append(infos[why], what)
					} else {
						infos[why] = []string{what}
					}
					updatesMutex.Unlock()
				}

				// Check locked change
				changedLocked := false
				if apiLocked != ghaLocked {
					changedLocked = true
					if ctx.Debug > 1 {
						Printf("Updating issue '%v' locked %v -> %v\n", cfg, ghaLocked, apiLocked)
					}
					why = "changed issue locked state"
					if manual {
						what = fmt.Sprintf("%s %d: %v -> %v", cfg.Repo, cfg.Number, ghaLocked, apiLocked)
					} else {
						what = fmt.Sprintf("%s %d %s %s: %v -> %v", cfg.Repo, cfg.Number, ToYMDHMSDate(cfg.CreatedAt), cfg.EventType, ghaLocked, apiLocked)
					}
					updatesMutex.Lock()
					_, ok := infos[why]
					if ok {
						infos[why] = append(infos[why], what)
					} else {
						infos[why] = []string{what}
					}
					updatesMutex.Unlock()
				}

				// Check closed_at change
				changedClosed := false
				if (apiClosedAt == nil && ghaClosedAt != nil) || (apiClosedAt != nil && ghaClosedAt == nil) || (apiClosedAt != nil && ghaClosedAt != nil && ToYMDHMSDate(*apiClosedAt) != ToYMDHMSDate(*ghaClosedAt)) {
					changedClosed = true
					from := Null
					if ghaClosedAt != nil {
						from = fmt.Sprintf("%v", ToYMDHMSDate(*ghaClosedAt))
					}
					to := Null
					if apiClosedAt != nil {
						to = fmt.Sprintf("%v", ToYMDHMSDate(*apiClosedAt))
					}
					if ctx.Debug > 1 {
						Printf("Updating issue '%v' closed_at %s -> %s\n", cfg, from, to)
					}
					why = "changed issue closed at"
					if manual {
						what = fmt.Sprintf("%s %d: %s -> %s", cfg.Repo, cfg.Number, from, to)
					} else {
						what = fmt.Sprintf("%s %d %s %s: %s -> %s", cfg.Repo, cfg.Number, ToYMDHMSDate(cfg.CreatedAt), cfg.EventType, from, to)
					}
					updatesMutex.Lock()
					_, ok := infos[why]
					if ok {
						infos[why] = append(infos[why], what)
					} else {
						infos[why] = []string{what}
					}
					updatesMutex.Unlock()
				}

				// Check milestone change
				changedMilestone := false
				if (apiMilestoneID == nil && ghaMilestoneID != nil) || (apiMilestoneID != nil && ghaMilestoneID == nil) || (apiMilestoneID != nil && ghaMilestoneID != nil && *apiMilestoneID != *ghaMilestoneID) {
					changedMilestone = true
					from := Null
					if ghaMilestoneID != nil {
						from = fmt.Sprintf("%d", *ghaMilestoneID)
					}
					to := Null
					if apiMilestoneID != nil {
						to = fmt.Sprintf("%d", *apiMilestoneID)
					}
					if ctx.Debug > 1 {
						Printf("Updating issue '%v' milestone %s -> %s\n", cfg, from, to)
					}
					why = "changed issue milestone"
					if manual {
						what = fmt.Sprintf("%s %d: %s -> %s", cfg.Repo, cfg.Number, from, to)
					} else {
						what = fmt.Sprintf("%s %d %s %s: %s -> %s", cfg.Repo, cfg.Number, ToYMDHMSDate(cfg.CreatedAt), cfg.EventType, from, to)
					}
					updatesMutex.Lock()
					_, ok := infos[why]
					if ok {
						infos[why] = append(infos[why], what)
					} else {
						infos[why] = []string{what}
					}
					updatesMutex.Unlock()
				}

				// Check assignee change
				changedAssignee := false
				if (apiAssigneeID == nil && ghaAssigneeID != nil) || (apiAssigneeID != nil && ghaAssigneeID == nil) || (apiAssigneeID != nil && ghaAssigneeID != nil && *apiAssigneeID != *ghaAssigneeID) {
					changedAssignee = true
					from := Null
					if ghaAssigneeID != nil {
						from = fmt.Sprintf("%d", *ghaAssigneeID)
					}
					to := Null
					if apiAssigneeID != nil {
						to = fmt.Sprintf("%d", *apiAssigneeID)
					}
					if ctx.Debug > 1 {
						Printf("Updating issue '%v' assignee %s -> %s\n", cfg, from, to)
					}
					why = "changed issue assignee"
					if manual {
						what = fmt.Sprintf("%s %d: %s -> %s", cfg.Repo, cfg.Number, from, to)
					} else {
						what = fmt.Sprintf("%s %d %s %s: %s -> %s", cfg.Repo, cfg.Number, ToYMDHMSDate(cfg.CreatedAt), cfg.EventType, from, to)
					}
					updatesMutex.Lock()
					_, ok := infos[why]
					if ok {
						infos[why] = append(infos[why], what)
					} else {
						infos[why] = []string{what}
					}
					updatesMutex.Unlock()
				}

				// Process current labels
				rowsL := QuerySQLWithErr(
					c,
					ctx,
					fmt.Sprintf(
						"select coalesce(string_agg(sub.label_id::text, ','), '') from "+
							"(select label_id from gha_issues_labels where event_id = %s "+
							"order by label_id) sub",
						NValue(1),
					),
					ghaEventID,
				)
				defer func() { FatalOnError(rowsL.Close()) }()
				ghaLabels := ""
				for rowsL.Next() {
					FatalOnError(rowsL.Scan(&ghaLabels))
				}
				FatalOnError(rowsL.Err())
				changedLabels := false
				if ghaLabels != cfg.Labels {
					if ctx.Debug > 1 {
						Printf("Updating issue '%v' labels to '%s', they were: '%s' (event_id %d)\n", cfg, cfg.Labels, ghaLabels, ghaEventID)
					}
					changedLabels = true
					why = "changed issue labels"
					if manual {
						what = fmt.Sprintf("%s %d: %s -> %s", cfg.Repo, cfg.Number, ghaLabels, cfg.Labels)
					} else {
						what = fmt.Sprintf("%s %d %s %s: %s -> %s", cfg.Repo, cfg.Number, ToYMDHMSDate(cfg.CreatedAt), cfg.EventType, ghaLabels, cfg.Labels)
					}
					updatesMutex.Lock()
					_, ok := infos[why]
					if ok {
						infos[why] = append(infos[why], what)
					} else {
						infos[why] = []string{what}
					}
					updatesMutex.Unlock()
				}

				// Process current assignees
				rowsA := QuerySQLWithErr(
					c,
					ctx,
					fmt.Sprintf(
						"select coalesce(string_agg(sub.assignee_id::text, ','), '') from "+
							"(select assignee_id from gha_issues_assignees where event_id = %s "+
							"order by assignee_id) sub",
						NValue(1),
					),
					ghaEventID,
				)
				defer func() { FatalOnError(rowsA.Close()) }()
				ghaAssignees := ""
				for rowsA.Next() {
					FatalOnError(rowsA.Scan(&ghaAssignees))
				}
				FatalOnError(rowsA.Err())
				changedAssignees := false
				if ghaAssignees != cfg.Assignees {
					if ctx.Debug > 1 {
						Printf("Updating issue '%v' assignees to '%s', they were: '%s' (event_id %d)\n", cfg, cfg.Assignees, ghaAssignees, ghaEventID)
					}
					changedAssignees = true
					why = "changed issue assignees"
					if manual {
						what = fmt.Sprintf("%s %d: %s -> %s", cfg.Repo, cfg.Number, ghaAssignees, cfg.Assignees)
					} else {
						what = fmt.Sprintf("%s %d %s %s: %s -> %s", cfg.Repo, cfg.Number, ToYMDHMSDate(cfg.CreatedAt), cfg.EventType, ghaAssignees, cfg.Assignees)
					}
					updatesMutex.Lock()
					_, ok := infos[why]
					if ok {
						infos[why] = append(infos[why], what)
					} else {
						infos[why] = []string{what}
					}
					updatesMutex.Unlock()
				}

				uidx := 1
				why = "previous issue state the same"
				if manual {
					what = fmt.Sprintf("%s %d", cfg.Repo, cfg.Number)
				} else {
					what = fmt.Sprintf("%s %d %s %s", cfg.Repo, cfg.Number, ToYMDHMSDate(cfg.CreatedAt), cfg.EventType)
				}
				// Do the update if needed
				changedAnything := changedMilestone || changedState || changedClosed || changedAssignee || changedTitle || changedLocked || changedLabels || changedAssignees
				if changedAnything {
					uidx = 2
					if manual {
						FatalOnError(
							ArtificialEvent(
								c,
								ctx,
								&cfg,
							),
						)
						why = "previous issue state different"
						what = fmt.Sprintf("%s %d", cfg.Repo, cfg.Number)
					} else {
						if ctx.Debug > 0 {
							Printf("Debug: Exact artificial event (%v, %d) already exists with different state, skipping: '%v'\n", cfg.CreatedAt, eventID, cfg)
						}
						why = "collision and issue state differs"
						what = fmt.Sprintf("%s %d %s %s: %d", cfg.Repo, cfg.Number, ToYMDHMSDate(cfg.CreatedAt), cfg.EventType, eventID)
						if !ctx.SkipUpdateEvents {
							why = "updated existing issue state"
							FatalOnError(DeleteArtificialEvent(c, ctx, &cfg))
							FatalOnError(ArtificialEvent(c, ctx, &cfg))
						}
					}
				}

				if ctx.Debug > 1 {
					if manual {
						Printf("Previous event (event_id: %d), added artificial: %v: '%v'\n", ghaEventID, changedAnything, cfg)
					} else {
						Printf("Event for the same date (%v) exist (event_id: %d), added artificial: %v: '%v'\n", cfg.CreatedAt, ghaEventID, changedAnything, cfg)
					}
				}
				updatesMutex.Lock()
				updates[uidx]++
				_, ok := infos[why]
				if ok {
					infos[why] = append(infos[why], what)
				} else {
					infos[why] = []string{what}
				}
				updatesMutex.Unlock()
				// Synchronize go routine
				ch <- changedAnything
			}(ch, key, idx)

			// go routine called with 'ch' channel to sync and tag index
			nThreads++
			for nThreads >= thrN {
				<-ch
				nThreads--
				prc++
				if prc%20 == 0 {
					thrN = GetThreadsNum(ctx)
				}
				checked++
				ProgressInfo(checked, nIssues, dtStart, &lastTime, time.Duration(10)*time.Second, "")
			}
		}
	}
	// Usually all work happens on '<-ch'
	for nThreads > 0 {
		<-ch
		nThreads--
		checked++
		ProgressInfo(checked, nIssues, dtStart, &lastTime, time.Duration(10)*time.Second, "")
	}
	// Get RateLimits info
	hint, _, rem, wait := GetRateLimits(gctx, ctx, gc, true)
	if manual {
		Printf(
			"ghapi2db.go: Manually processed %d issues/PRs (%d new issues, existing: %d not needed, %d added): %+v API points remain, resets in %+v, hint key: %d\n",
			checked, updates[0], updates[1], updates[2], rem, wait, hint,
		)
	} else {
		Printf(
			"ghapi2db.go: Automatically processed %d issues/PRs (%d new, %d the same exists, %d incorrect state exists): %+v API points remain, resets in %+v, hint key: %d\n",
			checked, updates[0], updates[1], updates[2], rem, wait, hint,
		)
	}
	// Info
	outputInfo(infos, "Issues")

	// PRs sync (using state at run date XX:08+)
	if ctx.SkipAPIPRs {
		return
	}
	// Use map key to pass to the closure
	outputPRsInfo(prs, "PRs to process")
	infos = make(map[string][]string)
	ch = make(chan bool)
	nThreads = 0
	dtStart = time.Now()
	lastTime = dtStart
	checked = 0
	updates = []int{0, 0, 0}
	var prsMutex = &sync.RWMutex{}
	for iid := range prs {
		go func(ch chan bool, iid int64) {
			why := ""
			what := ""
			prsMutex.RLock()
			pr := prs[iid]
			ica := issues[iid]
			l := len(ica)
			ic := ica[l-1]
			prsMutex.RUnlock()
			prid := *pr.ID
			updatedAt := *pr.UpdatedAt
			if ctx.Debug > 1 {
				Printf("GHA Issue ID '%d' --> PR ID %d, updated %v\n", iid, prid, updatedAt)
			}
			var (
				ghaMilestoneID *int64
				ghaEventID     int64
				ghaClosedAt    *time.Time
				ghaState       string
				ghaTitle       string
				ghaMergedByID  *int64
				ghaMergedAt    *time.Time
				ghaMerged      *bool
				ghaAssigneeID  *int64
				apiMilestoneID *int64
				apiAssigneeID  *int64
				apiMergedByID  *int64
			)

			// Process current milestone
			if pr.Milestone != nil {
				apiMilestoneID = pr.Milestone.ID
			}
			apiClosedAt := pr.ClosedAt
			apiState := *pr.State
			apiTitle := *pr.Title
			if pr.Assignee != nil {
				apiAssigneeID = pr.Assignee.ID
			}
			if pr.MergedBy != nil {
				apiMergedByID = pr.MergedBy.ID
			}
			apiMergedAt := pr.MergedAt
			apiMerged := pr.Merged
			eventID := 281474976710656 + ic.EventID

			// Get event for this date
			var rowsM *sql.Rows
			if manual {
				rowsM = QuerySQLWithErr(
					c,
					ctx,
					fmt.Sprintf(
						"select milestone_id, event_id, closed_at, state, title, assignee_id, "+
							"merged_by_id, merged_at, merged "+
							"from gha_pull_requests where id = %s "+
							"order by updated_at desc, event_id desc limit 1",
						NValue(1),
					),
					prid,
				)
			} else {
				rowsM = QuerySQLWithErr(
					c,
					ctx,
					fmt.Sprintf(
						"select milestone_id, event_id, closed_at, state, title, assignee_id, "+
							"merged_by_id, merged_at, merged "+
							"from gha_pull_requests where id = %s and event_id = %s",
						NValue(1),
						NValue(2),
					),
					prid,
					eventID,
				)
			}
			defer func() { FatalOnError(rowsM.Close()) }()
			got := false
			for rowsM.Next() {
				FatalOnError(
					rowsM.Scan(
						&ghaMilestoneID,
						&ghaEventID,
						&ghaClosedAt,
						&ghaState,
						&ghaTitle,
						&ghaAssigneeID,
						&ghaMergedByID,
						&ghaMergedAt,
						&ghaMerged,
					),
				)
				got = true
			}
			FatalOnError(rowsM.Err())
			if !got {
				if ctx.Debug > 1 {
					Printf("Adding missing (%v) PR event '%v', PR ID: %d\n", updatedAt, ic, prid)
				}
				FatalOnError(
					ArtificialPREvent(
						c,
						ctx,
						&ic,
						&pr,
					),
				)
				if manual {
					why = "no previous pr state"
					what = fmt.Sprintf("%s %d", ic.Repo, ic.Number)
				} else {
					why = "no pr event"
					what = fmt.Sprintf("%s %d %s %s", ic.Repo, ic.Number, ToYMDHMSDate(ic.CreatedAt), ic.EventType)
				}
				updatesMutex.Lock()
				updates[0]++
				_, ok := infos[why]
				if ok {
					infos[why] = append(infos[why], what)
				} else {
					infos[why] = []string{what}
				}
				updatesMutex.Unlock()
				ch <- true
				return
			}

			// Check state change
			changedState := false
			if apiState != ghaState {
				changedState = true
				if ctx.Debug > 1 {
					Printf("Updating PR '%v' state %s -> %s\n", ic, ghaState, apiState)
				}
				why = "changed pr state"
				if manual {
					what = fmt.Sprintf("%s %d: %s -> %s", ic.Repo, ic.Number, ghaState, apiState)
				} else {
					what = fmt.Sprintf("%s %d %s %s: %s -> %s", ic.Repo, ic.Number, ToYMDHMSDate(ic.CreatedAt), ic.EventType, ghaState, apiState)
				}
				updatesMutex.Lock()
				_, ok := infos[why]
				if ok {
					infos[why] = append(infos[why], what)
				} else {
					infos[why] = []string{what}
				}
				updatesMutex.Unlock()
			}

			// Check title change
			changedTitle := false
			if apiTitle != ghaTitle {
				changedTitle = true
				if ctx.Debug > 1 {
					Printf("Updating PR '%v' title %s -> %s\n", ic, ghaTitle, apiTitle)
				}
				why = "changed pr title"
				if manual {
					what = fmt.Sprintf("%s %d: %s -> %s", ic.Repo, ic.Number, ghaTitle, apiTitle)
				} else {
					what = fmt.Sprintf("%s %d %s %s: %s -> %s", ic.Repo, ic.Number, ToYMDHMSDate(ic.CreatedAt), ic.EventType, ghaTitle, apiTitle)
				}
				updatesMutex.Lock()
				_, ok := infos[why]
				if ok {
					infos[why] = append(infos[why], what)
				} else {
					infos[why] = []string{what}
				}
				updatesMutex.Unlock()
			}

			// Check merged change
			changedMerged := false
			if (apiMerged == nil && ghaMerged != nil) || (apiMerged != nil && ghaMerged == nil) || (apiMerged != nil && ghaMerged != nil && *apiMerged != *ghaMerged) {
				changedMerged = true
				from := Null
				if ghaMerged != nil {
					from = fmt.Sprintf("%v", *ghaMerged)
				}
				to := Null
				if apiMerged != nil {
					to = fmt.Sprintf("%v", *apiMerged)
				}
				if ctx.Debug > 1 {
					Printf("Updating PR '%v' merged %s -> %s\n", ic, from, to)
				}
				why = "changed pr merged"
				if manual {
					what = fmt.Sprintf("%s %d: %s -> %s", ic.Repo, ic.Number, from, to)
				} else {
					what = fmt.Sprintf("%s %d %s %s: %s -> %s", ic.Repo, ic.Number, ToYMDHMSDate(ic.CreatedAt), ic.EventType, from, to)
				}
				updatesMutex.Lock()
				_, ok := infos[why]
				if ok {
					infos[why] = append(infos[why], what)
				} else {
					infos[why] = []string{what}
				}
				updatesMutex.Unlock()
			}

			// Check closed_at change
			changedClosed := false
			if (apiClosedAt == nil && ghaClosedAt != nil) || (apiClosedAt != nil && ghaClosedAt == nil) || (apiClosedAt != nil && ghaClosedAt != nil && ToYMDHMSDate(*apiClosedAt) != ToYMDHMSDate(*ghaClosedAt)) {
				changedClosed = true
				from := Null
				if ghaClosedAt != nil {
					from = fmt.Sprintf("%v", ToYMDHMSDate(*ghaClosedAt))
				}
				to := Null
				if apiClosedAt != nil {
					to = fmt.Sprintf("%v", ToYMDHMSDate(*apiClosedAt))
				}
				if ctx.Debug > 1 {
					Printf("Updating PR '%v' closed_at %s -> %s\n", ic, from, to)
				}
				why = "changed pr closed at"
				if manual {
					what = fmt.Sprintf("%s %d: %s -> %s", ic.Repo, ic.Number, from, to)
				} else {
					what = fmt.Sprintf("%s %d %s %s: %s -> %s", ic.Repo, ic.Number, ToYMDHMSDate(ic.CreatedAt), ic.EventType, from, to)
				}
				updatesMutex.Lock()
				_, ok := infos[why]
				if ok {
					infos[why] = append(infos[why], what)
				} else {
					infos[why] = []string{what}
				}
				updatesMutex.Unlock()
			}

			// Check merged_at change
			changedMergedAt := false
			if (apiMergedAt == nil && ghaMergedAt != nil) || (apiMergedAt != nil && ghaMergedAt == nil) || (apiMergedAt != nil && ghaMergedAt != nil && ToYMDHMSDate(*apiMergedAt) != ToYMDHMSDate(*ghaMergedAt)) {
				changedMergedAt = true
				from := Null
				if ghaMergedAt != nil {
					from = fmt.Sprintf("%v", ToYMDHMSDate(*ghaMergedAt))
				}
				to := Null
				if apiMergedAt != nil {
					to = fmt.Sprintf("%v", ToYMDHMSDate(*apiMergedAt))
				}
				if ctx.Debug > 1 {
					Printf("Updating PR '%v' merged_at %s -> %s\n", ic, from, to)
				}
				why = "changed pr merged at"
				if manual {
					what = fmt.Sprintf("%s %d: %s -> %s", ic.Repo, ic.Number, from, to)
				} else {
					what = fmt.Sprintf("%s %d %s %s: %s -> %s", ic.Repo, ic.Number, ToYMDHMSDate(ic.CreatedAt), ic.EventType, from, to)
				}
				updatesMutex.Lock()
				_, ok := infos[why]
				if ok {
					infos[why] = append(infos[why], what)
				} else {
					infos[why] = []string{what}
				}
				updatesMutex.Unlock()
			}

			// Check milestone change
			changedMilestone := false
			if (apiMilestoneID == nil && ghaMilestoneID != nil) || (apiMilestoneID != nil && ghaMilestoneID == nil) || (apiMilestoneID != nil && ghaMilestoneID != nil && *apiMilestoneID != *ghaMilestoneID) {
				changedMilestone = true
				from := Null
				if ghaMilestoneID != nil {
					from = fmt.Sprintf("%d", *ghaMilestoneID)
				}
				to := Null
				if apiMilestoneID != nil {
					to = fmt.Sprintf("%d", *apiMilestoneID)
				}
				if ctx.Debug > 1 {
					Printf("Updating PR '%v' milestone %s -> %s\n", ic, from, to)
				}
				why = "changed pr milestone"
				if manual {
					what = fmt.Sprintf("%s %d: %s -> %s", ic.Repo, ic.Number, from, to)
				} else {
					what = fmt.Sprintf("%s %d %s %s: %s -> %s", ic.Repo, ic.Number, ToYMDHMSDate(ic.CreatedAt), ic.EventType, from, to)
				}
				updatesMutex.Lock()
				_, ok := infos[why]
				if ok {
					infos[why] = append(infos[why], what)
				} else {
					infos[why] = []string{what}
				}
				updatesMutex.Unlock()
			}

			// Check assignee change
			changedAssignee := false
			if (apiAssigneeID == nil && ghaAssigneeID != nil) || (apiAssigneeID != nil && ghaAssigneeID == nil) || (apiAssigneeID != nil && ghaAssigneeID != nil && *apiAssigneeID != *ghaAssigneeID) {
				changedAssignee = true
				from := Null
				if ghaAssigneeID != nil {
					from = fmt.Sprintf("%d", *ghaAssigneeID)
				}
				to := Null
				if apiAssigneeID != nil {
					to = fmt.Sprintf("%d", *apiAssigneeID)
				}
				if ctx.Debug > 1 {
					Printf("Updating PR '%v' assignee %s -> %s\n", ic, from, to)
				}
				why = "changed pr assignee"
				if manual {
					what = fmt.Sprintf("%s %d: %s -> %s", ic.Repo, ic.Number, from, to)
				} else {
					what = fmt.Sprintf("%s %d %s %s: %s -> %s", ic.Repo, ic.Number, ToYMDHMSDate(ic.CreatedAt), ic.EventType, from, to)
				}
				updatesMutex.Lock()
				_, ok := infos[why]
				if ok {
					infos[why] = append(infos[why], what)
				} else {
					infos[why] = []string{what}
				}
				updatesMutex.Unlock()
			}

			// Check merged by change
			changedMergedBy := false
			if (apiMergedByID == nil && ghaMergedByID != nil) || (apiMergedByID != nil && ghaMergedByID == nil) || (apiMergedByID != nil && ghaMergedByID != nil && *apiMergedByID != *ghaMergedByID) {
				changedMergedBy = true
				from := Null
				if ghaMergedByID != nil {
					from = fmt.Sprintf("%d", *ghaMergedByID)
				}
				to := Null
				if apiMergedByID != nil {
					to = fmt.Sprintf("%d", *apiMergedByID)
				}
				if ctx.Debug > 1 {
					Printf("Updating PR '%v' merged by %s -> %s\n", ic, from, to)
				}
				why = "changed pr merged by"
				if manual {
					what = fmt.Sprintf("%s %d: %s -> %s", ic.Repo, ic.Number, from, to)
				} else {
					what = fmt.Sprintf("%s %d %s %s: %s -> %s", ic.Repo, ic.Number, ToYMDHMSDate(ic.CreatedAt), ic.EventType, from, to)
				}
				updatesMutex.Lock()
				_, ok := infos[why]
				if ok {
					infos[why] = append(infos[why], what)
				} else {
					infos[why] = []string{what}
				}
				updatesMutex.Unlock()
			}

			// API Assignees
			AssigneesMap := make(map[int64]string)
			for _, assignee := range pr.Assignees {
				AssigneesMap[*assignee.ID] = *assignee.Login
			}
			assigneesAry := Int64Ary{}
			for assignee := range AssigneesMap {
				assigneesAry = append(assigneesAry, assignee)
			}
			sort.Sort(assigneesAry)
			l = len(assigneesAry)
			apiAssignees := ""
			for i, assignee := range assigneesAry {
				if i == l-1 {
					apiAssignees += fmt.Sprintf("%d", assignee)
				} else {
					apiAssignees += fmt.Sprintf("%d,", assignee)
				}
			}
			// GHA assignees
			rowsA := QuerySQLWithErr(
				c,
				ctx,
				fmt.Sprintf(
					"select coalesce(string_agg(sub.assignee_id::text, ','), '') from "+
						"(select assignee_id from gha_pull_requests_assignees where event_id = %s "+
						"order by assignee_id) sub",
					NValue(1),
				),
				ghaEventID,
			)
			defer func() { FatalOnError(rowsA.Close()) }()
			ghaAssignees := ""
			for rowsA.Next() {
				FatalOnError(rowsA.Scan(&ghaAssignees))
			}
			FatalOnError(rowsA.Err())
			changedAssignees := false
			if ghaAssignees != apiAssignees {
				if ctx.Debug > 1 {
					Printf("Updating PR '%v' assignees to '%s', they were: '%s' (event_id %d)\n", ic, apiAssignees, ghaAssignees, ghaEventID)
				}
				changedAssignees = true
				why = "changed pr assignees"
				if manual {
					what = fmt.Sprintf("%s %d: %s -> %s", ic.Repo, ic.Number, ghaAssignees, apiAssignees)
				} else {
					what = fmt.Sprintf("%s %d %s %s: %s -> %s", ic.Repo, ic.Number, ToYMDHMSDate(ic.CreatedAt), ic.EventType, ghaAssignees, apiAssignees)
				}
				updatesMutex.Lock()
				_, ok := infos[why]
				if ok {
					infos[why] = append(infos[why], what)
				} else {
					infos[why] = []string{what}
				}
				updatesMutex.Unlock()
			}

			// API Requested reviewers
			RequestedReviewersMap := make(map[int64]string)
			for _, reviewer := range pr.RequestedReviewers {
				RequestedReviewersMap[*reviewer.ID] = *reviewer.Login
			}
			reviewersAry := Int64Ary{}
			for reviewer := range RequestedReviewersMap {
				reviewersAry = append(reviewersAry, reviewer)
			}
			sort.Sort(reviewersAry)
			l = len(reviewersAry)
			apiRequestedReviewers := ""
			for i, reviewer := range reviewersAry {
				if i == l-1 {
					apiRequestedReviewers += fmt.Sprintf("%d", reviewer)
				} else {
					apiRequestedReviewers += fmt.Sprintf("%d,", reviewer)
				}
			}
			// GHA reviewers
			rowsRV := QuerySQLWithErr(
				c,
				ctx,
				fmt.Sprintf(
					"select coalesce(string_agg(sub.requested_reviewer_id::text, ','), '') from "+
						"(select requested_reviewer_id from gha_pull_requests_requested_reviewers where event_id = %s "+
						"order by requested_reviewer_id) sub",
					NValue(1),
				),
				ghaEventID,
			)
			defer func() { FatalOnError(rowsRV.Close()) }()
			ghaRequestedReviewers := ""
			for rowsRV.Next() {
				FatalOnError(rowsRV.Scan(&ghaRequestedReviewers))
			}
			FatalOnError(rowsRV.Err())
			changedRequestedReviewers := false
			if ghaRequestedReviewers != apiRequestedReviewers {
				if ctx.Debug > 1 {
					Printf("Updating PR '%v' requested reviewers to '%s', they were: '%s' (event_id %d)\n", ic, apiRequestedReviewers, ghaRequestedReviewers, ghaEventID)
				}
				changedRequestedReviewers = true
				why = "changed pr reqested reviewers"
				if manual {
					what = fmt.Sprintf("%s %d: %s -> %s", ic.Repo, ic.Number, ghaRequestedReviewers, apiRequestedReviewers)
				} else {
					what = fmt.Sprintf("%s %d %s %s: %s -> %s", ic.Repo, ic.Number, ToYMDHMSDate(ic.CreatedAt), ic.EventType, ghaRequestedReviewers, apiRequestedReviewers)
				}
				updatesMutex.Lock()
				_, ok := infos[why]
				if ok {
					infos[why] = append(infos[why], what)
				} else {
					infos[why] = []string{what}
				}
				updatesMutex.Unlock()
			}

			uidx := 1
			why = "previous pr state the same"
			if manual {
				what = fmt.Sprintf("%s %d", ic.Repo, ic.Number)
			} else {
				what = fmt.Sprintf("%s %d %s %s", ic.Repo, ic.Number, ToYMDHMSDate(ic.CreatedAt), ic.EventType)
			}
			// Do the update if needed
			changedAnything := changedMilestone || changedState || changedClosed || changedMerged || changedMergedAt || changedMergedBy || changedAssignee || changedTitle || changedAssignees || changedRequestedReviewers
			if changedAnything {
				uidx = 2
				if manual {
					FatalOnError(
						ArtificialPREvent(
							c,
							ctx,
							&ic,
							&pr,
						),
					)
					why = "previous pr state different"
					what = fmt.Sprintf("%s %d", ic.Repo, ic.Number)
				} else {
					if ctx.Debug > 0 {
						Printf("Warning: Exact artificial PR event (%v, %d) already exists with different state, skipping: '%v'\n", ic.CreatedAt, eventID, ic)
					}
					why = "collision and pr state differs"
					what = fmt.Sprintf("%s %d %s %s: %d", ic.Repo, ic.Number, ToYMDHMSDate(ic.CreatedAt), ic.EventType, eventID)
					if !ctx.SkipUpdateEvents {
						why = "updated existing pr state"
						FatalOnError(DeleteArtificialPREvent(c, ctx, &ic))
						FatalOnError(ArtificialPREvent(c, ctx, &ic, &pr))
					}
				}
			}

			if ctx.Debug > 1 {
				if manual {
					Printf("PR Event exist (event_id: %d), added artificial: %v: '%v'\n", ghaEventID, changedAnything, ic)
				} else {
					Printf("PR Event for the same date (%v) exist (event_id: %d), added artificial: %v: '%v'\n", updatedAt, ghaEventID, changedAnything, ic)
				}
			}
			updatesMutex.Lock()
			updates[uidx]++
			_, ok := infos[why]
			if ok {
				infos[why] = append(infos[why], what)
			} else {
				infos[why] = []string{what}
			}
			updatesMutex.Unlock()
			// Synchronize go routine
			ch <- changedAnything
		}(ch, iid)

		// go routine called with 'ch' channel to sync and tag index
		nThreads++
		for nThreads >= thrN {
			<-ch
			nThreads--
			prc++
			if prc%20 == 0 {
				thrN = GetThreadsNum(ctx)
			}
			checked++
			ProgressInfo(checked, nIssues, dtStart, &lastTime, time.Duration(10)*time.Second, "")
		}
	}
	// Usually all work happens on '<-ch'
	for nThreads > 0 {
		<-ch
		nThreads--
		checked++
		ProgressInfo(checked, nIssues, dtStart, &lastTime, time.Duration(10)*time.Second, "")
	}
	// Get RateLimits info
	hint, _, rem, wait = GetRateLimits(gctx, ctx, gc, true)
	if manual {
		Printf(
			"ghapi2db.go: Manually processed %d PRs (%d new PRs, existing: %d not needed, %d added): %+v API points remain, resets in %+v, hint key: %d\n",
			checked, updates[0], updates[1], updates[2], rem, wait, hint,
		)
	} else {
		Printf(
			"ghapi2db.go: Automatically processed %d PRs (%d new PRs, existing: %d not needed, %d added): %+v API points remain, resets in %+v, hint key: %d\n",
			checked, updates[0], updates[1], updates[2], rem, wait, hint,
		)
	}
	// Info
	outputInfo(infos, "PRs")
}
