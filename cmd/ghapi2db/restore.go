package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	lib "github.com/cncf/devstatscode"
	"github.com/google/go-github/v38/github"
)

const restorePageCap = 2000

type restoreStats struct {
	checked  int
	restored int
	pages    int
	// repos whose stargazer list GitHub refuses to return (restricted to repository admins
	// since 2026-06-30) although the repository has stars - stars restore only
	unavailable int
	minDt       time.Time
	maxDt       time.Time
	// event ids of restored rows that produce postprocessed data (comments/reviews - text
	// sources); forks/releases/stars restores add no gha_texts/labels/issue-PR-link rows,
	// so they are counted but never collected here (they must not trigger a postprocess)
	eids []int64
	// restored events per GHA event type - repo events feed and issues/PRs sweep
	types map[string]int
	// issues/PRs sweep: upgraded stub gha_pull_requests rows, the pull requests they belong to,
	// gha_issues rows attached to pull requests that had none
	stubRows  int
	stubPRs   int
	issueRows int
	// repo events feed: repositories and events outside the project's org/repo/actor rules (bug 75)
	filteredRepos  int
	filteredEvents int
}

// addType - count one restored event of the given type
func (st *restoreStats) addType(eType string) {
	if st.types == nil {
		st.types = make(map[string]int)
	}
	st.types[eType]++
}

// typesSummary - "TypeA N, TypeB M" sorted by type name
func (st *restoreStats) typesSummary() string {
	names := make([]string, 0, len(st.types))
	for name := range st.types {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("%s %d", name, st.types[name]))
	}
	return strings.Join(parts, ", ")
}

func (st *restoreStats) mark(dt time.Time) {
	if st.minDt.IsZero() || dt.Before(st.minDt) {
		st.minDt = dt
	}
	if dt.After(st.maxDt) {
		st.maxDt = dt
	}
}

func (st *restoreStats) merge(o restoreStats) {
	st.checked += o.checked
	st.restored += o.restored
	st.pages += o.pages
	st.unavailable += o.unavailable
	st.stubRows += o.stubRows
	st.stubPRs += o.stubPRs
	st.issueRows += o.issueRows
	st.filteredRepos += o.filteredRepos
	st.filteredEvents += o.filteredEvents
	if !o.minDt.IsZero() {
		st.mark(o.minDt)
	}
	if !o.maxDt.IsZero() {
		st.mark(o.maxDt)
	}
	st.eids = append(st.eids, o.eids...)
	for eType, n := range o.types {
		if st.types == nil {
			st.types = make(map[string]int)
		}
		st.types[eType] += n
	}
}

type restoreRepoFunc func(gctx context.Context, gc *ghClients, c *sql.DB, ctx *lib.Ctx, org, repo, orgRepo string, repoID int64, orgID interface{}, recentDt time.Time, maybeHide func(string) string, stats *restoreStats)

// ghClients - the GitHub clients (one per token) of a restore pass. do() picks the client for every single
// request (bug 65: one client per repository exhausted its token on a repository needing thousands of requests
// - the P1-C stub sweep - and skipped the rest of it while the other tokens were idle) by the rate limits
// observed in the responses (X-RateLimit-Remaining/Reset): GET /rate_limit no longer reflects the real usage
// (2026-09: it reported 5000 remaining for tokens whose responses said 0), so a token is only trusted after its
// first answer; one answering "rate limit exceeded" is not used again before its reset and the request is
// repeated with the next token - the rate limit error reaches the caller only when every token is exhausted.
type ghClients struct {
	gctx context.Context
	ctx  *lib.Ctx
	gcs  []*github.Client
	mtx  sync.Mutex
	seen []tokenState
}

// tokenState - the rate limit of one token as seen in its last response
type tokenState struct {
	known     bool
	remaining int
	reset     time.Time
}

// unknownRemaining - a token not seen yet (or past its reset) counts as full
const unknownRemaining = 1 << 30

func newGHClients(gctx context.Context, ctx *lib.Ctx, gcs []*github.Client) *ghClients {
	return &ghClients{gctx: gctx, ctx: ctx, gcs: gcs, seen: make([]tokenState, len(gcs))}
}

// pick - the index of the client with the most remaining points (never seen = full), -1 when every token is
// exhausted (then the soonest reset); the picked token's remaining is decremented so concurrent requests spread
func (g *ghClients) pick() (int, time.Time) {
	now := time.Now()
	g.mtx.Lock()
	defer g.mtx.Unlock()
	best, bestRem := -1, -1
	var soonest time.Time
	for i := range g.gcs {
		st := &g.seen[i]
		rem := unknownRemaining
		if st.known && st.reset.After(now) {
			rem = st.remaining
		}
		if rem <= 0 {
			if soonest.IsZero() || st.reset.Before(soonest) {
				soonest = st.reset
			}
			continue
		}
		if rem > bestRem {
			best, bestRem = i, rem
		}
	}
	if best >= 0 && bestRem != unknownRemaining {
		g.seen[best].remaining--
	}
	return best, soonest
}

// observe - record the rate limit seen in a response of the client i (exhausted: remaining 0 until reset)
func (g *ghClients) observe(i int, rate github.Rate, exhausted bool) {
	reset := rate.Reset.Time
	if exhausted && !reset.After(time.Now()) {
		// no usable reset in the answer: leave the token alone for a minute
		reset = time.Now().Add(time.Minute)
	}
	g.mtx.Lock()
	defer g.mtx.Unlock()
	g.seen[i] = tokenState{known: true, remaining: rate.Remaining, reset: reset}
	if exhausted {
		g.seen[i].remaining = 0
	}
}

// do - runs call with the picked client, moving to the next token on "rate limit exceeded"
func (g *ghClients) do(call func(cl *github.Client) (*github.Response, error)) (*github.Response, error) {
	for {
		idx, soonest := g.pick()
		if idx < 0 {
			return nil, &github.RateLimitError{
				Rate:    github.Rate{Reset: github.Timestamp{Time: soonest}},
				Message: fmt.Sprintf("all %d tokens exhausted until %v", len(g.gcs), soonest),
			}
		}
		resp, err := call(g.gcs[idx])
		var rle *github.RateLimitError
		if err != nil && errors.As(err, &rle) {
			g.observe(idx, rle.Rate, true)
			if g.ctx.GitHubDebug > 0 {
				lib.Printf("token %d exhausted until %v (%d tokens left)\n", idx, rle.Rate.Reset.Time, g.available())
			}
			continue
		}
		if resp != nil && resp.Rate.Limit > 0 {
			g.observe(idx, resp.Rate, false)
		}
		return resp, err
	}
}

// responseStatus - the HTTP status of a do() outcome: the response's, 403 when every token was exhausted (do()
// returns no response then, only the rate limit error), 0 when there is no response for another reason
func responseStatus(resp *github.Response, err error) int {
	if resp != nil {
		return resp.StatusCode
	}
	var rle *github.RateLimitError
	if errors.As(err, &rle) {
		return http.StatusForbidden
	}
	return 0
}

// available - the number of tokens not known to be exhausted
func (g *ghClients) available() int {
	now := time.Now()
	g.mtx.Lock()
	defer g.mtx.Unlock()
	n := 0
	for _, st := range g.seen {
		if !st.known || !st.reset.After(now) || st.remaining > 0 {
			n++
		}
	}
	return n
}

func numberFromURL(url *string) int {
	if url == nil {
		return 0
	}
	ary := strings.Split(*url, "/")
	n, err := strconv.Atoi(ary[len(ary)-1])
	if err != nil {
		return 0
	}
	return n
}

func idPresent(c *sql.DB, ctx *lib.Ctx, table, eType string, id int64) bool {
	rows := lib.QuerySQLWithErr(c, ctx, fmt.Sprintf("select 1 from %s where id = %s and dup_type = %s limit 1", table, lib.NValue(1), lib.NValue(2)), id, eType)
	defer func() { lib.FatalOnError(rows.Close()) }()
	present := false
	for rows.Next() {
		present = true
	}
	lib.FatalOnError(rows.Err())
	return present
}

func forkPresent(c *sql.DB, ctx *lib.Ctx, forkeeID int64) bool {
	rows := lib.QuerySQLWithErr(c, ctx, "select 1 from gha_forkees f, gha_payloads p where f.id = "+lib.NValue(1)+" and p.event_id = f.event_id and p.forkee_id = f.id and p.dup_type = 'ForkEvent' limit 1", forkeeID)
	defer func() { lib.FatalOnError(rows.Close()) }()
	present := false
	for rows.Next() {
		present = true
	}
	lib.FatalOnError(rows.Err())
	return present
}

func starPresent(c *sql.DB, ctx *lib.Ctx, actorID int64, orgRepo string, starredAt time.Time) bool {
	rows := lib.QuerySQLWithErr(c, ctx, "select 1 from gha_events where type = 'WatchEvent' and actor_id = "+lib.NValue(1)+" and dup_repo_name = "+lib.NValue(2)+" and created_at = "+lib.NValue(3)+" limit 1", actorID, orgRepo, starredAt)
	defer func() { lib.FatalOnError(rows.Close()) }()
	present := false
	for rows.Next() {
		present = true
	}
	lib.FatalOnError(rows.Err())
	return present
}

// repoIDs - repository id (0 when unknown) and organization id (nil when none): the current id of
// the name (lib.CurrentRepoID, bug 68: not max(repo_id), which returned a placeholder id for
// kubernetes/kubernetes), from gha_repos when the repository has no events yet (a GHA gap)
func repoIDs(c *sql.DB, ctx *lib.Ctx, orgRepo string) (repoID int64, orgID interface{}) {
	rid, oid, native, ok := lib.CurrentRepoID(c, ctx, orgRepo)
	if !ok {
		return
	}
	repoID = rid
	if oid != nil {
		orgID = *oid
	}
	if !native && ctx.Debug > 0 {
		lib.Printf("%s: no events, using gha_repos id %d\n", orgRepo, repoID)
	}
	return
}

// apiPage - true: process next page, false: skip repo; retries 403 abuse with backoff
func apiPage(ctx *lib.Ctx, info string, call func() (*github.Response, bool, error)) bool {
	for try := 1; try <= ctx.MaxGHAPIRetry; try++ {
		resp, more, err := call()
		if resp != nil && (resp.StatusCode == 404 || resp.StatusCode == 410) {
			return false
		}
		if err != nil {
			var rle *github.RateLimitError
			if errors.As(err, &rle) {
				wait := time.Until(rle.Rate.Reset.Time)
				if wait.Seconds() <= float64(ctx.MaxGHAPIWaitSeconds) {
					if wait > 0 {
						time.Sleep(wait + time.Second)
					}
					continue
				}
				if ctx.GHAPIErrorIsFatal {
					lib.Fatalf("%s: rate limited, don't want to wait %v", info, wait)
				}
				lib.Printf("%s: rate limited, reset in %v, skipping\n", info, wait)
				return false
			}
			var arle *github.AbuseRateLimitError
			if errors.As(err, &arle) {
				wait := time.Duration(10*try) * time.Second
				if arle.RetryAfter != nil {
					wait = *arle.RetryAfter
				}
				if wait.Seconds() <= float64(ctx.MaxGHAPIWaitSeconds) {
					lib.Printf("%s: abuse detected, waiting %v, retry %d/%d\n", info, wait, try, ctx.MaxGHAPIRetry)
					time.Sleep(wait)
					continue
				}
				if ctx.GHAPIErrorIsFatal {
					lib.Fatalf("%s: abuse detected, don't want to wait %v", info, wait)
				}
				lib.Printf("%s: abuse detected, don't want to wait %v, skipping\n", info, wait)
				return false
			}
		}
		if resp != nil && resp.StatusCode == 403 {
			lib.Printf("%s: abuse detected, retry %d/%d\n", info, try, ctx.MaxGHAPIRetry)
			time.Sleep(time.Duration(10*try) * time.Second)
			continue
		}
		if resp != nil && resp.StatusCode >= 400 {
			lib.Printf("%s: status %d, skipping\n", info, resp.StatusCode)
			return false
		}
		if err != nil {
			if ctx.GHAPIErrorIsFatal {
				lib.FatalOnError(err)
			}
			lib.Printf("%s: error: %+v, skipping\n", info, err)
			return false
		}
		return more
	}
	lib.Printf("%s: giving up after %d retries\n", info, ctx.MaxGHAPIRetry)
	return false
}

func restorePass(ctx *lib.Ctx, pass apiPass, process restoreRepoFunc) restoreStats {
	name := pass.label()
	repos, skipped, isSingleRepo, singleRepo, gctx, gcs, c, recentDt := getAPIParams(ctx, pass)
	defer func() { lib.FatalOnError(c.Close()) }()
	maybeHide := lib.MaybeHideFuncTS(lib.GetHidden(ctx, lib.HideCfgFile))
	nRepos := len(repos)
	lib.Printf("%s: processing %d repos%s, recent date: %v\n", name, nRepos, scopeSuffix(skipped), recentDt)
	hint, _, rem, _ := lib.GetRateLimits(gctx, ctx, gcs, true)
	thrN := lib.GetThreadsNum(ctx)
	mtx := &sync.Mutex{}
	total := restoreStats{}
	processed := 0
	lastTime := time.Now()
	dtStart := lastTime
	freq := time.Duration(30) * time.Second
	iter := func() {
		processed++
		if processed%20 == 0 {
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
				h, _, r, w = lib.GetRateLimits(gctx, ctx, gcs, true)
			}
			mtx.Lock()
			hint, rem = h, r
			mtx.Unlock()
		}
		mtx.Lock()
		msg := fmt.Sprintf("%s: API points: %+v, hint: %d", name, rem, hint)
		mtx.Unlock()
		lib.ProgressInfo(processed, nRepos, dtStart, &lastTime, freq, msg)
	}
	clients := newGHClients(gctx, ctx, gcs)
	processRepo := func(ch chan struct{}, orgRepo string) {
		defer func() {
			if ch != nil {
				ch <- struct{}{}
			}
		}()
		ary := strings.Split(orgRepo, "/")
		if len(ary) < 2 {
			lib.Printf("WARNING: %s: malformed repo name: '%s'\n", name, orgRepo)
			return
		}
		repoID, orgID := repoIDs(c, ctx, orgRepo)
		if repoID <= 0 {
			lib.Printf("%s: %s: no existing repo_id, skipping restore\n", name, orgRepo)
			return
		}
		stats := restoreStats{}
		process(gctx, clients, c, ctx, ary[0], ary[1], orgRepo, repoID, orgID, recentDt, maybeHide, &stats)
		mtx.Lock()
		total.merge(stats)
		mtx.Unlock()
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
	lib.Printf("%s: processed %d repos, %d pages, checked %d, restored %d\n", name, processed, total.pages, total.checked, total.restored)
	if len(total.types) > 0 {
		lib.Printf("%s: restored events by type: %s\n", name, total.typesSummary())
	}
	if total.unavailable > 0 {
		lib.Printf("%s: stargazer lists unavailable for %d/%d repos (GitHub restricted stargazer/watcher lists to repository admins on 2026-06-30), star events cannot be restored\n", name, total.unavailable, processed)
	}
	return total
}

func restoreCommentsRepo(gctx context.Context, gc *ghClients, c *sql.DB, ctx *lib.Ctx, org, repo, orgRepo string, repoID int64, orgID interface{}, recentDt time.Time, maybeHide func(string) string, stats *restoreStats) {
	sort := "updated"
	direction := "asc"
	opt := &github.IssueListCommentsOptions{Sort: &sort, Direction: &direction, Since: &recentDt}
	opt.PerPage = 100
	for page := 1; page <= restorePageCap; page++ {
		opt.Page = page
		more := apiPage(ctx, orgRepo+" issue comments", func() (*github.Response, bool, error) {
			var comments []*github.IssueComment
			resp, err := gc.do(func(cl *github.Client) (r *github.Response, e error) {
				comments, r, e = cl.Issues.ListComments(gctx, org, repo, 0, opt)
				return
			})
			if err != nil || resp == nil || resp.StatusCode >= 400 {
				return resp, false, err
			}
			stats.pages++
			for _, cmt := range comments {
				if cmt == nil || cmt.ID == nil {
					continue
				}
				stats.checked++
				if idPresent(c, ctx, "gha_comments", "IssueCommentEvent", *cmt.ID) {
					continue
				}
				if eid, ok := lib.RestoreIssueComment(c, ctx, orgRepo, repoID, orgID, numberFromURL(cmt.IssueURL), cmt, maybeHide); ok {
					stats.restored++
					stats.mark(*cmt.CreatedAt)
					stats.eids = append(stats.eids, eid)
				}

			}
			return resp, resp.NextPage != 0, nil
		})
		if !more {
			break
		}
	}
	popt := &github.PullRequestListCommentsOptions{Sort: "updated", Direction: "asc", Since: recentDt}
	popt.PerPage = 100
	for page := 1; page <= restorePageCap; page++ {
		popt.Page = page
		more := apiPage(ctx, orgRepo+" review comments", func() (*github.Response, bool, error) {
			var comments []*github.PullRequestComment
			resp, err := gc.do(func(cl *github.Client) (r *github.Response, e error) {
				comments, r, e = cl.PullRequests.ListComments(gctx, org, repo, 0, popt)
				return
			})
			if err != nil || resp == nil || resp.StatusCode >= 400 {
				return resp, false, err
			}
			stats.pages++
			for _, cmt := range comments {
				if cmt == nil || cmt.ID == nil {
					continue
				}
				stats.checked++
				if idPresent(c, ctx, "gha_comments", "PullRequestReviewCommentEvent", *cmt.ID) {
					continue
				}
				if eid, ok := lib.RestoreReviewComment(c, ctx, orgRepo, repoID, orgID, numberFromURL(cmt.PullRequestURL), cmt, maybeHide); ok {
					stats.restored++
					stats.mark(*cmt.CreatedAt)
					stats.eids = append(stats.eids, eid)
				}

			}
			return resp, resp.NextPage != 0, nil
		})
		if !more {
			break
		}
	}
	// commit comments API has no since filter and lists ascending - walk from the last page down
	copt := &github.ListOptions{PerPage: 100, Page: 1}
	last := 1
	apiPage(ctx, orgRepo+" commit comments last page", func() (*github.Response, bool, error) {
		resp, err := gc.do(func(cl *github.Client) (r *github.Response, e error) {
			_, r, e = cl.Repositories.ListComments(gctx, org, repo, copt)
			return
		})
		if err != nil || resp == nil || resp.StatusCode >= 400 {
			return resp, false, err
		}
		if resp.LastPage > 1 {
			last = resp.LastPage
		}
		return resp, false, nil
	})
	for page := last; page >= 1; page-- {
		copt.Page = page
		anyRecent := false
		ok := apiPage(ctx, orgRepo+" commit comments", func() (*github.Response, bool, error) {
			var comments []*github.RepositoryComment
			resp, err := gc.do(func(cl *github.Client) (r *github.Response, e error) {
				comments, r, e = cl.Repositories.ListComments(gctx, org, repo, copt)
				return
			})
			if err != nil || resp == nil || resp.StatusCode >= 400 {
				return resp, false, err
			}
			stats.pages++
			for _, cmt := range comments {
				if cmt == nil || cmt.ID == nil || cmt.CreatedAt == nil || cmt.CreatedAt.Before(recentDt) {
					continue
				}
				anyRecent = true
				stats.checked++
				if idPresent(c, ctx, "gha_comments", "CommitCommentEvent", *cmt.ID) {
					continue
				}
				if eid, ok := lib.RestoreCommitComment(c, ctx, orgRepo, repoID, orgID, cmt, maybeHide); ok {
					stats.restored++
					stats.mark(*cmt.CreatedAt)
					stats.eids = append(stats.eids, eid)
				}

			}
			return resp, true, nil
		})
		if !ok || !anyRecent {
			break
		}
	}
}

func ghTokens(ctx *lib.Ctx) []string {
	oAuth := strings.TrimSpace(ctx.GitHubOAuth)
	if strings.Contains(oAuth, "/") {
		bytes, err := lib.ReadFile(ctx, oAuth)
		lib.FatalOnError(err)
		oAuth = string(bytes)
	}
	parts := strings.FieldsFunc(oAuth, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == '\t' || r == ' '
	})
	tokens := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" && p != "-" {
			tokens = append(tokens, p)
		}
	}
	return tokens
}

type gqlStargazer struct {
	starredAt time.Time
	login     string
	id        int64
}

// ghGraphQLPost - POST a GraphQL payload trying the tokens round-robin from start; handle decodes a 200
// body (its error means: try the next token, unless it wraps errGraphQLNoRetry). 403/429 wait for the
// rate limit (Retry-After / X-RateLimit-Reset) up to MaxGHAPIWaitSeconds, other statuses, transport and
// decode errors move on to the next token. Returns the last error when no token succeeded.
func ghGraphQLPost(gctx context.Context, ctx *lib.Ctx, tokens []string, start int, what string, payload []byte, handle func(body []byte, tokenIdx int) error) (err error) {
	cl := &http.Client{Timeout: time.Duration(60) * time.Second}
	graphQLURL := "https://api.github.com/graphql"
	if ctx.GitHubAPIURL != "" {
		graphQLURL = ctx.GitHubAPIURL + "graphql"
	}
	n := len(tokens)
	for k := 0; k < n; k++ {
		i := (start + k) % n
		token := tokens[i]
		for try := 1; try <= ctx.MaxGHAPIRetry; try++ {
			var req *http.Request
			req, err = http.NewRequestWithContext(gctx, "POST", graphQLURL, bytes.NewReader(payload))
			if err != nil {
				return
			}
			req.Header.Set("Authorization", "bearer "+token)
			req.Header.Set("Content-Type", "application/json")
			var resp *http.Response
			resp, err = cl.Do(req)
			if err != nil {
				break
			}
			var body []byte
			body, err = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil {
				break
			}
			snippet := string(body[:min(len(body), 200)])
			if resp.StatusCode == 403 || resp.StatusCode == 429 {
				wait := time.Duration(10*try) * time.Second
				if ra := resp.Header.Get("Retry-After"); ra != "" {
					if secs, perr := strconv.Atoi(ra); perr == nil {
						wait = time.Duration(secs) * time.Second
					}
				} else if xr := resp.Header.Get("X-RateLimit-Reset"); xr != "" {
					if epoch, perr := strconv.ParseInt(xr, 10, 64); perr == nil {
						wait = time.Until(time.Unix(epoch, 0))
					}
				}
				if wait > 0 && wait.Seconds() <= float64(ctx.MaxGHAPIWaitSeconds) {
					time.Sleep(wait)
					continue
				}
				if ctx.GHAPIErrorIsFatal {
					lib.Fatalf("%s: graphql rate limited, don't want to wait %v: %s", what, wait, snippet)
				}
				err = fmt.Errorf("graphql rate limited (token %d/%d), reset in %v: %s", i+1, n, wait, snippet)
				break
			}
			if resp.StatusCode != 200 {
				err = fmt.Errorf("graphql status %d (token %d/%d): %s", resp.StatusCode, i+1, n, snippet)
				break
			}
			err = handle(body, i)
			if err == nil {
				return nil
			}
			if errors.Is(err, errGraphQLNoRetry) {
				return
			}
			break
		}
	}
	return
}

// ghGraphQLStargazers - stars restore uses GraphQL: the REST stargazers path returns 404 / no usable
// starred_at data on prod as of 2026-07; GraphQL exposes starredAt directly, ordered by STARRED_AT.
// Also returns the number of raw edges on the page and the repository's stargazerCount: since
// 2026-06-30 GitHub returns an empty stargazers connection for non-admins while the count still works,
// which is how an unavailable list is told apart from a repository nobody starred.
func ghGraphQLStargazers(gctx context.Context, ctx *lib.Ctx, tokens []string, org, repo, before string) (gazers []gqlStargazer, prevCursor string, hasPrev bool, nEdges int, starCount int64, err error) {
	vars := map[string]interface{}{"o": org, "r": repo}
	if before != "" {
		vars["b"] = before
	}
	payload, err := json.Marshal(map[string]interface{}{
		"query":     "query($o: String!, $r: String!, $b: String) { repository(owner: $o, name: $r) { stargazerCount stargazers(last: 100, before: $b, orderBy: {field: STARRED_AT, direction: ASC}) { pageInfo { hasPreviousPage startCursor } edges { starredAt node { login databaseId } } } } }",
		"variables": vars,
	})
	if err != nil {
		return
	}
	err = ghGraphQLPost(gctx, ctx, tokens, 0, org+"/"+repo, payload, func(body []byte, tokenIdx int) error {
		var out struct {
			Data struct {
				Repository struct {
					StargazerCount int64 `json:"stargazerCount"`
					Stargazers     struct {
						PageInfo struct {
							HasPreviousPage bool   `json:"hasPreviousPage"`
							StartCursor     string `json:"startCursor"`
						} `json:"pageInfo"`
						Edges []struct {
							StarredAt time.Time `json:"starredAt"`
							Node      struct {
								Login      string `json:"login"`
								DatabaseID int64  `json:"databaseId"`
							} `json:"node"`
						} `json:"edges"`
					} `json:"stargazers"`
				} `json:"repository"`
			} `json:"data"`
			Errors []struct {
				Message string `json:"message"`
			} `json:"errors"`
		}
		if derr := json.Unmarshal(body, &out); derr != nil {
			return derr
		}
		if len(out.Errors) > 0 {
			return fmt.Errorf("graphql (token %d/%d): %s", tokenIdx+1, len(tokens), out.Errors[0].Message)
		}
		sg := out.Data.Repository.Stargazers
		gazers = nil
		for _, edge := range sg.Edges {
			if edge.Node.DatabaseID <= 0 || edge.Node.Login == "" || edge.StarredAt.IsZero() {
				continue
			}
			gazers = append(gazers, gqlStargazer{starredAt: edge.StarredAt, login: edge.Node.Login, id: edge.Node.DatabaseID})
		}
		prevCursor, hasPrev, nEdges, starCount = sg.PageInfo.StartCursor, sg.PageInfo.HasPreviousPage, len(sg.Edges), out.Data.Repository.StargazerCount
		return nil
	})
	return
}

func restoreStarsRepo(gctx context.Context, gc *ghClients, c *sql.DB, ctx *lib.Ctx, org, repo, orgRepo string, repoID int64, orgID interface{}, recentDt time.Time, maybeHide func(string) string, stats *restoreStats) {
	tokens := ghTokens(ctx)
	if len(tokens) == 0 {
		lib.Printf("%s: stars restore needs GHA2DB_GITHUB_OAUTH token(s), skipping\n", orgRepo)
		return
	}
	before := ""
	for page := 1; page <= restorePageCap; page++ {
		gazers, prev, hasPrev, nEdges, starCount, err := ghGraphQLStargazers(gctx, ctx, tokens, org, repo, before)
		if err != nil {
			lib.Printf("%s: stargazers graphql: %+v, skipping\n", orgRepo, err)
			return
		}
		stats.pages++
		if page == 1 && nEdges == 0 && starCount > 0 {
			// the repository has stars but GitHub returns none: the list is restricted, not empty
			stats.unavailable++
			if ctx.Debug > 0 {
				lib.Printf("%s: stargazer list unavailable (%d stars), skipping\n", orgRepo, starCount)
			}
			return
		}
		anyRecent := false
		for _, g := range gazers {
			if g.starredAt.Before(recentDt) {
				continue
			}
			anyRecent = true
			stats.checked++
			if starPresent(c, ctx, g.id, orgRepo, g.starredAt) {
				continue
			}
			id, login, dt := g.id, g.login, g.starredAt
			star := &github.Stargazer{StarredAt: &github.Timestamp{Time: dt}, User: &github.User{ID: &id, Login: &login}}
			if _, ok := lib.RestoreStar(c, ctx, orgRepo, repoID, orgID, star, maybeHide); ok {
				stats.restored++
				stats.mark(dt)
			}
		}
		if !anyRecent || !hasPrev {
			break
		}
		before = prev
	}
}
func restoreReviewsRepo(gctx context.Context, gc *ghClients, c *sql.DB, ctx *lib.Ctx, org, repo, orgRepo string, repoID int64, orgID interface{}, recentDt time.Time, maybeHide func(string) string, stats *restoreStats) {
	prNumbers := []int{}
	opt := &github.PullRequestListOptions{State: "all", Sort: "updated", Direction: "desc"}
	opt.PerPage = 100
	for page := 1; page <= restorePageCap; page++ {
		opt.Page = page
		older := false
		more := apiPage(ctx, orgRepo+" PRs", func() (*github.Response, bool, error) {
			var prs []*github.PullRequest
			resp, err := gc.do(func(cl *github.Client) (r *github.Response, e error) {
				prs, r, e = cl.PullRequests.List(gctx, org, repo, opt)
				return
			})
			if err != nil || resp == nil || resp.StatusCode >= 400 {
				return resp, false, err
			}
			stats.pages++
			for _, pr := range prs {
				if pr == nil || pr.Number == nil {
					continue
				}
				if pr.UpdatedAt != nil && pr.UpdatedAt.Before(recentDt) {
					older = true
					break
				}
				prNumbers = append(prNumbers, *pr.Number)
			}
			return resp, resp.NextPage != 0, nil
		})
		if !more || older {
			break
		}
	}
	for _, number := range prNumbers {
		ropt := &github.ListOptions{PerPage: 100}
		for page := 1; page <= restorePageCap; page++ {
			ropt.Page = page
			more := apiPage(ctx, fmt.Sprintf("%s#%d reviews", orgRepo, number), func() (*github.Response, bool, error) {
				var reviews []*github.PullRequestReview
				resp, err := gc.do(func(cl *github.Client) (r *github.Response, e error) {
					reviews, r, e = cl.PullRequests.ListReviews(gctx, org, repo, number, ropt)
					return
				})
				if err != nil || resp == nil || resp.StatusCode >= 400 {
					return resp, false, err
				}
				stats.pages++
				for _, rev := range reviews {
					if rev == nil || rev.ID == nil || rev.SubmittedAt == nil {
						continue
					}
					stats.checked++
					if idPresent(c, ctx, "gha_reviews", "PullRequestReviewEvent", *rev.ID) {
						continue
					}
					if eid, ok := lib.RestoreReview(c, ctx, orgRepo, repoID, orgID, number, rev, maybeHide); ok {
						stats.restored++
						stats.mark(*rev.SubmittedAt)
						stats.eids = append(stats.eids, eid)
					}

				}
				return resp, resp.NextPage != 0, nil
			})
			if !more {
				break
			}
		}
	}
}

func restoreForksRepo(gctx context.Context, gc *ghClients, c *sql.DB, ctx *lib.Ctx, org, repo, orgRepo string, repoID int64, orgID interface{}, recentDt time.Time, maybeHide func(string) string, stats *restoreStats) {
	opt := &github.RepositoryListForksOptions{Sort: "newest"}
	opt.PerPage = 100
	for page := 1; page <= restorePageCap; page++ {
		opt.Page = page
		older := false
		more := apiPage(ctx, orgRepo+" forks", func() (*github.Response, bool, error) {
			var forks []*github.Repository
			resp, err := gc.do(func(cl *github.Client) (r *github.Response, e error) {
				forks, r, e = cl.Repositories.ListForks(gctx, org, repo, opt)
				return
			})
			if err != nil || resp == nil || resp.StatusCode >= 400 {
				return resp, false, err
			}
			stats.pages++
			for _, fork := range forks {
				if fork == nil || fork.ID == nil {
					continue
				}
				if fork.CreatedAt != nil && fork.CreatedAt.Time.Before(recentDt) {
					older = true
					break
				}
				stats.checked++
				// if idPresent(c, ctx, "gha_forkees", "ForkEvent", *fork.ID) {
				if forkPresent(c, ctx, *fork.ID) {
					continue
				}
				if _, ok := lib.RestoreFork(c, ctx, orgRepo, repoID, orgID, fork, maybeHide); ok {
					stats.restored++
					stats.mark(fork.CreatedAt.Time)
				}

			}
			return resp, resp.NextPage != 0, nil
		})
		if !more || older {
			break
		}
	}
}

func restoreReleasesRepo(gctx context.Context, gc *ghClients, c *sql.DB, ctx *lib.Ctx, org, repo, orgRepo string, repoID int64, orgID interface{}, recentDt time.Time, maybeHide func(string) string, stats *restoreStats) {
	opt := &github.ListOptions{PerPage: 100}
	for page := 1; page <= restorePageCap; page++ {
		opt.Page = page
		older := false
		more := apiPage(ctx, orgRepo+" releases", func() (*github.Response, bool, error) {
			var rels []*github.RepositoryRelease
			resp, err := gc.do(func(cl *github.Client) (r *github.Response, e error) {
				rels, r, e = cl.Repositories.ListReleases(gctx, org, repo, opt)
				return
			})
			if err != nil || resp == nil || resp.StatusCode >= 400 {
				return resp, false, err
			}
			stats.pages++
			for _, rel := range rels {
				if rel == nil || rel.ID == nil || rel.CreatedAt == nil {
					continue
				}
				relDt := rel.CreatedAt.Time
				if rel.PublishedAt != nil {
					relDt = rel.PublishedAt.Time
				}
				if relDt.Before(recentDt) {
					older = true
					break
				}
				stats.checked++
				if idPresent(c, ctx, "gha_releases", "ReleaseEvent", *rel.ID) {
					continue
				}
				if _, ok := lib.RestoreRelease(c, ctx, orgRepo, repoID, orgID, rel, maybeHide); ok {
					stats.restored++
					stats.mark(relDt)
				}
			}
			return resp, resp.NextPage != 0, nil
		})
		if !more || older {
			break
		}
	}
}

func syncComments(ctx *lib.Ctx) restoreStats {
	return restorePass(ctx, passComments, restoreCommentsRepo)
}

func syncReviews(ctx *lib.Ctx) restoreStats {
	return restorePass(ctx, passReviews, restoreReviewsRepo)
}

func syncForks(ctx *lib.Ctx) restoreStats {
	return restorePass(ctx, passForks, restoreForksRepo)
}

func syncStars(ctx *lib.Ctx) restoreStats {
	return restorePass(ctx, passStars, restoreStarsRepo)
}

func syncReleases(ctx *lib.Ctx) restoreStats {
	return restorePass(ctx, passReleases, restoreReleasesRepo)
}
