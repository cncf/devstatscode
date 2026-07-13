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
	minDt    time.Time
	maxDt    time.Time
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
	if !o.minDt.IsZero() {
		st.mark(o.minDt)
	}
	if !o.maxDt.IsZero() {
		st.mark(o.maxDt)
	}
}

type restoreRepoFunc func(gctx context.Context, gc *github.Client, c *sql.DB, ctx *lib.Ctx, org, repo, orgRepo string, repoID int64, orgID interface{}, recentDt time.Time, maybeHide func(string) string, stats *restoreStats)

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

func repoIDs(c *sql.DB, ctx *lib.Ctx, orgRepo string) (repoID int64, orgID interface{}) {
	rows := lib.QuerySQLWithErr(c, ctx, "select coalesce(max(repo_id), 0), max(org_id) from gha_events where dup_repo_name = "+lib.NValue(1), orgRepo)
	defer func() { lib.FatalOnError(rows.Close()) }()
	var oid *int64
	for rows.Next() {
		lib.FatalOnError(rows.Scan(&repoID, &oid))
	}
	lib.FatalOnError(rows.Err())
	if oid != nil {
		orgID = *oid
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

func restorePass(ctx *lib.Ctx, name string, process restoreRepoFunc) restoreStats {
	repos, isSingleRepo, singleRepo, gctx, gcs, c, recentDt := getAPIParams(ctx)
	defer func() { lib.FatalOnError(c.Close()) }()
	maybeHide := lib.MaybeHideFuncTS(lib.GetHidden(ctx, lib.HideCfgFile))
	nRepos := len(repos)
	lib.Printf("%s: processing %d repos, recent date: %v\n", name, nRepos, recentDt)
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
		mtx.Lock()
		cl := gcs[hint]
		mtx.Unlock()
		process(gctx, cl, c, ctx, ary[0], ary[1], orgRepo, repoID, orgID, recentDt, maybeHide, &stats)
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
	return total
}

func restoreCommentsRepo(gctx context.Context, gc *github.Client, c *sql.DB, ctx *lib.Ctx, org, repo, orgRepo string, repoID int64, orgID interface{}, recentDt time.Time, maybeHide func(string) string, stats *restoreStats) {
	sort := "updated"
	direction := "asc"
	opt := &github.IssueListCommentsOptions{Sort: &sort, Direction: &direction, Since: &recentDt}
	opt.PerPage = 100
	for page := 1; page <= restorePageCap; page++ {
		opt.Page = page
		more := apiPage(ctx, orgRepo+" issue comments", func() (*github.Response, bool, error) {
			comments, resp, err := gc.Issues.ListComments(gctx, org, repo, 0, opt)
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
				if lib.RestoreIssueComment(c, ctx, orgRepo, repoID, orgID, numberFromURL(cmt.IssueURL), cmt, maybeHide) {
					stats.restored++
					stats.mark(*cmt.CreatedAt)
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
			comments, resp, err := gc.PullRequests.ListComments(gctx, org, repo, 0, popt)
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
				if lib.RestoreReviewComment(c, ctx, orgRepo, repoID, orgID, numberFromURL(cmt.PullRequestURL), cmt, maybeHide) {
					stats.restored++
					stats.mark(*cmt.CreatedAt)
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
		_, resp, err := gc.Repositories.ListComments(gctx, org, repo, copt)
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
			comments, resp, err := gc.Repositories.ListComments(gctx, org, repo, copt)
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
				if lib.RestoreCommitComment(c, ctx, orgRepo, repoID, orgID, cmt, maybeHide) {
					stats.restored++
					stats.mark(*cmt.CreatedAt)
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

// ghGraphQLStargazers - stars restore uses GraphQL: the REST stargazers path returns 404 / no usable
// starred_at data on prod as of 2026-07; GraphQL exposes starredAt directly, ordered by STARRED_AT
func ghGraphQLStargazers(gctx context.Context, ctx *lib.Ctx, tokens []string, org, repo, before string) (gazers []gqlStargazer, prevCursor string, hasPrev bool, err error) {
	vars := map[string]interface{}{"o": org, "r": repo}
	if before != "" {
		vars["b"] = before
	}
	payload, err := json.Marshal(map[string]interface{}{
		"query":     "query($o: String!, $r: String!, $b: String) { repository(owner: $o, name: $r) { stargazers(last: 100, before: $b, orderBy: {field: STARRED_AT, direction: ASC}) { pageInfo { hasPreviousPage startCursor } edges { starredAt node { login databaseId } } } } }",
		"variables": vars,
	})
	if err != nil {
		return
	}
	cl := &http.Client{Timeout: time.Duration(60) * time.Second}
	for i, token := range tokens {
		for try := 1; try <= ctx.MaxGHAPIRetry; try++ {
			var req *http.Request
			req, err = http.NewRequestWithContext(gctx, "POST", "https://api.github.com/graphql", bytes.NewReader(payload))
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
					lib.Fatalf("%s/%s: graphql rate limited, don't want to wait %v: %s", org, repo, wait, snippet)
				}
				err = fmt.Errorf("graphql rate limited (token %d/%d), reset in %v: %s", i+1, len(tokens), wait, snippet)
				break
			}
			if resp.StatusCode != 200 {
				err = fmt.Errorf("graphql status %d (token %d/%d): %s", resp.StatusCode, i+1, len(tokens), snippet)
				break
			}
			var out struct {
				Data struct {
					Repository struct {
						Stargazers struct {
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
			if err = json.Unmarshal(body, &out); err != nil {
				break
			}
			if len(out.Errors) > 0 {
				err = fmt.Errorf("graphql (token %d/%d): %s", i+1, len(tokens), out.Errors[0].Message)
				break
			}
			sg := out.Data.Repository.Stargazers
			for _, edge := range sg.Edges {
				if edge.Node.DatabaseID <= 0 || edge.Node.Login == "" || edge.StarredAt.IsZero() {
					continue
				}
				gazers = append(gazers, gqlStargazer{starredAt: edge.StarredAt, login: edge.Node.Login, id: edge.Node.DatabaseID})
			}
			return gazers, sg.PageInfo.StartCursor, sg.PageInfo.HasPreviousPage, nil
		}
	}
	return
}

func restoreStarsRepo(gctx context.Context, gc *github.Client, c *sql.DB, ctx *lib.Ctx, org, repo, orgRepo string, repoID int64, orgID interface{}, recentDt time.Time, maybeHide func(string) string, stats *restoreStats) {
	tokens := ghTokens(ctx)
	if len(tokens) == 0 {
		lib.Printf("%s: stars restore needs GHA2DB_GITHUB_OAUTH token(s), skipping\n", orgRepo)
		return
	}
	before := ""
	for page := 1; page <= restorePageCap; page++ {
		gazers, prev, hasPrev, err := ghGraphQLStargazers(gctx, ctx, tokens, org, repo, before)
		if err != nil {
			lib.Printf("%s: stargazers graphql: %+v, skipping\n", orgRepo, err)
			return
		}
		stats.pages++
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
			if lib.RestoreStar(c, ctx, orgRepo, repoID, orgID, star, maybeHide) {
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
func restoreReviewsRepo(gctx context.Context, gc *github.Client, c *sql.DB, ctx *lib.Ctx, org, repo, orgRepo string, repoID int64, orgID interface{}, recentDt time.Time, maybeHide func(string) string, stats *restoreStats) {
	prNumbers := []int{}
	opt := &github.PullRequestListOptions{State: "all", Sort: "updated", Direction: "desc"}
	opt.PerPage = 100
	for page := 1; page <= restorePageCap; page++ {
		opt.Page = page
		older := false
		more := apiPage(ctx, orgRepo+" PRs", func() (*github.Response, bool, error) {
			prs, resp, err := gc.PullRequests.List(gctx, org, repo, opt)
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
				reviews, resp, err := gc.PullRequests.ListReviews(gctx, org, repo, number, ropt)
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
					if lib.RestoreReview(c, ctx, orgRepo, repoID, orgID, number, rev, maybeHide) {
						stats.restored++
						stats.mark(*rev.SubmittedAt)
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

func restoreForksRepo(gctx context.Context, gc *github.Client, c *sql.DB, ctx *lib.Ctx, org, repo, orgRepo string, repoID int64, orgID interface{}, recentDt time.Time, maybeHide func(string) string, stats *restoreStats) {
	opt := &github.RepositoryListForksOptions{Sort: "newest"}
	opt.PerPage = 100
	for page := 1; page <= restorePageCap; page++ {
		opt.Page = page
		older := false
		more := apiPage(ctx, orgRepo+" forks", func() (*github.Response, bool, error) {
			forks, resp, err := gc.Repositories.ListForks(gctx, org, repo, opt)
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
				if lib.RestoreFork(c, ctx, orgRepo, repoID, orgID, fork, maybeHide) {
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

func restoreReleasesRepo(gctx context.Context, gc *github.Client, c *sql.DB, ctx *lib.Ctx, org, repo, orgRepo string, repoID int64, orgID interface{}, recentDt time.Time, maybeHide func(string) string, stats *restoreStats) {
	opt := &github.ListOptions{PerPage: 100}
	for page := 1; page <= restorePageCap; page++ {
		opt.Page = page
		older := false
		more := apiPage(ctx, orgRepo+" releases", func() (*github.Response, bool, error) {
			rels, resp, err := gc.Repositories.ListReleases(gctx, org, repo, opt)
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
				if lib.RestoreRelease(c, ctx, orgRepo, repoID, orgID, rel, maybeHide) {
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
	return restorePass(ctx, "ghapi2db comments restore", restoreCommentsRepo)
}

func syncReviews(ctx *lib.Ctx) restoreStats {
	return restorePass(ctx, "ghapi2db reviews restore", restoreReviewsRepo)
}

func syncForks(ctx *lib.Ctx) restoreStats {
	return restorePass(ctx, "ghapi2db forks restore", restoreForksRepo)
}

func syncStars(ctx *lib.Ctx) restoreStats {
	return restorePass(ctx, "ghapi2db stars restore", restoreStarsRepo)
}

func syncReleases(ctx *lib.Ctx) restoreStats {
	return restorePass(ctx, "ghapi2db releases restore", restoreReleasesRepo)
}
