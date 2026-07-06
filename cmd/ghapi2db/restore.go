package main

import (
	"context"
	"database/sql"
	"fmt"
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

func starPresent(c *sql.DB, ctx *lib.Ctx, actorID int64, orgRepo string) bool {
	rows := lib.QuerySQLWithErr(c, ctx, "select 1 from gha_events where type = 'WatchEvent' and actor_id = "+lib.NValue(1)+" and dup_repo_name = "+lib.NValue(2)+" limit 1", actorID, orgRepo)
	defer func() { lib.FatalOnError(rows.Close()) }()
	present := false
	for rows.Next() {
		present = true
	}
	lib.FatalOnError(rows.Err())
	return present
}

func repoIDs(c *sql.DB, ctx *lib.Ctx, orgRepo string) (repoID int64, orgID interface{}) {
	rows := lib.QuerySQLWithErr(c, ctx, "select coalesce(max(repo_id), -1), max(org_id) from gha_events where dup_repo_name = "+lib.NValue(1), orgRepo)
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

func restorePass(ctx *lib.Ctx, name string, process restoreRepoFunc) {
	repos, isSingleRepo, singleRepo, gctx, gcs, c, recentDt := getAPIParams(ctx)
	defer func() { lib.FatalOnError(c.Close()) }()
	maybeHide := lib.MaybeHideFunc(lib.GetHidden(ctx, lib.HideCfgFile))
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
		stats := restoreStats{}
		mtx.Lock()
		cl := gcs[hint]
		mtx.Unlock()
		process(gctx, cl, c, ctx, ary[0], ary[1], orgRepo, repoID, orgID, recentDt, maybeHide, &stats)
		mtx.Lock()
		total.checked += stats.checked
		total.restored += stats.restored
		total.pages += stats.pages
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
				lib.RestoreIssueComment(c, ctx, orgRepo, repoID, orgID, numberFromURL(cmt.IssueURL), cmt, maybeHide)
				stats.restored++
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
				lib.RestoreReviewComment(c, ctx, orgRepo, repoID, orgID, numberFromURL(cmt.PullRequestURL), cmt, maybeHide)
				stats.restored++
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
				lib.RestoreCommitComment(c, ctx, orgRepo, repoID, orgID, cmt, maybeHide)
				stats.restored++
			}
			return resp, true, nil
		})
		if !ok || !anyRecent {
			break
		}
	}
}

func restoreStarsRepo(gctx context.Context, gc *github.Client, c *sql.DB, ctx *lib.Ctx, org, repo, orgRepo string, repoID int64, orgID interface{}, recentDt time.Time, maybeHide func(string) string, stats *restoreStats) {
	// stargazers list ascending by starred_at - walk from the last page down
	opt := &github.ListOptions{PerPage: 100, Page: 1}
	last := 1
	apiPage(ctx, orgRepo+" stargazers last page", func() (*github.Response, bool, error) {
		_, resp, err := gc.Activity.ListStargazers(gctx, org, repo, opt)
		if err != nil || resp == nil || resp.StatusCode >= 400 {
			return resp, false, err
		}
		if resp.LastPage > 1 {
			last = resp.LastPage
		}
		return resp, false, nil
	})
	for page := last; page >= 1; page-- {
		opt.Page = page
		anyRecent := false
		ok := apiPage(ctx, orgRepo+" stargazers", func() (*github.Response, bool, error) {
			stars, resp, err := gc.Activity.ListStargazers(gctx, org, repo, opt)
			if err != nil || resp == nil || resp.StatusCode >= 400 {
				return resp, false, err
			}
			stats.pages++
			for _, star := range stars {
				if star == nil || star.User == nil || star.User.ID == nil || star.StarredAt == nil || star.StarredAt.Time.Before(recentDt) {
					continue
				}
				anyRecent = true
				stats.checked++
				if starPresent(c, ctx, *star.User.ID, orgRepo) {
					continue
				}
				lib.RestoreStar(c, ctx, orgRepo, repoID, orgID, star, maybeHide)
				stats.restored++
			}
			return resp, true, nil
		})
		if !ok || !anyRecent {
			break
		}
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
					lib.RestoreReview(c, ctx, orgRepo, repoID, orgID, number, rev, maybeHide)
					stats.restored++
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
				if idPresent(c, ctx, "gha_forkees", "ForkEvent", *fork.ID) {
					continue
				}
				lib.RestoreFork(c, ctx, orgRepo, repoID, orgID, fork, maybeHide)
				stats.restored++
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
				if rel == nil || rel.ID == nil {
					continue
				}
				if rel.CreatedAt != nil && rel.CreatedAt.Time.Before(recentDt) {
					older = true
					break
				}
				stats.checked++
				if idPresent(c, ctx, "gha_releases", "ReleaseEvent", *rel.ID) {
					continue
				}
				lib.RestoreRelease(c, ctx, orgRepo, repoID, orgID, rel, maybeHide)
				stats.restored++
			}
			return resp, resp.NextPage != 0, nil
		})
		if !more || older {
			break
		}
	}
}

func syncComments(ctx *lib.Ctx) {
	restorePass(ctx, "ghapi2db comments restore", restoreCommentsRepo)
}

func syncReviews(ctx *lib.Ctx) {
	restorePass(ctx, "ghapi2db reviews restore", restoreReviewsRepo)
}

func syncForks(ctx *lib.Ctx) {
	restorePass(ctx, "ghapi2db forks restore", restoreForksRepo)
}

func syncStars(ctx *lib.Ctx) {
	restorePass(ctx, "ghapi2db stars restore", restoreStarsRepo)
}

func syncReleases(ctx *lib.Ctx) {
	restorePass(ctx, "ghapi2db releases restore", restoreReleasesRepo)
}
