package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"

	lib "github.com/cncf/devstatscode"
	"github.com/google/go-github/v38/github"
	jsoniter "github.com/json-iterator/go"
)

// Issues and pull requests sweep pass: GH Archive's `PullRequestEvent` payloads carry stubbed
// pull request objects since 2024-10-17 (`PullRequestReview{,Comment}Event` since 2025-10-09):
// only url/id/number/base/head, so `gha_pull_requests` gets rows with the zero created_at, no
// user, title or state. Objects GH Archive missed altogether have no rows at all. Per repository:
//  1. stub sweep (DB-driven): every pull request with stub rows is fetched (`GET /pulls/{n}`) and
//     its stub rows are filled with the current object; a pull request without any `gha_issues`
//     row gets one attached (`GET /issues/{n}`) to its newest upgraded event;
//  2. listing (API-driven, repositories with issue or PR updates since the recent date):
//     `GET /issues?state=all&since=<recent date>&sort=updated&direction=asc` - an object without a
//     good (non-stub) row in its primary table gets GHA-shaped synthetic `opened` (at created_at)
//     and, when closed, `closed` (at closed_at) events written with the gha2db writer under
//     deterministic ids (lib.ArtificialIssueIDBase / lib.ArtificialPRIDBase + 2*id + 0/1), so the
//     pass is idempotent and a later close of a synthesized open object adds its `closed` event.
//     Reopens are not synthesized (the object carries no reopen time) - the issue events pass
//     handles state changes of objects the database already knows.

const issuesPerPage = 100

// apiIssue - the REST issue object: the GHA issue plus who closed it
type apiIssue struct {
	lib.Issue
	ClosedBy *lib.Actor `json:"closed_by"`
}

// issuesPage - one page of the repository's issues (and pull requests) updated since the recent date
func issuesPage(gctx context.Context, gc *ghClients, org, repo string, since time.Time, page int) ([]json.RawMessage, *github.Response, error) {
	q := url.Values{}
	q.Set("state", "all")
	q.Set("since", since.UTC().Format(time.RFC3339))
	q.Set("sort", "updated")
	q.Set("direction", "asc")
	q.Set("per_page", strconv.Itoa(issuesPerPage))
	q.Set("page", strconv.Itoa(page))
	var issues []json.RawMessage
	resp, err := gc.do(func(cl *github.Client) (*github.Response, error) {
		req, err := cl.NewRequest("GET", fmt.Sprintf("repos/%s/%s/issues?%s", org, repo, q.Encode()), nil)
		if err != nil {
			return nil, err
		}
		issues = nil
		return cl.Do(gctx, req, &issues)
	})
	if err != nil {
		return nil, resp, err
	}
	return issues, resp, nil
}

// getObject - one REST object (pull request or issue) as raw JSON
func getObject(gctx context.Context, gc *ghClients, path string) (json.RawMessage, *github.Response, error) {
	var raw json.RawMessage
	resp, err := gc.do(func(cl *github.Client) (*github.Response, error) {
		req, err := cl.NewRequest("GET", path, nil)
		if err != nil {
			return nil, err
		}
		raw = nil
		return cl.Do(gctx, req, &raw)
	})
	if err != nil {
		return nil, resp, err
	}
	return raw, resp, nil
}

// fetchPullRequest - the full pull request object, nil when unavailable
func fetchPullRequest(gctx context.Context, gc *ghClients, ctx *lib.Ctx, info, org, repo string, number int) *lib.PullRequest {
	var raw json.RawMessage
	got := false
	apiPage(ctx, info, func() (*github.Response, bool, error) {
		r, resp, err := getObject(gctx, gc, fmt.Sprintf("repos/%s/%s/pulls/%d", org, repo, number))
		if err != nil {
			return resp, false, err
		}
		raw, got = r, true
		return resp, false, nil
	})
	if !got {
		return nil
	}
	var pr lib.PullRequest
	if err := jsoniter.Unmarshal(raw, &pr); err != nil {
		lib.Printf("WARNING: %s: cannot unmarshal the pull request: %v, skipping\n", info, err)
		return nil
	}
	if pr.ID == 0 {
		lib.Printf("WARNING: %s: pull request object without an id, skipping\n", info)
		return nil
	}
	return &pr
}

// fetchIssue - the full issue object, nil when unavailable
func fetchIssue(gctx context.Context, gc *ghClients, ctx *lib.Ctx, info, org, repo string, number int) *apiIssue {
	var raw json.RawMessage
	got := false
	apiPage(ctx, info, func() (*github.Response, bool, error) {
		r, resp, err := getObject(gctx, gc, fmt.Sprintf("repos/%s/%s/issues/%d", org, repo, number))
		if err != nil {
			return resp, false, err
		}
		raw, got = r, true
		return resp, false, nil
	})
	if !got {
		return nil
	}
	var issue apiIssue
	if err := jsoniter.Unmarshal(raw, &issue); err != nil {
		lib.Printf("WARNING: %s: cannot unmarshal the issue: %v, skipping\n", info, err)
		return nil
	}
	if issue.ID == 0 {
		lib.Printf("WARNING: %s: issue object without an id, skipping\n", info)
		return nil
	}
	return &issue
}

// stubPullRequests - (id, number) of the repository's pull requests that have stub rows
// (by repository id: the rows written under the historical names of a renamed repository are included)
func stubPullRequests(c *sql.DB, ctx *lib.Ctx, repoID int64) (ids []int64, numbers []int) {
	rows := lib.QuerySQLWithErr(c, ctx, "select id, number from gha_pull_requests where dup_repo_id = "+lib.NValue(1)+" and created_at < '"+lib.StubCreatedAtCut+"' group by id, number order by id, number", repoID)
	defer func() { lib.FatalOnError(rows.Close()) }()
	for rows.Next() {
		var (
			id     int64
			number int
		)
		lib.FatalOnError(rows.Scan(&id, &number))
		ids = append(ids, id)
		numbers = append(numbers, number)
	}
	lib.FatalOnError(rows.Err())
	return
}

func rowPresent(c *sql.DB, ctx *lib.Ctx, query string, args ...interface{}) bool {
	rows := lib.QuerySQLWithErr(c, ctx, query, args...)
	defer func() { lib.FatalOnError(rows.Close()) }()
	present := false
	for rows.Next() {
		present = true
	}
	lib.FatalOnError(rows.Err())
	return present
}

// issueRowPresent - does the issue (by id) have a gha_issues row
func issueRowPresent(c *sql.DB, ctx *lib.Ctx, issueID int64) bool {
	return rowPresent(c, ctx, "select 1 from gha_issues where id = "+lib.NValue(1)+" limit 1", issueID)
}

// issueRowPresentByNumber - does the repository's issue/PR number have a gha_issues row (under any of the repository's names)
func issueRowPresentByNumber(c *sql.DB, ctx *lib.Ctx, repoID int64, number int) bool {
	return rowPresent(c, ctx, "select 1 from gha_issues where dup_repo_id = "+lib.NValue(1)+" and number = "+lib.NValue(2)+" limit 1", repoID, number)
}

// goodPullRequestRowPresent - does the repository's pull request number have a non-stub gha_pull_requests row (under any of the repository's names)
func goodPullRequestRowPresent(c *sql.DB, ctx *lib.Ctx, repoID int64, number int) bool {
	return rowPresent(c, ctx, "select 1 from gha_pull_requests where dup_repo_id = "+lib.NValue(1)+" and number = "+lib.NValue(2)+" and created_at >= '"+lib.StubCreatedAtCut+"' limit 1", repoID, number)
}

// syntheticEvent - a GHA-shaped IssuesEvent/PullRequestEvent for the object's lifecycle step
func syntheticEvent(id int64, eType, action string, createdAt time.Time, actor lib.Actor, org, orgRepo string, repoID int64, orgID interface{}, number int, issue *lib.Issue, pr *lib.PullRequest) lib.Event {
	ev := lib.Event{
		ID:        strconv.FormatInt(id, 10),
		Type:      eType,
		Public:    true,
		CreatedAt: createdAt,
		Actor:     actor,
		Repo:      lib.Repo{ID: int(repoID), Name: orgRepo},
	}
	if orgID != nil {
		ev.Org = &lib.Org{ID: int(orgID.(int64)), Login: org}
	}
	act, num := action, number
	ev.Payload = lib.Payload{Action: &act, Number: &num, Issue: issue, PullRequest: pr}
	return ev
}

// writeSynthetic - write one synthetic event with the gha2db writer (actor filters, exists check)
func writeSynthetic(c *sql.DB, ctx *lib.Ctx, name, orgRepo string, ev *lib.Event, shas map[string]string, stats *restoreStats) {
	// the same actor filters gha2db applies to the archives
	if !lib.ActorHit(ctx, ev.Actor.Login) {
		return
	}
	if lib.WriteToDB(c, ctx, ev, shas) == 0 {
		return
	}
	stats.restored++
	stats.addType(ev.Type)
	stats.mark(ev.CreatedAt)
	if eid, err := strconv.ParseInt(ev.ID, 10, 64); err == nil {
		stats.eids = append(stats.eids, eid)
	}
	if ctx.Debug > 0 {
		lib.Printf("%s: %s: synthesized %s %s %s (%v)\n", name, orgRepo, ev.Type, *ev.Payload.Action, ev.ID, ev.CreatedAt)
	}
}

// synthesizeIssue - opened (+ closed) IssuesEvents of an issue the database does not know
func synthesizeIssue(c *sql.DB, ctx *lib.Ctx, name, org, orgRepo string, repoID int64, orgID interface{}, issue *apiIssue, shas map[string]string, stats *restoreStats) {
	base := lib.ArtificialIssueIDBase + 2*int64(issue.ID)
	ev := syntheticEvent(base, "IssuesEvent", "opened", issue.CreatedAt, issue.User, org, orgRepo, repoID, orgID, issue.Number, &issue.Issue, nil)
	writeSynthetic(c, ctx, name, orgRepo, &ev, shas, stats)
	if issue.State == "closed" && issue.ClosedAt != nil {
		actor := issue.User
		if issue.ClosedBy != nil && issue.ClosedBy.ID != 0 {
			actor = *issue.ClosedBy
		}
		ev = syntheticEvent(base+1, "IssuesEvent", "closed", *issue.ClosedAt, actor, org, orgRepo, repoID, orgID, issue.Number, &issue.Issue, nil)
		writeSynthetic(c, ctx, name, orgRepo, &ev, shas, stats)
	}
}

// synthesizePullRequest - opened (+ closed) PullRequestEvents of a pull request the database does not know;
// the payload carries both the pull request and its issue object (gha_pull_requests and gha_issues rows)
func synthesizePullRequest(c *sql.DB, ctx *lib.Ctx, name, org, orgRepo string, repoID int64, orgID interface{}, pr *lib.PullRequest, issue *apiIssue, shas map[string]string, stats *restoreStats) {
	base := lib.ArtificialPRIDBase + 2*int64(pr.ID)
	ev := syntheticEvent(base, "PullRequestEvent", "opened", pr.CreatedAt, pr.User, org, orgRepo, repoID, orgID, pr.Number, &issue.Issue, pr)
	writeSynthetic(c, ctx, name, orgRepo, &ev, shas, stats)
	if pr.State == "closed" && pr.ClosedAt != nil {
		actor := pr.User
		if pr.MergedBy != nil && pr.MergedBy.ID != 0 {
			actor = *pr.MergedBy
		} else if issue.ClosedBy != nil && issue.ClosedBy.ID != 0 {
			actor = *issue.ClosedBy
		}
		ev = syntheticEvent(base+1, "PullRequestEvent", "closed", *pr.ClosedAt, actor, org, orgRepo, repoID, orgID, pr.Number, &issue.Issue, pr)
		writeSynthetic(c, ctx, name, orgRepo, &ev, shas, stats)
	}
}

// sweepStubs - fill the stub gha_pull_requests rows of the repository with the current API objects
func sweepStubs(gctx context.Context, gc *ghClients, c *sql.DB, ctx *lib.Ctx, org, repo, orgRepo string, repoID int64, maybeHide func(string) string, stats *restoreStats) {
	name := passIssuesPRs.label()
	ids, numbers := stubPullRequests(c, ctx, repoID)
	if ctx.Debug > 0 {
		lib.Printf("%s: %s: %d pull requests with stub rows\n", name, orgRepo, len(ids))
	}
	for i, number := range numbers {
		stats.checked++
		pr := fetchPullRequest(gctx, gc, ctx, fmt.Sprintf("%s: %s pull request %d", name, orgRepo, number), org, repo, number)
		if pr == nil {
			// deleted on GitHub (404 is silent in apiPage) or not fetched: the stub rows stay
			if ctx.Debug > 0 {
				lib.Printf("%s: %s: pull request %d (%d): not available on GitHub, stub rows kept\n", name, orgRepo, number, ids[i])
			}
			continue
		}
		if int64(pr.ID) != ids[i] {
			lib.Printf("WARNING: %s: %s: pull request %d is %d on GitHub, %d in the database, skipping\n", name, orgRepo, number, pr.ID, ids[i])
			continue
		}
		var issue *lib.Issue
		if !issueRowPresentByNumber(c, ctx, repoID, number) {
			if ai := fetchIssue(gctx, gc, ctx, fmt.Sprintf("%s: %s issue %d", name, orgRepo, number), org, repo, number); ai != nil {
				issue = &ai.Issue
			}
		}
		eids, attached := lib.UpgradePullRequestStubs(c, ctx, pr, issue, maybeHide)
		if len(eids) == 0 {
			continue
		}
		stats.stubPRs++
		stats.stubRows += len(eids)
		stats.eids = append(stats.eids, eids...)
		if attached {
			stats.issueRows++
		}
		if ctx.Debug > 0 {
			lib.Printf("%s: %s: pull request %d (%d): upgraded %d stub rows, issue row attached: %v\n", name, orgRepo, number, pr.ID, len(eids), attached)
		}
	}
}

// sweepListing - synthesize the lifecycle events of the updated objects the database does not know
func sweepListing(gctx context.Context, gc *ghClients, c *sql.DB, ctx *lib.Ctx, org, repo, orgRepo string, repoID int64, orgID interface{}, recentDt time.Time, shas map[string]string, stats *restoreStats) {
	name := passIssuesPRs.label()
	for page := 1; page <= restorePageCap; page++ {
		var (
			issues []json.RawMessage
			got    bool
		)
		more := apiPage(ctx, fmt.Sprintf("%s: %s issues page %d", name, orgRepo, page), func() (*github.Response, bool, error) {
			objs, resp, err := issuesPage(gctx, gc, org, repo, recentDt, page)
			if err != nil {
				return resp, false, err
			}
			issues, got = objs, true
			return resp, resp.NextPage > 0, nil
		})
		if !got {
			return
		}
		stats.pages++
		for _, raw := range issues {
			var issue apiIssue
			if err := jsoniter.Unmarshal(raw, &issue); err != nil {
				lib.Printf("WARNING: %s: %s: cannot unmarshal an issue: %v, skipping the listing\n", name, orgRepo, err)
				return
			}
			if issue.ID == 0 {
				continue
			}
			stats.checked++
			if issue.PullRequest == nil {
				if issueRowPresent(c, ctx, int64(issue.ID)) {
					continue
				}
				synthesizeIssue(c, ctx, name, org, orgRepo, repoID, orgID, &issue, shas, stats)
				continue
			}
			if goodPullRequestRowPresent(c, ctx, repoID, issue.Number) {
				continue
			}
			pr := fetchPullRequest(gctx, gc, ctx, fmt.Sprintf("%s: %s pull request %d", name, orgRepo, issue.Number), org, repo, issue.Number)
			if pr == nil {
				continue
			}
			synthesizePullRequest(c, ctx, name, org, orgRepo, repoID, orgID, pr, &issue, shas, stats)
		}
		if ctx.Debug > 0 {
			lib.Printf("%s: %s: page %d: %d objects, synthesized so far %d\n", name, orgRepo, page, len(issues), stats.restored)
		}
		if !more {
			return
		}
	}
}

// restoreIssuesPRsRepo - the issues and pull requests sweep of one repository
func restoreIssuesPRsRepo(gctx context.Context, gc *ghClients, c *sql.DB, ctx *lib.Ctx, org, repo, orgRepo string, repoID int64, orgID interface{}, recentDt time.Time, maybeHide func(string) string, shas map[string]string, stats *restoreStats) {
	sweepStubs(gctx, gc, c, ctx, org, repo, orgRepo, repoID, maybeHide, stats)
	// the listing only for repositories with issue or PR updates since the recent date (when the heartbeat knows)
	if hb := heartbeatOf(orgRepo); hb != nil && !hb.active(passEvents, recentDt) {
		if ctx.Debug > 0 {
			lib.Printf("%s: %s: no issue or PR updates since %v, listing skipped\n", passIssuesPRs.label(), orgRepo, recentDt)
		}
		return
	}
	sweepListing(gctx, gc, c, ctx, org, repo, orgRepo, repoID, orgID, recentDt, shas, stats)
}

// syncIssuesPRs - issues and pull requests sweep pass
func syncIssuesPRs(ctx *lib.Ctx) restoreStats {
	shas := lib.GetHidden(ctx, lib.HideCfgFile)
	stats := restorePass(ctx, passIssuesPRs, func(gctx context.Context, gc *ghClients, c *sql.DB, ctx *lib.Ctx, org, repo, orgRepo string, repoID int64, orgID interface{}, recentDt time.Time, maybeHide func(string) string, stats *restoreStats) {
		restoreIssuesPRsRepo(gctx, gc, c, ctx, org, repo, orgRepo, repoID, orgID, recentDt, maybeHide, shas, stats)
	})
	lib.Printf("%s: upgraded %d stub rows of %d pull requests, attached %d issue rows\n", passIssuesPRs.label(), stats.stubRows, stats.stubPRs, stats.issueRows)
	return stats
}
