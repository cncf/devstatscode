package devstatscode

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/google/go-github/v38/github"
)

// API-restored objects get artificial event ids in per-class sub-bands (see const.go) so ids
// from different REST namespaces (issue events, comments, reviews, ...) can never collide.

func tsOrNil(t *github.Timestamp) interface{} {
	if t == nil {
		return nil
	}
	return t.Time
}

func tsOr(t *github.Timestamp, def time.Time) time.Time {
	if t == nil {
		return def
	}
	return t.Time
}

func timeOr(t *time.Time, def time.Time) time.Time {
	if t == nil {
		return def
	}
	return *t
}

func strOr(s *string, def string) string {
	if s == nil {
		return def
	}
	return *s
}

func intOr(i *int, def int) int {
	if i == nil {
		return def
	}
	return *i
}

func boolOr(b *bool, def bool) bool {
	if b == nil {
		return def
	}
	return *b
}

func lookupID(tc *sql.Tx, ctx *Ctx, query string, args ...interface{}) interface{} {
	rows := QuerySQLTxWithErr(tc, ctx, query, args...)
	defer func() { FatalOnError(rows.Close()) }()
	var id *int64
	for rows.Next() {
		FatalOnError(rows.Scan(&id))
	}
	FatalOnError(rows.Err())
	if id == nil {
		return nil
	}
	return *id
}

// NegativeArtificialID - deterministic negative event id for API/git-restored objects with no natural id (like pre-2015 events)
func NegativeArtificialID(parts []string) int64 {
	id := int64(HashStrings(parts))
	if id > 0 {
		id = -id
	}
	if id == 0 {
		id = -1
	}
	return id
}

func truncStringOrNilHidden(strPtr *string, maxLen int, maybeHide func(string) string) interface{} {
	if strPtr == nil {
		return nil
	}
	return TruncToBytes(maybeHide(*strPtr), maxLen)
}

func findRawEventID(tc *sql.Tx, ctx *Ctx, eType, repo string, actor *github.User, createdAt time.Time, payloadCol string, objID interface{}) interface{} {
	if actor == nil || actor.ID == nil {
		return nil
	}
	rows := QuerySQLTxWithErr(
		tc,
		ctx,
		"select id from gha_events where id < 281474976710657 and type = "+NValue(1)+" and dup_repo_name = "+NValue(2)+" and actor_id = "+NValue(3)+" and created_at = "+NValue(4)+" order by id limit 3",
		eType, repo, *actor.ID, createdAt,
	)
	defer func() { FatalOnError(rows.Close()) }()
	ids := []int64{}
	id := int64(0)
	for rows.Next() {
		FatalOnError(rows.Scan(&id))
		ids = append(ids, id)
	}
	FatalOnError(rows.Err())
	if len(ids) == 0 {
		return nil
	}
	if payloadCol == "" || objID == nil {
		if len(ids) == 1 {
			return ids[0]
		}
		Printf("findRawEventID: ambiguous raw events for (%s, %s, %d, %v), creating artificial event\n", eType, repo, *actor.ID, createdAt)
		return nil
	}
	// verify the object id via gha_payloads; NULL/missing payload value is acceptable only
	// for a single candidate (payload gets enriched on reuse), a different value is not
	matched := []int64{}
	weak := []int64{}
	oid := int64(0)
	switch o := objID.(type) {
	case int64:
		oid = o
	case int:
		oid = int64(o)
	default:
		return nil
	}
	for _, cid := range ids {
		prows := QuerySQLTxWithErr(tc, ctx, "select "+payloadCol+" from gha_payloads where event_id = "+NValue(1), cid)
		var pv *int64
		found := false
		for prows.Next() {
			FatalOnError(prows.Scan(&pv))
			found = true
		}
		FatalOnError(prows.Err())
		FatalOnError(prows.Close())
		if found && pv != nil {
			if *pv == oid {
				matched = append(matched, cid)
			}
		} else {
			weak = append(weak, cid)
		}
	}
	if len(matched) == 1 {
		return matched[0]
	}
	if len(matched) == 0 && len(weak) == 1 && len(ids) == 1 {
		return weak[0]
	}
	if len(matched) > 1 || len(weak) > 0 {
		Printf("findRawEventID: cannot verify raw event for (%s, %s, %d, %v, %s=%d), creating artificial event\n", eType, repo, *actor.ID, createdAt, payloadCol, oid)
	}
	return nil
}
func hashIDConflict(tc *sql.Tx, ctx *Ctx, eid int64, eType, repo string, actorID int64, createdAt time.Time) bool {
	rows := QuerySQLTxWithErr(tc, ctx, "select type, dup_repo_name, actor_id, created_at from gha_events where id = "+NValue(1), eid)
	defer func() { FatalOnError(rows.Close()) }()
	conflict := false
	eT, eR, eA, eD := "", "", int64(0), time.Time{}
	for rows.Next() {
		FatalOnError(rows.Scan(&eT, &eR, &eA, &eD))
		if eT != eType || eR != repo || eA != actorID || !eD.Equal(createdAt) {
			conflict = true
		}
	}
	FatalOnError(rows.Err())
	if conflict {
		Printf("hash id %d conflict: existing (%s, %s, %d, %v) vs new (%s, %s, %d, %v), skipping\n", eid, eT, eR, eA, eD, eType, repo, actorID, createdAt)
	}
	return conflict
}

func artificialIDOK(eid int64, what, repo string) bool {
	if eid >= SyncEventIDThreshold {
		Printf("%s: %s: artificial event id %d reached the sync events range, skipping\n", what, repo, eid)
		return false
	}
	return true
}

func restoreEventPayload(tc *sql.Tx, ctx *Ctx, eid int64, eType string, actor *github.User, repo string, repoID int64, orgID interface{}, createdAt time.Time, action, number, issueID, prID, commentID, forkeeID, releaseID, commitSHA interface{}, maybeHide func(string) string) sql.Result {
	res := ExecSQLTxWithErr(
		tc,
		ctx,
		InsertIgnore(
			"into gha_events(id, type, actor_id, repo_id, public, created_at, dup_actor_login, dup_repo_name, org_id, forkee_id) "+NValues(10),
		),
		AnyArray{
			eid,
			eType,
			ghActorIDOrNil(actor),
			repoID,
			true,
			createdAt,
			ghActorLoginOrNil(actor, maybeHide),
			repo,
			orgID,
			forkeeID,
		}...,
	)
	ExecSQLTxWithErr(
		tc,
		ctx,
		"insert into gha_payloads(event_id, action, number, issue_id, pull_request_id, comment_id, forkee_id, release_id, commit, "+
			"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at) "+NValues(15)+
			" on conflict(event_id) do update set "+
			"action = coalesce(gha_payloads.action, excluded.action), "+
			"number = coalesce(gha_payloads.number, excluded.number), "+
			"issue_id = coalesce(gha_payloads.issue_id, excluded.issue_id), "+
			"pull_request_id = coalesce(gha_payloads.pull_request_id, excluded.pull_request_id), "+
			"comment_id = coalesce(gha_payloads.comment_id, excluded.comment_id), "+
			"forkee_id = coalesce(gha_payloads.forkee_id, excluded.forkee_id), "+
			"release_id = coalesce(gha_payloads.release_id, excluded.release_id), "+
			"commit = coalesce(gha_payloads.commit, excluded.commit)",
		AnyArray{
			eid,
			action,
			number,
			issueID,
			prID,
			commentID,
			forkeeID,
			releaseID,
			commitSHA,
			ghActorIDOrNil(actor),
			ghActorLoginOrNil(actor, maybeHide),
			repoID,
			repo,
			eType,
			createdAt,
		}...,
	)
	return res
}

// RestoreIssueComment - restores a comment missed by GH Archive: artificial IssueCommentEvent + gha_comments row
func RestoreIssueComment(c *sql.DB, ctx *Ctx, repo string, repoID int64, orgID interface{}, issueNumber int, cmt *github.IssueComment, maybeHide func(string) string) bool {
	if ctx.SkipPDB {
		return false
	}
	if cmt == nil || cmt.ID == nil || cmt.User == nil || cmt.User.Login == nil || cmt.CreatedAt == nil {
		Printf("RestoreIssueComment: %s: skipping comment with missing id/user/created_at\n", repo)
		return false
	}
	cid := *cmt.ID
	eid := ArtificialCommentIDBase + cid
	if !artificialIDOK(eid, "RestoreIssueComment", repo) {
		return false
	}
	createdAt := *cmt.CreatedAt
	eType := "IssueCommentEvent"
	tc, err := c.Begin()
	FatalOnError(err)
	if raw := findRawEventID(tc, ctx, eType, repo, cmt.User, createdAt, "comment_id", cid); raw != nil {
		eid = raw.(int64)
	}
	ghActor(tc, ctx, cmt.User, maybeHide)
	issueID := lookupID(tc, ctx, "select max(id) from gha_issues where number = "+NValue(1)+" and dup_repo_name = "+NValue(2), issueNumber, repo)
	restoreEventPayload(tc, ctx, eid, eType, cmt.User, repo, repoID, orgID, createdAt, "created", issueNumber, issueID, nil, cid, nil, nil, nil, maybeHide)
	res := ExecSQLTxWithErr(
		tc,
		ctx,
		InsertIgnore(
			fmt.Sprintf(
				"into gha_comments(id, event_id, body, created_at, updated_at, user_id, "+
					"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_user_login) "+
					"values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
				NValue(1), NValue(2), NValue(3), NValue(4), NValue(5), NValue(6), NValue(7),
				NValue(8), NValue(9), NValue(10), NValue(11), NValue(12), NValue(13),
			),
		),
		AnyArray{
			cid,
			eid,
			TruncToBytes(maybeHide(strOr(cmt.Body, "")), 0xffff),
			createdAt,
			timeOr(cmt.UpdatedAt, createdAt),
			ghActorIDOrNil(cmt.User),
			ghActorIDOrNil(cmt.User),
			ghActorLoginOrNil(cmt.User, maybeHide),
			repoID,
			repo,
			eType,
			createdAt,
			ghActorLoginOrNil(cmt.User, maybeHide),
		}...,
	)
	FatalOnError(tc.Commit())
	n, _ := res.RowsAffected()
	return n > 0
}

// RestoreReviewComment - restores a PR review comment missed by GH Archive: artificial PullRequestReviewCommentEvent + gha_comments row
func RestoreReviewComment(c *sql.DB, ctx *Ctx, repo string, repoID int64, orgID interface{}, prNumber int, cmt *github.PullRequestComment, maybeHide func(string) string) bool {
	if ctx.SkipPDB {
		return false
	}
	if cmt == nil || cmt.ID == nil || cmt.User == nil || cmt.User.Login == nil || cmt.CreatedAt == nil {
		Printf("RestoreReviewComment: %s: skipping comment with missing id/user/created_at\n", repo)
		return false
	}
	cid := *cmt.ID
	eid := ArtificialReviewCommentIDBase + cid
	if !artificialIDOK(eid, "RestoreReviewComment", repo) {
		return false
	}
	createdAt := *cmt.CreatedAt
	eType := "PullRequestReviewCommentEvent"
	tc, err := c.Begin()
	FatalOnError(err)
	if raw := findRawEventID(tc, ctx, eType, repo, cmt.User, createdAt, "comment_id", cid); raw != nil {
		eid = raw.(int64)
	}
	ghActor(tc, ctx, cmt.User, maybeHide)
	prID := lookupID(tc, ctx, "select max(id) from gha_pull_requests where number = "+NValue(1)+" and dup_repo_name = "+NValue(2), prNumber, repo)
	restoreEventPayload(tc, ctx, eid, eType, cmt.User, repo, repoID, orgID, createdAt, "created", prNumber, nil, prID, cid, nil, nil, nil, maybeHide)
	res := ExecSQLTxWithErr(
		tc,
		ctx,
		InsertIgnore(
			fmt.Sprintf(
				"into gha_comments(id, event_id, body, created_at, updated_at, user_id, "+
					"commit_id, original_commit_id, diff_hunk, position, original_position, path, pull_request_review_id, "+
					"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_user_login) "+
					"values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
				NValue(1), NValue(2), NValue(3), NValue(4), NValue(5), NValue(6), NValue(7), NValue(8), NValue(9), NValue(10),
				NValue(11), NValue(12), NValue(13), NValue(14), NValue(15), NValue(16), NValue(17), NValue(18), NValue(19), NValue(20),
			),
		),
		AnyArray{
			cid,
			eid,
			TruncToBytes(maybeHide(strOr(cmt.Body, "")), 0xffff),
			createdAt,
			timeOr(cmt.UpdatedAt, createdAt),
			ghActorIDOrNil(cmt.User),
			StringOrNil(cmt.CommitID),
			StringOrNil(cmt.OriginalCommitID),
			truncStringOrNilHidden(cmt.DiffHunk, 0xffff, maybeHide),
			IntOrNil(cmt.Position),
			IntOrNil(cmt.OriginalPosition),
			StringOrNil(cmt.Path),
			cmt.PullRequestReviewID,
			ghActorIDOrNil(cmt.User),
			ghActorLoginOrNil(cmt.User, maybeHide),
			repoID,
			repo,
			eType,
			createdAt,
			ghActorLoginOrNil(cmt.User, maybeHide),
		}...,
	)
	FatalOnError(tc.Commit())
	n, _ := res.RowsAffected()
	return n > 0
}

// RestoreReview - restores a PR review missed by GH Archive: artificial PullRequestReviewEvent + gha_reviews row
func RestoreReview(c *sql.DB, ctx *Ctx, repo string, repoID int64, orgID interface{}, prNumber int, rev *github.PullRequestReview, maybeHide func(string) string) bool {
	if ctx.SkipPDB {
		return false
	}
	if rev == nil || rev.ID == nil || rev.User == nil || rev.User.Login == nil || rev.SubmittedAt == nil {
		return false
	}
	rid := *rev.ID
	eid := ArtificialReviewIDBase + rid
	if !artificialIDOK(eid, "RestoreReview", repo) {
		return false
	}
	createdAt := *rev.SubmittedAt
	eType := "PullRequestReviewEvent"
	tc, err := c.Begin()
	FatalOnError(err)
	if raw := findRawEventID(tc, ctx, eType, repo, rev.User, createdAt, "", nil); raw != nil {
		eid = raw.(int64)
	}
	ghActor(tc, ctx, rev.User, maybeHide)
	prID := lookupID(tc, ctx, "select max(id) from gha_pull_requests where number = "+NValue(1)+" and dup_repo_name = "+NValue(2), prNumber, repo)
	restoreEventPayload(tc, ctx, eid, eType, rev.User, repo, repoID, orgID, createdAt, "created", prNumber, nil, prID, nil, nil, nil, nil, maybeHide)
	res := ExecSQLTxWithErr(
		tc,
		ctx,
		InsertIgnore(
			fmt.Sprintf(
				"into gha_reviews(id, user_id, commit_id, submitted_at, author_association, state, body, event_id, "+
					"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_user_login) "+
					"values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
				NValue(1), NValue(2), NValue(3), NValue(4), NValue(5), NValue(6), NValue(7), NValue(8),
				NValue(9), NValue(10), NValue(11), NValue(12), NValue(13), NValue(14), NValue(15),
			),
		),
		AnyArray{
			rid,
			ghActorIDOrNil(rev.User),
			strOr(rev.CommitID, ""),
			createdAt,
			strOr(rev.AuthorAssociation, "NONE"),
			strOr(rev.State, ""),
			truncStringOrNilHidden(rev.Body, 0xffff, maybeHide),
			eid,
			ghActorIDOrNil(rev.User),
			ghActorLoginOrNil(rev.User, maybeHide),
			repoID,
			repo,
			eType,
			createdAt,
			ghActorLoginOrNil(rev.User, maybeHide),
		}...,
	)
	FatalOnError(tc.Commit())
	n, _ := res.RowsAffected()
	return n > 0
}

// RestoreFork - restores a fork missed by GH Archive: artificial ForkEvent + gha_forkees row
func RestoreFork(c *sql.DB, ctx *Ctx, repo string, repoID int64, orgID interface{}, fork *github.Repository, maybeHide func(string) string) bool {
	if ctx.SkipPDB {
		return false
	}
	if fork == nil || fork.ID == nil || fork.Owner == nil || fork.Owner.Login == nil || fork.CreatedAt == nil {
		return false
	}
	fid := *fork.ID
	eid := ArtificialForkIDBase + fid
	if !artificialIDOK(eid, "RestoreFork", repo) {
		return false
	}
	createdAt := fork.CreatedAt.Time
	eType := "ForkEvent"
	var organization interface{}
	if fork.Organization != nil && fork.Organization.Login != nil {
		organization = *fork.Organization.Login
	}
	tc, err := c.Begin()
	FatalOnError(err)
	if raw := findRawEventID(tc, ctx, eType, repo, fork.Owner, createdAt, "forkee_id", fid); raw != nil {
		eid = raw.(int64)
	}
	ghActor(tc, ctx, fork.Owner, maybeHide)
	restoreEventPayload(tc, ctx, eid, eType, fork.Owner, repo, repoID, orgID, createdAt, nil, nil, nil, nil, nil, fid, nil, nil, maybeHide)
	res := ExecSQLTxWithErr(
		tc,
		ctx,
		InsertIgnore(
			fmt.Sprintf(
				"into gha_forkees(id, event_id, name, full_name, owner_id, description, fork, created_at, updated_at, pushed_at, "+
					"homepage, size, stargazers_count, has_issues, has_projects, has_downloads, has_wiki, has_pages, forks, "+
					"open_issues, watchers, default_branch, public, language, organization, "+
					"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_owner_login) "+
					"values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
				NValue(1), NValue(2), NValue(3), NValue(4), NValue(5), NValue(6), NValue(7), NValue(8), NValue(9), NValue(10),
				NValue(11), NValue(12), NValue(13), NValue(14), NValue(15), NValue(16), NValue(17), NValue(18), NValue(19), NValue(20),
				NValue(21), NValue(22), NValue(23), NValue(24), NValue(25), NValue(26), NValue(27), NValue(28), NValue(29), NValue(30),
				NValue(31), NValue(32),
			),
		),
		AnyArray{
			fid,
			eid,
			TruncToBytes(strOr(fork.Name, ""), 80),
			TruncToBytes(strOr(fork.FullName, ""), 200),
			ghActorIDOrNil(fork.Owner),
			truncStringOrNilHidden(fork.Description, 0xffff, maybeHide),
			true,
			createdAt,
			tsOr(fork.UpdatedAt, createdAt),
			tsOrNil(fork.PushedAt),
			truncStringOrNilHidden(fork.Homepage, 0xffff, maybeHide),
			intOr(fork.Size, 0),
			intOr(fork.StargazersCount, 0),
			boolOr(fork.HasIssues, false),
			BoolOrNil(fork.HasProjects),
			boolOr(fork.HasDownloads, false),
			boolOr(fork.HasWiki, false),
			BoolOrNil(fork.HasPages),
			intOr(fork.ForksCount, 0),
			intOr(fork.OpenIssuesCount, 0),
			intOr(fork.WatchersCount, 0),
			TruncToBytes(strOr(fork.DefaultBranch, "master"), 200),
			!boolOr(fork.Private, false),
			StringOrNil(fork.Language),
			organization,
			ghActorIDOrNil(fork.Owner),
			ghActorLoginOrNil(fork.Owner, maybeHide),
			repoID,
			repo,
			eType,
			createdAt,
			ghActorLoginOrNil(fork.Owner, maybeHide),
		}...,
	)
	FatalOnError(tc.Commit())
	n, _ := res.RowsAffected()
	return n > 0
}

// RestoreRelease - restores a release missed by GH Archive: artificial ReleaseEvent + gha_releases (+assets) rows
func RestoreRelease(c *sql.DB, ctx *Ctx, repo string, repoID int64, orgID interface{}, rel *github.RepositoryRelease, maybeHide func(string) string) bool {
	if ctx.SkipPDB {
		return false
	}
	if rel == nil || rel.ID == nil || rel.Author == nil || rel.Author.Login == nil || rel.CreatedAt == nil {
		return false
	}
	rid := *rel.ID
	eid := ArtificialReleaseIDBase + rid
	if !artificialIDOK(eid, "RestoreRelease", repo) {
		return false
	}
	createdAt := tsOr(rel.PublishedAt, rel.CreatedAt.Time)
	eType := "ReleaseEvent"
	tc, err := c.Begin()
	FatalOnError(err)
	if raw := findRawEventID(tc, ctx, eType, repo, rel.Author, createdAt, "release_id", rid); raw != nil {
		eid = raw.(int64)
	}
	ghActor(tc, ctx, rel.Author, maybeHide)
	restoreEventPayload(tc, ctx, eid, eType, rel.Author, repo, repoID, orgID, createdAt, "published", nil, nil, nil, nil, nil, rid, nil, maybeHide)
	res := ExecSQLTxWithErr(
		tc,
		ctx,
		InsertIgnore(
			fmt.Sprintf(
				"into gha_releases(id, event_id, tag_name, target_commitish, name, draft, author_id, prerelease, "+
					"created_at, published_at, body, "+
					"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_author_login) "+
					"values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
				NValue(1), NValue(2), NValue(3), NValue(4), NValue(5), NValue(6), NValue(7), NValue(8), NValue(9),
				NValue(10), NValue(11), NValue(12), NValue(13), NValue(14), NValue(15), NValue(16), NValue(17), NValue(18),
			),
		),
		AnyArray{
			rid,
			eid,
			TruncToBytes(strOr(rel.TagName, ""), 200),
			TruncToBytes(strOr(rel.TargetCommitish, ""), 200),
			truncStringOrNilHidden(rel.Name, 200, maybeHide),
			boolOr(rel.Draft, false),
			ghActorIDOrNil(rel.Author),
			boolOr(rel.Prerelease, false),
			createdAt,
			tsOrNil(rel.PublishedAt),
			truncStringOrNilHidden(rel.Body, 0xffff, maybeHide),
			ghActorIDOrNil(rel.Author),
			ghActorLoginOrNil(rel.Author, maybeHide),
			repoID,
			repo,
			eType,
			createdAt,
			ghActorLoginOrNil(rel.Author, maybeHide),
		}...,
	)
	for _, asset := range rel.Assets {
		if asset == nil || asset.ID == nil {
			continue
		}
		aid := *asset.ID
		uploader := asset.Uploader
		if uploader == nil {
			uploader = rel.Author
		}
		ghActor(tc, ctx, uploader, maybeHide)
		ExecSQLTxWithErr(
			tc,
			ctx,
			InsertIgnore(
				fmt.Sprintf(
					"into gha_assets(id, event_id, name, label, uploader_id, content_type, state, size, download_count, "+
						"created_at, updated_at, "+
						"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_uploader_login) "+
						"values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
					NValue(1), NValue(2), NValue(3), NValue(4), NValue(5), NValue(6), NValue(7), NValue(8), NValue(9),
					NValue(10), NValue(11), NValue(12), NValue(13), NValue(14), NValue(15), NValue(16), NValue(17), NValue(18),
				),
			),
			AnyArray{
				aid,
				eid,
				TruncToBytes(maybeHide(strOr(asset.Name, "")), 200),
				truncStringOrNilHidden(asset.Label, 120, maybeHide),
				ghActorIDOrNil(uploader),
				TruncToBytes(strOr(asset.ContentType, ""), 80),
				TruncToBytes(strOr(asset.State, ""), 20),
				intOr(asset.Size, 0),
				intOr(asset.DownloadCount, 0),
				tsOr(asset.CreatedAt, createdAt),
				tsOr(asset.UpdatedAt, createdAt),
				ghActorIDOrNil(rel.Author),
				ghActorLoginOrNil(rel.Author, maybeHide),
				repoID,
				repo,
				eType,
				createdAt,
				ghActorLoginOrNil(uploader, maybeHide),
			}...,
		)
		ExecSQLTxWithErr(
			tc,
			ctx,
			InsertIgnore("into gha_releases_assets(release_id, event_id, asset_id) "+NValues(3)),
			AnyArray{rid, eid, aid}...,
		)
	}
	FatalOnError(tc.Commit())
	n, _ := res.RowsAffected()
	return n > 0
}

// RestoreCommitComment - restores a commit comment missed by GH Archive: artificial CommitCommentEvent + gha_comments row
func RestoreCommitComment(c *sql.DB, ctx *Ctx, repo string, repoID int64, orgID interface{}, cmt *github.RepositoryComment, maybeHide func(string) string) bool {
	if ctx.SkipPDB {
		return false
	}
	if cmt == nil || cmt.ID == nil || cmt.User == nil || cmt.User.Login == nil || cmt.CreatedAt == nil {
		Printf("RestoreCommitComment: %s: skipping comment with missing id/user/created_at\n", repo)
		return false
	}
	cid := *cmt.ID
	eid := ArtificialCommitCommentIDBase + cid
	if !artificialIDOK(eid, "RestoreCommitComment", repo) {
		return false
	}
	createdAt := *cmt.CreatedAt
	eType := "CommitCommentEvent"
	tc, err := c.Begin()
	FatalOnError(err)
	if raw := findRawEventID(tc, ctx, eType, repo, cmt.User, createdAt, "comment_id", cid); raw != nil {
		eid = raw.(int64)
	}
	ghActor(tc, ctx, cmt.User, maybeHide)
	restoreEventPayload(tc, ctx, eid, eType, cmt.User, repo, repoID, orgID, createdAt, "created", nil, nil, nil, cid, nil, nil, StringOrNil(cmt.CommitID), maybeHide)
	res := ExecSQLTxWithErr(
		tc,
		ctx,
		InsertIgnore(
			fmt.Sprintf(
				"into gha_comments(id, event_id, body, created_at, updated_at, user_id, commit_id, position, path, "+
					"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_user_login) "+
					"values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
				NValue(1), NValue(2), NValue(3), NValue(4), NValue(5), NValue(6), NValue(7), NValue(8),
				NValue(9), NValue(10), NValue(11), NValue(12), NValue(13), NValue(14), NValue(15), NValue(16),
			),
		),
		AnyArray{
			cid,
			eid,
			TruncToBytes(maybeHide(strOr(cmt.Body, "")), 0xffff),
			createdAt,
			timeOr(cmt.UpdatedAt, createdAt),
			ghActorIDOrNil(cmt.User),
			StringOrNil(cmt.CommitID),
			IntOrNil(cmt.Position),
			StringOrNil(cmt.Path),
			ghActorIDOrNil(cmt.User),
			ghActorLoginOrNil(cmt.User, maybeHide),
			repoID,
			repo,
			eType,
			createdAt,
			ghActorLoginOrNil(cmt.User, maybeHide),
		}...,
	)
	FatalOnError(tc.Commit())
	n, _ := res.RowsAffected()
	return n > 0
}

// RestoreStar - restores a star missed by GH Archive as an artificial WatchEvent (hash-based negative id, like pre-2015 events)
func RestoreStar(c *sql.DB, ctx *Ctx, repo string, repoID int64, orgID interface{}, star *github.Stargazer, maybeHide func(string) string) bool {
	if ctx.SkipPDB {
		return false
	}
	if star == nil || star.User == nil || star.User.Login == nil || star.StarredAt == nil {
		return false
	}
	createdAt := star.StarredAt.Time
	eType := "WatchEvent"
	if star.User.ID == nil {
		return false
	}
	eid := NegativeArtificialID([]string{eType, fmt.Sprint(*star.User.ID), repo, ToYMDHMSDate(createdAt)})
	tc, err := c.Begin()
	FatalOnError(err)
	if hashIDConflict(tc, ctx, eid, eType, repo, *star.User.ID, createdAt) {
		FatalOnError(tc.Rollback())
		return false
	}
	ghActor(tc, ctx, star.User, maybeHide)
	res := restoreEventPayload(tc, ctx, eid, eType, star.User, repo, repoID, orgID, createdAt, "started", nil, nil, nil, nil, nil, nil, nil, maybeHide)
	FatalOnError(tc.Commit())
	n, _ := res.RowsAffected()
	return n > 0
}

// RunRangePostprocess - runs the bounded *_range.sql postprocess scripts for [from, to),
// used after API restores that insert rows older than the hourly postprocess window
func RunRangePostprocess(ctx *Ctx, from, to time.Time) {
	RunRangePostprocessDB(ctx, "", from, to)
}

// RunRangePostprocessDB - RunRangePostprocess against an explicit database (empty db = ctx.PgDB)
func RunRangePostprocessDB(ctx *Ctx, db string, from, to time.Time) {
	if ctx.SkipPDB {
		return
	}
	c := PgConn(ctx)
	if db != "" {
		FatalOnError(c.Close())
		c = PgConnDB(ctx, db)
	}
	defer func() { FatalOnError(c.Close()) }()
	// empty (truncated) target: skip - the same cycle's structure run performs the full rebuild,
	// and inserting the bounded range here first would lock its 1 month window to a partial table
	rows := QuerySQLWithErr(c, ctx, "select 1 from gha_texts limit 1")
	empty := true
	for rows.Next() {
		empty = false
	}
	FatalOnError(rows.Err())
	FatalOnError(rows.Close())
	if empty {
		Printf("bounded postprocess skipped: gha_texts is empty, full structure rebuild pending\n")
		return
	}
	dataPrefix := ctx.DataDir
	if ctx.Local {
		dataPrefix = "./"
	}
	tc, err := c.Begin()
	FatalOnError(err)
	ExecSQLTxWithErr(tc, ctx, "select set_config('devstats.postprocess_from', "+NValue(1)+", true)", ToYMDHMSDate(from))
	ExecSQLTxWithErr(tc, ctx, "select set_config('devstats.postprocess_to', "+NValue(1)+", true)", ToYMDHMSDate(to))
	for _, script := range []string{
		"util_sql/postprocess_texts_range.sql",
		"util_sql/postprocess_labels_range.sql",
		"util_sql/postprocess_issues_prs_range.sql",
	} {
		bytes, err := ReadFile(ctx, dataPrefix+script)
		FatalOnError(err)
		ExecSQLTxWithErr(tc, ctx, string(bytes))
	}
	FatalOnError(tc.Commit())
	Printf("bounded postprocess executed for [%s, %s)\n", ToYMDHMSDate(from), ToYMDHMSDate(to))
}
