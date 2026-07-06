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

func artificialIDOK(eid int64, what, repo string) bool {
	if eid >= SyncEventIDThreshold {
		Printf("%s: %s: artificial event id %d reached the sync events range, skipping\n", what, repo, eid)
		return false
	}
	return true
}

func restoreEventPayload(tc *sql.Tx, ctx *Ctx, eid int64, eType string, actor *github.User, repo string, repoID int64, orgID interface{}, createdAt time.Time, action, number, issueID, prID, commentID, forkeeID, releaseID, commitSHA interface{}, maybeHide func(string) string) {
	ExecSQLTxWithErr(
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
		InsertIgnore(
			"into gha_payloads(event_id, action, number, issue_id, pull_request_id, comment_id, forkee_id, release_id, commit, "+
				"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at) "+NValues(15),
		),
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
}

// RestoreIssueComment - restores a comment missed by GH Archive: artificial IssueCommentEvent + gha_comments row
func RestoreIssueComment(c *sql.DB, ctx *Ctx, repo string, repoID int64, orgID interface{}, issueNumber int, cmt *github.IssueComment, maybeHide func(string) string) {
	if ctx.SkipPDB {
		return
	}
	if cmt == nil || cmt.ID == nil || cmt.User == nil || cmt.User.Login == nil || cmt.CreatedAt == nil {
		Printf("RestoreIssueComment: %s: skipping comment with missing id/user/created_at\n", repo)
		return
	}
	cid := *cmt.ID
	eid := ArtificialCommentIDBase + cid
	if !artificialIDOK(eid, "RestoreIssueComment", repo) {
		return
	}
	createdAt := *cmt.CreatedAt
	eType := "IssueCommentEvent"
	tc, err := c.Begin()
	FatalOnError(err)
	ghActor(tc, ctx, cmt.User, maybeHide)
	issueID := lookupID(tc, ctx, "select max(id) from gha_issues where number = "+NValue(1)+" and dup_repo_name = "+NValue(2), issueNumber, repo)
	restoreEventPayload(tc, ctx, eid, eType, cmt.User, repo, repoID, orgID, createdAt, "created", issueNumber, issueID, nil, cid, nil, nil, nil, maybeHide)
	ExecSQLTxWithErr(
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
			TruncToBytes(strOr(cmt.Body, ""), 0xffff),
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
}

// RestoreReviewComment - restores a PR review comment missed by GH Archive: artificial PullRequestReviewCommentEvent + gha_comments row
func RestoreReviewComment(c *sql.DB, ctx *Ctx, repo string, repoID int64, orgID interface{}, prNumber int, cmt *github.PullRequestComment, maybeHide func(string) string) {
	if ctx.SkipPDB {
		return
	}
	if cmt == nil || cmt.ID == nil || cmt.User == nil || cmt.User.Login == nil || cmt.CreatedAt == nil {
		Printf("RestoreReviewComment: %s: skipping comment with missing id/user/created_at\n", repo)
		return
	}
	cid := *cmt.ID
	eid := ArtificialReviewCommentIDBase + cid
	if !artificialIDOK(eid, "RestoreReviewComment", repo) {
		return
	}
	createdAt := *cmt.CreatedAt
	eType := "PullRequestReviewCommentEvent"
	tc, err := c.Begin()
	FatalOnError(err)
	ghActor(tc, ctx, cmt.User, maybeHide)
	prID := lookupID(tc, ctx, "select max(id) from gha_pull_requests where number = "+NValue(1)+" and dup_repo_name = "+NValue(2), prNumber, repo)
	restoreEventPayload(tc, ctx, eid, eType, cmt.User, repo, repoID, orgID, createdAt, "created", prNumber, nil, prID, cid, nil, nil, nil, maybeHide)
	ExecSQLTxWithErr(
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
			TruncToBytes(strOr(cmt.Body, ""), 0xffff),
			createdAt,
			timeOr(cmt.UpdatedAt, createdAt),
			ghActorIDOrNil(cmt.User),
			StringOrNil(cmt.CommitID),
			StringOrNil(cmt.OriginalCommitID),
			TruncStringOrNil(cmt.DiffHunk, 0xffff),
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
}

// RestoreReview - restores a PR review missed by GH Archive: artificial PullRequestReviewEvent + gha_reviews row
func RestoreReview(c *sql.DB, ctx *Ctx, repo string, repoID int64, orgID interface{}, prNumber int, rev *github.PullRequestReview, maybeHide func(string) string) {
	if ctx.SkipPDB {
		return
	}
	if rev == nil || rev.ID == nil || rev.User == nil || rev.User.Login == nil || rev.SubmittedAt == nil {
		return
	}
	rid := *rev.ID
	eid := ArtificialReviewIDBase + rid
	if !artificialIDOK(eid, "RestoreReview", repo) {
		return
	}
	createdAt := *rev.SubmittedAt
	eType := "PullRequestReviewEvent"
	tc, err := c.Begin()
	FatalOnError(err)
	ghActor(tc, ctx, rev.User, maybeHide)
	prID := lookupID(tc, ctx, "select max(id) from gha_pull_requests where number = "+NValue(1)+" and dup_repo_name = "+NValue(2), prNumber, repo)
	restoreEventPayload(tc, ctx, eid, eType, rev.User, repo, repoID, orgID, createdAt, "created", prNumber, nil, prID, nil, nil, nil, nil, maybeHide)
	ExecSQLTxWithErr(
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
			TruncStringOrNil(rev.Body, 0xffff),
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
}

// RestoreFork - restores a fork missed by GH Archive: artificial ForkEvent + gha_forkees row
func RestoreFork(c *sql.DB, ctx *Ctx, repo string, repoID int64, orgID interface{}, fork *github.Repository, maybeHide func(string) string) {
	if ctx.SkipPDB {
		return
	}
	if fork == nil || fork.ID == nil || fork.Owner == nil || fork.Owner.Login == nil || fork.CreatedAt == nil {
		return
	}
	fid := *fork.ID
	eid := ArtificialForkIDBase + fid
	if !artificialIDOK(eid, "RestoreFork", repo) {
		return
	}
	createdAt := fork.CreatedAt.Time
	eType := "ForkEvent"
	var organization interface{}
	if fork.Organization != nil && fork.Organization.Login != nil {
		organization = *fork.Organization.Login
	}
	tc, err := c.Begin()
	FatalOnError(err)
	ghActor(tc, ctx, fork.Owner, maybeHide)
	restoreEventPayload(tc, ctx, eid, eType, fork.Owner, repo, repoID, orgID, createdAt, "created", nil, nil, nil, nil, fid, nil, nil, maybeHide)
	ExecSQLTxWithErr(
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
			TruncStringOrNil(fork.Description, 0xffff),
			true,
			createdAt,
			tsOr(fork.UpdatedAt, createdAt),
			tsOrNil(fork.PushedAt),
			StringOrNil(fork.Homepage),
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
}

// RestoreRelease - restores a release missed by GH Archive: artificial ReleaseEvent + gha_releases (+assets) rows
func RestoreRelease(c *sql.DB, ctx *Ctx, repo string, repoID int64, orgID interface{}, rel *github.RepositoryRelease, maybeHide func(string) string) {
	if ctx.SkipPDB {
		return
	}
	if rel == nil || rel.ID == nil || rel.Author == nil || rel.Author.Login == nil || rel.CreatedAt == nil {
		return
	}
	rid := *rel.ID
	eid := ArtificialReleaseIDBase + rid
	if !artificialIDOK(eid, "RestoreRelease", repo) {
		return
	}
	createdAt := rel.CreatedAt.Time
	eType := "ReleaseEvent"
	tc, err := c.Begin()
	FatalOnError(err)
	ghActor(tc, ctx, rel.Author, maybeHide)
	restoreEventPayload(tc, ctx, eid, eType, rel.Author, repo, repoID, orgID, createdAt, "published", nil, nil, nil, nil, nil, rid, nil, maybeHide)
	ExecSQLTxWithErr(
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
			TruncStringOrNil(rel.Name, 200),
			boolOr(rel.Draft, false),
			ghActorIDOrNil(rel.Author),
			boolOr(rel.Prerelease, false),
			createdAt,
			tsOrNil(rel.PublishedAt),
			TruncStringOrNil(rel.Body, 0xffff),
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
				TruncToBytes(strOr(asset.Name, ""), 200),
				TruncStringOrNil(asset.Label, 120),
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
}

// RestoreCommitComment - restores a commit comment missed by GH Archive: artificial CommitCommentEvent + gha_comments row
func RestoreCommitComment(c *sql.DB, ctx *Ctx, repo string, repoID int64, orgID interface{}, cmt *github.RepositoryComment, maybeHide func(string) string) {
	if ctx.SkipPDB {
		return
	}
	if cmt == nil || cmt.ID == nil || cmt.User == nil || cmt.User.Login == nil || cmt.CreatedAt == nil {
		Printf("RestoreCommitComment: %s: skipping comment with missing id/user/created_at\n", repo)
		return
	}
	cid := *cmt.ID
	eid := ArtificialCommitCommentIDBase + cid
	if !artificialIDOK(eid, "RestoreCommitComment", repo) {
		return
	}
	createdAt := *cmt.CreatedAt
	eType := "CommitCommentEvent"
	tc, err := c.Begin()
	FatalOnError(err)
	ghActor(tc, ctx, cmt.User, maybeHide)
	restoreEventPayload(tc, ctx, eid, eType, cmt.User, repo, repoID, orgID, createdAt, "created", nil, nil, nil, cid, nil, nil, StringOrNil(cmt.CommitID), maybeHide)
	ExecSQLTxWithErr(
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
			TruncToBytes(strOr(cmt.Body, ""), 0xffff),
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
}

// RestoreStar - restores a star missed by GH Archive as an artificial WatchEvent (hash-based negative id, like pre-2015 events)
func RestoreStar(c *sql.DB, ctx *Ctx, repo string, repoID int64, orgID interface{}, star *github.Stargazer, maybeHide func(string) string) {
	if ctx.SkipPDB {
		return
	}
	if star == nil || star.User == nil || star.User.Login == nil || star.StarredAt == nil {
		return
	}
	createdAt := star.StarredAt.Time
	eType := "WatchEvent"
	eid := int64(HashStrings([]string{eType, *star.User.Login, repo, ToYMDHMSDate(createdAt)}))
	tc, err := c.Begin()
	FatalOnError(err)
	ghActor(tc, ctx, star.User, maybeHide)
	restoreEventPayload(tc, ctx, eid, eType, star.User, repo, repoID, orgID, createdAt, "started", nil, nil, nil, nil, nil, nil, nil, maybeHide)
	FatalOnError(tc.Commit())
}
