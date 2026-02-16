package main

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	lib "github.com/cncf/devstatscode"
)

// commitInfo holds commit metadata extracted from local git history.
// git/git_commits.sh output provides: sha,b64(author_name),b64(author_email),b64(committer_name),b64(committer_email),b64(message);
type commitInfo struct {
	Sha            string
	AuthorName     string
	AuthorEmail    string
	CommitterName  string
	CommitterEmail string
	Message        string
}

type pushEvent struct {
	EventID    int64
	ActorID    int64
	ActorLogin string
	RepoID     int64
	RepoName   string
	CreatedAt  time.Time
	Head       string
	Before     string
	Size       sql.NullInt64
	Cnt        int64
}

type actorCacheEntry struct {
	id    int64
	login string
}

type actorCache struct {
	mu sync.RWMutex
	m  map[[2]string]actorCacheEntry
}

var (
	// InsertCommitterRole - we don't add those roles
	InsertCommitterRole = false
	// InsertAuthorRole - we don't add those roles
	InsertAuthorRole = false
)

func newActorCache() *actorCache {
	return &actorCache{m: make(map[[2]string]actorCacheEntry)}
}

// isZeroSHA returns true for empty strings and all-zero 40-hex-like SHAs.
func isZeroSHA(sha string) bool {
	sha = strings.TrimSpace(sha)
	if sha == "" {
		return true
	}
	for _, c := range sha {
		if c != '0' {
			return false
		}
	}
	return true
}

func reverseStringsInPlace(a []string) {
	for i, j := 0, len(a)-1; i < j; i, j = i+1, j-1 {
		a[i], a[j] = a[j], a[i]
	}
}

// backfillPushEventCommits reconstructs gha_commits (and gha_commits_roles) for PushEvent payloads.
// dbs are processed sequentially; repos inside a DB are processed in parallel up to NCPUs.
func backfillPushEventCommits(ctx *lib.Ctx, dbs map[string]string, repoDBs map[string]map[string]struct{}) {
	if ctx.FetchCommitsMode == 0 {
		return
	}

	// Ensure git commands return output and don't abort the whole process from worker goroutines.
	prevExecOutput := ctx.ExecOutput
	prevExecFatal := ctx.ExecFatal
	prevExecQuiet := ctx.ExecQuiet
	ctx.ExecOutput = true
	ctx.ExecFatal = false
	ctx.ExecQuiet = true
	defer func() {
		ctx.ExecOutput = prevExecOutput
		ctx.ExecFatal = prevExecFatal
		ctx.ExecQuiet = prevExecQuiet
	}()

	hideCfg := lib.GetHidden(ctx, lib.HideCfgFile)
	maybeHide := lib.MaybeHideFuncTS(hideCfg)

	// DBs sequentially (deterministic order).
	dbNames := make([]string, 0, len(dbs))
	for db := range dbs {
		dbNames = append(dbNames, db)
	}
	sort.Strings(dbNames)

	for _, db := range dbNames {
		reposSet, ok := repoDBs[db]
		if !ok || len(reposSet) == 0 {
			continue
		}

		repos := make([]string, 0, len(reposSet))
		for r := range reposSet {
			repos = append(repos, r)
		}
		sort.Strings(repos)

		thrN := lib.GetThreadsNum(ctx)
		lib.Printf(
			"FetchCommitsMode=%d: processing DB '%s' (%d repos, threads %d, batch %d)\n",
			ctx.FetchCommitsMode, db, len(repos), thrN, ctx.GitCommitsBatch,
		)

		con := lib.PgConnDB(ctx, db)
		// Actor cache shared across repos processed for this DB (thread-safe).
		acache := newActorCache()

		thr := make(chan struct{}, thrN)
		done := make(chan struct{}, len(repos))

		for _, repo := range repos {
			thr <- struct{}{}
			repo := repo
			go func() {
				defer func() {
					<-thr
					done <- struct{}{}
				}()
				err := backfillRepo(ctx, con, repo, maybeHide, acache)
				if err != nil {
					lib.Printf("backfillRepo(DB=%s, repo=%s) error: %v\n", db, repo, err)
				}
			}()
		}

		for range repos {
			<-done
		}
		lib.FatalOnError(con.Close())
	}
}

func backfillRepo(ctx *lib.Ctx, con *sql.DB, repo string, maybeHide func(string) string, acache *actorCache) error {
	repoPath := ctx.ReposDir + repo
	if _, err := os.Stat(repoPath); os.IsNotExist(err) {
		// Do not silently skip: user explicitly requested tracking.
		return fmt.Errorf("repo not cloned: %s", repoPath)
	} else if err != nil {
		return fmt.Errorf("cannot stat repo path %s: %w", repoPath, err)
	}

	// For mode=1 (missing only) we can limit scanning by last commit time already inserted.
	dtFrom := ctx.DefaultStartDate
	if ctx.FetchCommitsMode == 1 {
		var maxDt sql.NullTime
		err := con.QueryRow(`select max(dup_created_at) from gha_commits where dup_repo_name = $1`, repo).Scan(&maxDt)
		if err != nil {
			return fmt.Errorf("select max(dup_created_at) from gha_commits failed (repo=%s): %w", repo, err)
		}
		if maxDt.Valid && maxDt.Time.After(dtFrom) {
			dtFrom = maxDt.Time
		}
	}

	events, err := selectPushEventsNeedingCommits(ctx, con, repo, dtFrom)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		lib.Printf("%s: no need to backfill commits since %s\n", repo, dtFrom)
		return nil
	}
	lib.Printf("%s: need to backfill %d events since %s\n", repo, len(events), dtFrom)

	// Build: event -> shas, plus global sha set.
	eventShas := make(map[int64][]string, len(events))
	shaSet := make(map[string]struct{})

	pageSize := ctx.GitCommitsBatch
	if pageSize <= 0 {
		pageSize = 1000
	}

	for _, ev := range events {
		head := strings.TrimSpace(ev.Head)
		before := strings.TrimSpace(ev.Before)

		if head == "" || isZeroSHA(head) {
			if ctx.Debug > 0 {
				lib.Printf("Warning: skipping PushEvent %d in %s: empty/zero head SHA\n", ev.EventID, repo)
			}
			continue
		}

		// BEFORE=0 is ambiguous. We use payload size (when available) to limit the scan.
		maxNeeded := 0
		if isZeroSHA(before) {
			if ev.Size.Valid {
				if ev.Size.Int64 <= 0 {
					// size=0 -> no commits; should be filtered out, but be defensive.
					if ctx.Debug > 0 {
						lib.Printf("Warning: skipping PushEvent %d in %s: non-positive payload size=%d with zero before SHA\n", ev.EventID, repo, ev.Size.Int64)
					}
					continue
				}
				// For before=0 we take the newest <size> commits reachable from head.
				// This is a practical approximation for "new ref" events (including the initial push).
				maxNeeded = int(ev.Size.Int64)
				before = strings.Repeat("0", 40)
			} else {
				// Unknown size; safest fallback is at least head.
				maxNeeded = 1
				before = strings.Repeat("0", 40)
			}
		}

		shas, gerr := gitRangeCommits(ctx, repoPath, before, head, pageSize, maxNeeded)
		if gerr != nil {
			lib.Printf(
				"Error listing commits range for %s (event %d, before %s, head %s): %v\n",
				repo, ev.EventID, ev.Before, ev.Head, gerr,
			)
			// Fallback: at least head commit.
			shas = []string{head}
		}

		if len(shas) == 0 {
			if ctx.Debug > 0 {
				lib.Printf("Warning: no commits found for %s PushEvent %d (before %s, head %s)\n", repo, ev.EventID, ev.Before, ev.Head)
			}
			continue
		}
		if ctx.Debug > 0 {
			lib.Printf("%s PushEvent %d: found %d commits (before %s, head %s): %+v\n", repo, ev.EventID, len(shas), ev.Before, ev.Head, shas)
		} else {
			lib.Printf("%s PushEvent %d: found %d commits (before %s, head %s)\n", repo, ev.EventID, len(shas), ev.Before, ev.Head)
		}
		eventShas[ev.EventID] = shas
		for _, s := range shas {
			s = strings.TrimSpace(s)
			if s == "" || isZeroSHA(s) {
				if ctx.Debug > 0 {
					lib.Printf("Warning: skipping empty/zero SHA for %s PushEvent %d\n", repo, ev.EventID)
				}
				continue
			}
			shaSet[s] = struct{}{}
		}
	}

	if len(eventShas) == 0 || len(shaSet) == 0 {
		lib.Printf("%s: no commits to backfill after processing %d events\n", repo, len(events))
		return nil
	}
	lib.Printf("%s: need to backfill %d commits for %d events\n", repo, len(shaSet), len(events))

	// Fetch commit metadata for all SHAs in batches.
	shaList := make([]string, 0, len(shaSet))
	for s := range shaSet {
		shaList = append(shaList, s)
	}
	sort.Strings(shaList)

	infoMap := make(map[string]commitInfo, len(shaSet))
	for i := 0; i < len(shaList); i += pageSize {
		j := i + pageSize
		if j > len(shaList) {
			j = len(shaList)
		}
		batchInfos, ierr := gitCommitInfoBatch(ctx, repoPath, shaList[i:j])
		for sha, info := range batchInfos {
			infoMap[sha] = info
		}
		if ierr != nil {
			lib.Printf("Warning: git_commits.sh error for %s batch %d-%d/%d: %v\n", repo, i, j, len(shaList), ierr)
		}
	}
	if len(infoMap) == 0 {
		return fmt.Errorf("git_commits.sh returned no commit metadata for repo=%s (shas=%d)", repo, len(shaSet))
	}
	if ctx.Debug > 0 {
		lib.Printf("Fetched commit metadata for %s: %d SHAs, %d records so far\n", repo, len(shaSet), len(infoMap))
	}

	tx, err := con.Begin()
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	insCommitSQL := `
insert into gha_commits(
  sha, event_id, author_name, encrypted_email, message,
  is_distinct, dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at,
  author_id, committer_id, dup_author_login, dup_committer_login,
  author_email, committer_name, committer_email
)
select
  $1::varchar(40),$2,$3,$4,$5,
  not exists(select 1 from gha_commits c2 where c2.sha = $1::varchar(40) limit 1),
  $6,$7,$8,$9,$10,$11,
  $12,$13,$14,$15,
  $16,$17,$18
on conflict do nothing
`
	insRoleSQL := `
insert into gha_commits_roles(
  sha, event_id, role, actor_id, actor_login, actor_name, actor_email, dup_repo_id, dup_repo_name, dup_created_at
) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
on conflict do nothing
`
	// Only fill missing payload size (NULL) with the computed count.
	updPayloadSQL := `update gha_payloads set size = $2 where event_id = $1 and (size is null or size <= 1) and (size is null or size <> $2)`

	insCommitStmt, err := tx.Prepare(insCommitSQL)
	if err != nil {
		return err
	}
	defer func() { _ = insCommitStmt.Close() }()

	insRoleStmt, err := tx.Prepare(insRoleSQL)
	if err != nil {
		return err
	}
	defer func() { _ = insRoleStmt.Close() }()

	updPayloadStmt, err := tx.Prepare(updPayloadSQL)
	if err != nil {
		return err
	}
	defer func() { _ = updPayloadStmt.Close() }()

	lib.Printf("%s: inserting commits for %d events\n", repo, len(events))
	nCommits := 0
	nRoles := 0
	for _, ev := range events {
		shas := eventShas[ev.EventID]
		if ctx.Debug > 0 {
			lib.Printf("%s PushEvent %d: inserting %d commits (before %s, head %s)\n", repo, ev.EventID, len(shas), ev.Before, ev.Head)
		}
		if len(shas) == 0 {
			if ctx.Debug > 0 {
				lib.Printf("Warning: no commits to insert for %s PushEvent %d (before %s, head %s)\n", repo, ev.EventID, ev.Before, ev.Head)
			}
			continue
		}

		// Update payload size to computed commit count only if size is currently NULL.
		if _, uerr := updPayloadStmt.Exec(ev.EventID, len(shas)); uerr != nil {
			return fmt.Errorf("update gha_payloads.size (repo=%s, event=%d): error: %w", repo, ev.EventID, uerr)
		}

		if ev.Size.Valid && int64(len(shas)) != ev.Size.Int64 {
			lib.Printf(
				"Warning: %s PushEvent %d payload size=%d, computed commits=%d (before %s, head %s)\n",
				repo, ev.EventID, ev.Size.Int64, len(shas), ev.Before, ev.Head,
			)
		}

		for _, sha := range shas {
			sha = strings.TrimSpace(sha)
			if sha == "" || isZeroSHA(sha) {
				if ctx.Debug > 0 {
					lib.Printf("Warning: skipping empty/zero SHA for %s PushEvent %d\n", repo, ev.EventID)
				}
				continue
			}

			ci, ok := infoMap[sha]
			if !ok {
				if ctx.Debug > 0 {
					lib.Printf("Warning: missing git metadata for %s sha %s (event %d)\n", repo, sha, ev.EventID)
				}
				continue
			}

			// Commit table fields.
			authorNameRaw := strings.ReplaceAll(ci.AuthorName, "\x00", "")
			authorEmailRaw := strings.ReplaceAll(ci.AuthorEmail, "\x00", "")
			commNameRaw := strings.ReplaceAll(ci.CommitterName, "\x00", "")
			commEmailRaw := strings.ReplaceAll(ci.CommitterEmail, "\x00", "")

			authorName := lib.TruncToBytes(maybeHide(authorNameRaw), 120)
			authorEmail := lib.TruncToBytes(maybeHide(authorEmailRaw), 160)
			msg := lib.TruncToBytes(strings.ReplaceAll(ci.Message, "\x00", ""), 0xffff)

			// Roles fields (longer allowed).
			authorRoleName := lib.TruncToBytes(maybeHide(authorNameRaw), 160)
			authorRoleEmail := lib.TruncToBytes(maybeHide(authorEmailRaw), 160)
			commRoleName := lib.TruncToBytes(maybeHide(commNameRaw), 160)
			commRoleEmail := lib.TruncToBytes(maybeHide(commEmailRaw), 160)

			authorID, authorLogin := lookupActorNameEmailCachedTx(ctx, tx, acache, maybeHide, authorNameRaw, authorEmailRaw)
			commID, commLogin := lookupActorNameEmailCachedTx(ctx, tx, acache, maybeHide, commNameRaw, commEmailRaw)
			if ctx.Debug > 0 && authorID == 0 {
				lib.Printf("Warning: could not find actor for author of %s sha %s (event %d): name=%q, email=%q\n", repo, sha, ev.EventID, authorNameRaw, authorEmailRaw)
			}
			if ctx.Debug > 0 && commID == 0 {
				lib.Printf("Warning: could not find actor for committer of %s sha %s (event %d): name=%q, email=%q\n", repo, sha, ev.EventID, commNameRaw, commEmailRaw)
			}

			dupActorLogin := lib.TruncToBytes(maybeHide(ev.ActorLogin), 120)

			dupAuthorLogin := ""
			if authorLogin != "" {
				dupAuthorLogin = lib.TruncToBytes(maybeHide(authorLogin), 120)
			}
			dupCommLogin := ""
			if commLogin != "" {
				dupCommLogin = lib.TruncToBytes(maybeHide(commLogin), 120)
			}

			// Insert commit.
			if _, err := insCommitStmt.Exec(
				sha,
				ev.EventID,
				authorName,
				authorEmail,
				msg,
				ev.ActorID,
				dupActorLogin,
				ev.RepoID,
				ev.RepoName,
				"PushEvent",
				ev.CreatedAt,
				authorID,
				commID,
				dupAuthorLogin,
				dupCommLogin,
				authorEmail,
				commRoleName,
				commRoleEmail,
			); err != nil {
				return fmt.Errorf("insert gha_commits (repo=%s, event=%d, sha=%s): error: %w", repo, ev.EventID, sha, err)
			}
			nCommits++

			// Insert roles: Author + Committer + trailers.
			if InsertAuthorRole {
				if err := insertRoles(insRoleStmt, sha, ev, "Author", authorID, authorLogin, authorRoleName, authorRoleEmail, maybeHide); err != nil {
					return fmt.Errorf("insert Author role (repo=%s, event=%d, sha=%s): error: %w", repo, ev.EventID, sha, err)
				}
				nRoles++
			}
			if InsertCommitterRole {
				if err := insertRoles(insRoleStmt, sha, ev, "Committer", commID, commLogin, commRoleName, commRoleEmail, maybeHide); err != nil {
					return fmt.Errorf("insert Committer role (repo=%s, event=%d, sha=%s): error: %w", repo, ev.EventID, sha, err)
				}
				nRoles++
			}

			trailerRoles := parseTrailers(ctx, ci.Message)
			for _, tr := range trailerRoles {
				name := lib.TruncToBytes(maybeHide(tr.Name), 160)
				email := lib.TruncToBytes(maybeHide(tr.Email), 160)

				tID, tLogin := lookupActorNameEmailCachedTx(ctx, tx, acache, maybeHide, tr.Name, tr.Email)
				if ctx.Debug > 0 && tID == 0 {
					lib.Printf("Warning: could not find actor for trailer role of %s sha %s (event %d): name=%q, email=%q\n", repo, sha, ev.EventID, tr.Name, tr.Email)
				}
				if err := insertRoles(insRoleStmt, sha, ev, tr.Role, tID, tLogin, name, email, maybeHide); err != nil {
					return fmt.Errorf("insert trailer role (repo=%s, event=%d, sha=%s, role=%s): error: %w", repo, ev.EventID, sha, tr.Role, err)
				}
				nRoles++
			}
		}
	}

	if err := tx.Commit(); err != nil {
		lib.Printf("Error committing transaction for %s: %v\n", repo, err)
		return err
	}
	lib.Printf("%s: successfully backfilled %d commits and %d commit roles for %d events\n", repo, nCommits, nRoles, len(events))
	return nil
}

func insertRoles(stmt *sql.Stmt, sha string, ev pushEvent, role string, actorID int64, actorLogin, actorName, actorEmail string, maybeHide func(string) string) error {
	// gha_commits_roles columns are NOT NULL (defaults: actor_id=0, actor_login/name/email='').
	if actorID < 0 {
		actorID = 0
	}
	if actorLogin != "" {
		actorLogin = lib.TruncToBytes(maybeHide(actorLogin), 120)
	} else {
		actorLogin = ""
	}
	if actorName != "" {
		actorName = lib.TruncToBytes(actorName, 160)
	} else {
		actorName = ""
	}
	if actorEmail != "" {
		actorEmail = lib.TruncToBytes(actorEmail, 160)
	} else {
		actorEmail = ""
	}
	if role != "" {
		role = lib.TruncToBytes(role, 60)
	}

	_, err := stmt.Exec(
		sha,
		ev.EventID,
		role,
		actorID,
		actorLogin,
		actorName,
		actorEmail,
		ev.RepoID,
		ev.RepoName,
		ev.CreatedAt,
	)
	return err
}

// parseGitCommitsOutput parses git/git_commits.sh output.
// Record separator: ';'
// Fields: sha,b64(author_name),b64(author_email),b64(committer_name),b64(committer_email),b64(message)
func parseGitCommitsOutput(out string, outMap map[string]commitInfo) error {
	s := strings.TrimSpace(out)
	if s == "" {
		return nil
	}

	records := strings.Split(s, ";")
	for _, rec := range records {
		rec = strings.TrimSpace(rec)
		if rec == "" {
			continue
		}
		parts := strings.Split(rec, ",")
		if len(parts) != 6 {
			return fmt.Errorf("invalid git_commits.sh record (expected 6 fields): %q", rec)
		}
		sha := strings.TrimSpace(parts[0])
		if sha == "" {
			return fmt.Errorf("empty sha in git_commits.sh record: %q", rec)
		}

		anB, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			return fmt.Errorf("base64 decode author_name for %s: %w", sha, err)
		}
		aeB, err := base64.StdEncoding.DecodeString(parts[2])
		if err != nil {
			return fmt.Errorf("base64 decode author_email for %s: %w", sha, err)
		}
		cnB, err := base64.StdEncoding.DecodeString(parts[3])
		if err != nil {
			return fmt.Errorf("base64 decode committer_name for %s: %w", sha, err)
		}
		ceB, err := base64.StdEncoding.DecodeString(parts[4])
		if err != nil {
			return fmt.Errorf("base64 decode committer_email for %s: %w", sha, err)
		}
		msgB, err := base64.StdEncoding.DecodeString(parts[5])
		if err != nil {
			return fmt.Errorf("base64 decode message for %s: %w", sha, err)
		}

		// PostgreSQL text cannot contain NUL bytes; strip defensively.
		an := strings.ReplaceAll(string(anB), "\x00", "")
		ae := strings.ReplaceAll(string(aeB), "\x00", "")
		cn := strings.ReplaceAll(string(cnB), "\x00", "")
		ce := strings.ReplaceAll(string(ceB), "\x00", "")
		msg := strings.ReplaceAll(string(msgB), "\x00", "")

		outMap[sha] = commitInfo{
			Sha:            sha,
			AuthorName:     an,
			AuthorEmail:    ae,
			CommitterName:  cn,
			CommitterEmail: ce,
			Message:        msg,
		}
	}
	return nil
}

// gitCommitInfoBatch runs git/git_commits.sh for a batch. If it fails, it bisects to salvage partial results.
func gitCommitInfoBatch(ctx *lib.Ctx, repoPath string, shas []string) (map[string]commitInfo, error) {
	outMap := make(map[string]commitInfo, len(shas))
	if len(shas) == 0 {
		if ctx.Debug > 0 {
			lib.Printf("Warning: empty SHA batch for repo %s\n", repoPath)
		}
		return outMap, nil
	}

	cmdPrefix := ""
	if ctx.LocalCmd {
		cmdPrefix = lib.LocalGitScripts
	}
	args := append([]string{cmdPrefix + "git_commits.sh", repoPath}, shas...)
	out, err := lib.ExecCommand(ctx, args, nil)
	if err == nil {
		perr := parseGitCommitsOutput(out, outMap)
		if perr != nil {
			lib.Printf("Parsed git_commits.sh output for repo %s, batch size %d: %d records, parse error: %v\n", repoPath, len(shas), len(outMap), perr)
		} else {
			if ctx.Debug > 1 {
				lib.Printf("Parsed git_commits.sh output for repo %s, batch size %d: %d records: %+v\n", repoPath, len(shas), len(outMap), outMap)
			}
		}
		return outMap, perr
	}

	if ctx.Debug > 0 {
		lib.Printf("Error running git_commits.sh for repo %s, batch size %d: %v\n", repoPath, len(shas), err)
	}
	// If a batch fails, split to isolate bad SHAs but keep partial output.
	if len(shas) == 1 {
		return outMap, err
	}
	mid := len(shas) / 2
	left, errL := gitCommitInfoBatch(ctx, repoPath, shas[:mid])
	right, errR := gitCommitInfoBatch(ctx, repoPath, shas[mid:])
	for k, v := range right {
		left[k] = v
	}

	if errL != nil && errR != nil {
		return left, fmt.Errorf("git_commits.sh error for both halves: (%v) and (%v)", errL, errR)
	}
	if errL != nil {
		return left, errL
	}
	if errR != nil {
		return left, errR
	}
	return left, nil
}

// gitRangeCommits lists commits between BEFORE..HEAD using git/git_commits_range.sh paging.
//
// Script output is newest->oldest for stable paging with --skip/--max-count.
// This function reverses the final slice to return oldest->newest.
func gitRangeCommits(ctx *lib.Ctx, repoPath, before, head string, pageSize int, maxNeeded int) ([]string, error) {
	before = strings.TrimSpace(before)
	head = strings.TrimSpace(head)

	if head == "" || isZeroSHA(head) {
		return []string{}, nil
	}

	zeros := strings.Repeat("0", 40)
	if before == "" {
		before = zeros
	}

	limit := pageSize
	if limit <= 0 {
		limit = 1000
	}

	cmdPrefix := ""
	if ctx.LocalCmd {
		cmdPrefix = lib.LocalGitScripts
	}

	all := make([]string, 0, limit)
	skip := 0
	for {
		args := []string{
			cmdPrefix + "git_commits_range.sh",
			repoPath,
			before,
			head,
			fmt.Sprintf("%d", skip),
			fmt.Sprintf("%d", limit),
		}
		out, err := lib.ExecCommand(ctx, args, nil)
		if err != nil {
			return all, err
		}

		var chunk []string
		for _, line := range strings.Split(out, "\n") {
			sha := strings.TrimSpace(line)
			if sha == "" {
				continue
			}
			chunk = append(chunk, sha)
			// For BEFORE=0 case we may only need the newest maxNeeded commits.
			if maxNeeded > 0 && len(all)+len(chunk) >= maxNeeded {
				break
			}
		}
		if len(chunk) == 0 {
			break
		}

		// If maxNeeded is set and we overshot by parsing a bigger page, trim.
		if maxNeeded > 0 && len(all)+len(chunk) > maxNeeded {
			chunk = chunk[:maxNeeded-len(all)]
		}

		all = append(all, chunk...)

		if maxNeeded > 0 && len(all) >= maxNeeded {
			break
		}
		if len(chunk) < limit {
			break
		}
		skip += limit
	}

	// Reverse newest->oldest to oldest->newest.
	reverseStringsInPlace(all)
	return all, nil
}

func selectPushEventsNeedingCommits(ctx *lib.Ctx, con *sql.DB, repo string, dtFrom time.Time) ([]pushEvent, error) {
	// mode=1: missing only; mode>=2: missing + truncated (cnt < payload.size).
	q := `
select
  e.id,
  e.actor_id,
  e.dup_actor_login,
  e.repo_id,
  e.dup_repo_name,
  e.created_at,
  p.head,
  p.befor,
  p.size,
  coalesce(c.cnt,0) as cnt
from gha_events e
join gha_payloads p on p.event_id = e.id
left join (
  select event_id, count(*) as cnt
  from gha_commits
	where dup_repo_name = $1
  and dup_created_at >= $2
  group by event_id
) c on c.event_id = e.id
where e.type = 'PushEvent'
  and e.dup_repo_name = $1
  and e.created_at >= $2
  and (
    p.size is null
    or p.size > 0
    or (
      p.size = 0
      and p.befor is not null
      and p.befor <> ''
      and p.befor <> '0000000000000000000000000000000000000000'
    )
  )
  and (
    c.cnt is null
    or c.cnt = 0
    or (
      $3 >= 2
      and p.size is not null
      and c.cnt < p.size
    )
  )
order by e.created_at, e.id
`
	rows, err := con.Query(q, repo, dtFrom, ctx.FetchCommitsMode)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []pushEvent
	for rows.Next() {
		var ev pushEvent
		var head, bef sql.NullString
		if err := rows.Scan(
			&ev.EventID,
			&ev.ActorID,
			&ev.ActorLogin,
			&ev.RepoID,
			&ev.RepoName,
			&ev.CreatedAt,
			&head,
			&bef,
			&ev.Size,
			&ev.Cnt,
		); err != nil {
			return nil, err
		}
		if head.Valid {
			ev.Head = head.String
		}
		if bef.Valid {
			ev.Before = bef.String
		}
		out = append(out, ev)
	}
	return out, nil
}

// lookupActorNameEmailCachedTx attempts to map (name,email) to (actor_id, actor_login) using the same tables
// used by gha2db:
// - gha_actors_emails (email -> actor)
// - gha_actors_names  (name  -> actor)
// - gha_actors        (name  -> actor)
//
// Cache key uses (lower(email), lower(name)).
func lookupActorNameEmailCachedTx(ctx *lib.Ctx, tx *sql.Tx, cache *actorCache, maybeHide func(string) string, name, email string) (int64, string) {
	key := [2]string{
		strings.ToLower(strings.TrimSpace(email)),
		strings.ToLower(strings.TrimSpace(name)),
	}

	cache.mu.RLock()
	if v, ok := cache.m[key]; ok {
		cache.mu.RUnlock()
		return v.id, v.login
	}
	cache.mu.RUnlock()

	aName := strings.TrimSpace(maybeHide(name))
	aEmail := strings.TrimSpace(maybeHide(email))

	id := int64(0)
	login := ""

	if aEmail != "" {
		err := tx.QueryRow(
			`select a.id, a.login from gha_actors a, gha_actors_emails ae where a.id = ae.actor_id and lower(ae.email) = lower($1) order by a.id desc limit 1`,
			aEmail,
		).Scan(&id, &login)
		if err != nil && err != sql.ErrNoRows {
			lib.Printf("Warning: lookup actor by email failed (email=%q): error: %v\n", aEmail, err)
		}
	}

	// if id == 0 && aName != "" && len(aName) > 2 {
	if id == 0 && aName != "" {
		err := tx.QueryRow(
			`select a.id, a.login from gha_actors a, gha_actors_names an where a.id = an.actor_id and lower(an.name) = lower($1) order by a.id desc limit 1`,
			aName,
		).Scan(&id, &login)
		if err != nil && err != sql.ErrNoRows {
			lib.Printf("Warning: lookup actor by gha_actors_names failed (name=%q): error: %v\n", aName, err)
		}
	}

	// if id == 0 && aName != "" && len(aName) > 3 {
	if id == 0 && aName != "" {
		err := tx.QueryRow(
			`select id, login from gha_actors where lower(name) = lower($1) order by id desc limit 1`,
			aName,
		).Scan(&id, &login)
		if err != nil && err != sql.ErrNoRows {
			lib.Printf("Warning: lookup actor by gha_actors.name failed (name=%q): error: %v\n", aName, err)
		}
	}

	// if id == 0 && aName != "" && len(aName) > 3 {
	if id == 0 && aName != "" {
		err := tx.QueryRow(
			`select id, login from gha_actors where lower(login) = lower($1) order by id desc limit 1`,
			aName,
		).Scan(&id, &login)
		if err != nil && err != sql.ErrNoRows {
			lib.Printf("Warning: lookup actor by gha_actors.login failed (name=%q): error: %v\n", aName, err)
		}
	}

	cache.mu.Lock()
	cache.m[key] = actorCacheEntry{id: id, login: login}
	cache.mu.Unlock()

	if ctx.Debug > 0 {
		lib.Printf("lookupActorNameEmailCachedTx: name=%q, email=%q -> id=%d, login=%q\n", name, email, id, login)
	}
	return id, login
}

type trailer struct {
	Role  string
	Name  string
	Email string
}

func matchGroups(re *regexp.Regexp, s string) map[string]string {
	matches := re.FindStringSubmatch(s)
	if matches == nil {
		return nil
	}
	out := make(map[string]string)
	for i, name := range re.SubexpNames() {
		if i != 0 && name != "" {
			out[name] = matches[i]
		}
	}
	return out
}

// parseTrailers extracts commit roles from message trailers.
// The set of recognized trailers and their canonical role names is shared with gha2db
// via lib.GitTrailerPattern and lib.GitAllowedTrailers.
func parseTrailers(ctx *lib.Ctx, msg string) []trailer {
	var out []trailer
	lines := strings.Split(msg, "\n")
	for _, l := range lines {
		l = strings.TrimSpace(strings.TrimRight(l, "\r"))
		if l == "" {
			continue
		}

		m := matchGroups(lib.GitTrailerPattern, l)
		if len(m) == 0 {
			continue
		}

		key := strings.ToLower(strings.TrimSpace(m["name"]))
		value := strings.TrimSpace(m["value"])
		roles, ok := lib.GitAllowedTrailers[key]
		if !ok || len(roles) == 0 {
			continue
		}

		// Expected: Name <email>
		nameEmail := strings.Split(value, "<")
		if len(nameEmail) < 2 {
			continue
		}
		name := strings.TrimSpace(nameEmail[0])
		emailEnd := strings.Split(nameEmail[1], ">")
		if len(emailEnd) < 2 {
			continue
		}
		email := strings.TrimSpace(emailEnd[0])
		if name == "" || email == "" {
			continue
		}

		for _, role := range roles {
			if role == "" {
				continue
			}
			out = append(out, trailer{Role: role, Name: name, Email: email})
		}
	}
	if ctx.Debug > 1 {
		lib.Printf("parse trailers: '%s' -> %+v\n", msg, out)
	}
	return out
}
