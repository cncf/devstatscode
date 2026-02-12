package main

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	lib "github.com/cncf/devstatscode"
)

// git_commits.sh output gives only author name/email and message (no committer)
type commitInfo struct {
	Sha         string
	AuthorName  string
	AuthorEmail string
	Message     string
}

type pushEvent struct {
	EventID    int64
	ActorID    sql.NullInt64
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
	ID    sql.NullInt64
	Login string
}

type actorCache struct {
	mu sync.RWMutex
	m  map[string]actorCacheEntry
}

func newActorCache() *actorCache {
	return &actorCache{m: make(map[string]actorCacheEntry)}
}

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

// Reconstruct gha_commits from local git history for PushEvents (missing and optionally truncated).
func backfillPushEventCommits(ctx *lib.Ctx, dbs map[string]string, repoDBs map[string]map[string]struct{}) {
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

	thrN := ctx.NCPUs
	if ctx.ST {
		thrN = 1
	}

	for db := range dbs {
		lib.Printf(
			"Backfilling gha_commits from git history (DB %s, threads %d, mode %d, batch %d)\n",
			db, thrN, ctx.FetchCommitsMode, ctx.GitCommitsBatch,
		)
		con := lib.PgConnDB(ctx, db)

		// Actor cache shared across repos processed for this DB (thread-safe).
		acache := newActorCache()

		done := make(chan struct{})
		throttle := make(chan struct{}, thrN)

		repos := []string{}
		for repo, inDBs := range repoDBs {
			if _, ok := inDBs[db]; ok {
				repos = append(repos, repo)
			}
		}
		sort.Strings(repos)

		for _, repo := range repos {
			throttle <- struct{}{}
			go func(r string) {
				defer func() {
					<-throttle
					done <- struct{}{}
				}()
				err := backfillRepo(ctx, con, r, maybeHide, acache)
				if err != nil {
					lib.Printf("backfillRepo(%s): %v\n", r, err)
				}
			}(repo)
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
		// IMPORTANT: do not silently skip — you asked to track this.
		return fmt.Errorf("repo not cloned: %s", repoPath)
	} else if err != nil {
		return fmt.Errorf("cannot stat repo path %s: %w", repoPath, err)
	}

	// Mode 1: only backfill since last dup_created_at in gha_commits for this repo.
	dtFrom := ctx.DefaultStartDate
	if ctx.FetchCommitsMode == 1 {
		maxDupCreatedAt := sql.NullTime{}
		if err := con.QueryRow(
			"select max(dup_created_at) from gha_commits where dup_repo_name = $1",
			repo,
		).Scan(&maxDupCreatedAt); err != nil {
			return fmt.Errorf("select max(dup_created_at) failed for %s: %w", repo, err)
		}
		if maxDupCreatedAt.Valid {
			dtFrom = maxDupCreatedAt.Time.Format(time.RFC3339)
		}
	}

	// Earliest PushEvent created_at for this repo (used to decide if BEFORE=0 should expand to full history).
	minPushDT := sql.NullTime{}
	if err := con.QueryRow(
		"select min(created_at) from gha_events where type = 'PushEvent' and dup_repo_name = $1",
		repo,
	).Scan(&minPushDT); err != nil {
		lib.Printf("Warning: cannot query earliest PushEvent created_at for %s: %v\n", repo, err)
	}

	pushEvents, err := selectPushEventsNeedingCommits(ctx, con, repo, dtFrom)
	if err != nil {
		return err
	}
	if len(pushEvents) == 0 {
		return nil
	}

	batchSize := ctx.GitCommitsBatch
	if batchSize <= 0 {
		batchSize = 1000
	}

	eventShas := make(map[int64][]string, len(pushEvents))
	shaSet := make(map[string]struct{})

	for _, ev := range pushEvents {
		if isZeroSHA(ev.Head) {
			lib.Printf("Warning: skipping PushEvent %d in %s: head SHA is empty/zero\n", ev.EventID, repo)
			continue
		}

		// BEFORE=0 is ambiguous (new ref); only expand to full history if this looks like the initial push.
		fullHistoryOnZero := false
		if isZeroSHA(ev.Before) && minPushDT.Valid && ev.CreatedAt.Equal(minPushDT.Time) {
			fullHistoryOnZero = true
		}

		shas, rerr := gitRangeCommits(ctx, repoPath, ev.Before, ev.Head, fullHistoryOnZero, batchSize)
		if rerr != nil {
			lib.Printf(
				"Error listing commits range for %s (event %d, before %s, head %s): %v\n",
				repo, ev.EventID, ev.Before, ev.Head, rerr,
			)
			// Fallback: at least head commit.
			shas = []string{ev.Head}
		}

		eventShas[ev.EventID] = shas
		for _, sha := range shas {
			sha = strings.TrimSpace(sha)
			if sha == "" || isZeroSHA(sha) {
				continue
			}
			shaSet[sha] = struct{}{}
		}
	}

	if len(shaSet) == 0 {
		lib.Printf("No commits to backfill for %s (push events %d)\n", repo, len(pushEvents))
		return nil
	}

	shaList := make([]string, 0, len(shaSet))
	for sha := range shaSet {
		shaList = append(shaList, sha)
	}
	sort.Strings(shaList)

	commitInfos := make(map[string]commitInfo, len(shaSet))
	for i := 0; i < len(shaList); i += batchSize {
		j := i + batchSize
		if j > len(shaList) {
			j = len(shaList)
		}
		infos, ierr := gitCommitInfoBatch(ctx, repoPath, shaList[i:j])
		for sha, info := range infos {
			commitInfos[sha] = info
		}
		if ierr != nil {
			lib.Printf("Warning: git_commits.sh error for %s batch %d-%d/%d: %v\n", repo, i, j, len(shaList), ierr)
		}
	}

	tx, err := con.Begin()
	if err != nil {
		return err
	}

	updPayloadSizeStmt, err := tx.Prepare("update gha_payloads set size = $2 where event_id = $1 and size is null")
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer updPayloadSizeStmt.Close()

	insCommitStmt, err := tx.Prepare(`
insert into gha_commits(sha, event_id, author_name, encrypted_email, message, is_distinct, dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, author_id, committer_id, dup_author_login, dup_committer_login)
values (
	$1, $2, $3, $4, $5,
	not exists(select 1 from gha_commits c2 where c2.sha = $1 limit 1),
	$6, $7, $8, $9, $10, $11,
	$12, $13, $14, $15
)
on conflict do nothing`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer insCommitStmt.Close()

	insRoleStmt, err := tx.Prepare(`
insert into gha_commits_roles(sha, event_id, role, actor_id, actor_login, actor_name, actor_email, dup_repo_id, dup_repo_name, dup_created_at)
values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
on conflict do nothing`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer insRoleStmt.Close()

	for _, ev := range pushEvents {
		shas, ok := eventShas[ev.EventID]
		if !ok {
			continue
		}

		if _, err := updPayloadSizeStmt.Exec(ev.EventID, len(shas)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("update payload size (repo %s, event %d): %w", repo, ev.EventID, err)
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
				continue
			}

			ci, ok := commitInfos[sha]
			if !ok {
				lib.Printf("Warning: missing git metadata for %s sha %s (event %d)\n", repo, sha, ev.EventID)
				continue
			}

			// Commit table fields.
			authorName := lib.TruncToBytes(maybeHide(ci.AuthorName), 120)
			authorEmail := lib.TruncToBytes(maybeHide(ci.AuthorEmail), 160)
			msg := lib.TruncToBytes(ci.Message, 0xffff)

			// Roles table fields.
			authorRoleName := lib.TruncToBytes(maybeHide(ci.AuthorName), 160)
			authorRoleEmail := lib.TruncToBytes(maybeHide(ci.AuthorEmail), 160)

			authorID, authorLogin := lookupActorCached(tx, acache, authorRoleName, authorRoleEmail)
			if authorLogin != "" {
				authorLogin = maybeHide(authorLogin)
				authorLogin = lib.TruncToBytes(authorLogin, 120)
			}

			// Committer is not available from PushEvent payload; keep NULL.
			commID := sql.NullInt64{}
			commLogin := sql.NullString{} // NULL

			_, err := insCommitStmt.Exec(
				sha, ev.EventID,
				authorName, authorEmail, msg,
				ev.ActorID,
				lib.TruncToBytes(maybeHide(ev.ActorLogin), 120),
				ev.RepoID, ev.RepoName,
				"PushEvent", ev.CreatedAt,
				authorID,
				commID,
				authorLogin,
				commLogin,
			)
			if err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("insert gha_commits (repo %s, event %d, sha %s): %w", repo, ev.EventID, sha, err)
			}

			// Roles: Author + trailers.
			if err := insertRoles(insRoleStmt, sha, ev, "Author", authorID, authorLogin, authorRoleName, authorRoleEmail); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("insert Author role (repo %s, event %d, sha %s): %w", repo, ev.EventID, sha, err)
			}

			trailerRoles := parseTrailers(ci.Message)
			for _, tr := range trailerRoles {
				name := lib.TruncToBytes(maybeHide(tr.Name), 160)
				email := lib.TruncToBytes(maybeHide(tr.Email), 160)
				if err := insertRoles(insRoleStmt, sha, ev, tr.Role, sql.NullInt64{}, "", name, email); err != nil {
					_ = tx.Rollback()
					return fmt.Errorf("insert trailer role (repo %s, event %d, sha %s): %w", repo, ev.EventID, sha, err)
				}
			}
		}
	}

	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		return err
	}
	return nil
}

func insertRoles(stmt *sql.Stmt, sha string, ev pushEvent, role string, actorID sql.NullInt64, actorLogin, actorName, actorEmail string) error {
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

// Parse git_commits.sh output:
// record separator: ';'
// fields: sha,b64(author_name),b64(author_email),b64(message)
func parseGitCommitsOutput(out []byte, outMap map[string]commitInfo) error {
	s := strings.TrimSpace(string(out))
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
		if len(parts) != 4 {
			return fmt.Errorf("invalid git_commits.sh record (expected 4 fields): %q", rec)
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
		msgB, err := base64.StdEncoding.DecodeString(parts[3])
		if err != nil {
			return fmt.Errorf("base64 decode message for %s: %w", sha, err)
		}

		// PostgreSQL text cannot contain NUL bytes; strip defensively.
		an := strings.ReplaceAll(string(anB), "\x00", "")
		ae := strings.ReplaceAll(string(aeB), "\x00", "")
		msg := strings.ReplaceAll(string(msgB), "\x00", "")

		outMap[sha] = commitInfo{
			Sha:         sha,
			AuthorName:  an,
			AuthorEmail: ae,
			Message:     msg,
		}
	}
	return nil
}

// Run git_commits.sh for a batch; if it fails, bisect to salvage partial results.
func gitCommitInfoBatch(ctx *lib.Ctx, repoPath string, shas []string) (map[string]commitInfo, error) {
	outMap := make(map[string]commitInfo, len(shas))
	if len(shas) == 0 {
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
		return outMap, perr
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
		return left, fmt.Errorf("git_commits.sh failed for both halves: (%v) and (%v)", errL, errR)
	}
	if errL != nil {
		return left, errL
	}
	if errR != nil {
		return left, errR
	}
	return left, nil
}

// List commits between BEFORE..HEAD using git_commits_range.sh paging.
// BEFORE=0 handling:
// - if fullHistoryOnZero=false: return [HEAD] only (safe for branch creation pointing at old history)
// - if fullHistoryOnZero=true: list full history reachable from HEAD (likely initial push)
func gitRangeCommits(ctx *lib.Ctx, repoPath, before, head string, fullHistoryOnZero bool, pageSize int) ([]string, error) {
	before = strings.TrimSpace(before)
	head = strings.TrimSpace(head)

	if head == "" || isZeroSHA(head) {
		return []string{}, nil
	}

	if before == "" || isZeroSHA(before) {
		if !fullHistoryOnZero {
			return []string{head}, nil
		}
		if before == "" {
			before = "0000000000000000000000000000000000000000"
		}
	}

	limit := pageSize
	if limit <= 0 {
		limit = 1000
	}

	cmdPrefix := ""
	if ctx.LocalCmd {
		cmdPrefix = lib.LocalGitScripts
	}

	all := []string{}
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
		chunk := []string{}
		for _, line := range strings.Split(string(out), "\n") {
			sha := strings.TrimSpace(line)
			if sha == "" {
				continue
			}
			chunk = append(chunk, sha)
		}
		if len(chunk) == 0 {
			break
		}
		all = append(all, chunk...)
		if len(chunk) < limit {
			break
		}
		skip += limit
	}
	return all, nil
}

func selectPushEventsNeedingCommits(ctx *lib.Ctx, con *sql.DB, repoName string, dtFrom string) ([]pushEvent, error) {
	q := `
select
	e.id as event_id,
	e.actor_id,
	e.dup_actor_login,
	e.repo_id,
	e.dup_repo_name,
	e.created_at,
	p.head,
	p.befor,
	p.size,
	coalesce(c.cnt, 0) as cnt
from gha_events e
join gha_payloads p on p.event_id = e.id
left join (
	select event_id, count(*) as cnt
	from gha_commits
	group by event_id
) c on c.event_id = e.id
where e.type = 'PushEvent'
	and e.dup_repo_name = $1
	and e.created_at >= $2
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
	rows, err := con.Query(q, repoName, dtFrom, ctx.FetchCommitsMode)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []pushEvent
	for rows.Next() {
		ev := pushEvent{}
		err := rows.Scan(
			&ev.EventID,
			&ev.ActorID,
			&ev.ActorLogin,
			&ev.RepoID,
			&ev.RepoName,
			&ev.CreatedAt,
			&ev.Head,
			&ev.Before,
			&ev.Size,
			&ev.Cnt,
		)
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, nil
}

// Thread-safe cache wrapper.
func lookupActorCached(tx *sql.Tx, cache *actorCache, name, email string) (sql.NullInt64, string) {
	k := strings.ToLower(email) + "♂♀" + strings.ToLower(name)

	cache.mu.RLock()
	if v, ok := cache.m[k]; ok {
		cache.mu.RUnlock()
		return v.ID, v.Login
	}
	cache.mu.RUnlock()

	id := sql.NullInt64{}
	login := sql.NullString{}

	if email != "" {
		err := tx.QueryRow(
			"select id, login from gha_actors where lower(email) = lower($1) order by id limit 1",
			email,
		).Scan(&id, &login)
		if err != nil && err != sql.ErrNoRows {
			lib.Printf("Warning: lookup actor by email failed (email=%q): %v\n", email, err)
		}
	}
	if !id.Valid && name != "" {
		err := tx.QueryRow(
			"select id, login from gha_actors where lower(name) = lower($1) order by id limit 1",
			name,
		).Scan(&id, &login)
		if err != nil && err != sql.ErrNoRows {
			lib.Printf("Warning: lookup actor by name failed (name=%q): %v\n", name, err)
		}
	}

	outLogin := ""
	if login.Valid {
		outLogin = login.String
	}

	cache.mu.Lock()
	cache.m[k] = actorCacheEntry{ID: id, Login: outLogin}
	cache.mu.Unlock()

	return id, outLogin
}

type trailer struct {
	Role  string
	Name  string
	Email string
}

func parseTrailers(msg string) []trailer {
	var out []trailer
	lines := strings.Split(msg, "\n")
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		// Common DevStats roles
		out = appendTrailer(out, l, "Signed-off-by")
		out = appendTrailer(out, l, "Co-authored-by")
		out = appendTrailer(out, l, "Reviewed-by")
		out = appendTrailer(out, l, "Acked-by")
		out = appendTrailer(out, l, "Tested-by")
		out = appendTrailer(out, l, "Reported-by")
	}
	return out
}

func appendTrailer(out []trailer, line, role string) []trailer {
	prefix := role + ":"
	if !strings.HasPrefix(strings.ToLower(line), strings.ToLower(prefix)) {
		return out
	}
	rest := strings.TrimSpace(line[len(prefix):])
	// Format: Name <email>
	lt := strings.LastIndex(rest, "<")
	gt := strings.LastIndex(rest, ">")
	if lt < 0 || gt < 0 || gt <= lt {
		return out
	}
	name := strings.TrimSpace(rest[:lt])
	email := strings.TrimSpace(rest[lt+1 : gt])
	if name == "" {
		return out
	}
	out = append(out, trailer{Role: role, Name: name, Email: email})
	return out
}
