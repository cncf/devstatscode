package devstatscode

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// GHA event writer shared by gha2db (hourly GH Archive files) and ghapi2db
// (the per-repository events feed of the GitHub API, same JSON shape).

var (
	// gUseCache - use gEmailName2LoginIDCache or not
	gUseCache = true
	// gCacheMtx - cache access mutex
	gCacheMtx = &sync.RWMutex{}
	// gEmailName2LoginIDCache - cache found actors (login, ID) pairs for (name, email) pairs
	gEmailName2LoginIDCache = make(map[[2]string][2]string)
)

// EmailNameCacheLen - number of (name, email) -> (login, ID) pairs cached by LookupActorNameEmail
func EmailNameCacheLen() int {
	gCacheMtx.RLock()
	defer gCacheMtx.RUnlock()
	return len(gEmailName2LoginIDCache)
}

// Inserts single GHA Actor
func ghaActor(con *sql.Tx, ctx *Ctx, actor *Actor, maybeHide func(string) string) {
	// gha_actors
	// {"id:Fixnum"=>48592, "login:String"=>48592, "display_login:String"=>48592,
	// "gravatar_id:String"=>48592, "url:String"=>48592, "avatar_url:String"=>48592}
	// {"id"=>8, "login"=>34, "display_login"=>34, "gravatar_id"=>0, "url"=>63, "avatar_url"=>49}
	InsertActorTx(con, ctx, actor.ID, maybeHide(actor.Login), "")
}

// Inserts single GHA Repo
func ghaRepo(db *sql.DB, ctx *Ctx, repo *Repo, orgID, orgLogin interface{}) {
	// gha_repos
	// {"id:Fixnum"=>48592, "name:String"=>48592, "url:String"=>48592}
	// {"id"=>8, "name"=>111, "url"=>140}
	ExecSQLWithErr(
		db,
		ctx,
		InsertIgnore("into gha_repos(id, name, org_id, org_login) "+NValues(4)),
		AnyArray{repo.ID, repo.Name, orgID, orgLogin}...,
	)
}

// Inserts single GHA Org
func ghaOrg(db *sql.DB, ctx *Ctx, org *Org) {
	// gha_orgs
	// {"id:Fixnum"=>18494, "login:String"=>18494, "gravatar_id:String"=>18494,
	// "url:String"=>18494, "avatar_url:String"=>18494}
	// {"id"=>8, "login"=>38, "gravatar_id"=>0, "url"=>66, "avatar_url"=>49}
	if org != nil {
		ExecSQLWithErr(
			db,
			ctx,
			InsertIgnore("into gha_orgs(id, login) "+NValues(2)),
			AnyArray{org.ID, org.Login}...,
		)
	}
}

// Inserts single GHA Milestone
func ghaMilestone(con *sql.Tx, ctx *Ctx, eid string, milestone *Milestone, ev *Event, maybeHide func(string) string) {
	// creator
	if milestone.Creator != nil {
		ghaActor(con, ctx, milestone.Creator, maybeHide)
	}

	// gha_milestones
	ExecSQLTxWithErr(
		con,
		ctx,
		"insert into gha_milestones("+
			"id, event_id, closed_at, closed_issues, created_at, creator_id, "+
			"description, due_on, number, open_issues, state, title, updated_at, "+
			"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
			"dupn_creator_login) "+NValues(20),
		AnyArray{
			milestone.ID,
			eid,
			TimeOrNil(milestone.ClosedAt),
			milestone.ClosedIssues,
			milestone.CreatedAt,
			ActorIDOrNil(milestone.Creator),
			TruncStringOrNil(milestone.Description, 0xffff),
			TimeOrNil(milestone.DueOn),
			milestone.Number,
			milestone.OpenIssues,
			milestone.State,
			TruncToBytes(milestone.Title, 200),
			milestone.UpdatedAt,
			ev.Actor.ID,
			maybeHide(ev.Actor.Login),
			ev.Repo.ID,
			ev.Repo.Name,
			ev.Type,
			ev.CreatedAt,
			ActorLoginOrNil(milestone.Creator, maybeHide),
		}...,
	)
}

// Inserts single GHA Forkee (old format < 2015)
func ghaForkeeOld(con *sql.Tx, ctx *Ctx, eid string, forkee *ForkeeOld, actor *Actor, repo *Repo, ev *EventOld, maybeHide func(string) string) {

	// Lookup author by GitHub login
	aid := lookupActorTx(con, ctx, forkee.Owner, maybeHide)

	// Owner
	owner := Actor{ID: aid, Login: forkee.Owner}
	ghaActor(con, ctx, &owner, maybeHide)

	// gha_forkees
	// Table details and analysis in `analysis/analysis.txt` and `analysis/forkee_*.json`
	ExecSQLTxWithErr(
		con,
		ctx,
		"insert into gha_forkees("+
			// "id, event_id, name, full_name, owner_id, description, fork, "+
			"id, event_id, name, full_name, owner_id, "+
			// "created_at, updated_at, pushed_at, homepage, size, language, organization, "+
			"updated_at, "+
			// "stargazers_count, has_issues, has_projects, has_downloads, "+
			"stargazers_count, "+
			// "has_wiki, has_pages, forks, default_branch, open_issues, watchers, public, "+
			"forks, open_issues, watchers, "+
			// "dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
			"dup_actor_id, dup_repo_id, dup_repo_name, dup_created_at"+
			// "dup_owner_login) "+NValues(32),
			") "+NValues(14),
		AnyArray{
			forkee.ID,
			eid,
			TruncToBytes(forkee.Name, 80),
			TruncToBytes(forkee.Name, 200), // ForkeeOld has no FullName
			owner.ID,
			// TruncStringOrNil(forkee.Description, 0xffff),
			// forkee.Fork,
			// forkee.CreatedAt,
			forkee.CreatedAt, // ForkeeOld has no UpdatedAt
			// TimeOrNil(forkee.PushedAt),
			// StringOrNil(forkee.Homepage),
			// forkee.Size,
			// StringOrNil(forkee.Language),
			// StringOrNil(forkee.Organization),
			forkee.Stargazers,
			// forkee.HasIssues,
			// nil,
			// forkee.HasDownloads,
			// forkee.HasWiki,
			// nil,
			forkee.Forks,
			// TruncToBytes(forkee.DefaultBranch, 200),
			forkee.OpenIssues,
			forkee.Watchers,
			// NegatedBoolOrNil(forkee.Private),
			actor.ID,
			// maybeHide(actor.Login),
			repo.ID,
			repo.Name,
			// ev.Type,
			ev.CreatedAt,
			// maybeHide(owner.Login),
		}...,
	)
}

// Inserts single GHA Forkee
func ghaForkee(con *sql.Tx, ctx *Ctx, eid string, forkee *Forkee, ev *Event, maybeHide func(string) string) {
	// owner
	ghaActor(con, ctx, &forkee.Owner, maybeHide)

	// gha_forkees
	// Table details and analysis in `analysis/analysis.txt` and `analysis/forkee_*.json`
	ExecSQLTxWithErr(
		con,
		ctx,
		"insert into gha_forkees("+
			// "id, event_id, name, full_name, owner_id, description, fork, "+
			"id, event_id, name, full_name, owner_id, "+
			// "created_at, updated_at, pushed_at, homepage, size, language, organization, "+
			"updated_at, "+
			// "stargazers_count, has_issues, has_projects, has_downloads, "+
			"stargazers_count, "+
			// "has_wiki, has_pages, forks, default_branch, open_issues, watchers, public, "+
			"forks, open_issues, watchers, "+
			// "dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
			"dup_actor_id, dup_repo_id, dup_repo_name, dup_created_at"+
			// "dup_owner_login) "+NValues(32),
			") "+NValues(14),
		AnyArray{
			forkee.ID,
			eid,
			TruncToBytes(forkee.Name, 80),
			TruncToBytes(forkee.FullName, 200),
			forkee.Owner.ID,
			// TruncStringOrNil(forkee.Description, 0xffff),
			// forkee.Fork,
			// forkee.CreatedAt,
			forkee.UpdatedAt,
			// TimeOrNil(forkee.PushedAt),
			// StringOrNil(forkee.Homepage),
			// forkee.Size,
			// nil,
			// nil,
			forkee.StargazersCount,
			// forkee.HasIssues,
			// BoolOrNil(forkee.HasProjects),
			// forkee.HasDownloads,
			// forkee.HasWiki,
			// BoolOrNil(forkee.HasPages),
			forkee.Forks,
			// TruncToBytes(forkee.DefaultBranch, 200),
			forkee.OpenIssues,
			forkee.Watchers,
			// BoolOrNil(forkee.Public),
			ev.Actor.ID,
			// maybeHide(ev.Actor.Login),
			ev.Repo.ID,
			ev.Repo.Name,
			// ev.Type,
			ev.CreatedAt,
			// maybeHide(forkee.Owner.Login),
		}...,
	)
}

// Inserts single GHA Branch
func ghaBranch(con *sql.Tx, ctx *Ctx, eid string, branch *Branch, ev *Event, skipIDs []int, maybeHide func(string) string) {
	// user
	if branch.User != nil {
		ghaActor(con, ctx, branch.User, maybeHide)
	}

	// repo
	if branch.Repo != nil {
		rid := branch.Repo.ID
		insert := true
		for _, skipID := range skipIDs {
			if rid == skipID {
				insert = false
				break
			}
		}
		if insert {
			ghaForkee(con, ctx, eid, branch.Repo, ev, maybeHide)
		}
	}

	// gha_branches
	ExecSQLTxWithErr(
		con,
		ctx,
		"insert into gha_branches("+
			// "sha, event_id, user_id, repo_id, label, ref, "+
			"sha, event_id, user_id, repo_id, "+
			// "dup_type, dup_created_at, dupn_user_login, dupn_forkee_name"+
			"dup_created_at"+
			// ") "+NValues(10),
			") "+NValues(5),
		AnyArray{
			branch.SHA,
			eid,
			ActorIDOrNil(branch.User),
			ForkeeIDOrNil(branch.Repo), // GitHub uses JSON "repo" but it conatins Forkee
			// TruncToBytes(branch.Label, 200),
			// TruncToBytes(branch.Ref, 200),
			// ev.Type,
			ev.CreatedAt,
			// ActorLoginOrNil(branch.User, maybeHide),
			// ForkeeNameOrNil(branch.Repo),
		}...,
	)
}

// Search for given label using name & color
// If not found, return hash as its ID
func lookupLabel(con *sql.Tx, ctx *Ctx, name string, color string) int {
	rows := QuerySQLTxWithErr(
		con,
		ctx,
		fmt.Sprintf(
			"select id from gha_labels where name=%s and color=%s",
			NValue(1),
			NValue(2),
		),
		name,
		color,
	)
	defer func() { FatalOnError(rows.Close()) }()
	lid := 0
	for rows.Next() {
		FatalOnError(rows.Scan(&lid))
	}
	FatalOnError(rows.Err())
	if lid == 0 {
		lid = HashStrings([]string{name, color})
	}
	return lid
}

// Search for given actor using his/her login
// If not found, return hash as its ID
func lookupActor(db *sql.DB, ctx *Ctx, login string, maybeHide func(string) string) int {
	hlogin := maybeHide(login)
	rows := QuerySQLWithErr(
		db,
		ctx,
		fmt.Sprintf("select id from gha_actors where login=%s order by id desc limit 1", NValue(1)),
		hlogin,
	)
	defer func() { FatalOnError(rows.Close()) }()
	aid := 0
	for rows.Next() {
		FatalOnError(rows.Scan(&aid))
	}
	FatalOnError(rows.Err())
	if aid == 0 {
		aid = HashStrings([]string{login})
	}
	return aid
}

// Search for given actor using his/her login
// If not found, return hash as its ID
func lookupActorTx(con *sql.Tx, ctx *Ctx, login string, maybeHide func(string) string) int {
	hlogin := maybeHide(login)
	rows := QuerySQLTxWithErr(
		con,
		ctx,
		fmt.Sprintf("select id from gha_actors where login=%s order by id desc limit 1", NValue(1)),
		hlogin,
	)
	defer func() { FatalOnError(rows.Close()) }()
	aid := 0
	for rows.Next() {
		FatalOnError(rows.Scan(&aid))
	}
	FatalOnError(rows.Err())
	if aid == 0 {
		aid = HashStrings([]string{login})
	}
	return aid
}

// LookupActorNameEmail - find actor (ID, login) by name and email (gha_actors_emails/gha_actors_names, cached)
func LookupActorNameEmail(con *sql.DB, ctx *Ctx, name, email string, maybeHide func(string) string) (int, string) {
	if gUseCache {
		gCacheMtx.RLock()
		data, ok := gEmailName2LoginIDCache[[2]string{email, name}]
		gCacheMtx.RUnlock()
		if ok {
			id, _ := strconv.Atoi(data[0])
			// fmt.Printf("cache success: (%s,%s) -> (%d,%s)\n", email, name, id, data[1])
			return id, data[1]
		}
	}
	// By email
	hemail := maybeHide(email)
	erows := QuerySQLWithErr(
		con,
		ctx,
		fmt.Sprintf("select a.id, a.login from gha_actors a, gha_actors_emails ae where a.id = ae.actor_id and ae.email=%s order by a.id desc limit 1", NValue(1)),
		hemail,
	)
	defer func() { FatalOnError(erows.Close()) }()
	eaid := 0
	elogin := ""
	for erows.Next() {
		FatalOnError(erows.Scan(&eaid, &elogin))
	}
	FatalOnError(erows.Err())
	if eaid != 0 {
		if gUseCache {
			gCacheMtx.Lock()
			gEmailName2LoginIDCache[[2]string{email, name}] = [2]string{strconv.Itoa(eaid), elogin}
			gCacheMtx.Unlock()
		}
		return eaid, elogin
	}

	// By name from actors names table
	hname := maybeHide(name)
	nrows := QuerySQLWithErr(
		con,
		ctx,
		fmt.Sprintf("select a.id, a.login from gha_actors a, gha_actors_names an where a.id = an.actor_id and an.name=%s order by a.id desc limit 1", NValue(1)),
		hname,
	)
	defer func() { FatalOnError(nrows.Close()) }()
	naid := 0
	nlogin := ""
	for nrows.Next() {
		FatalOnError(nrows.Scan(&naid, &nlogin))
	}
	FatalOnError(nrows.Err())
	if naid != 0 {
		if gUseCache {
			gCacheMtx.Lock()
			gEmailName2LoginIDCache[[2]string{email, name}] = [2]string{strconv.Itoa(naid), nlogin}
			gCacheMtx.Unlock()
		}
		return naid, nlogin
	}

	// By name from actors table
	n2rows := QuerySQLWithErr(
		con,
		ctx,
		fmt.Sprintf("select id, login from gha_actors where name=%s order by id desc limit 1", NValue(1)),
		hname,
	)
	defer func() { FatalOnError(n2rows.Close()) }()
	n2aid := 0
	n2login := ""
	for n2rows.Next() {
		FatalOnError(n2rows.Scan(&n2aid, &n2login))
	}
	FatalOnError(n2rows.Err())
	if n2aid != 0 {
		if gUseCache {
			gCacheMtx.Lock()
			gEmailName2LoginIDCache[[2]string{email, name}] = [2]string{strconv.Itoa(n2aid), n2login}
			gCacheMtx.Unlock()
		}
		return n2aid, n2login
	}

	// By login from actors table
	lrows := QuerySQLWithErr(
		con,
		ctx,
		fmt.Sprintf("select id, login from gha_actors where login=%s order by id desc limit 1", NValue(1)),
		hname,
	)
	defer func() { FatalOnError(lrows.Close()) }()
	laid := 0
	llogin := ""
	for lrows.Next() {
		FatalOnError(lrows.Scan(&laid, &llogin))
	}
	FatalOnError(lrows.Err())
	if laid != 0 {
		if gUseCache {
			gCacheMtx.Lock()
			gEmailName2LoginIDCache[[2]string{email, name}] = [2]string{strconv.Itoa(laid), llogin}
			gCacheMtx.Unlock()
		}
		return laid, llogin
	}
	return 0, ""
}

// Search for given actor using his/her name and email
// If not found, return hash as its ID
// Uses TX object not DB
func lookupActorNameEmailTx(con *sql.Tx, ctx *Ctx, name, email string, maybeHide func(string) string) (int, string) {
	if gUseCache {
		gCacheMtx.RLock()
		data, ok := gEmailName2LoginIDCache[[2]string{email, name}]
		gCacheMtx.RUnlock()
		if ok {
			id, _ := strconv.Atoi(data[0])
			// fmt.Printf("cache success: (%s,%s) -> (%d,%s)\n", email, name, id, data[1])
			return id, data[1]
		}
	}
	// By email
	hemail := maybeHide(email)
	erows := QuerySQLTxWithErr(
		con,
		ctx,
		fmt.Sprintf("select a.id, a.login from gha_actors a, gha_actors_emails ae where a.id = ae.actor_id and ae.email=%s order by a.id desc limit 1", NValue(1)),
		hemail,
	)
	defer func() { FatalOnError(erows.Close()) }()
	eaid := 0
	elogin := ""
	for erows.Next() {
		FatalOnError(erows.Scan(&eaid, &elogin))
	}
	FatalOnError(erows.Err())
	if eaid != 0 {
		if gUseCache {
			gCacheMtx.Lock()
			gEmailName2LoginIDCache[[2]string{email, name}] = [2]string{strconv.Itoa(eaid), elogin}
			gCacheMtx.Unlock()
		}
		return eaid, elogin
	}

	// By name from actors names table
	hname := maybeHide(name)
	nrows := QuerySQLTxWithErr(
		con,
		ctx,
		fmt.Sprintf("select a.id, a.login from gha_actors a, gha_actors_names an where a.id = an.actor_id and an.name=%s order by a.id desc limit 1", NValue(1)),
		hname,
	)
	defer func() { FatalOnError(nrows.Close()) }()
	naid := 0
	nlogin := ""
	for nrows.Next() {
		FatalOnError(nrows.Scan(&naid, &nlogin))
	}
	FatalOnError(nrows.Err())
	if naid != 0 {
		if gUseCache {
			gCacheMtx.Lock()
			gEmailName2LoginIDCache[[2]string{email, name}] = [2]string{strconv.Itoa(naid), nlogin}
			gCacheMtx.Unlock()
		}
		return naid, nlogin
	}

	// By name from actors table
	n2rows := QuerySQLTxWithErr(
		con,
		ctx,
		fmt.Sprintf("select id, login from gha_actors where name=%s order by id desc limit 1", NValue(1)),
		hname,
	)
	defer func() { FatalOnError(n2rows.Close()) }()
	n2aid := 0
	n2login := ""
	for n2rows.Next() {
		FatalOnError(n2rows.Scan(&n2aid, &n2login))
	}
	FatalOnError(n2rows.Err())
	if n2aid != 0 {
		if gUseCache {
			gCacheMtx.Lock()
			gEmailName2LoginIDCache[[2]string{email, name}] = [2]string{strconv.Itoa(n2aid), n2login}
			gCacheMtx.Unlock()
		}
		return n2aid, n2login
	}

	// By login from actors table
	lrows := QuerySQLTxWithErr(
		con,
		ctx,
		fmt.Sprintf("select id, login from gha_actors where login=%s order by id desc limit 1", NValue(1)),
		hname,
	)
	defer func() { FatalOnError(lrows.Close()) }()
	laid := 0
	llogin := ""
	for lrows.Next() {
		FatalOnError(lrows.Scan(&laid, &llogin))
	}
	FatalOnError(lrows.Err())
	if laid != 0 {
		if gUseCache {
			gCacheMtx.Lock()
			gEmailName2LoginIDCache[[2]string{email, name}] = [2]string{strconv.Itoa(laid), llogin}
			gCacheMtx.Unlock()
		}
		return laid, llogin
	}
	return 0, ""
}

// Try to find Repo by name and Organization
func findRepoFromNameAndOrg(db *sql.DB, ctx *Ctx, repoName string, orgID *int) (int, bool) {
	var rows *sql.Rows
	if orgID != nil {
		rows = QuerySQLWithErr(
			db,
			ctx,
			fmt.Sprintf(
				"select id from gha_repos where name=%s and org_id=%s",
				NValue(1),
				NValue(2),
			),
			repoName,
			orgID,
		)
	} else {
		rows = QuerySQLWithErr(
			db,
			ctx,
			fmt.Sprintf(
				"select id from gha_repos where name=%s and org_id is null",
				NValue(1),
			),
			repoName,
		)
	}
	defer func() { FatalOnError(rows.Close()) }()
	exists := false
	rid := 0
	for rows.Next() {
		FatalOnError(rows.Scan(&rid))
		exists = true
	}
	FatalOnError(rows.Err())
	return rid, exists
}

// Try to find OrgID for given OrgLogin (returns nil for nil)
func findOrgIDOrNil(db *sql.DB, ctx *Ctx, orgLogin *string) *int {
	var orgID int
	if orgLogin == nil {
		return nil
	}
	rows := QuerySQLWithErr(
		db,
		ctx,
		fmt.Sprintf(
			"select id from gha_orgs where login=%s",
			NValue(1),
		),
		*orgLogin,
	)
	defer func() { FatalOnError(rows.Close()) }()
	for rows.Next() {
		FatalOnError(rows.Scan(&orgID))
		return &orgID
	}
	FatalOnError(rows.Err())
	return nil
}

// Check if given event existis (given by ID)
func eventExists(db *sql.DB, ctx *Ctx, eventID string) bool {
	rows := QuerySQLWithErr(db, ctx, fmt.Sprintf("select 1 from gha_events where id=%s", NValue(1)), eventID)
	defer func() { FatalOnError(rows.Close()) }()
	exists := false
	for rows.Next() {
		exists = true
	}
	return exists
}

// eventExistsCollision - like eventExists, but logs when the existing row is a DIFFERENT event
// (GitHub reset the event id sequence on 2025-10-08; new real ids can reuse 2016-2020 ids)
func eventExistsCollision(db *sql.DB, ctx *Ctx, eventID string, eType, repoName string, createdAt time.Time) bool {
	rows := QuerySQLWithErr(db, ctx, fmt.Sprintf("select type, dup_repo_name, created_at from gha_events where id=%s", NValue(1)), eventID)
	defer func() { FatalOnError(rows.Close()) }()
	exists := false
	eT, eR, eD := "", "", time.Time{}
	for rows.Next() {
		FatalOnError(rows.Scan(&eT, &eR, &eD))
		exists = true
	}
	FatalOnError(rows.Err())
	// `created_at` is a `timestamp` (no zone): the DB keeps the wall clock of the
	// value written, so compare wall clocks - comparing instants (`Equal`) reported
	// bogus collisions for old-format (2012-2014) events whose `created_at` carries
	// a non-UTC offset (`2014-12-31T23:00:00-08:00` is stored as `2014-12-31 23:00:00`).
	if exists && (eT != eType || eR != repoName || ToYMDHMSDate(eD) != ToYMDHMSDate(createdAt)) {
		Printf("event id collision: id %s already exists as (%s, %s, %v), new event (%s, %s, %v) skipped\n", eventID, eT, eR, eD, eType, repoName, createdAt)
	}
	return exists
}

// MatchGroups - returns the named capture groups of the first match of re in arg
func MatchGroups(re *regexp.Regexp, arg string) (result map[string]string) {
	match := re.FindStringSubmatch(arg)
	result = make(map[string]string)
	for i, name := range re.SubexpNames() {
		if i > 0 && i <= len(match) {
			result[name] = match[i]
		}
	}
	return
}

// Process commit message trailers
func ghaCommitsRoles(con *sql.Tx, ctx *Ctx, msg, sha, eventID string, repoID int, repoName string, evCreatedAt time.Time, maybeHide func(string) string) {
	// fmt.Printf("got here: sha=%s, created=%v\nmsg:\n%s\n", sha, evCreatedAt, msg)
	msg = strings.Replace(msg, "\r", "\n", -1)
	lines := strings.Split(msg, "\n")
	for _, line := range lines {
		line := strings.TrimSpace(line)
		if line == "" {
			continue
		}
		m := MatchGroups(GitTrailerPattern, line)
		if len(m) == 0 {
			continue
		}
		oTrailer := m["name"]
		lTrailer := strings.ToLower(oTrailer)
		trailers, ok := GitAllowedTrailers[lTrailer]
		if !ok {
			continue
		}
		fields := strings.Split(m["value"], "<")
		name := strings.TrimSpace(fields[0])
		email := ""
		if len(fields) > 1 {
			fields2 := strings.Split(fields[1], ">")
			email = strings.TrimSpace(fields2[0])
		}
		if name == "" || email == "" {
			continue
		}
		id, login := lookupActorNameEmailTx(con, ctx, name, email, maybeHide)
		// fmt.Printf("got trailer(s) '%s': %+v -> ('%s', '%s', %d, '%s')\n", line, trailers, name, email, id, login)
		for _, role := range trailers {
			ExecSQLTxWithErr(
				con,
				ctx,
				InsertIgnore(
					"into gha_commits_roles("+
						"sha, event_id, role, actor_id, actor_login, actor_name, actor_email, "+
						"dup_repo_id, dup_repo_name, dup_created_at"+
						") "+NValues(10)),
				AnyArray{
					sha,
					eventID,
					role,
					id,
					maybeHide(TruncToBytes(login, 120)),
					maybeHide(TruncToBytes(name, 160)),
					maybeHide(TruncToBytes(email, 160)),
					repoID,
					repoName,
					evCreatedAt,
				}...,
			)
		}
	}
	// fmt.Printf("out of here: sha=%s, created=%v\n", sha, evCreatedAt)
}

// Process GHA pages
// gha_pages
// {"page_name:String"=>370, "title:String"=>370, "summary:NilClass"=>370,
// "action:String"=>370, "sha:String"=>370, "html_url:String"=>370}
// {"page_name"=>65, "title"=>65, "summary"=>0, "action"=>7, "sha"=>40, "html_url"=>130}
// 370
func ghaPages(con *sql.Tx, ctx *Ctx, payloadPages *[]Page, eventID string, actor *Actor, repo *Repo, eType string, eCreatedAt time.Time, maybeHide func(string) string) {
	pages := []Page{}
	if payloadPages != nil {
		pages = *payloadPages
	}
	for _, page := range pages {
		sha := page.SHA
		ExecSQLTxWithErr(
			con,
			ctx,
			InsertIgnore(
				"into gha_pages(sha, event_id, action, title, "+
					"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at"+
					") "+NValues(10)),
			AnyArray{
				sha,
				eventID,
				page.Action,
				TruncToBytes(page.Title, 300),
				actor.ID,
				maybeHide(actor.Login),
				repo.ID,
				repo.Name,
				eType,
				eCreatedAt,
			}...,
		)
	}
}

// gha_comments
// Table details and analysis in `analysis/analysis.txt` and `analysis/comment_*.json`
func ghaComment(con *sql.Tx, ctx *Ctx, payloadComment *Comment, eventID string, actor *Actor, repo *Repo, eType string, eCreatedAt time.Time, maybeHide func(string) string) {
	if payloadComment == nil {
		return
	}
	comment := *payloadComment

	// user
	ghaActor(con, ctx, &comment.User, maybeHide)

	// comment
	cid := comment.ID
	ExecSQLTxWithErr(
		con,
		ctx,
		InsertIgnore(
			"into gha_comments("+
				"id, event_id, body, created_at, updated_at, user_id, "+
				// "commit_id, original_commit_id, diff_hunk, position, "+
				"commit_id, original_commit_id, position, "+
				"original_position, path, pull_request_review_id, line, "+
				"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
				// "dup_user_login) "+NValues(21),
				"dup_user_login) "+NValues(20),
		),
		AnyArray{
			cid,
			eventID,
			TruncToBytes(comment.Body, 0xffff),
			comment.CreatedAt,
			comment.UpdatedAt,
			comment.User.ID,
			StringOrNil(comment.CommitID),
			StringOrNil(comment.OriginalCommitID),
			// StringOrNil(comment.DiffHunk),
			IntOrNil(comment.Position),
			IntOrNil(comment.OriginalPosition),
			StringOrNil(comment.Path),
			IntOrNil(comment.PullRequestReviewID),
			IntOrNil(comment.Line),
			actor.ID,
			maybeHide(actor.Login),
			repo.ID,
			repo.Name,
			eType,
			eCreatedAt,
			maybeHide(comment.User.Login),
		}...,
	)
}

// gha_reviews
// Table details and analysis in `analysis/analysis.txt` and `analysis/*review_*.json`
func ghaReview(con *sql.Tx, ctx *Ctx, payloadReview *Review, eventID string, actor *Actor, repo *Repo, eType string, eCreatedAt time.Time, maybeHide func(string) string) {
	if payloadReview == nil {
		return
	}
	review := *payloadReview

	// user
	ghaActor(con, ctx, &review.User, maybeHide)

	// review
	rid := review.ID
	ExecSQLTxWithErr(
		con,
		ctx,
		InsertIgnore(
			"into gha_reviews("+
				"id, event_id, state, author_association, submitted_at, user_id, commit_id, body, "+
				"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
				"dup_user_login) "+NValues(15),
		),
		AnyArray{
			rid,
			eventID,
			review.State,
			review.AuthorAssociation,
			review.SubmittedAt,
			review.User.ID,
			review.CommitID,
			TruncStringOrNil(review.Body, 0xffff),
			actor.ID,
			maybeHide(actor.Login),
			repo.ID,
			repo.Name,
			eType,
			eCreatedAt,
			maybeHide(review.User.Login),
		}...,
	)
}

// gha_releases
// Table details and analysis in `analysis/analysis.txt` and `analysis/release_*.json`
func ghaRelease(con *sql.Tx, ctx *Ctx, payloadRelease *Release, eventID string, actor *Actor, repo *Repo, eType string, eCreatedAt time.Time, maybeHide func(string) string) {
	if payloadRelease == nil {
		return
	}
	release := *payloadRelease

	// author
	ghaActor(con, ctx, &release.Author, maybeHide)

	// release
	rid := release.ID
	ExecSQLTxWithErr(
		con,
		ctx,
		"insert into gha_releases("+
			"id, event_id, tag_name, target_commitish, name, draft, "+
			"author_id, prerelease, created_at, published_at, body, "+
			"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
			"dup_author_login) "+NValues(18),
		AnyArray{
			rid,
			eventID,
			TruncToBytes(release.TagName, 200),
			TruncToBytes(release.TargetCommitish, 200),
			TruncStringOrNil(release.Name, 200),
			release.Draft,
			release.Author.ID,
			release.Prerelease,
			release.CreatedAt,
			TimeOrNil(release.PublishedAt),
			TruncStringOrNil(release.Body, 0xffff),
			actor.ID,
			maybeHide(actor.Login),
			repo.ID,
			repo.Name,
			eType,
			eCreatedAt,
			maybeHide(release.Author.Login),
		}...,
	)

	// Assets
	for _, asset := range release.Assets {
		// uploader
		ghaActor(con, ctx, &asset.Uploader, maybeHide)

		// asset
		aid := asset.ID
		ExecSQLTxWithErr(
			con,
			ctx,
			"insert into gha_assets("+
				"id, event_id, name, label, uploader_id, content_type, "+
				"state, size, download_count, created_at, updated_at, "+
				"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
				"dup_uploader_login) "+NValues(18),
			AnyArray{
				aid,
				eventID,
				TruncToBytes(asset.Name, 200),
				TruncStringOrNil(asset.Label, 120),
				asset.Uploader.ID,
				asset.ContentType,
				asset.State,
				asset.Size,
				asset.DownloadCount,
				asset.CreatedAt,
				asset.UpdatedAt,
				actor.ID,
				maybeHide(actor.Login),
				repo.ID,
				repo.Name,
				eType,
				eCreatedAt,
				maybeHide(asset.Uploader.Login),
			}...,
		)

		// release-asset connection
		ExecSQLTxWithErr(
			con,
			ctx,
			"insert into gha_releases_assets(release_id, event_id, asset_id) "+NValues(3),
			AnyArray{rid, eventID, aid}...,
		)
	}
}

// gha_pull_requests
// Table details and analysis in `analysis/analysis.txt` and `analysis/pull_request_*.json`
func ghaPullRequest(con *sql.Tx, ctx *Ctx, payloadPullRequest *PullRequest, eventID string, actor *Actor, repo *Repo, eType string, eCreatedAt time.Time, forkeeIDsToSkip []int, maybeHide func(string) string) {
	if payloadPullRequest == nil {
		return
	}

	// PR object
	pr := *payloadPullRequest

	// user
	ghaActor(con, ctx, &pr.User, maybeHide)

	baseSHA := pr.Base.SHA
	headSHA := pr.Head.SHA
	baseRepoID := ForkeeIDOrNil(pr.Base.Repo)

	// Create Event
	ev := Event{Actor: *actor, Repo: *repo, Type: eType, CreatedAt: eCreatedAt}

	// base
	ghaBranch(con, ctx, eventID, &pr.Base, &ev, forkeeIDsToSkip, maybeHide)

	// head (if different, and skip its repo if defined and the same as base repo)
	if baseSHA != headSHA {
		if baseRepoID != nil {
			forkeeIDsToSkip = append(forkeeIDsToSkip, baseRepoID.(int))
		}
		ghaBranch(con, ctx, eventID, &pr.Head, &ev, forkeeIDsToSkip, maybeHide)
	}

	// merged_by
	if pr.MergedBy != nil {
		ghaActor(con, ctx, pr.MergedBy, maybeHide)
	}

	// assignee
	if pr.Assignee != nil {
		ghaActor(con, ctx, pr.Assignee, maybeHide)
	}

	// milestone
	if pr.Milestone != nil {
		ghaMilestone(con, ctx, eventID, pr.Milestone, &ev, maybeHide)
	}

	// pull_request
	prid := pr.ID
	ExecSQLTxWithErr(
		con,
		ctx,
		"insert into gha_pull_requests("+
			"id, event_id, user_id, base_sha, head_sha, merged_by_id, assignee_id, milestone_id, "+
			"number, state, locked, title, body, created_at, updated_at, closed_at, merged_at, "+
			"merge_commit_sha, merged, mergeable, rebaseable, mergeable_state, comments, "+
			"review_comments, maintainer_can_modify, commits, additions, deletions, changed_files, "+
			"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
			// "dup_user_login, dupn_assignee_login, dupn_merged_by_login) "+NValues(38),
			"dup_user_login, dupn_merged_by_login) "+NValues(37),
		AnyArray{
			prid,
			eventID,
			pr.User.ID,
			baseSHA,
			headSHA,
			ActorIDOrNil(pr.MergedBy),
			ActorIDOrNil(pr.Assignee),
			MilestoneIDOrNil(pr.Milestone),
			pr.Number,
			pr.State,
			BoolOrNil(pr.Locked),
			CleanUTF8(pr.Title),
			TruncStringOrNil(pr.Body, 0xffff),
			pr.CreatedAt,
			pr.UpdatedAt,
			TimeOrNil(pr.ClosedAt),
			TimeOrNil(pr.MergedAt),
			StringOrNil(pr.MergeCommitSHA),
			BoolOrNil(pr.Merged),
			BoolOrNil(pr.Mergeable),
			BoolOrNil(pr.Rebaseable),
			StringOrNil(pr.MergeableState),
			IntOrNil(pr.Comments),
			IntOrNil(pr.ReviewComments),
			BoolOrNil(pr.MaintainerCanModify),
			IntOrNil(pr.Commits),
			IntOrNil(pr.Additions),
			IntOrNil(pr.Deletions),
			IntOrNil(pr.ChangedFiles),
			actor.ID,
			maybeHide(actor.Login),
			repo.ID,
			repo.Name,
			eType,
			eCreatedAt,
			maybeHide(pr.User.Login),
			// ActorLoginOrNil(pr.Assignee, maybeHide),
			ActorLoginOrNil(pr.MergedBy, maybeHide),
		}...,
	)

	// Arrays: actors: assignees, requested_reviewers
	// assignees
	var assignees []Actor

	prAid := ActorIDOrNil(pr.Assignee)
	if pr.Assignee != nil {
		assignees = append(assignees, *pr.Assignee)
	}

	if pr.Assignees != nil {
		for _, assignee := range *pr.Assignees {
			aid := assignee.ID
			if aid == prAid {
				continue
			}
			assignees = append(assignees, assignee)
		}
	}

	for _, assignee := range assignees {
		// assignee
		ghaActor(con, ctx, &assignee, maybeHide)

		// pull_request-assignee connection
		ExecSQLTxWithErr(
			con,
			ctx,
			"insert into gha_pull_requests_assignees(pull_request_id, event_id, assignee_id) "+NValues(3),
			AnyArray{prid, eventID, assignee.ID}...,
		)
	}

	// requested_reviewers
	if pr.RequestedReviewers != nil {
		for _, reviewer := range *pr.RequestedReviewers {
			// reviewer
			ghaActor(con, ctx, &reviewer, maybeHide)

			// pull_request-requested_reviewer connection
			ExecSQLTxWithErr(
				con,
				ctx,
				"insert into gha_pull_requests_requested_reviewers(pull_request_id, event_id, requested_reviewer_id) "+NValues(3),
				AnyArray{prid, eventID, reviewer.ID}...,
			)
		}
	}
}

// gha_teams
func ghaTeam(con *sql.Tx, ctx *Ctx, payloadTeam *Team, payloadRepo *Forkee, eventID string, actor *Actor, repo *Repo, eType string, eCreatedAt time.Time, maybeHide func(string) string) {
	if payloadTeam == nil {
		return
	}
	team := *payloadTeam

	// team
	tid := team.ID
	ExecSQLTxWithErr(
		con,
		ctx,
		"insert into gha_teams("+
			"id, event_id, name, slug, permission, "+
			"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at"+
			") "+NValues(11),
		AnyArray{
			tid,
			eventID,
			TruncToBytes(team.Name, 120),
			TruncToBytes(team.Slug, 100),
			TruncToBytes(team.Permission, 20),
			actor.ID,
			maybeHide(actor.Login),
			repo.ID,
			repo.Name,
			eType,
			eCreatedAt,
		}...,
	)

	// team-repository connection
	if payloadRepo != nil {
		ExecSQLTxWithErr(
			con,
			ctx,
			"insert into gha_teams_repositories(team_id, event_id, repository_id) "+NValues(3),
			AnyArray{tid, eventID, payloadRepo.ID}...,
		)
	}
}

// WriteToDBOldFmt - write entire GHA event (in the old pre-2015 format) into Postgres DB
func WriteToDBOldFmt(db *sql.DB, ctx *Ctx, eventID string, ev *EventOld, shas map[string]string) int {
	if eventExistsCollision(db, ctx, eventID, ev.Type, ev.Repository.Name, ev.CreatedAt) {
		return 0
	}

	// To handle GDPR
	maybeHide := MaybeHideFunc(shas)

	// Lookup author by GitHub login
	aid := lookupActor(db, ctx, ev.Actor, maybeHide)
	actor := Actor{ID: aid, Login: ev.Actor}

	// Repository
	repository := ev.Repository

	// Find Org ID from Repository.Organization
	oid := findOrgIDOrNil(db, ctx, repository.Organization)

	// Find Repo ID from Repository (this is a ForkeeOld before 2015).
	rid, ok := findRepoFromNameAndOrg(db, ctx, repository.Name, oid)
	if !ok {
		rid = repository.ID
	}

	// We defer transaction create until we're inserting data that can be shared between different events
	ExecSQLWithErr(
		db,
		ctx,
		"insert into gha_events("+
			// "id, type, actor_id, repo_id, public, created_at, "+
			"id, type, actor_id, repo_id, created_at, "+
			// "dup_actor_login, dup_repo_name, org_id, forkee_id) "+NValues(10),
			"dup_actor_login, dup_repo_name, org_id) "+NValues(8),
		AnyArray{
			eventID,
			ev.Type,
			aid,
			rid,
			// ev.Public,
			ev.CreatedAt,
			maybeHide(ev.Actor),
			ev.Repository.Name,
			oid,
			// ev.Repository.ID,
		}...,
	)

	// Organization
	if repository.Organization != nil {
		if oid == nil {
			h := HashStrings([]string{*repository.Organization})
			oid = &h
		}
		ghaOrg(db, ctx, &Org{ID: *oid, Login: *repository.Organization})
	}

	// Add Repository
	repo := Repo{ID: rid, Name: repository.Name}
	ghaRepo(db, ctx, &repo, oid, repository.Organization)

	// Pre 2015 Payload
	pl := ev.Payload
	if pl == nil {
		return 0
	}

	iid := FirstIntOrNil([]*int{pl.Issue, pl.IssueID})
	cid := CommentIDOrNil(pl.Comment)
	if cid == nil {
		cid = IntOrNil(pl.CommentID)
	}

	ExecSQLWithErr(
		db,
		ctx,
		"insert into gha_payloads("+
			"event_id, push_id, size, ref, head, befor, action, "+
			// "issue_id, pull_request_id, comment_id, ref_type, master_branch, commit, "+
			"issue_id, pull_request_id, comment_id, commit, "+
			// "description, number, forkee_id, release_id, member_id, "+
			"number, forkee_id, release_id, member_id, "+
			// "dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at"+
			"dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at"+
			// ") "+NValues(24),
			") "+NValues(20),
		AnyArray{
			eventID,
			nil,
			IntOrNil(pl.Size),
			TruncStringOrNil(pl.Ref, 200),
			StringOrNil(pl.Head),
			nil,
			StringOrNil(pl.Action),
			iid,
			PullRequestIDOrNil(pl.PullRequest),
			cid,
			// StringOrNil(pl.RefType),
			// TruncStringOrNil(pl.MasterBranch, 200),
			StringOrNil(pl.Commit),
			// TruncStringOrNil(pl.Description, 0xffff),
			IntOrNil(pl.Number),
			ForkeeIDOrNil(pl.Repository),
			ReleaseIDOrNil(pl.Release),
			ActorIDOrNil(pl.Member),
			// actor.ID,
			maybeHide(actor.Login),
			repo.ID,
			repo.Name,
			ev.Type,
			ev.CreatedAt,
		}...,
	)

	// Start transaction for data possibly shared between events
	con, err := db.Begin()
	FatalOnError(err)

	// gha_actors
	ghaActor(con, ctx, &actor, maybeHide)

	// Payload's Forkee (it uses new structure, so I'm giving it precedence over
	// Event's Forkee (which uses older structure)
	if pl.Repository != nil {
		// Reposotory is actually a Forkee (non old in this case!)
		// Artificial event is only used to allow duplicating EventOld's data
		// (passed as Event to avoid code duplication)
		artificialEv := Event{Actor: actor, Repo: repo, Type: ev.Type, CreatedAt: ev.CreatedAt}
		ghaForkee(con, ctx, eventID, pl.Repository, &artificialEv, maybeHide)
	}

	// Add Forkee in old mode if we didn't added it from payload or if it is a different Forkee
	if pl.Repository == nil || pl.Repository.ID != ev.Repository.ID {
		ghaForkeeOld(con, ctx, eventID, &ev.Repository, &actor, &repo, ev, maybeHide)
	}

	// SHAs - commits
	if pl.SHAs != nil {
		commits := *pl.SHAs
		for _, comm := range commits {
			commit, ok := comm.([]interface{})
			if !ok {
				Fatalf("comm is not []interface{}: %+v", comm)
			}
			sha, ok := commit[0].(string)
			if !ok {
				Fatalf("commit[0] is not string: %+v", commit[0])
			}
			ExecSQLTxWithErr(
				con,
				ctx,
				"insert into gha_commits("+
					// "sha, event_id, author_name, encrypted_email, message, is_distinct, "+
					"sha, event_id, author_name, message, is_distinct, "+
					"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, origin"+
					// ") "+NValues(13),
					") "+NValues(12),
				AnyArray{
					sha,
					eventID,
					maybeHide(TruncToBytes(commit[3].(string), 160)),
					// TruncToBytes(commit[1].(string), 160),
					TruncToBytes(commit[2].(string), 0xffff),
					commit[4].(bool),
					actor.ID,
					maybeHide(actor.Login),
					repo.ID,
					repo.Name,
					ev.Type,
					ev.CreatedAt,
					0,
				}...,
			)
			// Commit Roles
			ghaCommitsRoles(con, ctx, commit[2].(string), sha, eventID, repo.ID, repo.Name, ev.CreatedAt, maybeHide)
		}
	}

	// Pages
	ghaPages(con, ctx, pl.Pages, eventID, &actor, &repo, ev.Type, ev.CreatedAt, maybeHide)

	// Member
	if pl.Member != nil {
		ghaActor(con, ctx, pl.Member, maybeHide)
	}

	// Comment
	ghaComment(con, ctx, pl.Comment, eventID, &actor, &repo, ev.Type, ev.CreatedAt, maybeHide)

	// Release & assets
	ghaRelease(con, ctx, pl.Release, eventID, &actor, &repo, ev.Type, ev.CreatedAt, maybeHide)

	// Team & Repo connection
	ghaTeam(con, ctx, pl.Team, pl.Repository, eventID, &actor, &repo, ev.Type, ev.CreatedAt, maybeHide)

	// Pull Request
	forkeeIDsToSkip := []int{ev.Repository.ID}
	if pl.Repository != nil {
		forkeeIDsToSkip = append(forkeeIDsToSkip, pl.Repository.ID)
	}
	ghaPullRequest(con, ctx, pl.PullRequest, eventID, &actor, &repo, ev.Type, ev.CreatedAt, forkeeIDsToSkip, maybeHide)

	// We need artificial issue
	// gha_issues
	// Table details and analysis in `analysis/analysis.txt` and `analysis/issue_*.json`
	if pl.PullRequest != nil {
		pr := *pl.PullRequest

		// issue
		iid = -pr.ID
		isPR := true
		comments := 0
		locked := false
		if pr.Comments != nil {
			comments = *pr.Comments
		}
		if pr.Locked != nil {
			locked = *pr.Locked
		}
		ExecSQLTxWithErr(
			con,
			ctx,
			"insert into gha_issues("+
				"id, event_id, assignee_id, body, closed_at, comments, created_at, "+
				"locked, milestone_id, number, state, title, updated_at, user_id, "+
				"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
				// "dup_user_login, dupn_assignee_login, is_pull_request) "+NValues(23),
				"dup_user_login, is_pull_request) "+NValues(22),
			AnyArray{
				iid,
				eventID,
				ActorIDOrNil(pr.Assignee),
				TruncStringOrNil(pr.Body, 0xffff),
				TimeOrNil(pr.ClosedAt),
				comments,
				pr.CreatedAt,
				locked,
				MilestoneIDOrNil(pr.Milestone),
				pr.Number,
				pr.State,
				CleanUTF8(pr.Title),
				pr.UpdatedAt,
				pr.User.ID,
				actor.ID,
				maybeHide(actor.Login),
				repo.ID,
				repo.Name,
				ev.Type,
				ev.CreatedAt,
				maybeHide(pr.User.Login),
				// ActorLoginOrNil(pr.Assignee, maybeHide),
				isPR,
			}...,
		)

		var assignees []Actor

		prAid := ActorIDOrNil(pr.Assignee)
		if pr.Assignee != nil {
			assignees = append(assignees, *pr.Assignee)
		}

		if pr.Assignees != nil {
			for _, assignee := range *pr.Assignees {
				aid := assignee.ID
				if aid == prAid {
					continue
				}
				assignees = append(assignees, assignee)
			}
		}

		for _, assignee := range assignees {
			// pull_request-assignee connection
			ExecSQLTxWithErr(
				con,
				ctx,
				"insert into gha_issues_assignees(issue_id, event_id, assignee_id) "+NValues(3),
				AnyArray{iid, eventID, assignee.ID}...,
			)
		}
	}

	// Final commit
	FatalOnError(con.Commit())
	return 1
}

// WriteToDB - write entire GHA event (in a new 2015+ format) into Postgres DB, returns 1 when written, 0 when the event already exists
func WriteToDB(db *sql.DB, ctx *Ctx, ev *Event, shas map[string]string) int {
	eventID := ev.ID
	if eventExistsCollision(db, ctx, eventID, ev.Type, ev.Repo.Name, ev.CreatedAt) {
		return 0
	}

	// To handle GDPR
	maybeHide := MaybeHideFunc(shas)

	// We defer transaction create until we're inserting data that can be shared between different events
	// gha_events
	// {"id:String"=>48592, "type:String"=>48592, "actor:Hash"=>48592, "repo:Hash"=>48592,
	// "payload:Hash"=>48592, "public:TrueClass"=>48592, "created_at:String"=>48592,
	// "org:Hash"=>19451}
	// {"id"=>10, "type"=>29, "actor"=>278, "repo"=>290, "payload"=>216017, "public"=>4,
	// "created_at"=>20, "org"=>230}
	// Fields dup_actor_login, dup_repo_name are copied from (gha_actors and gha_repos) to save
	// joins on complex queries (MySQL has no hash joins and is very slow on big tables joins)
	ExecSQLWithErr(
		db,
		ctx,
		"insert into gha_events("+
			// "id, type, actor_id, repo_id, public, created_at, "+
			"id, type, actor_id, repo_id, created_at, "+
			// "dup_actor_login, dup_repo_name, org_id, forkee_id) "+NValues(10),
			"dup_actor_login, dup_repo_name, org_id) "+NValues(8),
		AnyArray{
			eventID,
			ev.Type,
			ev.Actor.ID,
			ev.Repo.ID,
			// ev.Public,
			ev.CreatedAt,
			maybeHide(ev.Actor.Login),
			ev.Repo.Name,
			OrgIDOrNil(ev.Org),
			// nil,
		}...,
	)

	// Repository
	repo := ev.Repo
	org := ev.Org
	ghaRepo(db, ctx, &repo, OrgIDOrNil(org), OrgLoginOrNil(org))

	// Organization
	if org != nil {
		ghaOrg(db, ctx, org)
	}

	// gha_payloads
	// {"push_id:Fixnum"=>24636, "size:Fixnum"=>24636, "distinct_size:Fixnum"=>24636,
	// "ref:String"=>30522, "head:String"=>24636, "before:String"=>24636, "commits:Array"=>24636,
	// "action:String"=>14317, "issue:Hash"=>6446, "comment:Hash"=>6055, "ref_type:String"=>8010,
	// "master_branch:String"=>6724, "description:String"=>3701, "pusher_type:String"=>8010,
	// "pull_request:Hash"=>4475, "ref:NilClass"=>2124, "description:NilClass"=>3023,
	// "number:Fixnum"=>2992, "forkee:Hash"=>1211, "pages:Array"=>370, "release:Hash"=>156,
	// "member:Hash"=>219}
	// {"push_id"=>10, "size"=>4, "distinct_size"=>4, "ref"=>110, "head"=>40, "before"=>40,
	// "commits"=>33215, "action"=>9, "issue"=>87776, "comment"=>177917, "ref_type"=>10,
	// "master_branch"=>34, "description"=>3222, "pusher_type"=>4, "pull_request"=>70565,
	// "number"=>5, "forkee"=>6880, "pages"=>855, "release"=>31206, "member"=>1040}
	// 48746
	// using exec_stmt (without select), because payload are per event_id.
	// Columns duplicated from gha_events starts with "dup_"
	pl := ev.Payload
	ExecSQLWithErr(
		db,
		ctx,
		"insert into gha_payloads("+
			"event_id, push_id, size, ref, head, befor, action, "+
			// "issue_id, pull_request_id, comment_id, ref_type, master_branch, commit, "+
			"issue_id, pull_request_id, comment_id, commit, "+
			// "description, number, forkee_id, release_id, member_id, "+
			"number, forkee_id, release_id, member_id, "+
			// "dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at"+
			"dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at"+
			// ") "+NValues(24),
			") "+NValues(20),
		AnyArray{
			eventID,
			IntOrNil(pl.PushID),
			IntOrNil(pl.Size),
			TruncStringOrNil(pl.Ref, 200),
			StringOrNil(pl.Head),
			StringOrNil(pl.Before),
			StringOrNil(pl.Action),
			IssueIDOrNil(pl.Issue),
			PullRequestIDOrNil(pl.PullRequest),
			CommentIDOrNil(pl.Comment),
			// StringOrNil(pl.RefType),
			// TruncStringOrNil(pl.MasterBranch, 200),
			nil,
			// TruncStringOrNil(pl.Description, 0xffff),
			IntOrNil(pl.Number),
			ForkeeIDOrNil(pl.Forkee),
			ReleaseIDOrNil(pl.Release),
			ActorIDOrNil(pl.Member),
			// ev.Actor.ID,
			maybeHide(ev.Actor.Login),
			ev.Repo.ID,
			ev.Repo.Name,
			ev.Type,
			ev.CreatedAt,
		}...,
	)

	// Start transaction for data possibly shared between events
	con, err := db.Begin()
	FatalOnError(err)

	// gha_actors
	ghaActor(con, ctx, &ev.Actor, maybeHide)

	// Make sure that entry is gha_actors is most up-to-date
	/*
		ExecSQLWithErr(
			db,
			ctx,
			fmt.Sprintf(
				"update gha_actors set login=%s where id=%s"+
					NValue(1),
				  NValue(2),
			),
			AnyArray{
				maybeHide(ev.Actor.Login),
				ev.Actor.ID,
			}...,
		)
	*/

	// gha_commits
	// {"sha:String"=>23265, "author:Hash"=>23265, "message:String"=>23265,
	// "distinct:TrueClass"=>21789, "url:String"=>23265, "distinct:FalseClass"=>1476}
	// {"sha"=>40, "author"=>177, "message"=>19005, "distinct"=>5, "url"=>191}
	// author: {"name:String"=>23265, "email:String"=>23265} (only git username/email)
	// author: {"name"=>96, "email"=>95}
	// 23265
	commits := []Commit{}
	if pl.Commits != nil {
		commits = *pl.Commits
	}
	for _, commit := range commits {
		sha := commit.SHA
		ExecSQLTxWithErr(
			con,
			ctx,
			"insert into gha_commits("+
				// "sha, event_id, author_name, encrypted_email, message, is_distinct, "+
				"sha, event_id, author_name, message, is_distinct, "+
				"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, origin"+
				// ") "+NValues(13),
				") "+NValues(12),
			AnyArray{
				sha,
				eventID,
				maybeHide(TruncToBytes(commit.Author.Name, 160)),
				// TruncToBytes(commit.Author.Email, 160),
				TruncToBytes(commit.Message, 0xffff),
				commit.Distinct,
				ev.Actor.ID,
				maybeHide(ev.Actor.Login),
				ev.Repo.ID,
				ev.Repo.Name,
				ev.Type,
				ev.CreatedAt,
				0,
			}...,
		)
		// Commit Roles
		ghaCommitsRoles(con, ctx, commit.Message, sha, eventID, ev.Repo.ID, ev.Repo.Name, ev.CreatedAt, maybeHide)
	}

	// Pages
	ghaPages(con, ctx, pl.Pages, eventID, &ev.Actor, &ev.Repo, ev.Type, ev.CreatedAt, maybeHide)

	// Member
	if pl.Member != nil {
		ghaActor(con, ctx, pl.Member, maybeHide)
	}

	// Comment
	ghaComment(con, ctx, pl.Comment, eventID, &ev.Actor, &ev.Repo, ev.Type, ev.CreatedAt, maybeHide)

	// gha_issues
	// Table details and analysis in `analysis/analysis.txt` and `analysis/issue_*.json`
	if pl.Issue != nil {
		issue := *pl.Issue

		// user, assignee
		ghaActor(con, ctx, &issue.User, maybeHide)
		if issue.Assignee != nil {
			ghaActor(con, ctx, issue.Assignee, maybeHide)
		}

		// issue
		iid := issue.ID
		isPR := false
		if issue.PullRequest != nil {
			isPR = true
		}
		ExecSQLTxWithErr(
			con,
			ctx,
			"insert into gha_issues("+
				"id, event_id, assignee_id, body, closed_at, comments, created_at, "+
				"locked, milestone_id, number, state, title, updated_at, user_id, "+
				"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
				// "dup_user_login, dupn_assignee_login, is_pull_request) "+NValues(23),
				"dup_user_login, is_pull_request) "+NValues(22),
			AnyArray{
				iid,
				eventID,
				ActorIDOrNil(issue.Assignee),
				TruncStringOrNil(issue.Body, 0xffff),
				TimeOrNil(issue.ClosedAt),
				issue.Comments,
				issue.CreatedAt,
				issue.Locked,
				MilestoneIDOrNil(issue.Milestone),
				issue.Number,
				issue.State,
				CleanUTF8(issue.Title),
				issue.UpdatedAt,
				issue.User.ID,
				ev.Actor.ID,
				maybeHide(ev.Actor.Login),
				ev.Repo.ID,
				ev.Repo.Name,
				ev.Type,
				ev.CreatedAt,
				maybeHide(issue.User.Login),
				// ActorLoginOrNil(issue.Assignee, maybeHide),
				isPR,
			}...,
		)

		// milestone
		if issue.Milestone != nil {
			ghaMilestone(con, ctx, eventID, issue.Milestone, ev, maybeHide)
		}

		pAid := ActorIDOrNil(issue.Assignee)
		for _, assignee := range issue.Assignees {
			aid := assignee.ID
			if aid == pAid {
				continue
			}

			// assignee
			ghaActor(con, ctx, &assignee, maybeHide)

			// issue-assignee connection
			ExecSQLTxWithErr(
				con,
				ctx,
				"insert into gha_issues_assignees(issue_id, event_id, assignee_id) "+NValues(3),
				AnyArray{iid, eventID, aid}...,
			)
		}

		// labels
		for _, label := range issue.Labels {
			lid := IntOrNil(label.ID)
			if lid == nil {
				lid = lookupLabel(con, ctx, TruncToBytes(label.Name, 160), label.Color)
			}

			// label
			ExecSQLTxWithErr(
				con,
				ctx,
				InsertIgnore("into gha_labels(id, name, color, is_default) "+NValues(4)),
				AnyArray{lid, TruncToBytes(label.Name, 160), label.Color, BoolOrNil(label.Default)}...,
			)

			// issue-label connection
			ExecSQLTxWithErr(
				con,
				ctx,
				InsertIgnore(
					"into gha_issues_labels(issue_id, event_id, label_id, "+
						"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
						"dup_issue_number, dup_label_name"+
						") "+NValues(11)),
				AnyArray{
					iid,
					eventID,
					lid,
					ev.Actor.ID,
					maybeHide(ev.Actor.Login),
					ev.Repo.ID,
					ev.Repo.Name,
					ev.Type,
					ev.CreatedAt,
					issue.Number,
					label.Name,
				}...,
			)
		}
	}

	// gha_forkees
	if pl.Forkee != nil {
		ghaForkee(con, ctx, eventID, pl.Forkee, ev, maybeHide)
	}

	// Release & assets
	ghaRelease(con, ctx, pl.Release, eventID, &ev.Actor, &ev.Repo, ev.Type, ev.CreatedAt, maybeHide)

	// Pull Request
	ghaPullRequest(con, ctx, pl.PullRequest, eventID, &ev.Actor, &ev.Repo, ev.Type, ev.CreatedAt, []int{}, maybeHide)

	// Review
	ghaReview(con, ctx, pl.Review, eventID, &ev.Actor, &ev.Repo, ev.Type, ev.CreatedAt, maybeHide)

	// Final commit
	FatalOnError(con.Commit())
	return 1
}
