package main

// reconcile_dbs - pull the GitHub events (and their dependent rows) that the peer DevStats
// database(s) have for the repositories this database tracks but this database lacks.
//
// Every project database and the shared database of its `shared_db` (projects.yaml, e.g. `allprj`)
// are fed from the same GH Archive files, but the GitHub API restores differ per database
// (ghapi2db runs 4x/day for projects and once a day for the shared one, the repo events feed is
// capped at 300 events per pass, get_repos restores orphan commits per database), so each side ends
// up with events the other one misses. gha2db_sync runs this tool after ghapi2db and before the
// `structure` postprocess, so the copied rows get their repo groups and derived tables in the same
// sync and calc_metric sees them.
//
// Modes (a database is only ever written by its own sync - pull only):
//   - project: target = PG_DB (project database), source = its `shared_db` from projects.yaml
//     (project found by GHA2DB_PROJECT, else by `psql_db` = PG_DB); no `shared_db` - nothing to do,
//   - shared: PG_DB is the `shared_db` of some enabled projects (e.g. allprj) - sources = all of
//     their `psql_db` (ordered by project order, name), unavailable databases are skipped,
//   - explicit: GHA2DB_RECONCILE_DBS=db1,db2 - those sources (no projects.yaml needed).
//
// Scope: repositories present in both `gha_repos` (ids). Window: GHA2DB_RECONCILE_RANGE (PostgreSQL
// interval, default "90 days") before now. Classes: native GitHub ids (0 < id < 2^48, GH Archive and
// events feed restores share them) and synthetic orphan pushes (id < 0, deterministic ids);
// artificial API-restored events (id >= 2^48) only with GHA2DB_RECONCILE_ARTIFICIAL=1 (each side
// regenerates them itself).
//
// Stateless idempotency: per (repo_id, day) digests (count, sum of ids) on both sides; only the
// differing buckets are diffed by id; a second run copies nothing.
//
// Copy rules (per batch of missing events, one transaction on the target):
//   - a synthetic orphan push whose commits already exist in the target (under any event) is skipped,
//   - `gha_events` and all event scoped tables are copied with `insert ... on conflict do nothing`,
//     the referenced `gha_repos` (id, name, org_id, org_login - the repo groups come from the
//     `structure` postprocess), `gha_orgs`, `gha_labels` and (only without a shared affiliations DB)
//     `gha_actors` rows too,
//   - the commits of copied GHA pushes take over the rows the orphan restore wrote for the same SHAs
//     under synthetic events (like get_repos does), emptied synthetic events are removed,
//   - `is_distinct` of the copied commits is recomputed in the target,
//   - the targeted postprocess (texts, labels, issues/PRs) runs for the copied and affected events.
//
// GHA2DB_RECONCILE_DRY_RUN=1 computes and reports everything without writing.
// GHA2DB_RECONCILE_SKIP_DBS=a,b skips the listed sources. GHA2DB_RECONCILESKIP makes gha2db_sync skip the tool.

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	lib "github.com/cncf/devstatscode"
	"github.com/lib/pq"
	yaml "gopkg.in/yaml.v2"
)

// maxParams is the maximum number of bind parameters a single Postgres query can use.
const maxParams = 65535

// idBatch - number of missing events copied per target transaction (and ids per array literal)
const idBatch = 1000

// defaultRange - default reconciliation window (PostgreSQL interval)
const defaultRange = "90 days"

// eventTable - a table copied by event id
type eventTable struct {
	name string
	key  string
}

// eventTables - copied tables, `gha_events` first (key `id`), the rest by `event_id`, in this order
var eventTables = []eventTable{
	{"gha_events", "id"},
	{"gha_payloads", "event_id"},
	{"gha_commits", "event_id"},
	{"gha_commits_roles", "event_id"},
	{"gha_pages", "event_id"},
	{"gha_comments", "event_id"},
	{"gha_issues", "event_id"},
	{"gha_issues_assignees", "event_id"},
	{"gha_issues_labels", "event_id"},
	{"gha_milestones", "event_id"},
	{"gha_forkees", "event_id"},
	{"gha_releases", "event_id"},
	{"gha_releases_assets", "event_id"},
	{"gha_assets", "event_id"},
	{"gha_pull_requests", "event_id"},
	{"gha_pull_requests_assignees", "event_id"},
	{"gha_pull_requests_requested_reviewers", "event_id"},
	{"gha_branches", "event_id"},
	{"gha_teams", "event_id"},
	{"gha_teams_repositories", "event_id"},
	{"gha_reviews", "event_id"},
}

// dimTables - dimension tables copied for the referenced ids (`gha_actors` only in legacy mode), in this order
var dimTables = []string{"gha_repos", "gha_orgs", "gha_labels", "gha_actors"}

// bucket - digest key: repository and day
type bucket struct {
	repoID int64
	day    string
}

// digest - digest value: number of events and the sum of their ids (numeric text)
type digest struct {
	count int64
	sum   string
}

// tableStats - rows read from the source and rows inserted into the target
type tableStats struct {
	rows     int
	inserted int
}

// sourceStats - per source result
type sourceStats struct {
	copied      int
	native      int
	orphan      int
	artificial  int
	skipped     int
	inserted    int
	taken       int
	takenFrom   int
	removed     int
	postprocess int
	tables      map[string]*tableStats
}

// config - tool configuration from the environment
type config struct {
	mode       string
	detail     string
	sources    []string
	explicit   bool
	rangeStr   string
	dryRun     bool
	artificial bool
}

func envFlag(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "t", "true", "y", "yes":
		return true
	}
	return false
}

// parseDBList - comma separated list of database names: trimmed, empty items dropped, duplicates removed (first wins)
func parseDBList(value string) []string {
	seen := make(map[string]struct{})
	dbs := []string{}
	for _, db := range strings.Split(value, ",") {
		db = strings.TrimSpace(db)
		if db == "" {
			continue
		}
		if _, ok := seen[db]; ok {
			continue
		}
		seen[db] = struct{}{}
		dbs = append(dbs, db)
	}
	return dbs
}

// classOf - event id class: "native" (0 < id < 2^48), "orphan" (id < 0), "artificial" (id >= 2^48), "zero" (id = 0)
func classOf(id int64) string {
	switch {
	case id < 0:
		return "orphan"
	case id == 0:
		return "zero"
	case id >= lib.ArtificialIDBase:
		return "artificial"
	}
	return "native"
}

// int64ArrayLiteral - `'{1,2,3}'::bigint[]` (safe: integers only)
func int64ArrayLiteral(ids []int64) string {
	var sb strings.Builder
	sb.WriteString("'{")
	for i, id := range ids {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(strconv.FormatInt(id, 10))
	}
	sb.WriteString("}'::bigint[]")
	return sb.String()
}

// textArrayLiteral - `'{"a","b"}'::text[]` with array element and SQL literal quoting
func textArrayLiteral(values []string) string {
	var sb strings.Builder
	sb.WriteString("'{")
	for i, value := range values {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("\"")
		for _, r := range value {
			switch r {
			case '\\', '"':
				sb.WriteString("\\")
				sb.WriteRune(r)
			case '\'':
				sb.WriteString("''")
			default:
				sb.WriteRune(r)
			}
		}
		sb.WriteString("\"")
	}
	sb.WriteString("}'::text[]")
	return sb.String()
}

// dateArrayLiteral - `'{2024-01-02,2024-01-03}'::date[]` (values are `YYYY-MM-DD` texts from the database)
func dateArrayLiteral(days []string) string {
	var sb strings.Builder
	sb.WriteString("'{")
	for i, day := range days {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(day)
	}
	sb.WriteString("}'::date[]")
	return sb.String()
}

// batchValues - `values ($1,$2),($3,$4),...` for nRows rows of nCols columns
func batchValues(nRows, nCols int) string {
	var sb strings.Builder
	sb.WriteString("values ")
	k := 1
	for r := 0; r < nRows; r++ {
		if r > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(")
		for c := 0; c < nCols; c++ {
			if c > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("$")
			sb.WriteString(strconv.Itoa(k))
			k++
		}
		sb.WriteString(")")
	}
	return sb.String()
}

// intersectSorted - ids present in both sorted slices (sorted, unique)
func intersectSorted(a, b []int64) []int64 {
	res := []int64{}
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			if len(res) == 0 || res[len(res)-1] != a[i] {
				res = append(res, a[i])
			}
			i++
			j++
		}
	}
	return res
}

// diffSorted - ids of the sorted slice a that are not in the sorted slice b (sorted, unique)
func diffSorted(a, b []int64) []int64 {
	res := []int64{}
	j := 0
	for _, id := range a {
		for j < len(b) && b[j] < id {
			j++
		}
		if j < len(b) && b[j] == id {
			continue
		}
		if len(res) == 0 || res[len(res)-1] != id {
			res = append(res, id)
		}
	}
	return res
}

// sortedInt64Keys - sorted keys of a set
func sortedInt64Keys(set map[int64]struct{}) []int64 {
	keys := make([]int64, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// differingBuckets - source buckets missing in the target or with a different digest, sorted by (repo_id, day)
func differingBuckets(source, target map[bucket]digest) []bucket {
	res := []bucket{}
	for key, sd := range source {
		td, ok := target[key]
		if ok && td == sd {
			continue
		}
		res = append(res, key)
	}
	sort.Slice(res, func(i, j int) bool {
		if res[i].repoID != res[j].repoID {
			return res[i].repoID < res[j].repoID
		}
		return res[i].day < res[j].day
	})
	return res
}

// chunkInt64 - split ids into chunks of at most size items
func chunkInt64(ids []int64, size int) [][]int64 {
	chunks := [][]int64{}
	for i := 0; i < len(ids); i += size {
		j := i + size
		if j > len(ids) {
			j = len(ids)
		}
		chunks = append(chunks, ids[i:j])
	}
	return chunks
}

// chunkStrings - split values into chunks of at most size items
func chunkStrings(values []string, size int) [][]string {
	chunks := [][]string{}
	for i := 0; i < len(values); i += size {
		j := i + size
		if j > len(values) {
			j = len(values)
		}
		chunks = append(chunks, values[i:j])
	}
	return chunks
}

// classCondition - SQL condition selecting the reconciled event id classes
func classCondition(artificial bool) string {
	if artificial {
		return ""
	}
	return " and id < " + strconv.FormatInt(lib.ArtificialIDBase, 10)
}

// classesInfo - human readable classes list
func classesInfo(artificial bool) string {
	if artificial {
		return "native, orphan, artificial"
	}
	return "native, orphan"
}

func isNoDBError(err error) bool {
	if e, ok := err.(*pq.Error); ok {
		return e.Code.Name() == "invalid_catalog_name" || string(e.Code) == "3D000"
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "database") && strings.Contains(msg, "does not exist")
}

// readProjects - projects.yaml (GHA2DB_PROJECTS_YAML) from the data directory (or ./ in local mode)
func readProjects(ctx *lib.Ctx) (*lib.AllProjects, string) {
	dataPrefix := ctx.DataDir
	if ctx.Local {
		dataPrefix = "./"
	}
	path := dataPrefix + ctx.ProjectsYaml
	data, err := ioutil.ReadFile(path)
	lib.FatalOnError(err)
	var projects lib.AllProjects
	lib.FatalOnError(yaml.Unmarshal(data, &projects))
	return &projects, path
}

// sharedDBSources - `psql_db` of the enabled projects with `shared_db` = target (ordered by order, name, db; unique)
func sharedDBSources(ctx *lib.Ctx, projects *lib.AllProjects, target string) []string {
	type projectDB struct {
		order int
		name  string
		db    string
	}
	projectDBs := []projectDB{}
	for name, project := range projects.Projects {
		if lib.IsProjectDisabled(ctx, name, project.Disabled) {
			continue
		}
		if strings.TrimSpace(project.SharedDB) != target {
			continue
		}
		db := strings.TrimSpace(project.PDB)
		if db == "" || db == target {
			continue
		}
		projectDBs = append(projectDBs, projectDB{order: project.Order, name: name, db: db})
	}
	sort.SliceStable(projectDBs, func(i, j int) bool {
		if projectDBs[i].order == projectDBs[j].order {
			if projectDBs[i].name == projectDBs[j].name {
				return projectDBs[i].db < projectDBs[j].db
			}
			return projectDBs[i].name < projectDBs[j].name
		}
		return projectDBs[i].order < projectDBs[j].order
	})
	seen := make(map[string]struct{})
	dbs := []string{}
	for _, projectDB := range projectDBs {
		if _, ok := seen[projectDB.db]; ok {
			continue
		}
		seen[projectDB.db] = struct{}{}
		dbs = append(dbs, projectDB.db)
	}
	return dbs
}

// projectForDB - the project of the target database: GHA2DB_PROJECT when it names an enabled project with
// that database, else the first (order, name) enabled project with `psql_db` = target
func projectForDB(ctx *lib.Ctx, projects *lib.AllProjects, target string) (string, *lib.Project) {
	if ctx.Project != "" {
		if project, ok := projects.Projects[ctx.Project]; ok && !lib.IsProjectDisabled(ctx, ctx.Project, project.Disabled) {
			if strings.TrimSpace(project.PDB) == target {
				return ctx.Project, &project
			}
		}
	}
	bestName := ""
	var best *lib.Project
	for name, project := range projects.Projects {
		if lib.IsProjectDisabled(ctx, name, project.Disabled) {
			continue
		}
		if strings.TrimSpace(project.PDB) != target {
			continue
		}
		if best == nil || project.Order < best.Order || (project.Order == best.Order && name < bestName) {
			p := project
			best = &p
			bestName = name
		}
	}
	return bestName, best
}

// resolveConfig - mode, sources and knobs from the environment (and projects.yaml when needed)
// Returns false when there is nothing to reconcile (message already printed).
func resolveConfig(ctx *lib.Ctx, target string) (config, bool) {
	cfg := config{
		rangeStr:   strings.TrimSpace(os.Getenv("GHA2DB_RECONCILE_RANGE")),
		dryRun:     envFlag("GHA2DB_RECONCILE_DRY_RUN"),
		artificial: envFlag("GHA2DB_RECONCILE_ARTIFICIAL"),
	}
	if cfg.rangeStr == "" {
		cfg.rangeStr = defaultRange
	}
	explicit := parseDBList(os.Getenv("GHA2DB_RECONCILE_DBS"))
	if len(explicit) > 0 {
		for _, db := range explicit {
			if db == target {
				lib.Fatalf("reconcile_dbs: source database '%s' is the target database", db)
			}
		}
		cfg.mode = "explicit"
		cfg.detail = "GHA2DB_RECONCILE_DBS"
		cfg.sources = explicit
		cfg.explicit = true
	} else {
		projects, path := readProjects(ctx)
		shared := sharedDBSources(ctx, projects, target)
		if len(shared) > 0 {
			cfg.mode = "shared"
			cfg.detail = fmt.Sprintf("projects with shared_db '%s' in %s", target, path)
			cfg.sources = shared
		} else {
			name, project := projectForDB(ctx, projects, target)
			if project == nil {
				lib.Printf("reconcile_dbs: %s: no enabled project uses this database in %s, nothing to reconcile\n", target, path)
				return cfg, false
			}
			sharedDB := strings.TrimSpace(project.SharedDB)
			if sharedDB == "" || sharedDB == target {
				lib.Printf("reconcile_dbs: %s: project '%s' has no shared database in %s, nothing to reconcile\n", target, name, path)
				return cfg, false
			}
			cfg.mode = "project"
			cfg.detail = fmt.Sprintf("project '%s' shared_db '%s' in %s", name, sharedDB, path)
			cfg.sources = []string{sharedDB}
		}
	}
	skipDBs := parseDBList(os.Getenv("GHA2DB_RECONCILE_SKIP_DBS"))
	if len(skipDBs) > 0 {
		skip := make(map[string]struct{})
		for _, db := range skipDBs {
			skip[db] = struct{}{}
		}
		sources := []string{}
		skipped := []string{}
		for _, db := range cfg.sources {
			if _, ok := skip[db]; ok {
				skipped = append(skipped, db)
				continue
			}
			sources = append(sources, db)
		}
		cfg.sources = sources
		lib.Printf("reconcile_dbs: %s: skipped %d source(s) using GHA2DB_RECONCILE_SKIP_DBS: %s\n", target, len(skipped), strings.Join(skipped, ", "))
	}
	if len(cfg.sources) == 0 {
		lib.Printf("reconcile_dbs: %s: no source databases, nothing to reconcile\n", target)
		return cfg, false
	}
	return cfg, true
}

// connectSource - connect to a source database: fatal when it is not available, unless nonFatal (returns the error)
func connectSource(ctx *lib.Ctx, db string, nonFatal bool) (*sql.DB, error) {
	if !nonFatal {
		return lib.PgConnDB(ctx, db), nil
	}
	lctx := *ctx
	lctx.PgDB = db
	lctx.ExecFatal = false
	lctx.ExecOutput = true
	c, err := lib.PgConnErr(&lctx)
	if err != nil {
		return nil, err
	}
	if err = c.Ping(); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

// queryInt64s - single bigint column query results (in query order)
func queryInt64s(con *sql.DB, ctx *lib.Ctx, query string, args ...interface{}) []int64 {
	rows := lib.QuerySQLWithErr(con, ctx, query, args...)
	defer func() { lib.FatalOnError(rows.Close()) }()
	ids := []int64{}
	for rows.Next() {
		var id int64
		lib.FatalOnError(rows.Scan(&id))
		ids = append(ids, id)
	}
	lib.FatalOnError(rows.Err())
	return ids
}

// queryStrings - single text column query results (in query order)
func queryStrings(con *sql.DB, ctx *lib.Ctx, query string, args ...interface{}) []string {
	rows := lib.QuerySQLWithErr(con, ctx, query, args...)
	defer func() { lib.FatalOnError(rows.Close()) }()
	values := []string{}
	for rows.Next() {
		var value string
		lib.FatalOnError(rows.Scan(&value))
		values = append(values, value)
	}
	lib.FatalOnError(rows.Err())
	return values
}

// repoIDs - distinct repository ids of `gha_repos` (sorted)
func repoIDs(con *sql.DB, ctx *lib.Ctx) []int64 {
	return queryInt64s(con, ctx, "select distinct id from gha_repos order by id")
}

// digestsSQL - digest query for a chunk of scope repositories
func digestsSQL(chunk []int64, classCond string) string {
	return "select repo_id, date_trunc('day', created_at)::date::text, count(*), sum(id)::text " +
		"from gha_events where created_at >= " + lib.NValue(1) + "::timestamp and repo_id = any(" + int64ArrayLiteral(chunk) + ")" +
		classCond + " group by 1, 2"
}

// digests - per (repo_id, day) event count and sum of ids for the scope repositories within the window
func digests(con *sql.DB, ctx *lib.Ctx, scope []int64, dtFrom, classCond string) map[bucket]digest {
	res := make(map[bucket]digest)
	for _, chunk := range chunkInt64(scope, idBatch) {
		rows := lib.QuerySQLWithErr(con, ctx, digestsSQL(chunk, classCond), dtFrom)
		for rows.Next() {
			var (
				key bucket
				val digest
			)
			lib.FatalOnError(rows.Scan(&key.repoID, &key.day, &val.count, &val.sum))
			res[key] = val
		}
		lib.FatalOnError(rows.Err())
		lib.FatalOnError(rows.Close())
	}
	return res
}

// eventIDsSQL - event ids of one repository on the given days within the window
func eventIDsSQL(repoID int64, days []string, classCond string) string {
	return "select id from gha_events where repo_id = " + strconv.FormatInt(repoID, 10) +
		" and created_at >= " + lib.NValue(1) + "::timestamp and date_trunc('day', created_at)::date = any(" + dateArrayLiteral(days) + ")" +
		classCond + " order by id"
}

// eventIDs - event ids of one repository on the given days within the window (sorted)
func eventIDs(con *sql.DB, ctx *lib.Ctx, repoID int64, days []string, dtFrom, classCond string) []int64 {
	return queryInt64s(con, ctx, eventIDsSQL(repoID, days, classCond), dtFrom)
}

// orphanEventsToSkip - synthetic orphan push events (from the given missing ones) with at least one commit
// SHA already present in the target `gha_commits` (under any event) - copying them would count those commits twice
func orphanEventsToSkip(ctx *lib.Ctx, src, tgt *sql.DB, orphans []int64) map[int64]struct{} {
	skip := make(map[int64]struct{})
	if len(orphans) == 0 {
		return skip
	}
	shaEvents := make(map[string][]int64)
	shas := []string{}
	for _, chunk := range chunkInt64(orphans, idBatch) {
		rows := lib.QuerySQLWithErr(
			src,
			ctx,
			"select sha, event_id from gha_commits where event_id = any("+int64ArrayLiteral(chunk)+") order by sha, event_id",
		)
		for rows.Next() {
			var (
				sha string
				eid int64
			)
			lib.FatalOnError(rows.Scan(&sha, &eid))
			if _, ok := shaEvents[sha]; !ok {
				shas = append(shas, sha)
			}
			shaEvents[sha] = append(shaEvents[sha], eid)
		}
		lib.FatalOnError(rows.Err())
		lib.FatalOnError(rows.Close())
	}
	for _, chunk := range chunkStrings(shas, idBatch) {
		present := queryStrings(tgt, ctx, "select distinct sha from gha_commits where sha = any("+textArrayLiteral(chunk)+")")
		for _, sha := range present {
			for _, eid := range shaEvents[sha] {
				skip[eid] = struct{}{}
			}
		}
	}
	return skip
}

// copyRows - copy the rows of `selectSQL` (run on the source) into the target table (same column names) with
// `on conflict do nothing`; returns rows read and rows inserted
func copyRows(ctx *lib.Ctx, src *sql.DB, tx *sql.Tx, table, selectSQL string) (int, int) {
	rows := lib.QuerySQLWithErr(src, ctx, selectSQL)
	columns, err := rows.Columns()
	lib.FatalOnError(err)
	nColumns := len(columns)
	quoted := make([]string, nColumns)
	for i, col := range columns {
		quoted[i] = "\"" + col + "\""
	}
	cols := "(" + strings.Join(quoted, ", ") + ")"
	vals := make([]interface{}, nColumns)
	for i := range vals {
		vals[i] = new(interface{})
	}
	effBatch := maxParams / nColumns
	if effBatch > idBatch {
		effBatch = idBatch
	}
	if effBatch < 1 {
		effBatch = 1
	}
	insertPrefix := "insert into " + table + cols + " "
	args := make([]interface{}, 0, effBatch*nColumns)
	rowsInBatch := 0
	nRows := 0
	nInserted := 0
	flush := func() {
		if rowsInBatch == 0 {
			return
		}
		res, err := lib.ExecSQLTx(tx, ctx, insertPrefix+batchValues(rowsInBatch, nColumns)+" on conflict do nothing", args...)
		if err != nil {
			// "on conflict do nothing" never raises a unique violation, so this is a real problem
			// (the transaction is aborted anyway, so no retry status of FatalOnError applies)
			lib.Printf("reconcile_dbs: failing batch insert into %s (rows: %d, columns: %d)\n", table, rowsInBatch, nColumns)
			lib.FatalOnError(err)
			lib.Fatalf("reconcile_dbs: batch insert into %s failed: %+v", table, err)
		}
		affected, err := res.RowsAffected()
		lib.FatalOnError(err)
		ins := int(affected)
		if ins < 0 {
			ins = 0
		}
		if ins > rowsInBatch {
			ins = rowsInBatch
		}
		nInserted += ins
		nRows += rowsInBatch
		args = args[:0]
		rowsInBatch = 0
	}
	for rows.Next() {
		lib.FatalOnError(rows.Scan(vals...))
		for vi := range vals {
			args = append(args, *(vals[vi].(*interface{})))
		}
		rowsInBatch++
		if rowsInBatch >= effBatch {
			flush()
		}
	}
	lib.FatalOnError(rows.Err())
	lib.FatalOnError(rows.Close())
	flush()
	return nRows, nInserted
}

// countRows - `select count(*) ...` on the source (dry run)
func countRows(ctx *lib.Ctx, src *sql.DB, fromSQL string) int {
	var n int
	lib.FatalOnError(lib.QueryRowSQL(src, ctx, "select count(*) "+fromSQL).Scan(&n))
	return n
}

// dimensionSelect - `from ...` part selecting the dimension rows referenced by the events `idsLit`
// (empty = the table is not copied: `gha_actors` with a shared affiliations database)
func dimensionSelect(ctx *lib.Ctx, table, idsLit string) string {
	switch table {
	case "gha_repos":
		return "from gha_repos where id in (select repo_id from gha_events where id = any(" + idsLit + "))"
	case "gha_orgs":
		return "from gha_orgs where id in (select org_id from gha_events where id = any(" + idsLit + ") and org_id is not null)"
	case "gha_labels":
		return "from gha_labels where id in (select label_id from gha_issues_labels where event_id = any(" + idsLit + "))"
	case "gha_actors":
		if ctx.AffiliationsDB != "" {
			return ""
		}
		return "from gha_actors where id in (select actor_id from gha_events where id = any(" + idsLit + "))"
	}
	return ""
}

// dimensionColumns - copied columns of a dimension table (`*` = all)
func dimensionColumns(table string) string {
	if table == "gha_repos" {
		return "id, name, org_id, org_login"
	}
	return "*"
}

// takeOver - the commits of the copied GHA push events take over the rows the orphan restore wrote for the same
// SHAs under synthetic (negative id) events: those rows are deleted (roles first), emptied synthetic events are
// removed with their payloads, texts and files, partially emptied ones are returned for the postprocess.
// Returns: commits taken over, synthetic events they were taken from, removed synthetic events, events to postprocess.
func takeOver(ctx *lib.Ctx, tx *sql.Tx, nativeLit string) (int, int, int, []int64) {
	rows := lib.QuerySQLTxWithErr(tx, ctx, "select distinct sha from gha_commits where event_id = any("+nativeLit+") order by sha")
	shas := []string{}
	for rows.Next() {
		var sha string
		lib.FatalOnError(rows.Scan(&sha))
		shas = append(shas, sha)
	}
	lib.FatalOnError(rows.Err())
	lib.FatalOnError(rows.Close())
	if len(shas) == 0 {
		return 0, 0, 0, nil
	}
	nTaken := 0
	synth := make(map[int64]struct{})
	for _, chunk := range chunkStrings(shas, idBatch) {
		shasLit := textArrayLiteral(chunk)
		lib.ExecSQLTxWithErr(tx, ctx, "delete from gha_commits_roles where event_id < 0 and sha = any("+shasLit+")")
		rows := lib.QuerySQLTxWithErr(tx, ctx, "delete from gha_commits where event_id < 0 and sha = any("+shasLit+") returning event_id")
		for rows.Next() {
			var eid int64
			lib.FatalOnError(rows.Scan(&eid))
			synth[eid] = struct{}{}
			nTaken++
		}
		lib.FatalOnError(rows.Err())
		lib.FatalOnError(rows.Close())
	}
	if nTaken == 0 {
		return 0, 0, 0, nil
	}
	nRemoved := 0
	ppEids := []int64{}
	for _, sid := range sortedInt64Keys(synth) {
		var left int
		lib.FatalOnError(lib.QueryRowSQLTx(tx, ctx, "select count(*) from gha_commits where event_id = "+lib.NValue(1), sid).Scan(&left))
		if left > 0 {
			ppEids = append(ppEids, sid)
			continue
		}
		for _, q := range []string{
			"delete from gha_texts where event_id = " + lib.NValue(1),
			"delete from gha_events_commits_files where event_id = " + lib.NValue(1),
			"delete from gha_payloads where event_id = " + lib.NValue(1),
			"delete from gha_events where id = " + lib.NValue(1),
		} {
			lib.ExecSQLTxWithErr(tx, ctx, q, sid)
		}
		nRemoved++
	}
	return nTaken, len(synth), nRemoved, ppEids
}

// isDistinctSQL - `is_distinct` of the copied commits: true only when no other pre-existing row has the same SHA
// and the row is the first (lowest event id) among the copied rows of that SHA
func isDistinctSQL(idsLit string) string {
	return "update gha_commits c set is_distinct = (" +
		"not exists (select 1 from gha_commits o where o.sha = c.sha and o.event_id <> c.event_id and not (o.event_id = any(" + idsLit + ")))" +
		" and c.event_id = (select min(n.event_id) from gha_commits n where n.sha = c.sha and n.event_id = any(" + idsLit + "))" +
		") where c.event_id = any(" + idsLit + ")"
}

// addTableStats - accumulate rows/inserted of a table
func (s *sourceStats) addTableStats(table string, rows, inserted int) {
	ts := s.tables[table]
	if ts == nil {
		ts = &tableStats{}
		s.tables[table] = ts
	}
	ts.rows += rows
	ts.inserted += inserted
	s.inserted += inserted
}

// reconcileSource - reconcile the target from one source database
func reconcileSource(ctx *lib.Ctx, cfg *config, tgt, src *sql.DB, target, source string, targetRepos []int64, dtFrom string) *sourceStats {
	stats := &sourceStats{tables: make(map[string]*tableStats)}
	prefix := fmt.Sprintf("reconcile_dbs: %s <- %s", target, source)
	sourceRepos := repoIDs(src, ctx)
	scope := intersectSorted(targetRepos, sourceRepos)
	lib.Printf("%s: scope %d repo(s) (target %d, source %d)\n", prefix, len(scope), len(targetRepos), len(sourceRepos))
	if len(scope) == 0 {
		return stats
	}
	classCond := classCondition(cfg.artificial)
	sourceDigests := digests(src, ctx, scope, dtFrom, classCond)
	targetDigests := digests(tgt, ctx, scope, dtFrom, classCond)
	differing := differingBuckets(sourceDigests, targetDigests)
	lib.Printf("%s: buckets: source %d, target %d, differing %d\n", prefix, len(sourceDigests), len(targetDigests), len(differing))
	if len(differing) == 0 {
		return stats
	}

	// Missing events: per repository with differing days
	repoDays := make(map[int64][]string)
	repoOrder := []int64{}
	for _, b := range differing {
		if _, ok := repoDays[b.repoID]; !ok {
			repoOrder = append(repoOrder, b.repoID)
		}
		repoDays[b.repoID] = append(repoDays[b.repoID], b.day)
	}
	missing := []int64{}
	targetOnly := 0
	for _, repoID := range repoOrder {
		days := repoDays[repoID]
		sourceIDs := eventIDs(src, ctx, repoID, days, dtFrom, classCond)
		targetIDs := eventIDs(tgt, ctx, repoID, days, dtFrom, classCond)
		missing = append(missing, diffSorted(sourceIDs, targetIDs)...)
		targetOnly += len(diffSorted(targetIDs, sourceIDs))
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
	orphans := []int64{}
	for _, id := range missing {
		switch classOf(id) {
		case "native":
			stats.native++
		case "orphan":
			stats.orphan++
			orphans = append(orphans, id)
		case "artificial":
			stats.artificial++
		}
	}
	lib.Printf(
		"%s: events: source-only %d (native %d, orphan %d, artificial %d), target-only %d\n",
		prefix, len(missing), stats.native, stats.orphan, stats.artificial, targetOnly,
	)
	if len(missing) == 0 {
		return stats
	}

	// Orphan pushes whose commits the target already has are not copied
	skip := orphanEventsToSkip(ctx, src, tgt, orphans)
	if len(skip) > 0 {
		toCopy := make([]int64, 0, len(missing))
		for _, id := range missing {
			if _, ok := skip[id]; ok {
				stats.skipped++
				stats.orphan--
				continue
			}
			toCopy = append(toCopy, id)
		}
		missing = toCopy
		lib.Printf("%s: skipped %d orphan event(s) whose commits are already present\n", prefix, stats.skipped)
	}
	if len(missing) == 0 {
		return stats
	}

	// Copy in batches, one transaction per batch
	verb := "copied"
	if cfg.dryRun {
		verb = "would copy"
	}
	ppEids := []int64{}
	for _, chunk := range chunkInt64(missing, idBatch) {
		idsLit := int64ArrayLiteral(chunk)
		native := []int64{}
		for _, id := range chunk {
			if classOf(id) == "native" {
				native = append(native, id)
			}
		}
		if cfg.dryRun {
			for _, t := range eventTables {
				stats.addTableStats(t.name, countRows(ctx, src, "from "+t.name+" where "+t.key+" = any("+idsLit+")"), 0)
			}
			for _, table := range dimTables {
				sel := dimensionSelect(ctx, table, idsLit)
				if sel == "" {
					continue
				}
				stats.addTableStats(table, countRows(ctx, src, sel), 0)
			}
			stats.copied += len(chunk)
			ppEids = append(ppEids, chunk...)
			continue
		}
		tx, err := tgt.Begin()
		lib.FatalOnError(err)
		for _, t := range eventTables {
			n, ins := copyRows(ctx, src, tx, t.name, "select * from "+t.name+" where "+t.key+" = any("+idsLit+")")
			stats.addTableStats(t.name, n, ins)
		}
		for _, table := range dimTables {
			sel := dimensionSelect(ctx, table, idsLit)
			if sel == "" {
				continue
			}
			n, ins := copyRows(ctx, src, tx, table, "select "+dimensionColumns(table)+" "+sel)
			stats.addTableStats(table, n, ins)
		}
		if len(native) > 0 {
			nTaken, nFrom, nRemoved, partial := takeOver(ctx, tx, int64ArrayLiteral(native))
			stats.taken += nTaken
			stats.takenFrom += nFrom
			stats.removed += nRemoved
			ppEids = append(ppEids, partial...)
		}
		lib.ExecSQLTxWithErr(tx, ctx, isDistinctSQL(idsLit))
		lib.FatalOnError(tx.Commit())
		stats.copied += len(chunk)
		ppEids = append(ppEids, chunk...)
	}
	for _, t := range eventTables {
		ts := stats.tables[t.name]
		if ts != nil && ts.rows > 0 {
			lib.Printf("%s: table %s: rows %d, inserted %d\n", prefix, t.name, ts.rows, ts.inserted)
		}
	}
	for _, table := range dimTables {
		ts := stats.tables[table]
		if ts != nil && ts.rows > 0 {
			lib.Printf("%s: table %s: rows %d, inserted %d\n", prefix, table, ts.rows, ts.inserted)
		}
	}
	if stats.taken > 0 {
		lib.Printf(
			"%s: taken over %d commit(s) from %d restored push event(s), removed %d emptied restored event(s)\n",
			prefix, stats.taken, stats.takenFrom, stats.removed,
		)
	}
	sort.Slice(ppEids, func(i, j int) bool { return ppEids[i] < ppEids[j] })
	stats.postprocess = len(ppEids)
	lib.Printf("%s: %s %d event(s), inserted %d row(s), postprocess %d event id(s)\n", prefix, verb, stats.copied, stats.inserted, stats.postprocess)
	if !cfg.dryRun && len(ppEids) > 0 {
		lib.RunEventIDsPostprocessDB(ctx, "", ppEids)
	}
	return stats
}

func reconcileDBs() {
	// Environment context parse
	var ctx lib.Ctx
	ctx.Init()
	lib.SetupTimeoutSignal(&ctx)

	target := ctx.PgDB
	if target == "" {
		lib.Fatalf("reconcile_dbs: target database required (PG_DB)")
		return
	}
	cfg, ok := resolveConfig(&ctx, target)
	if !ok {
		return
	}

	// Connect to the target database
	tgt := lib.PgConnDB(&ctx, target)
	defer func() { lib.FatalOnError(tgt.Close()) }()
	dtFrom := lib.ToYMDHMSDate(lib.GetDateAgo(tgt, &ctx, time.Now(), cfg.rangeStr))
	lib.Printf(
		"reconcile_dbs: %s: mode %s (%s), %d source(s): %s, since %s (range '%s'), classes: %s, dry run: %v\n",
		target, cfg.mode, cfg.detail, len(cfg.sources), strings.Join(cfg.sources, ", "), dtFrom, cfg.rangeStr, classesInfo(cfg.artificial), cfg.dryRun,
	)
	targetRepos := repoIDs(tgt, &ctx)

	total := sourceStats{}
	nSources := 0
	for _, source := range cfg.sources {
		src, err := connectSource(&ctx, source, !cfg.explicit)
		if err != nil {
			if isNoDBError(err) {
				lib.Printf("reconcile_dbs: %s <- %s: source database unavailable, skipping: %v\n", target, source, err)
				continue
			}
			lib.FatalOnError(err)
			lib.Fatalf("reconcile_dbs: cannot connect to source database '%s': %+v", source, err)
		}
		stats := reconcileSource(&ctx, &cfg, tgt, src, target, source, targetRepos, dtFrom)
		lib.FatalOnError(src.Close())
		nSources++
		total.copied += stats.copied
		total.inserted += stats.inserted
		total.skipped += stats.skipped
		total.taken += stats.taken
		total.postprocess += stats.postprocess
	}
	verb := "copied"
	if cfg.dryRun {
		verb = "would copy"
	}
	lib.Printf(
		"reconcile_dbs: %s: %d source(s), %s %d event(s), inserted %d row(s), skipped %d orphan event(s), taken over %d commit(s)\n",
		target, nSources, verb, total.copied, total.inserted, total.skipped, total.taken,
	)
}

func main() {
	dtStart := time.Now()
	reconcileDBs()
	dtEnd := time.Now()
	lib.Printf("Time: %v\n", dtEnd.Sub(dtStart))
}
