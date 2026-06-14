package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	lib "github.com/cncf/devstatscode"
	"github.com/lib/pq"
	yaml "gopkg.in/yaml.v2"
)

const allInputDBs = "-all-"

// maxParams is the maximum number of bind parameters a single Postgres query can use.
// The wire protocol stores the parameter count in a 16-bit field, so the limit is 65535.
const maxParams = 65535

func projectDBsForSharedDB(ctx *lib.Ctx) []string {
	dataPrefix := ctx.DataDir
	if ctx.Local {
		dataPrefix = "./"
	}
	data, err := ioutil.ReadFile(dataPrefix + ctx.ProjectsYaml)
	lib.FatalOnError(err)

	var projects lib.AllProjects
	lib.FatalOnError(yaml.Unmarshal(data, &projects))

	type projectDB struct {
		order int
		name  string
		db    string
	}

	projectDBs := []projectDB{}
	for projectName, projectData := range projects.Projects {
		if lib.IsProjectDisabled(ctx, projectName, projectData.Disabled) {
			continue
		}
		if strings.TrimSpace(projectData.SharedDB) != ctx.OutputDB {
			continue
		}
		db := strings.TrimSpace(projectData.PDB)
		if db == "" || db == ctx.OutputDB {
			continue
		}
		projectDBs = append(projectDBs, projectDB{
			order: projectData.Order,
			name:  projectName,
			db:    db,
		})
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

func envFlag(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "t", "true", "y", "yes":
		return true
	}
	return false
}

// parseBatchSize reads BATCH_SIZE used in batch insert mode (default 1000, clamped to [2, 1000]).
// The effective batch is additionally capped at insert time so that rows*columns
// never exceeds maxParams.
func parseBatchSize() int {
	value := strings.TrimSpace(os.Getenv("BATCH_SIZE"))
	if value == "" {
		return 1000
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		lib.Fatalf("invalid BATCH_SIZE=%q: %v", value, err)
	}
	if n < 2 {
		lib.Printf("merge_dbs: BATCH_SIZE=%d is below minimum, using 2\n", n)
		n = 2
	}
	if n > 1000 {
		lib.Printf("merge_dbs: BATCH_SIZE=%d is above maximum, using 1000\n", n)
		n = 1000
	}
	return n
}

// parseParallel reads PARALLEL - the number of tables processed concurrently (default 1, clamped to [1, 16]).
func parseParallel() int {
	value := strings.TrimSpace(os.Getenv("PARALLEL"))
	if value == "" {
		return 1
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		lib.Fatalf("invalid PARALLEL=%q: %v", value, err)
	}
	if n < 1 {
		lib.Printf("merge_dbs: PARALLEL=%d is below minimum, using 1\n", n)
		n = 1
	}
	if n > 16 {
		lib.Printf("merge_dbs: PARALLEL=%d is above maximum, using 16\n", n)
		n = 16
	}
	return n
}

// batchValues builds a multi-row VALUES clause for a batch insert:
// "values ($1,...,$nCols),($nCols+1,...),..." for nRows rows of nCols columns each.
func batchValues(nRows, nCols int) string {
	var sb strings.Builder
	sb.WriteString("values ")
	param := 1
	for r := 0; r < nRows; r++ {
		if r > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('(')
		for c := 0; c < nCols; c++ {
			if c > 0 {
				sb.WriteByte(',')
			}
			sb.WriteByte('$')
			sb.WriteString(strconv.Itoa(param))
			param++
		}
		sb.WriteByte(')')
	}
	return sb.String()
}

func parseMergeDtFrom() (time.Time, bool) {
	dtFrom := strings.TrimSpace(os.Getenv("MERGE_DT_FROM"))
	dtDrom := strings.TrimSpace(os.Getenv("MERGE_DT_DROM"))
	if dtFrom != "" && dtDrom != "" && dtFrom != dtDrom {
		lib.Fatalf("MERGE_DT_FROM and MERGE_DT_DROM are both set but differ: %q != %q", dtFrom, dtDrom)
	}
	if dtFrom == "" {
		dtFrom = dtDrom
	}
	if dtFrom == "" {
		return time.Time{}, false
	}

	formats := []string{
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02 15",
		"2006-01-02",
	}
	for _, format := range formats {
		tm, err := time.Parse(format, dtFrom)
		if err == nil {
			return tm, true
		}
	}
	lib.Fatalf("MERGE_DT_FROM/MERGE_DT_DROM must be YYYY-MM-DD or parseable timestamp, got %q", dtFrom)
	return time.Time{}, false
}

func addWhereCondition(queryRoot, condition string) string {
	if condition == "" {
		return queryRoot
	}
	if strings.Contains(queryRoot, " where ") {
		return queryRoot + " and " + condition
	}
	return queryRoot + " where " + condition
}

func mergeDateCondition(table string) string {
	switch table {
	case "gha_assets",
		"gha_branches",
		"gha_comments",
		"gha_commits",
		"gha_commits_roles",
		"gha_forkees",
		"gha_issues",
		"gha_issues_labels",
		"gha_milestones",
		"gha_pages",
		"gha_payloads",
		"gha_pull_requests",
		"gha_releases",
		"gha_teams":
		return "dup_created_at >= $1"
	case "gha_events",
		"gha_issues_events_labels",
		"gha_issues_pull_requests",
		"gha_texts":
		return "created_at >= $1"
	case "gha_commits_files",
		"gha_repos_langs",
		"gha_skip_commits":
		return "dt >= $1"
	case "gha_issues_assignees",
		"gha_pull_requests_assignees",
		"gha_pull_requests_requested_reviewers",
		"gha_releases_assets",
		"gha_teams_repositories":
		return "event_id in (select id from gha_events where created_at >= $1)"
	default:
		return ""
	}
}

func resolveInputDBs(ctx *lib.Ctx) bool {
	inputDBs := []string{}
	for _, db := range ctx.InputDBs {
		db = strings.TrimSpace(db)
		if db == "" {
			continue
		}
		inputDBs = append(inputDBs, db)
	}
	ctx.InputDBs = inputDBs

	for _, db := range ctx.InputDBs {
		if db == allInputDBs && len(ctx.InputDBs) != 1 {
			lib.Fatalf("%s must be used alone in GHA2DB_INPUT_DBS, got %+v", allInputDBs, ctx.InputDBs)
		}
	}

	allMode := len(ctx.InputDBs) == 1 && ctx.InputDBs[0] == allInputDBs
	skipDBs := parseTableList("SKIP_DBS")
	if len(skipDBs) > 0 && !allMode {
		lib.Fatalf("SKIP_DBS can only be used with GHA2DB_INPUT_DBS=%q", allInputDBs)
	}

	if allMode {
		ctx.InputDBs = projectDBsForSharedDB(ctx)
		if len(skipDBs) > 0 {
			filteredInputDBs := []string{}
			skippedInputDBs := []string{}
			for _, db := range ctx.InputDBs {
				if _, ok := skipDBs[db]; ok {
					skippedInputDBs = append(skippedInputDBs, db)
					continue
				}
				filteredInputDBs = append(filteredInputDBs, db)
			}
			ctx.InputDBs = filteredInputDBs
			lib.Printf(
				"merge_dbs: skipped %d DB(s) using SKIP_DBS=%q: %+v\n",
				len(skippedInputDBs),
				os.Getenv("SKIP_DBS"),
				skippedInputDBs,
			)
		}
		if len(ctx.InputDBs) == 0 {
			lib.Fatalf("no enabled projects in %s have shared_db=%q", ctx.ProjectsYaml, ctx.OutputDB)
		}
		lib.Printf("merge_dbs: expanded GHA2DB_INPUT_DBS=%q to %d DB(s) with shared_db=%q: %+v\n", allInputDBs, len(ctx.InputDBs), ctx.OutputDB, ctx.InputDBs)
	}

	return allMode
}

func isNoDBError(err error) bool {
	if e, ok := err.(*pq.Error); ok {
		return e.Code.Name() == "invalid_catalog_name" || string(e.Code) == "3D000"
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "database") && strings.Contains(msg, "does not exist")
}

func parseTableList(envName string) map[string]struct{} {
	value := strings.TrimSpace(os.Getenv(envName))
	if value == "" {
		return nil
	}
	tables := make(map[string]struct{})
	for _, table := range strings.Split(value, ",") {
		table = strings.TrimSpace(table)
		if table == "" {
			continue
		}
		tables[table] = struct{}{}
	}
	return tables
}

func connectInputDB(ctx *lib.Ctx, db string, allMode, ignoreNoDB bool) (*sql.DB, error) {
	if !(allMode && ignoreNoDB) {
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

func mergePDBs() {
	// Environment context parse
	var ctx lib.Ctx
	ctx.Init()
	lib.SetupTimeoutSignal(&ctx)

	if ctx.OutputDB == "" {
		lib.Fatalf("output database required")
		return
	}
	mergeDtFrom, mergeDtFromSet := parseMergeDtFrom()
	if mergeDtFromSet {
		lib.Printf("merge_dbs date filter: MERGE_DT_FROM=%q; tables without a merge date mapping are copied fully\n", lib.ToYMDHMSDate(mergeDtFrom))
	}
	allMode := resolveInputDBs(&ctx)
	ignoreNoDB := envFlag("IGNORE_NO_DB")
	if ignoreNoDB && !allMode {
		lib.Fatalf("IGNORE_NO_DB=1 can only be used with GHA2DB_INPUT_DBS=%q", allInputDBs)
	}
	if len(ctx.InputDBs) < 1 {
		lib.Fatalf("required at least 1 input database, got %d: %+v", len(ctx.InputDBs), ctx.InputDBs)
		return
	}

	// Connect to input Postgres DBs
	ci := []*sql.DB{}
	iNames := []string{}
	for _, iName := range ctx.InputDBs {
		c, err := connectInputDB(&ctx, iName, allMode, ignoreNoDB)
		if err != nil {
			if allMode && ignoreNoDB && isNoDBError(err) {
				lib.Printf("merge_dbs: skipping unavailable input DB %q due to IGNORE_NO_DB=1: %v\n", iName, err)
				continue
			}
			lib.FatalOnError(err)
		}
		ci = append(ci, c)
		iNames = append(iNames, iName)
	}

	if len(ci) < 1 {
		lib.Fatalf(
			"required at least 1 available input database after filtering/connection, got %d from %+v",
			len(ci), ctx.InputDBs,
		)
	}

	// Defer closing all input connections
	defer func() {
		for _, c := range ci {
			lib.FatalOnError(c.Close())
		}
	}()

	// Connect to the output Postgres DB
	co := lib.PgConnDB(&ctx, ctx.OutputDB)
	// Defer close output connection
	defer func() { lib.FatalOnError(co.Close()) }()

	// process this tables
	// 1st pass uses 1st condition
	// 2nd pass uses 2nd condition
	// "-" means that this pass is skipped
	// Some tables are commented out because we're going to
	// run other tools on merged database to fill them
	tableData := [][]string{
		{"gha_actors", "id > 0", "id <= 0"},
		//{"gha_actors_affiliations", "", "-"},
		//{"gha_actors_emails", "", "-"},
		//{"gha_actors_names", "", "-"},
		{"gha_assets", "", "-"},
		{"gha_branches", "", "-"},
		{"gha_comments", "", "-"},
		{"gha_reviews", "", "-"},
		{"gha_commits", "", "-"},
		{"gha_commits_files", "", "-"},
		{"gha_commits_roles", "", "-"},
		//{"gha_companies", "", "-"},
		//{"gha_computed", "", "-"},
		{"gha_events", "id > 0", "id <= 0"},
		//{"gha_events_commits_files", "", "-"},
		{"gha_forkees", "", "-"},
		{"gha_issues", "id > 0", "id <= 0"},
		{"gha_issues_assignees", "", "-"},
		{"gha_issues_events_labels", "", "-"},
		{"gha_issues_labels", "", "-"},
		{"gha_issues_pull_requests", "", "-"},
		{"gha_labels", "id > 0", "id <= 0"},
		//{"gha_logs", "", "-"},
		{"gha_milestones", "", "-"},
		{"gha_orgs", "", "-"},
		{"gha_pages", "", "-"},
		{"gha_payloads", "event_id > 0", "event_id <= 0"},
		//{"gha_postprocess_scripts", "", "-"},
		{"gha_pull_requests", "", "-"},
		{"gha_pull_requests_assignees", "", "-"},
		{"gha_pull_requests_requested_reviewers", "", "-"},
		{"gha_releases", "", "-"},
		{"gha_releases_assets", "", "-"},
		{"gha_repos", "", "-"},
		{"gha_repo_groups", "", "-"},
		{"gha_repos_langs", "", "-"},
		{"gha_skip_commits", "", "-"},
		{"gha_teams", "", "-"},
		{"gha_teams_repositories", "", "-"},
		{"gha_texts", "", "-"},
		// {"gha_parsed", "", "-"},
		// {"gha_last_computed", "", "-"},
	}

	onlyTables := parseTableList("ONLY_TABLES")
	skipTables := parseTableList("SKIP_TABLES")
	if len(onlyTables) > 0 || len(skipTables) > 0 {
		knownTables := make(map[string]struct{}, len(tableData))
		for _, data := range tableData {
			knownTables[data[0]] = struct{}{}
		}

		for table := range onlyTables {
			if _, ok := knownTables[table]; !ok {
				lib.Fatalf("ONLY_TABLES contains unknown table '%s'", table)
			}
		}
		if len(onlyTables) == 0 {
			for table := range skipTables {
				if _, ok := knownTables[table]; !ok {
					lib.Fatalf("SKIP_TABLES contains unknown table '%s'", table)
				}
			}
		}

		filteredTableData := [][]string{}
		for _, data := range tableData {
			table := data[0]
			if len(onlyTables) > 0 {
				if _, ok := onlyTables[table]; !ok {
					continue
				}
			} else {
				if _, ok := skipTables[table]; ok {
					continue
				}
			}
			filteredTableData = append(filteredTableData, data)
		}
		tableData = filteredTableData
		lib.Printf(
			"merge_dbs table filter: selected %d table(s), ONLY_TABLES=%q, SKIP_TABLES=%q\n",
			len(tableData),
			os.Getenv("ONLY_TABLES"),
			os.Getenv("SKIP_TABLES"),
		)
	}

	// Batch / parallelism configuration (all default to the legacy single-row, single-threaded behavior)
	useBatch := envFlag("USE_BATCH")
	batchSize := 1000
	if useBatch {
		batchSize = parseBatchSize()
	}
	parallel := parseParallel()
	lib.Printf("merge_dbs: USE_BATCH=%v, BATCH_SIZE=%d, PARALLEL=%d (max %d psql params per batch insert)\n", useBatch, batchSize, parallel, maxParams)

	// processTable merges a single table (for a single pass) from all input DBs into the output DB.
	// It is called directly (sequentially) when PARALLEL=1 and from up to PARALLEL goroutines otherwise.
	// All state it uses is either read-only and shared (ctx, co, ci, iNames, ...) or local to this call,
	// so concurrent invocations for different tables are safe.
	processTable := func(pass int, passInfo string, i int, data []string) {
		table := data[0]
		cond := data[pass+1]
		if cond == "-" {
			return
		}
		allRows := 0
		allErrs := 0
		allIns := 0
		for dbi, c := range ci {
			// First get row count
			rc := 0
			queryRoot := "from " + table
			queryArgs := []interface{}{}
			if cond != "" {
				queryRoot += " where " + cond
			}

			if mergeDtFromSet {
				mergeDtCond := mergeDateCondition(table)
				if mergeDtCond != "" {
					queryRoot = addWhereCondition(queryRoot, mergeDtCond)
					queryArgs = append(queryArgs, mergeDtFrom)
				} else if dbi == 0 {
					lib.Printf("merge_dbs date filter: table %s has no merge date mapping, copying all rows\n", table)
				}
			}
			row := lib.QueryRowSQL(c, &ctx, "select count(*) "+queryRoot, queryArgs...)
			lib.FatalOnError(row.Scan(&rc))

			// Now get all data
			lib.Printf(
				"%s: start table: #%d: %s, DB #%d: %s, rows: %d...\n",
				passInfo, i, table, dbi, iNames[dbi], rc,
			)
			rows := lib.QuerySQLWithErr(
				c,
				&ctx,
				"select * "+queryRoot,
				queryArgs...,
			)
			//defer func() { lib.FatalOnError(rows.Close()) }()
			// Now unknown rows, with unknown types
			columns, err := rows.Columns()
			lib.FatalOnError(err)

			// Vals to hold any type as []interface{}
			nColumns := len(columns)
			vals := make([]interface{}, nColumns)
			cols := "("
			for i, col := range columns {
				vals[i] = new(interface{})
				cols += "\"" + col + "\", "
			}
			cols = cols[:len(cols)-2] + ")"

			// Get results into `results` array of maps
			rowCount := 0
			errCount := 0
			insCount := 0
			// For ProgressInfo()
			dtStart := time.Now()
			lastTime := dtStart
			if useBatch {
				// Batch insert mode: insert many rows per statement using
				// "insert into t(cols) values (...),(...),... on conflict do nothing".
				// Conflicts are not raised as errors here, so the number of inserted rows
				// is taken from RowsAffected() and the rest of the batch are collisions.
				// Cap rows per insert so that rows*columns never exceeds maxParams.
				maxRowsByParams := maxParams / nColumns
				if maxRowsByParams < 1 {
					maxRowsByParams = 1
				}
				effBatch := batchSize
				if effBatch > maxRowsByParams {
					effBatch = maxRowsByParams
				}
				if effBatch < batchSize {
					lib.Printf(
						"%s: table #%d %s, DB #%d %s: batch size capped from %d to %d (%d columns, max %d psql params)\n",
						passInfo, i, table, dbi, iNames[dbi], batchSize, effBatch, nColumns, maxParams,
					)
				}
				insertPrefix := "insert into " + table + cols + " "
				// Precompute the VALUES clause for a full batch; only the last (partial) batch differs.
				fullValues := batchValues(effBatch, nColumns)
				batchArgs := make([]interface{}, 0, effBatch*nColumns)
				rowsInBatch := 0
				flush := func() {
					if rowsInBatch == 0 {
						return
					}
					valuesClause := fullValues
					if rowsInBatch != effBatch {
						valuesClause = batchValues(rowsInBatch, nColumns)
					}
					res, err := lib.ExecSQL(co, &ctx, insertPrefix+valuesClause+" on conflict do nothing", batchArgs...)
					if err != nil {
						// "on conflict do nothing" never raises a unique violation, so any error
						// here is a real problem (usually different columns order).
						lib.Printf("Failing batch insert into %s (rows: %d, columns: %d)\n", table, rowsInBatch, nColumns)
						lib.FatalOnError(err)
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
					insCount += ins
					errCount += rowsInBatch - ins
					rowCount += rowsInBatch
					batchArgs = batchArgs[:0]
					rowsInBatch = 0
					lib.ProgressInfo(
						rowCount, rc, dtStart, &lastTime, time.Duration(10)*time.Second,
						fmt.Sprintf("%s: table #%d %s, DB #%d %s", passInfo, i, table, dbi, iNames[dbi]),
					)
				}
				for rows.Next() {
					lib.FatalOnError(rows.Scan(vals...))
					for vi := range vals {
						batchArgs = append(batchArgs, *(vals[vi].(*interface{})))
					}
					rowsInBatch++
					if rowsInBatch >= effBatch {
						flush()
					}
				}
				flush()
			} else {
				for rows.Next() {
					lib.FatalOnError(rows.Scan(vals...))
					_, err := lib.ExecSQL(
						co,
						&ctx,
						"insert into "+table+cols+" "+lib.NValues(nColumns),
						vals...,
					)
					if err != nil {
						switch e := err.(type) {
						case *pq.Error:
							if e.Code.Name() != "unique_violation" {
								// Problem here usually means different columns order because it uses unordered inserts like
								// insert into table_name ($1, $2, $3)
								lib.Printf("Failing values:\n")
								for vi, vv := range vals {
									lib.Printf("%d: %+v\n", vi, reflect.ValueOf(vv).Elem())
								}
								lib.FatalOnError(err)
							}
						default:
							lib.FatalOnError(err)
						}
						errCount++
					} else {
						insCount++
					}
					rowCount++
					lib.ProgressInfo(
						rowCount, rc, dtStart, &lastTime, time.Duration(10)*time.Second,
						fmt.Sprintf("%s: table #%d %s, DB #%d %s", passInfo, i, table, dbi, iNames[dbi]),
					)
				}
			}
			lib.FatalOnError(rows.Err())
			lib.FatalOnError(rows.Close())
			perc := 0.0
			if rowCount > 0 {
				perc = float64(errCount) * 100.0 / (float64(rowCount))
			}
			lib.Printf(
				"%s: done table: #%d: %s, DB #%d: %s, rows: %d, inserted: %d, collisions: %d (%.3f%%)\n",
				passInfo, i, table, dbi, iNames[dbi], rowCount, insCount, errCount, perc,
			)
			allRows += rowCount
			allErrs += errCount
			allIns += insCount
		}
		perc := 0.0
		if allRows > 0 {
			perc = float64(allErrs) * 100.0 / (float64(allRows))
		}
		lib.Printf(
			"%s: done table: #%d: %s, all rows: %d, inserted: %d, collisions: %d (%.3f%%)\n",
			passInfo, i, table, allRows, allIns, allErrs, perc,
		)
	}

	for pass, passInfo := range []string{"1st pass", "2nd pass"} {
		if parallel > 1 {
			// Parallel mode: process up to `parallel` tables concurrently within a pass.
			// Passes stay sequential (we join all tables of a pass before starting the next),
			// preserving the original ordering between the 1st and 2nd pass.
			var wg sync.WaitGroup
			sem := make(chan struct{}, parallel)
			for i, data := range tableData {
				sem <- struct{}{}
				wg.Add(1)
				go func(pass int, passInfo string, i int, data []string) {
					defer wg.Done()
					defer func() { <-sem }()
					processTable(pass, passInfo, i, data)
				}(pass, passInfo, i, data)
			}
			wg.Wait()
		} else {
			// Sequential mode (PARALLEL=1): no goroutines at all, exactly as the legacy code.
			for i, data := range tableData {
				processTable(pass, passInfo, i, data)
			}
		}
	}
}

func main() {
	dtStart := time.Now()
	mergePDBs()
	dtEnd := time.Now()
	lib.Printf("Time: %v\n", dtEnd.Sub(dtStart))
	fmt.Printf("Consider running './devel/remove_db_dups.sh' if you merged into existing database.\n")
}
