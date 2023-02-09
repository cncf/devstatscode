package main

import (
	"database/sql"
	"fmt"
	"html"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	lib "github.com/cncf/devstatscode"
	jsoniter "github.com/json-iterator/go"
	"github.com/rs/cors"
	yaml "gopkg.in/yaml.v2"
)

// allAPIs - List all currently defined APIs
var allAPIs = []string{
	lib.Health,
	lib.ListAPIs,
	lib.ListProjects,
	lib.RepoGroups,
	lib.Ranges,
	lib.Countries,
	lib.Companies,
	lib.Events,
	lib.Repos,
	lib.CompaniesTable,
	lib.ComContribRepoGrp,
	lib.DevActCnt,
	lib.DevActCntComp,
	lib.ComStatsRepoGrp,
	lib.SiteStats,
}

var (
	gNameToDB map[string]string
	gProjects []string
	gMtx      *sync.RWMutex
	gBgMtx    *sync.RWMutex
	gNumBg    = 0
	gMaxBg    = 3
	gBgMap    = map[string]struct{}{}
)

type apiPayload struct {
	API     string                 `json:"api"`
	Payload map[string]interface{} `json:"payload"`
}

type errorPayload struct {
	Error string `json:"error"`
}

type healthPayload struct {
	Project string `json:"project"`
	DB      string `json:"db_name"`
	Events  int    `json:"events"`
}

type listAPIsPayload struct {
	APIs []string `json:"apis"`
}

type listProjectsPayload struct {
	Projects []string `json:"projects"`
}

type eventsPayload struct {
	Project    string      `json:"project"`
	DB         string      `json:"db_name"`
	TimeStamps []time.Time `json:"timestamps"`
	From       string      `json:"from"`
	To         string      `json:"to"`
	Values     []int64     `json:"values"`
}

type siteStatsPayload struct {
	Project       string `json:"project"`
	DB            string `json:"db_name"`
	Contributors  int64  `json:"contributors"`
	Contributions int64  `json:"contributions"`
	BOC           int64  `json:"boc"`
	Committers    int64  `json:"committers"`
	Commits       int64  `json:"commits"`
	Events        int64  `json:"events"`
	Forkers       int64  `json:"forkers"`
	Repositories  int64  `json:"repositories"`
	Stargazers    int64  `json:"stargazers"`
	Countries     int64  `json:"countries"`
	Companies     int64  `json:"companies"`
}

type siteStatsCacheEntry struct {
	dt        time.Time
	siteStats siteStatsPayload
}

var (
	siteStatsCache    = map[[2]string]siteStatsCacheEntry{}
	siteStatsCacheMtx = &sync.Mutex{}
)

type companiesTablePayload struct {
	Project string    `json:"project"`
	DB      string    `json:"db_name"`
	Range   string    `json:"range"`
	Metric  string    `json:"metric"`
	Rank    []int     `json:"rank"`
	Company []string  `json:"company"`
	Number  []float64 `json:"number"`
}

type comContribRepoGrpPayload struct {
	Project              string      `json:"project"`
	DB                   string      `json:"db_name"`
	Period               string      `json:"period"`
	RepositoryGroup      string      `json:"repository_group"`
	Companies            []float64   `json:"companies"`
	Developers           []float64   `json:"developers"`
	CompaniesTimestamps  []time.Time `json:"companies_timestamps"`
	DevelopersTimestamps []time.Time `json:"developers_timestamps"`
}

type devActCntPayload struct {
	Project         string   `json:"project"`
	DB              string   `json:"db_name"`
	Range           string   `json:"range"`
	Metric          string   `json:"metric"`
	RepositoryGroup string   `json:"repository_group"`
	Country         string   `json:"country"`
	GitHubID        string   `json:"github_id"`
	Filter          string   `json:"filter"`
	Rank            []int    `json:"rank"`
	Login           []string `json:"login"`
	Number          []int    `json:"number"`
}

type devActCntReposPayload struct {
	Project    string   `json:"project"`
	DB         string   `json:"db_name"`
	Range      string   `json:"range"`
	Metric     string   `json:"metric"`
	Repository string   `json:"repository"`
	Country    string   `json:"country"`
	GitHubID   string   `json:"github_id"`
	Filter     string   `json:"filter"`
	Rank       []int    `json:"rank"`
	Login      []string `json:"login"`
	Number     []int    `json:"number"`
}

type devActCntCompPayload struct {
	Project         string   `json:"project"`
	DB              string   `json:"db_name"`
	Range           string   `json:"range"`
	Metric          string   `json:"metric"`
	RepositoryGroup string   `json:"repository_group"`
	Country         string   `json:"country"`
	Companies       []string `json:"companies"`
	GitHubID        string   `json:"github_id"`
	Rank            []int    `json:"rank"`
	Login           []string `json:"login"`
	Company         []string `json:"company"`
	Number          []int    `json:"number"`
}

type devActCntCompReposPayload struct {
	Project    string   `json:"project"`
	DB         string   `json:"db_name"`
	Range      string   `json:"range"`
	Metric     string   `json:"metric"`
	Repository string   `json:"repository"`
	Country    string   `json:"country"`
	Companies  []string `json:"companies"`
	GitHubID   string   `json:"github_id"`
	Rank       []int    `json:"rank"`
	Login      []string `json:"login"`
	Company    []string `json:"company"`
	Number     []int    `json:"number"`
}

type comStatsRepoGrpPayload struct {
	Project         string               `json:"project"`
	DB              string               `json:"db_name"`
	Period          string               `json:"period"`
	Metric          string               `json:"metric"`
	RepositoryGroup string               `json:"repository_group"`
	Companies       []string             `json:"companies"`
	From            string               `json:"from"`
	To              string               `json:"to"`
	Values          []map[string]float64 `json:"values"`
	Timestamps      []time.Time          `json:"timestamps"`
}

type repoGroupsPayload struct {
	Project    string   `json:"project"`
	DB         string   `json:"db_name"`
	RepoGroups []string `json:"repo_groups"`
}

type companiesPayload struct {
	Project   string   `json:"project"`
	DB        string   `json:"db_name"`
	Companies []string `json:"companies"`
}

type rangesPayload struct {
	Project string   `json:"project"`
	DB      string   `json:"db_name"`
	Ranges  []string `json:"ranges"`
}

type countriesPayload struct {
	Project   string   `json:"project"`
	DB        string   `json:"db_name"`
	Countries []string `json:"countries"`
}

type reposPayload struct {
	Project    string   `json:"project"`
	DB         string   `json:"db_name"`
	RepoGroups []string `json:"repo_groups"`
	Repos      []string `json:"repos"`
}

func returnError(apiName string, w http.ResponseWriter, err error) {
	errStr := err.Error()
	if !strings.HasPrefix(errStr, "API '") {
		errStr = "API '" + apiName + "': " + errStr
	}
	lib.Printf(errStr + "\n")
	epl := errorPayload{Error: errStr}
	w.WriteHeader(http.StatusBadRequest)
	jsoniter.NewEncoder(w).Encode(epl)
}

func timeParseAny(dtStr string) (time.Time, error) {
	formats := []string{
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02 15",
		"2006-01-02",
		"2006-01",
		"2006",
	}
	for _, format := range formats {
		t, e := time.Parse(format, dtStr)
		if e == nil {
			return t, nil
		}
	}
	err := fmt.Errorf("cannot parse datetime: '%s'", dtStr)
	return time.Now(), err
}

func nameToDB(name string) (db string, err error) {
	gMtx.RLock()
	db, ok := gNameToDB[name]
	gMtx.RUnlock()
	if !ok {
		err = fmt.Errorf("database not found for project '%s'", name)
	}
	return
}

func getContextAndDB(w http.ResponseWriter, db string) (ctx *lib.Ctx, c *sql.DB, err error) {
	var lctx lib.Ctx
	lctx.Init()
	lctx.PgHost = os.Getenv("PG_HOST_RO")
	lctx.PgUser = os.Getenv("PG_USER_RO")
	lctx.PgPass = os.Getenv("PG_PASS_RO")
	lctx.PgDB = db
	lctx.ExecFatal = false
	lctx.ExecOutput = true
	c, err = lib.PgConnErr(&lctx)
	if err != nil {
		return
	}
	ctx = &lctx
	return
}

func handleSharedPayload(w http.ResponseWriter, payload map[string]interface{}) (project, db string, err error) {
	if len(payload) == 0 {
		err = fmt.Errorf("'payload' section empty or missing")
		return
	}
	iproject, ok := payload["project"]
	if !ok {
		err = fmt.Errorf("missing 'project' field in 'payload' section")
		return
	}
	project, ok = iproject.(string)
	if !ok {
		err = fmt.Errorf("'payload' 'project' field '%+v' is not a string", iproject)
		return
	}
	db, err = nameToDB(project)
	if err != nil {
		return
	}
	return
}

func getPayloadStringParam(paramName string, w http.ResponseWriter, payload map[string]interface{}, optional bool) (param string, err error) {
	iparam, ok := payload[paramName]
	if !ok {
		if optional {
			return
		}
		err = fmt.Errorf("missing '%s' field in 'payload' section (optional %v)", paramName, optional)
		return
	}
	param, ok = iparam.(string)
	if !ok {
		err = fmt.Errorf("'payload' '%s' field '%+v'/%T is not a string (optional %v)", paramName, iparam, iparam, optional)
		return
	}
	return
}

func getPayloadStringArrayParam(paramName string, w http.ResponseWriter, payload map[string]interface{}, optional, allowEmpty bool) (param []string, err error) {
	iparam, ok := payload[paramName]
	if !ok {
		if optional {
			return
		}
		err = fmt.Errorf("missing '%s' field in 'payload' section (optional %v, allow empty %v)", paramName, optional, allowEmpty)
		return
	}
	iary, ok := iparam.([]interface{})
	if !ok {
		err = fmt.Errorf("'payload' '%s' field '%+v'/%T is not an array (optional %v, allow empty %v)", paramName, iparam, iparam, optional, allowEmpty)
		return
	}
	for idx, item := range iary {
		s, ok := item.(string)
		if !ok {
			err = fmt.Errorf("'payload' '%s' field '%+v' #%d item '%+v'/%T is not a string (optional %v, allow empty %v)", paramName, iary, idx+1, item, item, optional, allowEmpty)
		}
		param = append(param, s)
	}
	if !allowEmpty && len(param) == 0 {
		err = fmt.Errorf("'payload' '%s' field '%+v' cannot be empty (optional %v, allow empty %v)", paramName, param, optional, allowEmpty)
		return
	}
	return
}

func periodNameToValue(c *sql.DB, ctx *lib.Ctx, periodName string, allowManual bool) (periodValue string, manual bool, err error) {
	if allowManual && strings.HasPrefix(periodName, "range:") {
		ary := strings.Split(periodName[6:], ",")
		if len(ary) != 2 {
			err = fmt.Errorf("range should be specified as 'range:YYYY[-MM[-DD [HH[-MM[-SS]]]]],YYYY[-MM[-DD [HH[-MM[-SS]]]]]'")
			return
		}
		from, e := timeParseAny(ary[0])
		if e != nil {
			err = e
			return
		}
		to, e := timeParseAny(ary[1])
		if e != nil {
			err = e
			return
		}
		sFrom, sTo := lib.ToYMDHMSDate(from), lib.ToYMDHMSDate(to)
		maxDt := lib.DayStart(time.Now().AddDate(0, 0, -1))
		if from.After(maxDt) || to.After(maxDt) || !from.Before(to) {
			err = fmt.Errorf("from (%s) and to (%s) dates must not be after %v, from date must be before to date", sFrom, sTo, maxDt)
			return
		}
		periodValue = "range:" + sFrom + "," + sTo
		manual = true
		return
	}
	rows, err := lib.QuerySQLLogErr(c, ctx, "select quick_ranges_suffix from tquick_ranges where quick_ranges_name = $1", periodName)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		err = rows.Scan(&periodValue)
		if err != nil {
			return
		}
	}
	err = rows.Err()
	if err != nil {
		return
	}
	if periodValue == "" {
		err = fmt.Errorf("invalid period name: '%s'", periodName)
	}
	return
}

func ensureManualData(c *sql.DB, ctx *lib.Ctx, project, db, apiName, metric, period string, reposMode, bg bool) (err error) {
	file, mode, extra := "", "", ""
	switch apiName {
	case lib.DevActCnt, lib.DevActCntComp:
		file, mode = "project_developer_stats", "multi_row_single_column"
		if metric == "approves" {
			if db != lib.GHA {
				err = fmt.Errorf("ensureManualData: approves mode only allowed for kubernetes projectreturn (%s,%s,%s,%s,%s,%v)", project, db, apiName, metric, period, reposMode)
			}
			file = "hist_approvers"
		}
		if metric == "reviews" {
			if db != lib.GHA {
				err = fmt.Errorf("ensureManualData: reviews mode only allowed for kubernetes projectreturn (%s,%s,%s,%s,%s,%v)", project, db, apiName, metric, period, reposMode)
			}
			file = "hist_reviewers"
		}
	default:
		err = fmt.Errorf("ensureManualData: unknown API configuration (%s,%s,%s,%s,%s,%v)", project, db, apiName, metric, period, reposMode)
		return
	}
	if file == "" {
		err = fmt.Errorf("ensureManualData: cannot find manual SQL file for configuration (%s,%s,%s,%s,%s,%v)", project, db, apiName, metric, period, reposMode)
		return
	}
	if reposMode {
		file += "_repos"
	}
	// lib.Printf("file detected: %s\n", file)
	query := ""
	var args []interface{}
	switch file {
	case "hist_reviewers", "hist_approvers", "project_developer_stats":
		extra = "hist,merge_series:hdev"
		query = "select 1 from shdev where period = $1 and series like $2 limit 1"
		args = []interface{}{period, "hdev_" + metric + "%"}
	case "hist_reviewers_repos", "hist_approvers_repos", "project_developer_stats_repos":
		extra = "hist,merge_series:hdev_repos"
		query = "select 1 from shdev_repos where period = $1 and series like $2 limit 1"
		args = []interface{}{period, "hdev_" + metric + "%"}
	default:
		err = fmt.Errorf("ensureManualData: don't know how to check for existing data for configuration (%s,%s,%s,%s,%s,%v)", project, db, apiName, metric, period, reposMode)
		return
	}
	file += ".sql"
	// lib.Printf("query,args: %s,%+v\n", query, args)
	rows, err := lib.QuerySQLLogErr(c, ctx, query, args...)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	dummy := 0
	for rows.Next() {
		err = rows.Scan(&dummy)
		if err != nil {
			return
		}
		break
	}
	err = rows.Err()
	if err != nil {
		return
	}
	// lib.Printf("dummy=%d\n", dummy)
	if dummy != 0 {
		return
	}
	dtNow := lib.ToYMDHDate(time.Now())
	// GHA2DB_PROJECT=project calc_metric multi_row_single_column /etc/gha2db/metrics/project/project_developer_stats.sql '2021-08-25 0' '2021-08-25 0' 'range:2021-08-20,2022' 'hist,merge_series:hdev'
	// range:2021-08-20 00:00:00,2022-01-01 00:00:00
	var key string
	if bg {
		key = project + file + mode + period + extra
	}
	calc := func(bg bool) {
		if bg {
			gBgMtx.Lock()
			gNumBg++
			gBgMap[key] = struct{}{}
			gBgMtx.Unlock()
			defer func() {
				gBgMtx.Lock()
				gNumBg--
				delete(gBgMap, key)
				gBgMtx.Unlock()
			}()
		}
		var data string
		data, err = lib.ExecCommand(
			ctx,
			[]string{
				"calc_metric",
				mode,
				"/etc/gha2db/metrics/" + project + "/" + file,
				dtNow,
				dtNow,
				period,
				extra,
			},
			map[string]string{
				"PG_DB":          db,
				"GHA2DB_PROJECT": project,
			},
		)
		if err != nil {
			return
		}
		lib.Printf("Calculated manually:\n")
		lib.Printf("%s", data)
	}
	if bg {
		gBgMtx.RLock()
		num := gNumBg
		_, runs := gBgMap[key]
		gBgMtx.RUnlock()
		if runs {
			err = fmt.Errorf("configuration already running in background (%s,%s,%s,%s,%s,%v)", project, db, apiName, metric, period, reposMode)
			return
		}
		if num >= gMaxBg {
			err = fmt.Errorf("too many background calculations: %d", num)
			return
		}
		go calc(true)
	} else {
		calc(false)
	}
	return
}

func allRepoGroupNameToValue(c *sql.DB, ctx *lib.Ctx, repoGroupName string) (repoGroupValue string, err error) {
	rows, err := lib.QuerySQLLogErr(c, ctx, "select all_repo_group_value from tall_repo_groups where all_repo_group_name = $1", repoGroupName)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		err = rows.Scan(&repoGroupValue)
		if err != nil {
			return
		}
	}
	err = rows.Err()
	if err != nil {
		return
	}
	if repoGroupValue == "" {
		err = fmt.Errorf("invalid repository_group name: '%s'", repoGroupName)
	}
	return
}

func repoNameToValue(c *sql.DB, ctx *lib.Ctx, repoName string) (repoValue string, err error) {
	rows, err := lib.QuerySQLLogErr(c, ctx, "select repo_value from trepos where repo_name = $1", repoName)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		err = rows.Scan(&repoValue)
		if err != nil {
			return
		}
	}
	err = rows.Err()
	if err != nil {
		return
	}
	if repoValue == "" {
		err = fmt.Errorf("invalid repository name: '%s'", repoName)
	}
	return
}

func allCountryNameToValue(c *sql.DB, ctx *lib.Ctx, countryName string) (countryValue string, err error) {
	rows, err := lib.QuerySQLLogErr(
		c,
		ctx,
		"select sub.value from (select country_value as value, 0 as ord from tcountries "+
			"where country_name = $1 union select 'all', 1 as ord) sub order by sub.ord limit 1",
		countryName,
	)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		err = rows.Scan(&countryValue)
		if err != nil {
			return
		}
	}
	err = rows.Err()
	if err != nil {
		return
	}
	if countryValue == "" || (countryValue == "all" && countryName != lib.ALL) {
		err = fmt.Errorf("invalid country name: '%s'", countryName)
	}
	return
}

func getStringTags(c *sql.DB, ctx *lib.Ctx, tag, col string) (values []string, err error) {
	if col == "" || tag == "" {
		err = fmt.Errorf("tag and col must both be non-empty, got (%s, %s)", tag, col)
		return
	}
	rows, err := lib.QuerySQLLogErr(c, ctx, fmt.Sprintf("select %s from %s", col, tag))
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	value := ""
	for rows.Next() {
		err = rows.Scan(&value)
		if err != nil {
			return
		}
		values = append(values, value)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	return
}

func metricNameToValueMap(db, apiName string) (nameToValue map[string]string, err error) {
	switch apiName {
	case lib.CompaniesTable:
		nameToValue = map[string]string{
			"Commenters":                   "commenters",
			"Comments":                     "comments",
			"Commit commenters":            "commitcommenters",
			"Commits":                      "commits",
			"Committers":                   "committers",
			"Documentation commits":        "documentationcommits",
			"Documentation committers":     "documentationcommitters",
			"Pushers":                      "pushers",
			"GitHub Events":                "events",
			"Forkers":                      "forkers",
			"Issue commenters":             "issuecommenters",
			"Issuers":                      "issues",
			"PR authors":                   "prcreators",
			"PR reviews":                   "prreviewers",
			"Pull requests":                "prs",
			"Contributing in repositories": "repositories",
			"Contributors":                 "contributors",
			"Contributions":                "contributions",
			"Watchers":                     "watchers",
		}
	case lib.ComStatsRepoGrp:
		nameToValue = map[string]string{
			"All activity":          "activity",
			"Active authors":        "authors",
			"Issues created":        "issues",
			"Pull requests created": "prs",
			"Commits":               "commits",
			"Committers":            "committers",
			"Pushers":               "pushers",
			"Pushes":                "pushes",
			"Contributions":         "contributions",
			"Contributors":          "contributors",
			"Comments":              "comments",
		}
	case lib.DevActCnt, lib.DevActCntComp:
		nameToValue = map[string]string{
			"Comments":            "comments",
			"Commit comments":     "commit_comments",
			"Commits":             "commits",
			"GitHub Events":       "events",
			"GitHub pushes":       "pushes",
			"Issue comments":      "issue_comments",
			"Issues":              "issues",
			"PRs":                 "prs",
			"Merged PRs":          "merged_prs",
			"Review comments":     "review_comments",
			"Contributions":       "contributions",
			"Active repositories": "active_repos",
		}
		if db == lib.GHA {
			nameToValue["Approves"] = "approves"
			nameToValue["Reviews"] = "reviews"
		}
	default:
		return nil, fmt.Errorf("metricNameToValueMap: unknown db/api pair: '%s'/'%s'", db, apiName)
	}
	return nameToValue, nil
}

func periodNameToValueMap(db, apiName string) (map[string]string, error) {
	switch apiName {
	case lib.ComContribRepoGrp:
		return map[string]string{
			"7 Days MA":  "d7",
			"28 Days MA": "d28",
			"Week":       "w",
			"Month":      "m",
			"Quarter":    "q",
		}, nil
	case lib.ComStatsRepoGrp:
		return map[string]string{
			"Day":       "d",
			"7 Days MA": "d7",
			"Week":      "w",
			"Month":     "m",
			"Quarter":   "q",
			"Year":      "y",
		}, nil
	default:
		return nil, fmt.Errorf("periodNameToValueMap: unknown db/api pair: '%s'/'%s'", db, apiName)
	}
}

func apiComContribRepoGrp(info string, w http.ResponseWriter, payload map[string]interface{}) {
	apiName := lib.ComContribRepoGrp
	var err error
	project, db, err := handleSharedPayload(w, payload)
	defer func() {
		lib.Printf("%s(exit): project:%s db:%s payload: %+v err:%v\n", apiName, project, db, payload, err)
	}()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	params := map[string]string{"from": "", "to": "", "period": "", "repository_group": ""}
	for paramName := range params {
		paramValue, err := getPayloadStringParam(paramName, w, payload, false)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		params[paramName] = paramValue
	}
	_, err = timeParseAny(params["from"])
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	_, err = timeParseAny(params["to"])
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	periodMap, err := periodNameToValueMap(db, apiName)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	for _, v := range periodMap {
		periodMap[v] = v
	}
	period, ok := periodMap[params["period"]]
	if !ok {
		err = fmt.Errorf("invalid period value: '%s'", params["period"])
		returnError(apiName, w, err)
		return
	}
	ctx, c, err := getContextAndDB(w, db)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = c.Close() }()
	repogroup, err := allRepoGroupNameToValue(c, ctx, params["repository_group"])
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	query := `
  select
    time,
    value
  from
    snum_stats
  where
    time >= $1
    and time < $2
    and period = $3
    and series = $4
  order by
    time
	`
	seriesComps := "nstats" + repogroup + "comps"
	seriesDevs := "nstats" + repogroup + "devs"
	rows, err := lib.QuerySQLLogErr(c, ctx, query, params["from"], params["to"], period, seriesComps)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = rows.Close() }()
	var (
		t                    time.Time
		v                    float64
		companies            []float64
		developers           []float64
		companiesTimestamps  []time.Time
		developersTimestamps []time.Time
	)
	for rows.Next() {
		err = rows.Scan(&t, &v)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		companiesTimestamps = append(companiesTimestamps, t)
		companies = append(companies, v)
	}
	err = rows.Err()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	rows, err = lib.QuerySQLLogErr(c, ctx, query, params["from"], params["to"], period, seriesDevs)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		err = rows.Scan(&t, &v)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		developersTimestamps = append(developersTimestamps, t)
		developers = append(developers, v)
	}
	err = rows.Err()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	pl := comContribRepoGrpPayload{
		Project:              project,
		DB:                   db,
		Period:               params["period"],
		RepositoryGroup:      params["repository_group"],
		Companies:            companies,
		CompaniesTimestamps:  companiesTimestamps,
		Developers:           developers,
		DevelopersTimestamps: developersTimestamps,
	}
	w.WriteHeader(http.StatusOK)
	jsoniter.NewEncoder(w).Encode(pl)
}

func apiCompaniesTable(info string, w http.ResponseWriter, payload map[string]interface{}) {
	apiName := lib.CompaniesTable
	var err error
	project, db, err := handleSharedPayload(w, payload)
	defer func() {
		lib.Printf("%s(exit): project:%s db:%s payload: %+v err:%v\n", apiName, project, db, payload, err)
	}()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	params := map[string]string{"range": "", "metric": ""}
	for paramName := range params {
		paramValue, err := getPayloadStringParam(paramName, w, payload, false)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		params[paramName] = paramValue
	}
	metricMap, err := metricNameToValueMap(db, apiName)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	for _, v := range metricMap {
		metricMap[v] = v
	}
	metric, ok := metricMap[params["metric"]]
	if !ok {
		err = fmt.Errorf("invalid metric value: '%s'", params["metric"])
		returnError(apiName, w, err)
		return
	}
	ctx, c, err := getContextAndDB(w, db)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = c.Close() }()
	period, _, err := periodNameToValue(c, ctx, params["range"], false)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	series := fmt.Sprintf("hcom%s", metric)
	query := `
    select (row_number() over (order by value desc) -1), name, value from shcom where series = $1 and period = $2
	`
	rows, err := lib.QuerySQLLogErr(c, ctx, query, series, period)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = rows.Close() }()
	var (
		rank      int
		company   string
		number    float64
		ranks     []int
		companies []string
		numbers   []float64
	)
	for rows.Next() {
		err = rows.Scan(&rank, &company, &number)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		ranks = append(ranks, rank)
		companies = append(companies, company)
		numbers = append(numbers, number)
	}
	err = rows.Err()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	pl := companiesTablePayload{
		Project: project,
		DB:      db,
		Range:   params["range"],
		Metric:  params["metric"],
		Rank:    ranks,
		Company: companies,
		Number:  numbers,
	}
	w.WriteHeader(http.StatusOK)
	jsoniter.NewEncoder(w).Encode(pl)
}

func apiDevActCntRepos(apiName, project, db, info string, w http.ResponseWriter, payload map[string]interface{}) {
	var err error
	defer func() {
		lib.Printf("%s(exit): project:%s db:%s payload: %+v err:%v\n", apiName, project, db, payload, err)
	}()
	params := map[string]string{"range": "", "metric": "", "repository": "", "country": "", "github_id": ""}
	for paramName := range params {
		paramValue, err := getPayloadStringParam(paramName, w, payload, false)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		params[paramName] = paramValue
	}
	bg := false
	sbg, _ := getPayloadStringParam("bg", w, payload, true)
	if sbg != "" {
		bg = true
	}
	metricMap, err := metricNameToValueMap(db, apiName)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	for _, v := range metricMap {
		metricMap[v] = v
	}
	metric, ok := metricMap[params["metric"]]
	if !ok {
		err = fmt.Errorf("invalid metric value: '%s'", params["metric"])
		returnError(apiName, w, err)
		return
	}
	ctx, c, err := getContextAndDB(w, db)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = c.Close() }()
	repo, err := repoNameToValue(c, ctx, params["repository"])
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	country, err := allCountryNameToValue(c, ctx, params["country"])
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	period, manual, err := periodNameToValue(c, ctx, params["range"], true)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	if manual {
		err = ensureManualData(c, ctx, project, db, apiName, metric, period, true, bg)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
	}
	series := fmt.Sprintf("hdev_%s%s%s", metric, repo, country)
	query := `
   select
     sub."Rank",
     sub.name,
     sub.value
   from (
     select row_number() over (order by sum(value) desc) as "Rank",
       split_part(name, '$$$', 1) as name,
       sum(value) as value
     from
       shdev_repos
     where
       series = $1
       and period = $2
     group by
       split_part(name, '$$$', 1)
   ) sub
	`
	rows, err := lib.QuerySQLLogErr(c, ctx, query, series, period)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = rows.Close() }()
	var (
		rank    int
		login   string
		number  int
		ranks   []int
		logins  []string
		numbers []int
	)
	ghID := params["github_id"]
	for rows.Next() {
		err = rows.Scan(&rank, &login, &number)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		if ghID != "" && login != ghID {
			continue
		}
		ranks = append(ranks, rank)
		logins = append(logins, login)
		numbers = append(numbers, number)
	}
	err = rows.Err()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	if len(ranks) == 0 && ghID != "" {
		returnError(apiName, w, fmt.Errorf("github_id '%s' not found in results", ghID))
		return
	}
	filter := fmt.Sprintf("series:%s period:%s", series, period)
	if ghID != "" {
		filter += " github_id:" + ghID
	}
	pl := devActCntReposPayload{
		Project:    project,
		DB:         db,
		Range:      params["range"],
		Metric:     params["metric"],
		Repository: params["repository"],
		Country:    params["country"],
		GitHubID:   ghID,
		Filter:     filter,
		Rank:       ranks,
		Login:      logins,
		Number:     numbers,
	}
	w.WriteHeader(http.StatusOK)
	jsoniter.NewEncoder(w).Encode(pl)
}

func apiDevActCnt(info string, w http.ResponseWriter, payload map[string]interface{}) {
	apiName := lib.DevActCnt
	var err error
	project, db, err := handleSharedPayload(w, payload)
	defer func() {
		lib.Printf("%s(exit): project:%s db:%s payload: %+v err:%v\n", apiName, project, db, payload, err)
	}()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	if db == "gha" {
		paramValue, _ := getPayloadStringParam("repository", w, payload, true)
		if paramValue != "" {
			// Repository mode
			apiDevActCntRepos(apiName, project, db, info, w, payload)
			return
		}
	}
	params := map[string]string{"range": "", "metric": "", "repository_group": "", "country": "", "github_id": ""}
	for paramName := range params {
		paramValue, err := getPayloadStringParam(paramName, w, payload, false)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		params[paramName] = paramValue
	}
	bg := false
	sbg, _ := getPayloadStringParam("bg", w, payload, true)
	if sbg != "" {
		bg = true
	}
	metricMap, err := metricNameToValueMap(db, apiName)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	for _, v := range metricMap {
		metricMap[v] = v
	}
	metric, ok := metricMap[params["metric"]]
	if !ok {
		err = fmt.Errorf("invalid metric value: '%s'", params["metric"])
		returnError(apiName, w, err)
		return
	}
	ctx, c, err := getContextAndDB(w, db)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = c.Close() }()
	repogroup, err := allRepoGroupNameToValue(c, ctx, params["repository_group"])
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	country, err := allCountryNameToValue(c, ctx, params["country"])
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	period, manual, err := periodNameToValue(c, ctx, params["range"], true)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	if manual {
		err = ensureManualData(c, ctx, project, db, apiName, metric, period, false, bg)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
	}
	series := fmt.Sprintf("hdev_%s%s%s", metric, repogroup, country)
	query := `
   select
     sub."Rank",
     sub.name,
     sub.value
   from (
     select row_number() over (order by sum(value) desc) as "Rank",
       split_part(name, '$$$', 1) as name,
       sum(value) as value
     from
       shdev
     where
       series = $1
       and period = $2
     group by
       split_part(name, '$$$', 1)
   ) sub
	`
	rows, err := lib.QuerySQLLogErr(c, ctx, query, series, period)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = rows.Close() }()
	var (
		rank    int
		login   string
		number  int
		ranks   []int
		logins  []string
		numbers []int
	)
	ghID := params["github_id"]
	for rows.Next() {
		err = rows.Scan(&rank, &login, &number)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		if ghID != "" && login != ghID {
			continue
		}
		ranks = append(ranks, rank)
		logins = append(logins, login)
		numbers = append(numbers, number)
	}
	err = rows.Err()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	if len(ranks) == 0 && ghID != "" {
		returnError(apiName, w, fmt.Errorf("github_id '%s' not found in results", ghID))
		return
	}
	filter := fmt.Sprintf("series:%s period:%s", series, period)
	if ghID != "" {
		filter += " github_id:" + ghID
	}
	pl := devActCntPayload{
		Project:         project,
		DB:              db,
		Range:           params["range"],
		Metric:          params["metric"],
		RepositoryGroup: params["repository_group"],
		Country:         params["country"],
		GitHubID:        ghID,
		Filter:          filter,
		Rank:            ranks,
		Login:           logins,
		Number:          numbers,
	}
	w.WriteHeader(http.StatusOK)
	jsoniter.NewEncoder(w).Encode(pl)
}

func apiDevActCntCompRepos(apiName, project, db, info string, w http.ResponseWriter, payload map[string]interface{}) {
	var err error
	defer func() {
		lib.Printf("%s(exit): project:%s db:%s payload: %+v err:%v\n", apiName, project, db, payload, err)
	}()
	params := map[string]string{"range": "", "metric": "", "repository": "", "country": "", "github_id": ""}
	for paramName := range params {
		paramValue, err := getPayloadStringParam(paramName, w, payload, false)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		params[paramName] = paramValue
	}
	paramsAry := map[string][]string{"companies": {}}
	for paramName := range paramsAry {
		paramValue, err := getPayloadStringArrayParam(paramName, w, payload, false, false)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		paramsAry[paramName] = paramValue
	}
	bg := false
	sbg, _ := getPayloadStringParam("bg", w, payload, true)
	if sbg != "" {
		bg = true
	}
	metricMap, err := metricNameToValueMap(db, apiName)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	for _, v := range metricMap {
		metricMap[v] = v
	}
	metric, ok := metricMap[params["metric"]]
	if !ok {
		err = fmt.Errorf("invalid metric value: '%s'", params["metric"])
		returnError(apiName, w, err)
		return
	}
	ctx, c, err := getContextAndDB(w, db)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = c.Close() }()
	repo, err := repoNameToValue(c, ctx, params["repository"])
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	country, err := allCountryNameToValue(c, ctx, params["country"])
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	period, manual, err := periodNameToValue(c, ctx, params["range"], true)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	companiesParam := paramsAry["companies"]
	if len(companiesParam) == 0 {
		err = fmt.Errorf("you need to specify at least one company, for example 'All'")
		returnError(apiName, w, err)
		return
	}
	if manual {
		err = ensureManualData(c, ctx, project, db, apiName, metric, period, true, bg)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
	}
	var rows *sql.Rows
	series := fmt.Sprintf("hdev_%s%s%s", metric, repo, country)
	query := `
  select
    sub."Rank",
    split_part(sub.name, '$$$', 1),
    split_part(sub.name, '$$$', 2),
    sub.value
  from (
    select row_number() over (order by value desc) as "Rank",
      name,
      value
    from
      shdev_repos
    where
      series = $1
      and period = $2
  `
	if len(companiesParam) == 1 && companiesParam[0] == lib.ALL {
		query += ") sub"
		rows, err = lib.QuerySQLLogErr(c, ctx, query, series, period)
	} else {
		query += " and split_part(name, '$$$', 2) in " + lib.NArray(len(companiesParam), 2) + ") sub"
		rows, err = lib.QuerySQLLogErr(c, ctx, query, toInterfaceArray([]string{series, period}, companiesParam, []string{})...)
	}
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = rows.Close() }()
	var (
		rank      int
		login     string
		company   string
		number    int
		ranks     []int
		logins    []string
		companies []string
		numbers   []int
	)
	ghID := params["github_id"]
	for rows.Next() {
		err = rows.Scan(&rank, &login, &company, &number)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		if ghID != "" && login != ghID {
			continue
		}
		ranks = append(ranks, rank)
		logins = append(logins, login)
		companies = append(companies, company)
		numbers = append(numbers, number)
	}
	err = rows.Err()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	if len(ranks) == 0 && ghID != "" {
		returnError(apiName, w, fmt.Errorf("github_id '%s' not found in results", ghID))
		return
	}
	cpl := devActCntCompReposPayload{
		Project:    project,
		DB:         db,
		Range:      params["range"],
		Metric:     params["metric"],
		Repository: params["repository"],
		Country:    params["country"],
		Companies:  companiesParam,
		GitHubID:   ghID,
		Rank:       ranks,
		Login:      logins,
		Company:    companies,
		Number:     numbers,
	}
	w.WriteHeader(http.StatusOK)
	jsoniter.NewEncoder(w).Encode(cpl)
}

func apiDevActCntComp(info string, w http.ResponseWriter, payload map[string]interface{}) {
	apiName := lib.DevActCntComp
	var err error
	project, db, err := handleSharedPayload(w, payload)
	defer func() {
		lib.Printf("%s(exit): project:%s db:%s payload: %+v err:%v\n", apiName, project, db, payload, err)
	}()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	if db == "gha" {
		paramValue, _ := getPayloadStringParam("repository", w, payload, true)
		if paramValue != "" {
			// Repository mode
			apiDevActCntCompRepos(apiName, project, db, info, w, payload)
			return
		}
	}
	params := map[string]string{"range": "", "metric": "", "repository_group": "", "country": "", "github_id": ""}
	for paramName := range params {
		paramValue, err := getPayloadStringParam(paramName, w, payload, false)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		params[paramName] = paramValue
	}
	bg := false
	sbg, _ := getPayloadStringParam("bg", w, payload, true)
	if sbg != "" {
		bg = true
	}
	paramsAry := map[string][]string{"companies": {}}
	for paramName := range paramsAry {
		paramValue, err := getPayloadStringArrayParam(paramName, w, payload, false, false)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		paramsAry[paramName] = paramValue
	}
	metricMap, err := metricNameToValueMap(db, apiName)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	for _, v := range metricMap {
		metricMap[v] = v
	}
	metric, ok := metricMap[params["metric"]]
	if !ok {
		err = fmt.Errorf("invalid metric value: '%s'", params["metric"])
		returnError(apiName, w, err)
		return
	}
	ctx, c, err := getContextAndDB(w, db)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = c.Close() }()
	repogroup, err := allRepoGroupNameToValue(c, ctx, params["repository_group"])
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	country, err := allCountryNameToValue(c, ctx, params["country"])
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	period, manual, err := periodNameToValue(c, ctx, params["range"], true)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	companiesParam := paramsAry["companies"]
	if len(companiesParam) == 0 {
		err = fmt.Errorf("you need to specify at least one company, for example 'All'")
		returnError(apiName, w, err)
		return
	}
	if manual {
		err = ensureManualData(c, ctx, project, db, apiName, metric, period, false, bg)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
	}
	var rows *sql.Rows
	series := fmt.Sprintf("hdev_%s%s%s", metric, repogroup, country)
	query := `
  select
    sub."Rank",
    split_part(sub.name, '$$$', 1),
    split_part(sub.name, '$$$', 2),
    sub.value
  from (
    select row_number() over (order by value desc) as "Rank",
      name,
      value
    from
      shdev
    where
      series = $1
      and period = $2
  `
	if len(companiesParam) == 1 && companiesParam[0] == lib.ALL {
		query += ") sub"
		rows, err = lib.QuerySQLLogErr(c, ctx, query, series, period)
	} else {
		query += " and split_part(name, '$$$', 2) in " + lib.NArray(len(companiesParam), 2) + ") sub"
		rows, err = lib.QuerySQLLogErr(c, ctx, query, toInterfaceArray([]string{series, period}, companiesParam, []string{})...)
	}
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = rows.Close() }()
	var (
		rank      int
		login     string
		company   string
		number    int
		ranks     []int
		logins    []string
		companies []string
		numbers   []int
	)
	ghID := params["github_id"]
	for rows.Next() {
		err = rows.Scan(&rank, &login, &company, &number)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		if ghID != "" && login != ghID {
			continue
		}
		ranks = append(ranks, rank)
		logins = append(logins, login)
		companies = append(companies, company)
		numbers = append(numbers, number)
	}
	err = rows.Err()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	if len(ranks) == 0 && ghID != "" {
		returnError(apiName, w, fmt.Errorf("github_id '%s' not found in results", ghID))
		return
	}
	cpl := devActCntCompPayload{
		Project:         project,
		DB:              db,
		Range:           params["range"],
		Metric:          params["metric"],
		RepositoryGroup: params["repository_group"],
		Country:         params["country"],
		Companies:       companiesParam,
		GitHubID:        ghID,
		Rank:            ranks,
		Login:           logins,
		Company:         companies,
		Number:          numbers,
	}
	w.WriteHeader(http.StatusOK)
	jsoniter.NewEncoder(w).Encode(cpl)
}

func apiListAPIs(info string, w http.ResponseWriter) {
	apiName := lib.ListAPIs
	lapl := listAPIsPayload{APIs: allAPIs}
	w.WriteHeader(http.StatusOK)
	jsoniter.NewEncoder(w).Encode(lapl)
	lib.Printf("%s(exit)\n", apiName)
}

func apiListProjects(info string, w http.ResponseWriter) {
	apiName := lib.ListProjects
	names := []string{}
	gMtx.RLock()
	for _, name := range gProjects {
		names = append(names, name)
	}
	gMtx.RUnlock()
	lppl := listProjectsPayload{Projects: names}
	w.WriteHeader(http.StatusOK)
	jsoniter.NewEncoder(w).Encode(lppl)
	lib.Printf("%s(exit)\n", apiName)
}

func apiHealth(info string, w http.ResponseWriter, payload map[string]interface{}) {
	apiName := lib.Health
	var err error
	project, db, err := handleSharedPayload(w, payload)
	defer func() {
		lib.Printf("%s(exit): project:%s db:%s payload: %+v err:%v\n", apiName, project, db, payload, err)
	}()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	ctx, c, err := getContextAndDB(w, db)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = c.Close() }()
	rows, err := lib.QuerySQLLogErr(c, ctx, "select count(*) from gha_events")
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = rows.Close() }()
	events := 0
	for rows.Next() {
		err = rows.Scan(&events)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
	}
	err = rows.Err()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	hpl := healthPayload{Project: project, DB: db, Events: events}
	w.WriteHeader(http.StatusOK)
	jsoniter.NewEncoder(w).Encode(hpl)
}

func apiRepoGroups(info string, w http.ResponseWriter, payload map[string]interface{}) {
	apiName := lib.RepoGroups
	var err error
	project, db, err := handleSharedPayload(w, payload)
	defer func() {
		lib.Printf("%s(exit): project:%s db:%s payload: %+v err:%v\n", apiName, project, db, payload, err)
	}()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	params := map[string]string{"raw": ""}
	for paramName := range params {
		paramValue, err := getPayloadStringParam(paramName, w, payload, true)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		params[paramName] = paramValue
	}
	ctx, c, err := getContextAndDB(w, db)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = c.Close() }()
	repoGroups := []string{}
	if params["raw"] == "" {
		repoGroups, err = getStringTags(c, ctx, "tall_repo_groups", "all_repo_group_name")
	} else {
		repoGroups, err = getStringTags(c, ctx, "tall_repo_groups", "all_repo_group_value")
	}
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	rgpl := repoGroupsPayload{Project: project, DB: db, RepoGroups: repoGroups}
	w.WriteHeader(http.StatusOK)
	jsoniter.NewEncoder(w).Encode(rgpl)
}

func apiCompanies(info string, w http.ResponseWriter, payload map[string]interface{}) {
	apiName := lib.Companies
	var err error
	project, db, err := handleSharedPayload(w, payload)
	defer func() {
		lib.Printf("%s(exit): project:%s db:%s payload: %+v err:%v\n", apiName, project, db, payload, err)
	}()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	ctx, c, err := getContextAndDB(w, db)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = c.Close() }()
	companies := []string{}
	companies, err = getStringTags(c, ctx, "tcompanies", "companies_name")
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	cpl := companiesPayload{Project: project, DB: db, Companies: companies}
	w.WriteHeader(http.StatusOK)
	jsoniter.NewEncoder(w).Encode(cpl)
}

func apiRanges(info string, w http.ResponseWriter, payload map[string]interface{}) {
	apiName := lib.Ranges
	var err error
	project, db, err := handleSharedPayload(w, payload)
	defer func() {
		lib.Printf("%s(exit): project:%s db:%s payload: %+v err:%v\n", apiName, project, db, payload, err)
	}()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	params := map[string]string{"raw": ""}
	for paramName := range params {
		paramValue, err := getPayloadStringParam(paramName, w, payload, true)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		params[paramName] = paramValue
	}
	ctx, c, err := getContextAndDB(w, db)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = c.Close() }()
	ranges := []string{}
	if params["raw"] == "" {
		ranges, err = getStringTags(c, ctx, "tquick_ranges", "quick_ranges_name")
	} else {
		ranges, err = getStringTags(c, ctx, "tquick_ranges", "quick_ranges_suffix")
	}
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	rpl := rangesPayload{Project: project, DB: db, Ranges: ranges}
	w.WriteHeader(http.StatusOK)
	jsoniter.NewEncoder(w).Encode(rpl)
}

func apiCountries(info string, w http.ResponseWriter, payload map[string]interface{}) {
	apiName := lib.Countries
	var err error
	project, db, err := handleSharedPayload(w, payload)
	defer func() {
		lib.Printf("%s(exit): project:%s db:%s payload: %+v err:%v\n", apiName, project, db, payload, err)
	}()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	params := map[string]string{"raw": ""}
	for paramName := range params {
		paramValue, err := getPayloadStringParam(paramName, w, payload, true)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		params[paramName] = paramValue
	}
	ctx, c, err := getContextAndDB(w, db)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = c.Close() }()
	countries := []string{}
	if params["raw"] == "" {
		countries, err = getStringTags(c, ctx, "gha_countries", "name")
	} else {
		countries, err = getStringTags(c, ctx, "gha_countries", "code")
	}
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	cpl := countriesPayload{Project: project, DB: db, Countries: countries}
	w.WriteHeader(http.StatusOK)
	jsoniter.NewEncoder(w).Encode(cpl)
}

func toInterfaceArray(beforeArray, stringArray, afterArray []string) (interfaceArray []interface{}) {
	for _, str := range beforeArray {
		interfaceArray = append(interfaceArray, str)
	}
	for _, str := range stringArray {
		interfaceArray = append(interfaceArray, str)
	}
	for _, str := range afterArray {
		interfaceArray = append(interfaceArray, str)
	}
	return
}

func apiRepos(info string, w http.ResponseWriter, payload map[string]interface{}) {
	apiName := lib.Repos
	var err error
	project, db, err := handleSharedPayload(w, payload)
	defer func() {
		lib.Printf("%s(exit): project:%s db:%s payload: %+v err:%v\n", apiName, project, db, payload, err)
	}()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	params := map[string][]string{"repository_group": {}}
	for paramName := range params {
		paramValue, err := getPayloadStringArrayParam(paramName, w, payload, false, false)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		params[paramName] = paramValue
	}
	ctx, c, err := getContextAndDB(w, db)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	repositoryGroupParam := params["repository_group"]
	var rows *sql.Rows
	query := `
    select
      distinct coalesce(case repo_group when '' then 'Not specified' else repo_group end, 'Not specified') as "Repository group",
      name as "Repository"
    from
      gha_repos
    where
      name like '%_/_%'
      and name not like '%/%/%'
  `
	if len(repositoryGroupParam) == 1 && repositoryGroupParam[0] == lib.ALL {
		rows, err = lib.QuerySQLLogErr(c, ctx, query)
	} else {
		query += ` and coalesce(case repo_group when '' then 'Not specified' else repo_group end, 'Not specified') in ` + lib.NArray(len(repositoryGroupParam), 0)
		rows, err = lib.QuerySQLLogErr(c, ctx, query, toInterfaceArray([]string{}, repositoryGroupParam, []string{})...)
	}
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = rows.Close() }()
	repoGroups := []string{}
	repoGroup := ""
	repos := []string{}
	repo := ""
	for rows.Next() {
		err = rows.Scan(&repoGroup, &repo)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		repoGroups = append(repoGroups, repoGroup)
		repos = append(repos, repo)
	}
	err = rows.Err()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	rpl := reposPayload{Project: project, DB: db, RepoGroups: repoGroups, Repos: repos}
	w.WriteHeader(http.StatusOK)
	jsoniter.NewEncoder(w).Encode(rpl)
}

func apiComStatsRepoGrp(info string, w http.ResponseWriter, payload map[string]interface{}) {
	apiName := lib.ComStatsRepoGrp
	var err error
	project, db, err := handleSharedPayload(w, payload)
	defer func() {
		lib.Printf("%s(exit): project:%s db:%s payload: %+v err:%v\n", apiName, project, db, payload, err)
	}()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	params := map[string]string{"from": "", "to": "", "period": "", "metric": "", "repository_group": ""}
	for paramName := range params {
		paramValue, err := getPayloadStringParam(paramName, w, payload, false)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		params[paramName] = paramValue
	}
	_, err = timeParseAny(params["from"])
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	_, err = timeParseAny(params["to"])
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	periodMap, err := periodNameToValueMap(db, apiName)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	for _, v := range periodMap {
		periodMap[v] = v
	}
	period, ok := periodMap[params["period"]]
	if !ok {
		err = fmt.Errorf("invalid period value: '%s'", params["period"])
		returnError(apiName, w, err)
		return
	}
	metricMap, err := metricNameToValueMap(db, apiName)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	for _, v := range metricMap {
		metricMap[v] = v
	}
	metric, ok := metricMap[params["metric"]]
	if !ok {
		err = fmt.Errorf("invalid metric value: '%s'", params["metric"])
		returnError(apiName, w, err)
		return
	}
	paramsAry := map[string][]string{"companies": {}}
	for paramName := range paramsAry {
		paramValue, err := getPayloadStringArrayParam(paramName, w, payload, false, false)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		paramsAry[paramName] = paramValue
	}
	companiesParam := paramsAry["companies"]
	if len(companiesParam) == 0 {
		err = fmt.Errorf("you need to specify at least one company, for example 'All'")
		returnError(apiName, w, err)
		return
	}
	ctx, c, err := getContextAndDB(w, db)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = c.Close() }()
	repogroup, err := allRepoGroupNameToValue(c, ctx, params["repository_group"])
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	query := "select "
	if len(companiesParam) == 1 && companiesParam[0] == lib.ALL {
		query += "*"
	} else {
		query += "time, "
		for _, company := range companiesParam {
			query += "\"" + company + "\", "
		}
		query = query[0 : len(query)-2]
	}
	query += " from scompany_activity where time >= $1 and time < $2 and period = $3 and series = $4 order by time"
	series := "company" + repogroup + metric
	rows, err := lib.QuerySQLLogErr(c, ctx, query, params["from"], params["to"], period, series)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	vals := make([]interface{}, len(columns))
	for i, column := range columns {
		switch column {
		case lib.TimeCol:
			vals[i] = new(time.Time)
		case lib.SeriesCol, lib.PeriodCol:
			vals[i] = new(string)
		default:
			vals[i] = new(float64)
		}
	}
	times := []time.Time{}
	values := []map[string]float64{}
	now := time.Now()
	for rows.Next() {
		err = rows.Scan(vals...)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		vMap := make(map[string]float64)
		for index, val := range vals {
			column := columns[index]
			switch column {
			case lib.TimeCol:
				if val != nil {
					times = append(times, *(val.(*time.Time)))
				} else {
					times = append(times, now)
				}
				continue
			case lib.SeriesCol, lib.PeriodCol:
				continue
			default:
				if val != nil {
					vMap[column] = *val.(*float64)
				} else {
					vMap[column] = 0.0
				}
			}
		}
		values = append(values, vMap)
	}
	err = rows.Err()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	pl := comStatsRepoGrpPayload{
		Project:         project,
		DB:              db,
		From:            params["from"],
		To:              params["to"],
		Period:          params["period"],
		Metric:          params["metric"],
		RepositoryGroup: params["repository_group"],
		Companies:       companiesParam,
		Timestamps:      times,
		Values:          values,
	}
	w.WriteHeader(http.StatusOK)
	jsoniter.NewEncoder(w).Encode(pl)
}

func apiEvents(info string, w http.ResponseWriter, payload map[string]interface{}) {
	apiName := lib.Events
	var err error
	project, db, err := handleSharedPayload(w, payload)
	defer func() {
		lib.Printf("%s(exit): project:%s db:%s payload: %+v err:%v\n", apiName, project, db, payload, err)
	}()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	params := map[string]string{"from": "", "to": ""}
	for paramName := range params {
		paramValue, err := getPayloadStringParam(paramName, w, payload, false)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		params[paramName] = paramValue
	}
	_, err = timeParseAny(params["from"])
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	_, err = timeParseAny(params["to"])
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	ctx, c, err := getContextAndDB(w, db)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = c.Close() }()
	query := `
  select
    time,
    value
  from
    sevents_h
  where
    time >= $1
    and time < $2
  order by
    time
  `
	rows, err := lib.QuerySQLLogErr(c, ctx, query, params["from"], params["to"])
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = rows.Close() }()
	times := []time.Time{}
	values := []int64{}
	var (
		t time.Time
		v int64
	)
	for rows.Next() {
		err = rows.Scan(&t, &v)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		times = append(times, t)
		values = append(values, v)
	}
	err = rows.Err()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	epl := eventsPayload{Project: project, DB: db, TimeStamps: times, Values: values, From: params["from"], To: params["to"]}
	w.WriteHeader(http.StatusOK)
	jsoniter.NewEncoder(w).Encode(epl)
}

func apiSiteStats(info string, w http.ResponseWriter, payload map[string]interface{}) {
	apiName := lib.SiteStats
	var err error
	project, db, err := handleSharedPayload(w, payload)
	defer func() {
		lib.Printf("%s(exit): project:%s db:%s payload: %+v err:%v\n", apiName, project, db, payload, err)
	}()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	key := [2]string{project, db}
	siteStatsCacheMtx.Lock()
	data, ok := siteStatsCache[key]
	siteStatsCacheMtx.Unlock()
	if ok {
		age := time.Now().Sub(data.dt).Seconds()
		if age < 43200 {
			lib.Printf("Using cached value %+v (age is %.0f < 43200)\n", data, age)
			w.WriteHeader(http.StatusOK)
			jsoniter.NewEncoder(w).Encode(data.siteStats)
			return
		}
		siteStatsCacheMtx.Lock()
		delete(siteStatsCache, key)
		siteStatsCacheMtx.Unlock()
	}
	ctx, c, err := getContextAndDB(w, db)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = c.Close() }()
	ch := make(chan error)
	mtx := &sync.Mutex{}
	sspl := siteStatsPayload{Project: project, DB: db}
	go func(ch chan error) {
		var err error
		//lib.Printf("pstatall start\n")
		defer func() {
			ch <- err
			//lib.Printf("pstatall end\n")
		}()
		query := `
  select
    name,
    value
  from
    spstat
  where
    series = 'pstatall'
    and period = 'y10'
    and name in (
      'Contributors', 'Contributions', 'Code committers',
      'Commits', 'Events', 'Forkers',
      'Repositories', 'Stargazers'
    )
  `
		var rows *sql.Rows
		rows, err = lib.QuerySQLLogErr(c, ctx, query)
		if err != nil {
			return
		}
		defer func() { _ = rows.Close() }()
		var (
			value float64
			name  string
		)
		for rows.Next() {
			err = rows.Scan(&name, &value)
			if err != nil {
				return
			}
			//lib.Printf("pstatall > %v %v\n", name, value)
			mtx.Lock()
			switch name {
			case "Contributors":
				sspl.Contributors = int64(value)
			case "Contributions":
				sspl.Contributions = int64(value)
			case "Code committers":
				sspl.Committers = int64(value)
			case "Commits":
				sspl.Commits = int64(value)
			case "Events":
				sspl.Events = int64(value)
			case "Forkers":
				sspl.Forkers = int64(value)
			case "Repositories":
				sspl.Repositories = int64(value)
			case "Stargazers":
				sspl.Stargazers = int64(value)
			default:
				//lib.Printf("site stats: unknown metric: '%s'\n", name)
			}
			mtx.Unlock()
			//lib.Printf("pstatall < %v %v\n", name, value)
		}
		err = rows.Err()
	}(ch)
	go func(ch chan error) {
		//lib.Printf("BOC start\n")
		var err error
		defer func() {
			ch <- err
			//lib.Printf("BOC end\n")
		}()
		query := `
  select
    sum(rl.lang_loc)
  from
    gha_repos r,
    gha_repos_langs rl
  where
    r.name = rl.repo_name
    and (r.name, r.id) = (
      select i.name,
        i.id
      from
        gha_repos i
      where
        i.alias = r.alias
        and i.name like '%_/_%'
        and i.name not like '%/%/%'
      limit 1
    )
  `
		var rows *sql.Rows
		rows, err = lib.QuerySQLLogErr(c, ctx, query)
		if err != nil {
			return
		}
		defer func() { _ = rows.Close() }()
		var value float64
		for rows.Next() {
			err = rows.Scan(&value)
			if err != nil {
				return
			}
			//lib.Printf("BOC > %v\n", value)
			mtx.Lock()
			sspl.BOC = int64(value)
			mtx.Unlock()
			//lib.Printf("BOC < %v\n", value)
		}
		err = rows.Err()
	}(ch)
	go func(ch chan error) {
		//lib.Printf("countries start\n")
		var err error
		defer func() {
			ch <- err
			//lib.Printf("countries end\n")
		}()
		query := `
  select
    count(distinct sub.country_id) as num_countries
  from (
    select
      a.country_id
    from
      gha_events e,
      gha_actors a
    where
      e.actor_id = a.id
      and e.type in (
        'PushEvent', 'PullRequestEvent', 'IssuesEvent', 'PullRequestReviewEvent',
        'CommitCommentEvent', 'IssueCommentEvent', 'PullRequestReviewCommentEvent'
      )
    union select
      a.country_id
    from
      gha_actors a,
      gha_commits c
    where
      (
        c.author_id = a.id
        or c.committer_id = a.id
      )
    union select
      a.country_id
    from
      gha_actors a,
      gha_commits_roles cr
    where
      cr.actor_id = a.id
      and cr.role = 'Co-authored-by'
  ) sub
  `
		var rows *sql.Rows
		rows, err = lib.QuerySQLLogErr(c, ctx, query)
		if err != nil {
			return
		}
		defer func() { _ = rows.Close() }()
		var value float64
		for rows.Next() {
			err = rows.Scan(&value)
			if err != nil {
				return
			}
			//lib.Printf("contries > %v\n", value)
			mtx.Lock()
			sspl.Countries = int64(value)
			mtx.Unlock()
			//lib.Printf("contries < %v\n", value)
		}
		err = rows.Err()
	}(ch)
	go func(ch chan error) {
		//lib.Printf("companies start\n")
		var err error
		defer func() {
			ch <- err
			//lib.Printf("companies end\n")
		}()
		query := `
  select
    count(distinct sub.company_name) as num_companis
  from (
    select
      af.company_name
    from
      gha_events e,
      gha_actors_affiliations af
    where
      e.actor_id = af.actor_id
      and af.dt_from <= e.created_at
      and af.dt_to > e.created_at
      and af.company_name not in ('Independent', 'Unknown', 'NotFound', '')
      and e.type in (
        'PushEvent', 'PullRequestEvent', 'IssuesEvent', 'PullRequestReviewEvent',
        'CommitCommentEvent', 'IssueCommentEvent', 'PullRequestReviewCommentEvent'
      )
    union select
      af.company_name
    from
      gha_actors_affiliations af,
      gha_commits c
    where
      (
        c.author_id = af.actor_id
        or c.committer_id = af.actor_id
      )
      and af.dt_from <= c.dup_created_at
      and af.dt_to > c.dup_created_at
      and af.company_name not in ('Independent', 'Unknown', 'NotFound', '')
    union select
      af.company_name
    from
      gha_actors_affiliations af,
      gha_commits_roles cr
    where
      cr.actor_id = af.actor_id
      and cr.role = 'Co-authored-by'
      and af.dt_from <= c.dup_created_at
      and af.dt_to > c.dup_created_at
      and af.company_name not in ('Independent', 'Unknown', 'NotFound', '')
  ) sub
  `
		var rows *sql.Rows
		rows, err = lib.QuerySQLLogErr(c, ctx, query)
		if err != nil {
			return
		}
		defer func() { _ = rows.Close() }()
		var value float64
		for rows.Next() {
			err = rows.Scan(&value)
			if err != nil {
				return
			}
			//lib.Printf("orgs > %v\n", value)
			mtx.Lock()
			sspl.Companies = int64(value)
			mtx.Unlock()
			//lib.Printf("orgs < %v\n", value)
		}
		err = rows.Err()
	}(ch)
	for i := 0; i < 4; i++ {
		//lib.Printf("before %d\n", i)
		err := <-ch
		//lib.Printf("after %d\n", i)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
	}
	//lib.Printf("out\n")
	w.WriteHeader(http.StatusOK)
	jsoniter.NewEncoder(w).Encode(sspl)
	siteStatsCacheMtx.Lock()
	siteStatsCache[key] = siteStatsCacheEntry{dt: time.Now(), siteStats: sspl}
	siteStatsCacheMtx.Unlock()
}

func requestInfo(r *http.Request) string {
	agent := ""
	hdr := r.Header
	method := r.Method
	path := html.EscapeString(r.URL.Path)
	if hdr != nil {
		uAgentAry, ok := hdr["User-Agent"]
		if ok {
			agent = strings.Join(uAgentAry, ", ")
		}
	}
	if agent != "" {
		return fmt.Sprintf("IP: %s, agent: %s, method: %s, path: %s", r.RemoteAddr, agent, method, path)
	}
	return fmt.Sprintf("IP: %s, method: %s, path: %s", r.RemoteAddr, method, path)
}

func handleAPI(w http.ResponseWriter, req *http.Request) {
	info := requestInfo(req)
	gBgMtx.RLock()
	num := gNumBg
	gBgMtx.RUnlock()
	if num == 0 {
		lib.Printf("Request: %s\n", info)
	} else {
		lib.Printf("Request (%d bg runners): %s\n", num, info)
	}
	w.Header().Set("Content-Type", "application/json")
	var (
		pl  apiPayload
		err error
	)
	defer func() {
		gBgMtx.RLock()
		num := gNumBg
		gBgMtx.RUnlock()
		if num == 0 {
			lib.Printf("Request(exit): %s err:%v\n", info, err)
		} else {
			lib.Printf("Request(exit, %d bg runners): %s err:%v\n", num, info, err)
		}
	}()
	err = jsoniter.NewDecoder(req.Body).Decode(&pl)
	if err != nil {
		returnError("unknown", w, err)
		return
	}
	lib.Printf("Request: %s, Payload: %+v\n", info, pl)
	switch pl.API {
	case lib.Health:
		apiHealth(info, w, pl.Payload)
	case lib.ListAPIs:
		apiListAPIs(info, w)
	case lib.ListProjects:
		apiListProjects(info, w)
	case lib.RepoGroups:
		apiRepoGroups(info, w, pl.Payload)
	case lib.Ranges:
		apiRanges(info, w, pl.Payload)
	case lib.Countries:
		apiCountries(info, w, pl.Payload)
	case lib.Companies:
		apiCompanies(info, w, pl.Payload)
	case lib.Events:
		apiEvents(info, w, pl.Payload)
	case lib.Repos:
		apiRepos(info, w, pl.Payload)
	case lib.CompaniesTable:
		apiCompaniesTable(info, w, pl.Payload)
	case lib.ComContribRepoGrp:
		apiComContribRepoGrp(info, w, pl.Payload)
	case lib.ComStatsRepoGrp:
		apiComStatsRepoGrp(info, w, pl.Payload)
	case lib.DevActCnt:
		apiDevActCnt(info, w, pl.Payload)
	case lib.DevActCntComp:
		apiDevActCntComp(info, w, pl.Payload)
	case lib.SiteStats:
		apiSiteStats(info, w, pl.Payload)
	default:
		err = fmt.Errorf("unknown API '%s'", pl.API)
		returnError("unknown:"+pl.API, w, err)
	}
}

func checkEnv() {
	requiredEnv := []string{"PG_PASS", "PG_PASS_RO", "PG_USER_RO", "PG_HOST_RO"}
	for _, env := range requiredEnv {
		if os.Getenv(env) == "" {
			lib.Fatalf("%s env variable must be set", env)
		}
	}
}

func readProjects(ctx *lib.Ctx) {
	dataPrefix := ctx.DataDir
	if ctx.Local {
		dataPrefix = "./"
	}
	data, err := ioutil.ReadFile(dataPrefix + ctx.ProjectsYaml)
	lib.FatalOnError(err)
	var projects lib.AllProjects
	lib.FatalOnError(yaml.Unmarshal(data, &projects))
	gNameToDB = make(map[string]string)
	for projName, projData := range projects.Projects {
		disabled := projData.Disabled
		if disabled {
			continue
		}
		db := projData.PDB
		gNameToDB[projName] = db
		gNameToDB[projData.FullName] = db
		gNameToDB[projData.PDB] = db
		gProjects = append(gProjects, projData.FullName)
	}
	gMtx = &sync.RWMutex{}
}

func serveAPI() {
	var ctx lib.Ctx
	ctx.Init()
	lib.SetupTimeoutSignal(&ctx)
	lib.Printf("Starting API server\n")
	checkEnv()
	readProjects(&ctx)
	gBgMtx = &sync.RWMutex{}
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGALRM)
	go func() {
		for {
			sig := <-sigs
			lib.Printf("Exiting due to signal %v\n", sig)
			os.Exit(1)
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1", handleAPI)
	handler := cors.AllowAll().Handler(mux)
	lib.FatalOnError(http.ListenAndServe("0.0.0.0:8080", handler))
}

func main() {
	serveAPI()
	lib.Fatalf("serveAPI exited without error, returning error state anyway")
}
