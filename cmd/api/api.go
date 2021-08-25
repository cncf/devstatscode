package main

import (
	"database/sql"
	"encoding/json"
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
}

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
	json.NewEncoder(w).Encode(epl)
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

func periodNameToValue(c *sql.DB, ctx *lib.Ctx, periodName string) (periodValue string, err error) {
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
	json.NewEncoder(w).Encode(pl)
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
	period, err := periodNameToValue(c, ctx, params["range"])
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
	json.NewEncoder(w).Encode(pl)
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
	period, err := periodNameToValue(c, ctx, params["range"])
	if err != nil {
		returnError(apiName, w, err)
		return
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
	json.NewEncoder(w).Encode(pl)
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
	paramValue, err := getPayloadStringParam("repository", w, payload, true)
	if paramValue != "" {
		// Repository mode
		apiDevActCntRepos(apiName, project, db, info, w, payload)
		return
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
	period, err := periodNameToValue(c, ctx, params["range"])
	if err != nil {
		returnError(apiName, w, err)
		return
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
	json.NewEncoder(w).Encode(pl)
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
	period, err := periodNameToValue(c, ctx, params["range"])
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
	json.NewEncoder(w).Encode(cpl)
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
	paramValue, err := getPayloadStringParam("repository", w, payload, true)
	if paramValue != "" {
		// Repository mode
		apiDevActCntCompRepos(apiName, project, db, info, w, payload)
		return
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
	period, err := periodNameToValue(c, ctx, params["range"])
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
	json.NewEncoder(w).Encode(cpl)
}

func apiListAPIs(info string, w http.ResponseWriter) {
	apiName := lib.ListAPIs
	lapl := listAPIsPayload{APIs: allAPIs}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(lapl)
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
	json.NewEncoder(w).Encode(lppl)
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
	json.NewEncoder(w).Encode(hpl)
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
	json.NewEncoder(w).Encode(rgpl)
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
	json.NewEncoder(w).Encode(cpl)
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
	json.NewEncoder(w).Encode(rpl)
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
	json.NewEncoder(w).Encode(cpl)
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
	json.NewEncoder(w).Encode(rpl)
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
	json.NewEncoder(w).Encode(pl)
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
	json.NewEncoder(w).Encode(epl)
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
	ctx, c, err := getContextAndDB(w, db)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = c.Close() }()
	query := `
  select
    name,
    value
  from
    spstat
  where
    series = 'pstatall'
    and period = 'y10'
    and name in ('Contributors', 'Contributions')
  `
	rows, err := lib.QuerySQLLogErr(c, ctx, query)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = rows.Close() }()
	sspl := siteStatsPayload{Project: project, DB: db}
	var (
		value float64
		name  string
	)
	for rows.Next() {
		err = rows.Scan(&name, &value)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		if name == "Contributors" {
			sspl.Contributors = int64(value)
		} else {
			sspl.Contributions = int64(value)
		}
	}
	err = rows.Err()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	query = `
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
	rows2, err := lib.QuerySQLLogErr(c, ctx, query)
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	defer func() { _ = rows2.Close() }()
	for rows2.Next() {
		err = rows2.Scan(&value)
		if err != nil {
			returnError(apiName, w, err)
			return
		}
		sspl.BOC = int64(value)
	}
	err = rows2.Err()
	if err != nil {
		returnError(apiName, w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(sspl)
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
	lib.Printf("Request: %s\n", info)
	w.Header().Set("Content-Type", "application/json")
	var (
		pl  apiPayload
		err error
	)
	defer func() {
		lib.Printf("Request(exit): %s err:%v\n", info, err)
	}()
	err = json.NewDecoder(req.Body).Decode(&pl)
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
	lib.Printf("Starting API server\n")
	checkEnv()
	readProjects(&ctx)
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
