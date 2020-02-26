package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	lib "github.com/cncf/devstatscode"
	yaml "gopkg.in/yaml.v2"
)

var (
	gNameToDB map[string]string
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

type devActCntRepoGrpPayload struct {
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

func returnError(w http.ResponseWriter, err error) {
	lib.Printf("Returning error: %+v\n", err)
	epl := errorPayload{Error: err.Error()}
	w.WriteHeader(http.StatusBadRequest)
	json.NewEncoder(w).Encode(epl)
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

func getContextAndDB(w http.ResponseWriter, db string) (ctx *lib.Ctx, c *sql.DB, passed bool) {
	var lctx lib.Ctx
	lctx.Init()
	lctx.PgHost = os.Getenv("PG_HOST_RO")
	lctx.PgUser = os.Getenv("PG_USER_RO")
	lctx.PgPass = os.Getenv("PG_PASS_RO")
	lctx.PgDB = db
	c, err := lib.PgConnErr(&lctx)
	if err != nil {
		returnError(w, err)
		return
	}
	ctx = &lctx
	passed = true
	return
}

func handleSharedPayload(apiName string, w http.ResponseWriter, payload map[string]interface{}) (project, db string, passed bool) {
	if len(payload) == 0 {
		returnError(w, fmt.Errorf("API '%s' 'payload' section empty or missing", apiName))
		return
	}
	iproject, ok := payload["project"]
	if !ok {
		returnError(w, fmt.Errorf("API '%s' missing 'project' field in 'payload' section", apiName))
		return
	}
	project, ok = iproject.(string)
	if !ok {
		returnError(w, fmt.Errorf("API '%s' 'payload' 'project' field '%+v' is not a string", apiName, iproject))
		return
	}
	db, err := nameToDB(project)
	if err != nil {
		returnError(w, err)
		return
	}
	passed = true
	return
}

func getPayloadStringParam(apiName, paramName string, w http.ResponseWriter, payload map[string]interface{}) (param string, passed bool) {
	iparam, ok := payload[paramName]
	if !ok {
		returnError(w, fmt.Errorf("API '%s' missing '%s' field in 'payload' section", apiName, paramName))
		return
	}
	param, ok = iparam.(string)
	if !ok {
		returnError(w, fmt.Errorf("API '%s' 'payload' '%s' field '%+v' is not a string", apiName, paramName, iparam))
		return
	}
	passed = true
	return
}

func periodNameToValue(c *sql.DB, ctx *lib.Ctx, apiName, periodName string) (periodValue string, err error) {
	rows, err := lib.QuerySQL(c, ctx, "select quick_ranges_suffix from tquick_ranges where quick_ranges_name = $1", periodName)
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
		err = fmt.Errorf("API '%s' invalid period name: '%s'", apiName, periodName)
	}
	return
}

func allRepoGroupNameToValue(c *sql.DB, ctx *lib.Ctx, apiName, repoGroupName string) (repoGroupValue string, err error) {
	rows, err := lib.QuerySQL(c, ctx, "select all_repo_group_value from tall_repo_groups where all_repo_group_name = $1", repoGroupName)
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
		err = fmt.Errorf("API '%s' invalid repository_group name: '%s'", apiName, repoGroupName)
	}
	return
}

func allCountryNameToValue(c *sql.DB, ctx *lib.Ctx, apiName, countryName string) (countryValue string, err error) {
	rows, err := lib.QuerySQL(
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
	if countryValue == "" || (countryValue == "all" && countryName != "All") {
		err = fmt.Errorf("API '%s' invalid country name: '%s'", apiName, countryName)
	}
	return
}

func apiDevActCntRepoGrp(w http.ResponseWriter, payload map[string]interface{}) {
	apiName := lib.DevActCntRepoGrp
	project, db, ok := handleSharedPayload(apiName, w, payload)
	if !ok {
		return
	}
	params := map[string]string{"range": "", "metric": "", "repository_group": "", "country": "", "github_id": ""}
	for paramName := range params {
		paramValue, ok := getPayloadStringParam(apiName, paramName, w, payload)
		if !ok {
			return
		}
		params[paramName] = paramValue
	}
	metricMap := map[string]string{
		"Approves":            "approves",
		"Reviews":             "reviews",
		"Comments":            "comments",
		"Commit comments":     "commit_comments",
		"Commits":             "commits",
		"GitHub Events":       "events",
		"GitHub pushes":       "pushes",
		"Issue comments":      "issue_comments",
		"Issues":              "issues",
		"PRs":                 "prs",
		"Review comments":     "review_comments",
		"Contributions":       "contributions",
		"Active repositories": "active_repos",
	}
	for _, v := range metricMap {
		metricMap[v] = v
	}
	metric, ok := metricMap[params["metric"]]
	if !ok {
		returnError(w, fmt.Errorf("API '%s' invalid metric value: '%s'", apiName, params["metric"]))
		return
	}
	ctx, c, ok := getContextAndDB(w, db)
	if !ok {
		return
	}
	defer func() { _ = c.Close() }()
	repogroup, err := allRepoGroupNameToValue(c, ctx, apiName, params["repository_group"])
	if err != nil {
		returnError(w, err)
		return
	}
	country, err := allCountryNameToValue(c, ctx, apiName, params["country"])
	if err != nil {
		returnError(w, err)
		return
	}
	period, err := periodNameToValue(c, ctx, apiName, params["range"])
	if err != nil {
		returnError(w, err)
		return
	}
	series := fmt.Sprintf("hdev_%s%s%s", metric, repogroup, country)
	query := `
	    select
	      sub."Rank",
	      sub.name as name,
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
	rows, err := lib.QuerySQL(c, ctx, query, series, period)
	if err != nil {
		returnError(w, err)
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
			returnError(w, err)
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
		returnError(w, err)
		return
	}
	filter := fmt.Sprintf("series:%s period:%s", series, period)
	if ghID != "" {
		filter += " github_id:" + ghID
	}
	pl := devActCntRepoGrpPayload{
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
	// RO connections, such operations are impossible
	/*
		_, err = lib.ExecSQL(c, ctx, "insert into gha_repos(id, name) values($1, $2)", 999999999, "xxx")
		if err != nil {
			returnError(w, err)
			return
		}
	*/
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(pl)
}

func apiHealth(w http.ResponseWriter, payload map[string]interface{}) {
	apiName := lib.Health
	project, db, ok := handleSharedPayload(apiName, w, payload)
	if !ok {
		return
	}
	ctx, c, ok := getContextAndDB(w, db)
	if !ok {
		return
	}
	defer func() { _ = c.Close() }()
	rows, err := lib.QuerySQL(c, ctx, "select count(*) from gha_events")
	if err != nil {
		returnError(w, err)
		return
	}
	defer func() { _ = rows.Close() }()
	events := 0
	for rows.Next() {
		err = rows.Scan(&events)
		if err != nil {
			returnError(w, err)
			return
		}
	}
	err = rows.Err()
	if err != nil {
		returnError(w, err)
		return
	}
	hpl := healthPayload{Project: project, DB: db, Events: events}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(hpl)
}

func handleAPI(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var pl apiPayload
	err := json.NewDecoder(req.Body).Decode(&pl)
	if err != nil {
		returnError(w, err)
		return
	}
	switch pl.API {
	case lib.Health:
		apiHealth(w, pl.Payload)
	case lib.DevActCntRepoGrp:
		apiDevActCntRepoGrp(w, pl.Payload)
	default:
		returnError(w, fmt.Errorf("unknown API '%s'", pl.API))
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
	}
	gMtx = &sync.RWMutex{}
}

func serveAPI() {
	var ctx lib.Ctx
	ctx.Init()
	lib.Printf("Starting API serve\n")
	checkEnv()
	readProjects(&ctx)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGALRM)
	go func() {
		for {
			sig := <-sigs
			lib.Fatalf("Exiting due to signal %v\n", sig)
		}
	}()
	http.HandleFunc("/api/v1", handleAPI)
	lib.FatalOnError(http.ListenAndServe("0.0.0.0:8080", nil))
}

func main() {
	serveAPI()
	lib.Fatalf("serveAPI exited without error, returning error state anyway")
}
