package main

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	lib "github.com/cncf/devstatscode"
	jsoniter "github.com/json-iterator/go"
	yaml "gopkg.in/yaml.v2"
)

// gitHubUsers - list of GitHub user data from cncf/gitdm.
type gitHubUsers []gitHubUser

// gitHubUser - single GitHug user entry from cncf/gitdm `github_users.json` JSON.
type gitHubUser struct {
	Login       string   `json:"login"`
	Email       string   `json:"email"`
	Affiliation string   `json:"affiliation"`
	Source      string   `json:"source"`
	Name        string   `json:"name"`
	CountryID   *string  `json:"country_id"`
	Sex         *string  `json:"sex"`
	Tz          *string  `json:"tz"`
	SexProb     *float64 `json:"sex_prob"`
	Age         *int     `json:"age"`
}

// AllAcquisitions contain all company acquisitions data
// Acquisition contains acquired company name regular expression and new company name for it.
type allAcquisitions struct {
	Acquisitions [][2]string `yaml:"acquisitions"`
}

// stringSet - set of strings
type stringSet map[string]struct{}

// mapIntSet - this is a map from int to set of string
type mapIntSet map[int]stringSet

// mapStringSet - this is a map from string to Set of strings
type mapStringSet map[string]stringSet

// mapStringIntSet - this is a map from string to map from int to set of string
type mapStringIntSet map[string]mapIntSet

// mapIntArray - this is a map form string to array of ints
type mapIntArray map[string][]int

// mapStringArray - this is a map form string to array of strings
type mapStringArray map[string][]string

// affData - holds single affiliation data
type affData struct {
	Login   string
	Company string
	Source  string
	From    time.Time
	To      time.Time
}

// csData holds country_id, tz, tz_offset, sex, sex_prob, age
type csData struct {
	CountryID *string
	Sex       *string
	Tz        *string
	SexProb   *float64
	TzOffset  *int
	Age       *int
}

// decode emails with ! instead of @
func emailDecode(line string) string {
	re := regexp.MustCompile(`([^\s!]+)!([^\s!]+)`)
	return re.ReplaceAllString(line, `$1@$2`)
}

// returns timezone offset in minutes for a given tz string
func tzOffset(db *sql.DB, ctx *lib.Ctx, ptz *string, cache map[string]*int) *int {
	if ptz == nil {
		return nil
	}
	tz := *ptz
	if tz == "" {
		return nil
	}
	off, ok := cache[tz]
	if ok {
		return off
	}
	rows := lib.QuerySQLWithErr(
		db,
		ctx,
		"select extract(epoch from utc_offset) / 60 "+
			"from pg_timezone_names where name = "+lib.NValue(1)+
			" union select null order by 1 limit 1",
		tz,
	)
	defer func() { lib.FatalOnError(rows.Close()) }()
	var offset *int
	for rows.Next() {
		lib.FatalOnError(rows.Scan(&offset))
	}
	lib.FatalOnError(rows.Err())
	cache[tz] = offset
	return offset
}

// Search for given actor using his/her login
// Returns first author found with maximum ID or sets ok=false when not found
func findActor(db *sql.DB, ctx *lib.Ctx, login string, maybeHide func(string) string) (actor lib.Actor, csd csData, ok bool) {
	//if login == "kkosaka" {
	//	defer func() {
	//		lib.Printf("findActor %s -> (%+v, %+v, %v)\n", login, actor, csd, ok)
	//	}()
	//}
	login = maybeHide(login)
	rows := lib.QuerySQLWithErr(
		db,
		ctx,
		fmt.Sprintf(
			"select id, name, country_id, tz, tz_offset, sex, sex_prob, age from gha_actors where login=%s "+
				"union select id, name, country_id, tz, tz_offset, sex, sex_prob, age from gha_actors where lower(login)=%s "+
				"order by id desc limit 1",
			lib.NValue(1),
			lib.NValue(2),
		),
		login,
		strings.ToLower(login),
	)
	defer func() { lib.FatalOnError(rows.Close()) }()
	var name *string
	for rows.Next() {
		lib.FatalOnError(
			rows.Scan(
				&actor.ID,
				&name,
				&csd.CountryID,
				&csd.Tz,
				&csd.TzOffset,
				&csd.Sex,
				&csd.SexProb,
				&csd.Age,
			),
		)
		actor.Login = login
		if name != nil {
			actor.Name = *name
		}
		ok = true
	}
	lib.FatalOnError(rows.Err())
	return
}

// Search for given actor ID(s) using His/Her login
// Return list of actor IDs correlated with that login (downcased) - search deep by lower(login)/id correlations
func findActors(db *sql.DB, ctx *lib.Ctx, login string, maybeHide func(string) string) (actIDs []int, actLogins []string) {
	login = maybeHide(login)
	//if login == "kkosaka" {
	//	defer func() {
	//		lib.Printf("findActors: %s -> (%+v, %+v)\n", login, actIDs, actLogins)
	//	}()
	//}
	ids := make(map[int]struct{})
	logins := make(map[string]struct{})
	logins[login] = struct{}{}
	actLogins = append(actLogins, login)
	aid := 0
	alogin := ""
	prevIDs := ""
	prevLogins := login
	depth := 0
	for {
		query := "select id from gha_actors where lower(login) in ("
		nLogins := len(logins)
		args := []interface{}{}
		idx := 0
		for aLogin := range logins {
			idx++
			args = append(args, strings.ToLower(aLogin))
			query += lib.NValue(idx)
			if idx != nLogins {
				query += ","
			}
		}
		query += ")"
		rows := lib.QuerySQLWithErr(db, ctx, query, args...)
		for rows.Next() {
			lib.FatalOnError(rows.Scan(&aid))
			ids[aid] = struct{}{}
		}
		if len(ids) == 0 {
			return
		}
		lib.FatalOnError(rows.Err())
		lib.FatalOnError(rows.Close())
		query = "select login from gha_actors where id in ("
		nIDs := len(ids)
		args = []interface{}{}
		idx = 0
		for anID := range ids {
			idx++
			args = append(args, anID)
			query += lib.NValue(idx)
			if idx != nIDs {
				query += ","
			}
		}
		query += ")"
		rows = lib.QuerySQLWithErr(db, ctx, query, args...)
		for rows.Next() {
			lib.FatalOnError(rows.Scan(&alogin))
			alogin = maybeHide(alogin)
			logins[alogin] = struct{}{}
		}
		lib.FatalOnError(rows.Err())
		lib.FatalOnError(rows.Close())
		currLoginsAry := []string{}
		for aLogin := range logins {
			currLoginsAry = append(currLoginsAry, aLogin)
		}
		sort.Strings(currLoginsAry)
		currLogins := strings.Join(currLoginsAry, ",")
		currIDsAry := []string{}
		for aID := range ids {
			currIDsAry = append(currIDsAry, fmt.Sprintf("%d", aID))
		}
		sort.Strings(currIDsAry)
		currIDs := strings.Join(currIDsAry, ",")
		depth++
		if prevLogins == currLogins && prevIDs == currIDs {
			break
		}
		if depth >= 10 {
			lib.Printf("Error (non fatal): gone too deep: logins map: %+v, ids map: %+v\n", logins, ids)
			lib.Printf("Error (non fatal): gone too deep: Logins: '%s'=='%s', IDs: '%s'=='%s'\n", prevLogins, currLogins, prevIDs, currIDs)
			break
		}
		prevLogins = currLogins
		prevIDs = currIDs
	}
	for aID := range ids {
		actIDs = append(actIDs, aID)
	}
	for aLogin := range logins {
		if aLogin == login {
			continue
		}
		actLogins = append(actLogins, aLogin)
	}
	return
}

// returns first value from stringSet
func firstKey(strMap stringSet) string {
	for key := range strMap {
		return key
	}
	return ""
}

// Adds non-existing actor
func addActor(con *sql.DB, ctx *lib.Ctx, login, name string, countryID, sex, tz *string, sexProb *float64, tzOff *int, age *int, maybeHide func(string) string) int {
	hlogin := maybeHide(login)
	name = maybeHide(name)
	aid := lib.HashStrings([]string{login})
	lib.ExecSQLWithErr(con, ctx,
		"insert into gha_actors(id, login, name, country_id, sex, tz, sex_prob, tz_offset, age) "+lib.NValues(9),
		lib.AnyArray{aid, hlogin, lib.TruncToBytes(name, 120), countryID, sex, tz, sexProb, tzOff, age}...,
	)
	return aid
}

// mapCompanyName: maps company name to possibly new company name (when one was acquired by the another)
// If mapping happens, store it in the cache for speed
// stat:
// --- [no_regexp_match, cache] (unmapped)
// Company_name [match_regexp, match_cache]
func mapCompanyName(comMap map[string][2]string, acqMap map[*regexp.Regexp]string, stat map[string][2]int, company string) string {
	res, ok := comMap[company]
	if ok {
		if res[1] == "m" {
			ary := stat[res[0]]
			ary[1]++
			stat[res[0]] = ary
		} else {
			ary := stat["---"]
			ary[1]++
			stat["---"] = ary
		}
		return res[0]
	}
	for re, res := range acqMap {
		if re.MatchString(company) {
			comMap[company] = [2]string{res, "m"}
			ary := stat[res]
			ary[0]++
			stat[res] = ary
			return res
		}
	}
	comMap[company] = [2]string{company, "u"}
	ary := stat["---"]
	ary[0]++
	stat["---"] = ary
	return company
}

// Check if given file was already imported
func alreadyImported(db *sql.DB, ctx *lib.Ctx, fn string) (imported bool, sha string) {
	data, err := lib.ReadFile(ctx, fn)
	if err != nil {
		lib.FatalOnError(err)
		return
	}
	sha = fmt.Sprintf("%x", sha256.Sum256(data))
	rows := lib.QuerySQLWithErr(
		db,
		ctx,
		fmt.Sprintf("select sha from gha_imported_shas where sha=%s", lib.NValue(1)),
		sha,
	)
	defer func() { lib.FatalOnError(rows.Close()) }()
	sha2 := ""
	for rows.Next() {
		lib.FatalOnError(rows.Scan(&sha2))
	}
	lib.FatalOnError(rows.Err())
	return sha2 == sha, sha
}

// Sets given SHA as imported
func setImportedSHA(db *sql.DB, ctx *lib.Ctx, sha string) {
	lib.ExecSQLWithErr(db, ctx, "insert into gha_imported_shas(sha) select "+lib.NValue(1)+" on conflict do nothing", sha)
}

func scoreCSD(csd *csData) (score float64) {
	if csd.CountryID != nil && *csd.CountryID != "" {
		score += 2.0
	}
	if csd.Tz != nil && *csd.Tz != "" {
		score += 1.0
	}
	if csd.TzOffset != nil {
		score += 1.0
	}
	if csd.Sex != nil && (*csd.Sex == "m" || *csd.Sex == "f" || *csd.Sex == "b") {
		score += 1.0
	}
	if csd.SexProb != nil {
		score += *csd.SexProb
	}
	if csd.Age != nil {
		score += 0.5
	}
	return
}

// Imports given JSON file.
func importAffs(jsonFN string) int {
	// Environment context parse
	var ctx lib.Ctx
	ctx.Init()
	lib.SetupTimeoutSignal(&ctx)

	// Files path
	dataPrefix := ctx.DataDir
	if ctx.Local {
		dataPrefix = "./"
	}

	// Handle default file name
	if jsonFN == "" {
		// Local or cron mode?
		jsonFN = dataPrefix + ctx.AffiliationsJSON
	}
	lib.Printf("Importing %s\n", jsonFN)

	// Connect to Postgres DB
	con := lib.PgConn(&ctx)
	defer func() { lib.FatalOnError(con.Close()) }()

	// Check if given file was already imported
	currentSHA := ""
	currentSHA2 := ""
	if ctx.CheckImportedSHA {
		imported, sha := alreadyImported(con, &ctx, jsonFN)
		if imported {
			if ctx.SkipCompanyAcq {
				lib.Printf("%s (SHA: %s) was already imported and skip company acquisitions mode is set, exiting\n", jsonFN, sha)
				return 3
			}
			lib.Printf("%s (SHA: %s) was already imported, checking company acquisitions file import status\n", jsonFN, sha)
		}
		currentSHA = sha
		fn := dataPrefix + ctx.CompanyAcqYaml
		imported2, sha := alreadyImported(con, &ctx, fn)
		if imported2 {
			if imported {
				lib.Printf("%s (SHA: %s) was already imported, %s (SHA: %s) also imported, exiting\n", fn, sha, jsonFN, currentSHA)
				return 3
			}
			lib.Printf("%s (SHA: %s) was already imported, but %s (SHA: %s) wasn't, continuying\n", fn, sha, jsonFN, currentSHA)
		}
		currentSHA2 = sha
		if ctx.OnlyCheckImportedSHA {
			lib.Printf("Returining not-imported state\n")
			return 0
		}
	}

	// Read company acquisitions mapping
	var (
		acqs   allAcquisitions
		acqMap map[*regexp.Regexp]string
		comMap map[string][2]string
		stat   map[string][2]int
	)
	if !ctx.SkipCompanyAcq {
		data, err := lib.ReadFile(&ctx, dataPrefix+ctx.CompanyAcqYaml)
		if err != nil {
			lib.Printf("Cannot read company acquisitions mapping '%v', continuying without\n", err)
		} else {
			lib.FatalOnError(yaml.Unmarshal(data, &acqs))
			if ctx.Debug > 0 {
				lib.Printf("Acquisitions: %+v\n", acqs)
			}
		}
		var re *regexp.Regexp
		acqMap = make(map[*regexp.Regexp]string)
		comMap = make(map[string][2]string)
		stat = make(map[string][2]int)
		srcMap := make(map[string]string)
		resMap := make(map[string]struct{})
		idxMap := make(map[*regexp.Regexp]int)
		for idx, acq := range acqs.Acquisitions {
			re = regexp.MustCompile(acq[0])
			res, ok := srcMap[acq[0]]
			if ok {
				lib.Fatalf("Acquisition number %d '%+v' is already present in the mapping and maps into '%s'", idx, acq, res)
			}
			srcMap[acq[0]] = acq[1]
			_, ok = resMap[acq[1]]
			if ok {
				lib.Fatalf("Acquisition number %d '%+v': some other acquisition already maps into '%s', merge them", idx, acq, acq[1])
			}
			resMap[acq[1]] = struct{}{}
			acqMap[re] = acq[1]
			idxMap[re] = idx
		}
		for re, res := range acqMap {
			i := idxMap[re]
			for idx, acq := range acqs.Acquisitions {
				if re.MatchString(acq[1]) && i != idx {
					lib.Fatalf("Acquisition's number %d '%s' result '%s' matches other acquisition number %d '%s' which maps to '%s', simplify it: '%v' -> '%s'", idx, acq[0], acq[1], i, re, res, acq[0], res)
				}
				if re.MatchString(acq[0]) && res != acq[1] {
					lib.Fatalf("Acquisition's number %d '%s' regexp '%s' matches other acquisition number %d '%s' which maps to '%s': result is different '%s'", idx, acq, acq[0], i, re, res, acq[1])
				}
			}
		}
	}

	// Parse github_users.json
	var users gitHubUsers
	data, err := lib.ReadFile(&ctx, jsonFN)
	if err != nil {
		lib.FatalOnError(err)
		return 1
	}
	lib.FatalOnError(jsoniter.Unmarshal(data, &users))

	// Process users affiliations
	emptyVal := struct{}{}
	loginEmails := make(mapStringSet)
	loginNames := make(mapStringSet)
	loginAffs := make(mapStringIntSet)
	loginCSData := make(map[string]csData)
	tzCache := make(map[string]*int)
	sourceToPrio := map[string]int{"notfound": -20, "domain": -10, "": 0, "config": 10, "manual": 20, "user_manual": 30, "user": 40}
	prioToSource := map[int]string{-20: "notfound", -10: "domain", 0: "", 10: "config", 20: "manual", 30: "user_manual", 40: "user"}
	eNames, eEmails, eAffs := 0, 0, 0
	lib.Printf("Processing %d JSON entries\n", len(users))
	for _, user := range users {
		// Email decode ! --> @
		user.Email = strings.ToLower(emailDecode(user.Email))
		login := strings.ToLower(user.Login)

		// Affiliation source
		source := strings.ToLower(user.Source)
		sourcePrio, ok := sourceToPrio[source]
		if !ok {
			sourcePrio = 0
		}

		// Email
		email := user.Email
		if email != "" {
			_, ok := loginEmails[login]
			if !ok {
				loginEmails[login] = stringSet{}
			}
			loginEmails[login][email] = emptyVal
		} else {
			eEmails++
		}

		// Name
		name := user.Name
		if name != "" {
			_, ok := loginNames[login]
			if !ok {
				loginNames[login] = stringSet{}
			}
			loginNames[login][name] = emptyVal
		} else {
			eNames++
		}

		// Affiliation
		aff := user.Affiliation
		if aff != "NotFound" && aff != "(Unknown)" && aff != "?" && aff != "-" && aff != "" {
			aff = strings.Replace(aff, `"`, "", -1)
			_, ok := loginAffs[login]
			if !ok {
				loginAffs[login] = mapIntSet{}
			}
			_, ok = loginAffs[login][sourcePrio]
			if !ok {
				loginAffs[login][sourcePrio] = stringSet{}
			}
			loginAffs[login][sourcePrio][aff] = emptyVal
		} else {
			eAffs++
		}

		// Country & sex data
		newCsd := csData{
			CountryID: user.CountryID,
			Sex:       user.Sex,
			Tz:        user.Tz,
			SexProb:   user.SexProb,
			TzOffset:  tzOffset(con, &ctx, user.Tz, tzCache),
			Age:       user.Age,
		}
		csd, ok := loginCSData[login]
		if ok {
			newScore := scoreCSD(&newCsd)
			score := scoreCSD(&csd)
			// fmt.Printf("login already has score %f\n", score)
			if newScore > score {
				// fmt.Printf("Got better score %f > %f for %+v vs %+v\n", newScore, score, newCsd, csd)
				loginCSData[login] = newCsd
			}
		} else {
			loginCSData[login] = newCsd
		}
	}
	lib.Printf(
		"Processing non-empty: %d name lists, %d email lists, %d affiliations lists, %d objects\n",
		len(loginNames), len(loginEmails), len(loginAffs), len(loginCSData),
	)
	lib.Printf("Empty/Not found: names: %d, emails: %d, affiliations: %d\n", eNames, eEmails, eAffs)

	if ctx.DryRun {
		lib.Printf("Exiting due to dry-run mode.\n")
		return 2
	}

	// Threads
	thrN := lib.GetThreadsNum(&ctx)
	var (
		mtx       *sync.Mutex
		hmtx      *sync.Mutex
		ch        chan struct{}
		maybeHide func(string) string
		lock      func()
		unlock    func()
	)
	if thrN > 1 {
		if thrN > 10 {
			thrN = 10
		}
		ch = make(chan struct{})
		mtx = &sync.Mutex{}
		hmtx = &sync.Mutex{}
		lock = func() {
			mtx.Lock()
		}
		unlock = func() {
			mtx.Unlock()
		}
		maybeHideInternal := lib.MaybeHideFunc(lib.GetHidden(&ctx, lib.HideCfgFile))
		maybeHide = func(arg string) string {
			hmtx.Lock()
			result := maybeHideInternal(arg)
			hmtx.Unlock()
			return result
		}
	} else {
		maybeHide = lib.MaybeHideFunc(lib.GetHidden(&ctx, lib.HideCfgFile))
		lock = func() {}
		unlock = func() {}
	}
	// Login - Names should be 1:1 (also handle records without name set)
	added, updated, noName, mulNames, notChanged := 0, 0, 0, 0, 0
	processLoginCSData := func(ch chan struct{}, login string, csD csData) {
		if ch != nil {
			defer func() {
				ch <- struct{}{}
			}()
		}
		lock()
		names, foundName := loginNames[login]
		unlock()
		name := ""
		if foundName {
			// Other option would be to join all names via ", " - but it's better to query gha_actors_names then
			name = firstKey(names)
			if len(names) > 1 {
				lock()
				mulNames++
				unlock()
			}
		} else {
			lock()
			noName++
			unlock()
		}
		// Try to find actor by login
		actor, csd, ok := findActor(con, &ctx, login, maybeHide)
		if !ok {
			// If no such actor, add with artificial ID (just like data from pre-2015)
			addActor(con, &ctx, login, name, csD.CountryID, csD.Sex, csD.Tz, csD.SexProb, csD.TzOffset, csD.Age, maybeHide)
			lock()
			added++
			unlock()
		} else {
			if (foundName && name != actor.Name) || !lib.CompareStringPtr(csd.CountryID, csD.CountryID) ||
				!lib.CompareStringPtr(csd.Sex, csD.Sex) || !lib.CompareFloat64Ptr(csd.SexProb, csD.SexProb) ||
				!lib.CompareStringPtr(csd.Tz, csD.Tz) || !lib.CompareIntPtr(csd.TzOffset, csD.TzOffset) ||
				!lib.CompareIntPtr(csd.Age, csD.Age) {
				if foundName {
					// If actor found, but with different name (actually with name == "" after standard GHA import), update name
					// Because there can be the same actor (by id) with different IDs (pre-2015 and post 2015), update His/Her name
					// for all records with this login
					lib.ExecSQLWithErr(con, &ctx,
						"update gha_actors set "+
							"name="+lib.NValue(1)+
							", country_id="+lib.NValue(2)+
							", sex="+lib.NValue(3)+
							", tz="+lib.NValue(4)+
							", sex_prob="+lib.NValue(5)+
							", tz_offset="+lib.NValue(6)+
							", age="+lib.NValue(7)+
							" where lower(login)="+lib.NValue(8),
						lib.AnyArray{
							maybeHide(lib.TruncToBytes(name, 120)),
							csD.CountryID,
							csD.Sex,
							csD.Tz,
							csD.SexProb,
							csD.TzOffset,
							csD.Age,
							strings.ToLower(maybeHide(login)),
						}...,
					)
				} else {
					lib.ExecSQLWithErr(con, &ctx,
						"update gha_actors set "+
							"country_id="+lib.NValue(1)+
							", sex="+lib.NValue(2)+
							", tz="+lib.NValue(3)+
							", sex_prob="+lib.NValue(4)+
							", tz_offset="+lib.NValue(5)+
							", age="+lib.NValue(6)+
							" where lower(login)="+lib.NValue(7),
						lib.AnyArray{
							csD.CountryID,
							csD.Sex,
							csD.Tz,
							csD.SexProb,
							csD.TzOffset,
							csD.Age,
							strings.ToLower(maybeHide(login)),
						}...,
					)
				}
				lock()
				updated++
				unlock()
			} else {
				lock()
				notChanged++
				unlock()
			}
		}
		//if login == "kkosaka" {
		//	lib.Printf("processLoginCSData: %s -> (%s, %+v, %v) -> %+v\n", login, name, names, foundName, actor)
		//}
	}
	if thrN > 1 {
		lib.Printf("Processing using MT%d version\n", thrN)
		nThreads := 0
		for login, csD := range loginCSData {
			go processLoginCSData(ch, login, csD)
			nThreads++
			if nThreads >= thrN {
				<-ch
				nThreads--
			}
		}
		lib.Printf("Final threads join\n")
		for nThreads > 0 {
			<-ch
			nThreads--
		}
	} else {
		lib.Printf("Processing using ST version\n")
		for login, csD := range loginCSData {
			processLoginCSData(nil, login, csD)
		}
	}
	lib.Printf("Added actors: %d, updated actors: %d, empty names: %d, non-unique names: %d, non-changed: %d\n", added, updated, noName, mulNames, notChanged)

	// Main caches
	cacheActIDs := make(mapIntArray)
	cacheActLogins := make(mapStringArray)

	// Login - Possible multiple logins, possibly multiple affs
	findActorData := func(ch chan struct{}, login string) {
		if ch != nil {
			defer func() {
				ch <- struct{}{}
			}()
		}
		actIDs, actLogins := findActors(con, &ctx, login, maybeHide)
		if len(actIDs) < 1 {
			lock()
			csD := loginCSData[login]
			unlock()
			aID := addActor(con, &ctx, login, "", csD.CountryID, csD.Sex, csD.Tz, csD.SexProb, csD.TzOffset, csD.Age, maybeHide)
			lock()
			actIDs = append(actIDs, aID)
			added++
			unlock()
		}
		// Store given login's actor IDs in the case
		lock()
		for _, aLogin := range actLogins {
			cacheActIDs[aLogin] = actIDs
			cacheActLogins[aLogin] = actLogins
		}
		unlock()
	}
	added = 0
	if thrN > 1 {
		nThreads := 0
		for login := range loginAffs {
			go findActorData(ch, login)
			nThreads++
			if nThreads >= thrN {
				<-ch
				nThreads--
			}
		}
		for nThreads > 0 {
			<-ch
			nThreads--
		}
	} else {
		for login := range loginAffs {
			findActorData(nil, login)
		}
	}
	if added > 0 {
		lib.Printf("Unexpected: added actors: %d while caching affiliations\n", added)
	}

	// Handle GitHub login changes
	newLogins, copiedAffs, otherPrios := 0, 0, 0
	for login, prios := range loginAffs {
		actLogins, ok := cacheActLogins[login]
		if !ok {
			continue
		}
		for _, otherLogin := range actLogins {
			if otherLogin == login {
				continue
			}
			// fmt.Printf("found %s correlated to %s\n", otherLogin, login)
			_, ok := loginAffs[otherLogin]
			if !ok {
				loginAffs[otherLogin] = mapIntSet{}
				newLogins++
			}
			for prio, affs := range prios {
				_, ok := loginAffs[otherLogin][prio]
				if !ok {
					loginAffs[otherLogin][prio] = stringSet{}
					otherPrios++
				}
				for aff := range affs {
					_, ok := loginAffs[otherLogin][prio][aff]
					if !ok {
						// fmt.Printf("other login %s (correlated to %s) has no %s affiliation, adding\n", otherLogin, login, aff)
						loginAffs[otherLogin][prio][aff] = emptyVal
						copiedAffs++
					}
				}
			}
		}
	}
	lib.Printf("%d new logins added by correlations, copied affiliations: %d (%d different priority)\n", newLogins, copiedAffs, otherPrios)

	// Login - Email(s) 1:N
	added, allEmails := 0, 0
	processEmails := func(ch chan struct{}, login string, emails stringSet) {
		if ch != nil {
			defer func() {
				ch <- struct{}{}
			}()
		}
		actIDs, actLogins := findActors(con, &ctx, login, maybeHide)
		if len(actIDs) < 1 {
			lock()
			csD := loginCSData[login]
			unlock()
			// Should not happen
			aID := addActor(con, &ctx, login, "", csD.CountryID, csD.Sex, csD.Tz, csD.SexProb, csD.TzOffset, csD.Age, maybeHide)
			lock()
			actIDs = append(actIDs, aID)
			added++
			unlock()
		}
		lock()
		for _, aLogin := range actLogins {
			cacheActIDs[aLogin] = actIDs
			cacheActLogins[aLogin] = actLogins
		}
		unlock()
		for email := range emails {
			// One actor can have multiple emails but...
			// One email can also belong to multiple actors
			// This happens when actor was first defined in pre-2015 era (so He/She have negative ID then)
			// And then in new API era 2015+ that actor was active too (so He/She will
			// have entry with valid GitHub actor_id > 0)
			for _, aid := range actIDs {
				lib.ExecSQLWithErr(con, &ctx,
					lib.InsertIgnore("into gha_actors_emails(actor_id, email) "+lib.NValues(2)),
					lib.AnyArray{aid, maybeHide(lib.TruncToBytes(email, 120))}...,
				)
				lock()
				allEmails++
				unlock()
			}
		}
	}
	if thrN > 1 {
		nThreads := 0
		for login, emails := range loginEmails {
			go processEmails(ch, login, emails)
			nThreads++
			if nThreads >= thrN {
				<-ch
				nThreads--
			}
		}
		for nThreads > 0 {
			<-ch
			nThreads--
		}
	} else {
		for login, emails := range loginEmails {
			processEmails(nil, login, emails)
		}
	}
	if added > 0 {
		lib.Printf("Unexpected: added %d actors while processing emails\n", added)
	}
	lib.Printf("Added up to %d actors emails\n", allEmails)

	// Login - Names(s) 1:N
	allNames := 0
	processNames := func(ch chan struct{}, login string, names stringSet) {
		if ch != nil {
			defer func() {
				ch <- struct{}{}
			}()
		}
		actIDs, actLogins := findActors(con, &ctx, login, maybeHide)
		if len(actIDs) < 1 {
			lib.Fatalf("actor login not found %s", login)
		}
		// Store given login's actor IDs in the case
		lock()
		for _, aLogin := range actLogins {
			cacheActIDs[aLogin] = actIDs
			cacheActLogins[aLogin] = actLogins
		}
		unlock()
		for name := range names {
			// One actor can have multiple names but...
			// One name can also belong to multiple actors
			for _, aid := range actIDs {
				lib.ExecSQLWithErr(con, &ctx,
					lib.InsertIgnore("into gha_actors_names(actor_id, name) "+lib.NValues(2)),
					lib.AnyArray{aid, maybeHide(lib.TruncToBytes(name, 120))}...,
				)
				lock()
				allNames++
				unlock()
			}
		}
	}
	if thrN > 1 {
		nThreads := 0
		for login, names := range loginNames {
			go processNames(ch, login, names)
			nThreads++
			if nThreads >= thrN {
				<-ch
				nThreads--
			}
		}
		for nThreads > 0 {
			<-ch
			nThreads--
		}
	} else {
		for login, names := range loginNames {
			processNames(nil, login, names)
		}
	}
	lib.Printf("Added up to %d actors names\n", allNames)

	// Login - Affiliation should be 1:1, but it is sometimes 1:2 or 1:3
	// There are some ambigous affiliations in github_users.json
	// For such cases we're picking up the one with top source priority
	// If there are still multiple such we're taking one with most entries
	// And then if more than 1 with the same number of entries, then pick up first
	unique, nonUnique, allAffs, nonUniquePrio := 0, 0, 0, 0
	defaultStartDate := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	defaultEndDate := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	companies := make(stringSet)
	var affList []affData
	processAffs := func(ch chan struct{}, login string, prios mapIntSet) {
		if ch != nil {
			defer func() {
				ch <- struct{}{}
			}()
		}
		aPrios := []int{}
		for prio := range prios {
			aPrios = append(aPrios, prio)
		}
		sort.Ints(aPrios)
		nPrios := len(aPrios)
		if nPrios > 1 {
			lock()
			nonUniquePrio++
			unlock()
		}
		maxPrio := aPrios[nPrios-1]
		source, _ := prioToSource[maxPrio]
		affs, _ := prios[maxPrio]
		//fmt.Printf("Prio: %d -> %s\n", maxPrio, source)
		var affsAry []string
		if len(affs) > 1 {
			// This login has different affiliations definitions in the input JSON
			// Look for an affiliation that list most companies
			maxNum := 1
			for aff := range affs {
				num := len(strings.Split(aff, ","))
				if num > maxNum {
					maxNum = num
				}
			}
			// maxNum holds max number of companies listed in any of affiliations
			for aff := range affs {
				ary := strings.Split(aff, ",")
				// Just pick first affiliation definition that lists most companies
				if len(ary) == maxNum {
					affsAry = ary
					break
				}
			}
			// Count this as non-unique
			lock()
			nonUnique++
			unlock()
		} else {
			// This is a good definition, only one list of companies affiliation for this GitHub user login
			affsAry = strings.Split(firstKey(affs), ",")
			lock()
			unique++
			unlock()
		}
		// Affiliation has a form "com1 < dt1, com2 < dt2, ..., com(N-1) < dt(N-1), comN"
		// We have array of companies affiliation with eventual end date: array item is:
		// "company name" or "company name < date", lets iterate and parse it
		prevDate := defaultStartDate
		for _, aff := range affsAry {
			var dtFrom, dtTo time.Time
			aff = strings.TrimSpace(aff)
			ary := strings.Split(aff, "<")
			company := strings.TrimSpace(ary[0])
			if len(ary) > 1 {
				// "company < date" form
				dtFrom = prevDate
				dtTo = lib.TimeParseAny(strings.TrimSpace(ary[1]))
			} else {
				// "company" form
				dtFrom = prevDate
				dtTo = defaultEndDate
			}
			if company == "" {
				continue
			}
			lock()
			companies[company] = emptyVal
			affList = append(affList, affData{Login: login, Company: company, From: dtFrom, To: dtTo, Source: source})
			prevDate = dtTo
			allAffs++
			unlock()
		}
	}
	if thrN > 1 {
		nThreads := 0
		for login, prios := range loginAffs {
			go processAffs(ch, login, prios)
			nThreads++
			if nThreads >= thrN {
				<-ch
				nThreads--
			}
		}
		for nThreads > 0 {
			<-ch
			nThreads--
		}
	} else {
		for login, prios := range loginAffs {
			processAffs(nil, login, prios)
		}
	}
	lib.Printf("Affiliations unique: %d, non-unique: %d, with multiple priorities: %d, all user-company connections: %d\n", unique, nonUnique, nonUniquePrio, allAffs)

	// Add companies
	processCompany := func(ch chan struct{}, company string) {
		if ch != nil {
			defer func() {
				ch <- struct{}{}
			}()
		}
		l := len(company)
		if l > 63 {
			company = lib.StripUnicode(company[:32] + company[l-31:])
		}
		lib.ExecSQLWithErr(con, &ctx,
			lib.InsertIgnore("into gha_companies(name) "+lib.NValues(1)),
			lib.AnyArray{maybeHide(lib.TruncToBytes(company, 160))}...,
		)
		lock()
		mappedCompany := mapCompanyName(comMap, acqMap, stat, company)
		unlock()
		if mappedCompany != company {
			lib.ExecSQLWithErr(con, &ctx,
				lib.InsertIgnore("into gha_companies(name) "+lib.NValues(1)),
				lib.AnyArray{maybeHide(lib.TruncToBytes(mappedCompany, 160))}...,
			)
		}
	}
	if thrN > 1 {
		nThreads := 0
		for company := range companies {
			if company == "" {
				continue
			}
			go processCompany(ch, company)
			nThreads++
			if nThreads >= thrN {
				<-ch
				nThreads--
			}
		}
		for nThreads > 0 {
			<-ch
			nThreads--
		}
	} else {
		for company := range companies {
			if company == "" {
				continue
			}
			processCompany(nil, company)
		}
	}
	lib.Printf("Processed %d companies\n", len(companies))

	// Add affiliations
	added, nonCached, addedAffs := 0, 0, 0
	processRoll := func(ch chan struct{}, aff affData) {
		if ch != nil {
			defer func() {
				ch <- struct{}{}
			}()
		}
		login := aff.Login
		// Check if we have that actor IDs cached
		lock()
		actLogins, okL := cacheActLogins[login]
		actIDs, okI := cacheActIDs[login]
		unlock()
		//if login == "kkosaka" {
		//	lib.Printf("processRoll: %s -> (%+v, %+v)\n", login, actLogins, actIDs)
		//}
		if !okL || !okI {
			actIDs, actLogins = findActors(con, &ctx, login, maybeHide)
			if len(actIDs) < 1 {
				lock()
				csD := loginCSData[login]
				unlock()
				// Should not happen
				aID := addActor(con, &ctx, login, "", csD.CountryID, csD.Sex, csD.Tz, csD.SexProb, csD.TzOffset, csD.Age, maybeHide)
				lock()
				actIDs = append(actIDs, aID)
				added++
				unlock()
			}
			lock()
			for _, aLogin := range actLogins {
				cacheActIDs[aLogin] = actIDs
				cacheActLogins[aLogin] = actLogins
			}
			nonCached++
			unlock()
		}
		company := aff.Company
		if company == "" {
			return
		}
		lock()
		mappedCompany := mapCompanyName(comMap, acqMap, stat, company)
		unlock()
		dtFrom := aff.From
		dtTo := aff.To
		source := lib.TruncToBytes(aff.Source, 30)
		for _, aid := range actIDs {
			lib.ExecSQLWithErr(con, &ctx,
				lib.InsertIgnore(
					"into gha_actors_affiliations(actor_id, company_name, original_company_name, dt_from, dt_to, source) "+lib.NValues(6)),
				lib.AnyArray{aid, maybeHide(lib.TruncToBytes(mappedCompany, 160)), maybeHide(lib.TruncToBytes(company, 160)), dtFrom, dtTo, source}...,
			)
			lock()
			addedAffs++
			unlock()
		}
	}
	if thrN > 1 {
		nThreads := 0
		for _, aff := range affList {
			go processRoll(ch, aff)
			nThreads++
			if nThreads >= thrN {
				<-ch
				nThreads--
			}
		}
		for nThreads > 0 {
			<-ch
			nThreads--
		}
	} else {
		for _, aff := range affList {
			processRoll(nil, aff)
		}
	}
	if added > 0 {
		lib.Printf("Unexpected: added %d actors while processing affiliations\n", added)
	}
	if nonCached > 0 {
		lib.Printf("Unexpected: %d cache misses\n", nonCached)
	}
	lib.Printf("Affiliations added up to: %d\n", addedAffs)
	for company, data := range stat {
		if company == "---" {
			lib.Printf("Non-acquired companies: checked all regexp: %d, cache hit: %d\n", data[0], data[1])
		} else {
			lib.Printf("Mapped to '%s': checked regexp: %d, cache hit: %d\n", company, data[0], data[1])
		}
	}
	for company, data := range comMap {
		if data[1] == "u" {
			continue
		}
		lib.Printf("Used mapping '%s' --> '%s'\n", company, data[0])
	}

	// If check imported flag is set, then mark imported file
	if ctx.CheckImportedSHA {
		setImportedSHA(con, &ctx, currentSHA)
		if !ctx.SkipCompanyAcq {
			setImportedSHA(con, &ctx, currentSHA2)
		}
	}
	return 0
}

func main() {
	dtStart := time.Now()
	ret := 0
	if len(os.Args) < 2 {
		ret = importAffs("")
	} else {
		ret = importAffs(os.Args[1])
	}
	dtEnd := time.Now()
	lib.Printf("Time: %v\n", dtEnd.Sub(dtStart))
	os.Exit(ret)
}
