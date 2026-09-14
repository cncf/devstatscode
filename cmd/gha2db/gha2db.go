package main

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	lib "github.com/cncf/devstatscode"
	jsoniter "github.com/json-iterator/go"
	yaml "gopkg.in/yaml.v2"
)

var (
	// gGitTrailerPattern - message trailer pattern
	gGitTrailerPattern = lib.GitTrailerPattern
	// gGitAllowedTrailers - allowed commit trailer flags (lowercase/case insensitive -> correct case)
	gGitAllowedTrailers = lib.GitAllowedTrailers
)

// parseJSON - parse signle GHA JSON event
func parseJSON(con *sql.DB, ctx *lib.Ctx, idx, njsons int, jsonStr []byte, dt time.Time, forg, frepo map[string]struct{}, orgRE, repoRE *regexp.Regexp, shas map[string]string) (f int, e int) {
	var (
		h         lib.Event
		hOld      lib.EventOld
		err       error
		fullName  string
		eid       string
		actorName string
	)
	if ctx.OldFormat {
		err = jsoniter.Unmarshal(jsonStr, &hOld)
	} else {
		err = jsoniter.Unmarshal(jsonStr, &h)
	}
	// jsonStr = bytes.Replace(jsonStr, []byte("\x00"), []byte(""), -1)
	if err != nil {
		lib.Printf("Error(%v): %v\n", lib.ToGHADate(dt), err)
		ofn := fmt.Sprintf("jsons/error_%v-%d-%d.json", lib.ToGHADate(dt), idx+1, njsons)
		lib.FatalOnError(ioutil.WriteFile(ofn, jsonStr, 0644))
		lib.Printf("%v: Cannot unmarshal:\n%s\n%v\n", dt, string(jsonStr), err)
		fmt.Fprintf(os.Stderr, "%v: Cannot unmarshal:\n%s\n%v\n", dt, string(jsonStr), err)
		if ctx.AllowBrokenJSON {
			return
		}
		pretty := lib.PrettyPrintJSON(jsonStr)
		lib.Printf("%v: JSON Unmarshal failed for:\n'%v'\n", dt, string(pretty))
		fmt.Fprintf(os.Stderr, "%v: JSON Unmarshal failed for:\n'%v'\n", dt, string(pretty))
	}
	lib.FatalOnError(err)
	if ctx.OldFormat {
		fullName = lib.MakeOldRepoName(&hOld.Repository)
		actorName = hOld.Actor
	} else {
		fullName = h.Repo.Name
		actorName = h.Actor.Login
	}
	if lib.RepoHit(ctx, fullName, forg, frepo, orgRE, repoRE) && lib.ActorHit(ctx, actorName) {
		if ctx.OldFormat {
			eid = fmt.Sprintf("%v", lib.HashStrings([]string{hOld.Type, hOld.Actor, hOld.Repository.Name, lib.ToYMDHMSDate(hOld.CreatedAt)}))
		} else {
			eid = h.ID
		}
		if ctx.JSONOut {
			// We want to Unmarshal/Marshall ALL JSON data, regardless of what is defined in lib.Event
			pretty := lib.PrettyPrintJSON(jsonStr)
			ofn := fmt.Sprintf("jsons/%v_%v.json", dt.Unix(), eid)
			lib.FatalOnError(ioutil.WriteFile(ofn, pretty, 0644))
		}
		if ctx.DBOut {
			if ctx.OldFormat {
				e = lib.WriteToDBOldFmt(con, ctx, eid, &hOld, shas)
			} else {
				e = lib.WriteToDB(con, ctx, &h, shas)
			}
		}
		if ctx.Debug >= 1 {
			lib.Printf("Processed: '%v' event: %v\n", dt, eid)
		}
		f = 1
	}
	return
}

// markAsProcessed mark maximum processed date
func markAsProcessed(con *sql.DB, ctx *lib.Ctx, dt time.Time) {
	if !ctx.DBOut {
		return
	}
	lib.ExecSQLWithErr(
		con,
		ctx,
		lib.InsertIgnore("into gha_parsed(dt) values("+lib.NValue(1)+")"),
		dt,
	)
}

// refreshCommitRoles - process/create gha_commits_roles for all commits in DB
func refreshCommitRoles(ctx *lib.Ctx) {
	// GDPR data hiding
	shaMap := lib.GetHidden(ctx, lib.HideCfgFile)
	maybeHide := lib.MaybeHideFuncTS(shaMap)
	igc := 0
	maybeGC := func(val int) {
		igc++
		if igc%val == 0 {
			runGC()
		}
	}
	// Connect to Postgres DB
	con := lib.PgConn(ctx)
	defer func() { lib.FatalOnError(con.Close()) }()
	now := time.Now()
	offset := 0
	limit := 1000
	// Get number of CPUs available
	thrN := lib.GetThreadsNum(ctx)
	updated := 0
	grandUpdated := 0
	var mtx *sync.Mutex
	var rmtx *sync.RWMutex
	rolesMap := make(map[string]lib.AnyArray)
	addMappingFunc := func(ch chan struct{}, sha string, eventID, repoID int, repoName string, evCreatedAt time.Time, msg string) {
		if ch != nil {
			defer func() { ch <- struct{}{} }()
		}
		kyRoot := sha + "-" + strconv.Itoa(eventID) + "-"
		roleAdded := false
		msg = strings.Replace(msg, "\r", "\n", -1)
		lines := strings.Split(msg, "\n")
		for _, line := range lines {
			line := strings.TrimSpace(line)
			if line == "" {
				continue
			}
			m := lib.MatchGroups(gGitTrailerPattern, line)
			if len(m) == 0 {
				continue
			}
			oTrailer := m["name"]
			lTrailer := strings.ToLower(oTrailer)
			trailers, ok := gGitAllowedTrailers[lTrailer]
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
			id, login := lib.LookupActorNameEmail(con, ctx, name, email, maybeHide)
			// fmt.Printf("got trailer(s) '%s': %+v -> ('%s', '%s', %d, '%s')\n", line, trailers, name, email, id, login)
			for _, role := range trailers {
				ky := kyRoot + role + "-" + strings.ToLower(email)
				if ch != nil {
					rmtx.RLock()
				}
				_, ok := rolesMap[ky]
				if ch != nil {
					rmtx.RUnlock()
				}
				if ok {
					continue
				}
				if ch != nil {
					rmtx.Lock()
				}
				rolesMap[ky] = lib.AnyArray{
					sha,
					eventID,
					role,
					id,
					maybeHide(lib.TruncToBytes(login, 120)),
					maybeHide(lib.TruncToBytes(name, 160)),
					maybeHide(lib.TruncToBytes(email, 160)),
					repoID,
					repoName,
					evCreatedAt,
				}
				if ch != nil {
					rmtx.Unlock()
				}
				roleAdded = true
			}
		}
		if roleAdded {
			if ch != nil {
				mtx.Lock()
			}
			updated++
			if ch != nil {
				mtx.Unlock()
			}
		}
	}
	firstLoop := true
	allCommits := 0
	for {
		// role, actor_id, actor_login, actor_name, actor_email, "+
		rows := lib.QuerySQLWithErr(
			con,
			ctx,
			fmt.Sprintf("select distinct sha, event_id, dup_repo_id, dup_repo_name, dup_created_at, message "+
				"from gha_commits where (sha, event_id) not in (select sha, event_id from gha_commits_roles) "+
				"order by sha, event_id limit %d offset %d",
				limit,
				offset,
			),
		)
		shas, eventIDs, repoIDs, repoNames, evCreatedAts, msgs := []string{}, []int{}, []int{}, []string{}, []time.Time{}, []string{}
		sha, eventID, repoID, repoName, evCreatedAt, msg := "", 0, 0, "", now, ""
		for rows.Next() {
			lib.FatalOnError(rows.Scan(&sha, &eventID, &repoID, &repoName, &evCreatedAt, &msg))
			shas = append(shas, sha)
			eventIDs = append(eventIDs, eventID)
			repoIDs = append(repoIDs, repoID)
			repoNames = append(repoNames, repoName)
			evCreatedAts = append(evCreatedAts, evCreatedAt)
			msgs = append(msgs, msg)
		}
		lib.FatalOnError(rows.Err())
		lib.FatalOnError(rows.Close())
		nCommits := len(shas)
		if firstLoop {
			allCommits = nCommits
		}
		if nCommits == 0 {
			break
		}
		if nCommits == limit && firstLoop {
			firstLoop = false
			arows := lib.QuerySQLWithErr(
				con,
				ctx,
				"select count(distinct sha || event_id) from gha_commits "+
					"where (sha, event_id) not in (select sha, event_id from gha_commits_roles)",
			)
			for arows.Next() {
				lib.FatalOnError(arows.Scan(&allCommits))
				break
			}
			lib.FatalOnError(arows.Err())
			lib.FatalOnError(arows.Close())
		}
		nCache := lib.EmailNameCacheLen()
		lib.Printf("Processing %d commits (all: %d) using %d CPUs, cached: %d\n", nCommits, allCommits, thrN, nCache)
		maybeGC(10)
		updated = 0
		// MT or ST
		prc := 0
		if thrN > 1 {
			ch := make(chan struct{})
			mtx = &sync.Mutex{}
			rmtx = &sync.RWMutex{}
			nThreads := 0
			for i, sha := range shas {
				eventID := eventIDs[i]
				repoID := repoIDs[i]
				repoName := repoNames[i]
				evCreatedAt := evCreatedAts[i]
				msg := msgs[i]
				go addMappingFunc(ch, sha, eventID, repoID, repoName, evCreatedAt, msg)
				nThreads++
				for nThreads >= thrN {
					_ = <-ch
					nThreads--
					prc++
					if prc%20 == 0 {
						thrN = lib.GetThreadsNum(ctx)
					}
				}
			}
			for nThreads > 0 {
				_ = <-ch
				nThreads--
			}
		} else {
			for i, sha := range shas {
				eventID := eventIDs[i]
				repoID := repoIDs[i]
				repoName := repoNames[i]
				evCreatedAt := evCreatedAts[i]
				msg := msgs[i]
				addMappingFunc(nil, sha, eventID, repoID, repoName, evCreatedAt, msg)
			}
		}
		grandUpdated += updated
		lib.Printf("Processed %d/%d commits using %d CPUs (%d so far, offset %d)\n", updated, nCommits, thrN, grandUpdated, offset)
		offset += limit
	}
	nRols := len(rolesMap)
	lib.Printf("Processed %d commits with at least 1 commit role\n", grandUpdated)
	lib.Printf("Now updating/inserting %d commit roles\n", nRols)
	updateFunc := func(ch chan struct{}, data lib.AnyArray) {
		if ch != nil {
			defer func() { ch <- struct{}{} }()
		}
		lib.ExecSQLWithErr(
			con,
			ctx,
			lib.InsertIgnore(
				"into gha_commits_roles("+
					"sha, event_id, role, actor_id, actor_login, actor_name, actor_email, "+
					"dup_repo_id, dup_repo_name, dup_created_at"+
					") "+lib.NValues(10)),
			data...,
		)
	}
	idx := 0
	if thrN > 8 {
		thrN = 8
	}
	prc := 0
	if thrN > 1 {
		ch := make(chan struct{})
		nThreads := 0
		for _, data := range rolesMap {
			idx++
			if idx%limit == 0 {
				lib.Printf("Updating/inserting commit roles: %d/%d\n", idx, nRols)
				maybeGC(20)
			}
			go updateFunc(ch, data)
			nThreads++
			for nThreads >= thrN {
				_ = <-ch
				nThreads--
				prc++
				if prc%20 == 0 {
					thrN = lib.GetThreadsNum(ctx)
				}
			}
		}
		for nThreads > 0 {
			_ = <-ch
			nThreads--
		}
	} else {
		for _, data := range rolesMap {
			idx++
			if idx%limit == 0 {
				lib.Printf("Updating/inserting commit roles: %d/%d\n", idx, nRols)
				maybeGC(20)
			}
			updateFunc(nil, data)
		}
	}
}

// updateCommitRoles - try to find missing actor IDs/Logins in gha_commits_roles table
func updateCommitRoles(ctx *lib.Ctx) {
	// GDPR data hiding
	shaMap := lib.GetHidden(ctx, lib.HideCfgFile)
	maybeHide := lib.MaybeHideFuncTS(shaMap)
	// Connect to Postgres DB
	con := lib.PgConn(ctx)
	defer func() { lib.FatalOnError(con.Close()) }()
	rows := lib.QuerySQLWithErr(
		con,
		ctx,
		"select distinct actor_email, actor_name from gha_commits_roles where actor_id = 0 or actor_login = '' or actor_id is null or actor_login is null",
	)
	defer func() { lib.FatalOnError(rows.Close()) }()
	emails, names, email, name := []string{}, []string{}, "", ""
	for rows.Next() {
		lib.FatalOnError(rows.Scan(&email, &name))
		emails = append(emails, email)
		names = append(names, name)
	}
	lib.FatalOnError(rows.Err())
	// Get number of CPUs available
	thrN := lib.GetThreadsNum(ctx)
	nRoles := len(emails)
	var mtx *sync.Mutex
	lib.Printf("Processing %d commit roles using %d CPUs\n", nRoles, thrN)
	updated := 0
	updateFunc := func(ch chan struct{}, name, email string) {
		if ch != nil {
			defer func() { ch <- struct{}{} }()
		}
		// fmt.Printf("Processing (%s,%s)\n", email, name)
		id, login := lib.LookupActorNameEmail(con, ctx, name, email, maybeHide)
		if id != 0 {
			// fmt.Printf("Got (%d,%s) for (%s,%s)\n", id, login, email, name)
			lib.ExecSQLWithErr(
				con,
				ctx,
				fmt.Sprintf(
					"update gha_commits_roles set actor_id=%s, actor_login=%s where actor_name=%s and actor_email=%s",
					lib.NValue(1),
					lib.NValue(2),
					lib.NValue(3),
					lib.NValue(4),
				),
				lib.AnyArray{
					id,
					maybeHide(login),
					maybeHide(name),
					maybeHide(email),
				}...,
			)
			if ch != nil {
				mtx.Lock()
			}
			updated++
			if ch != nil {
				mtx.Unlock()
			}
		}
	}
	// MT or ST
	prc := 0
	if thrN > 1 {
		ch := make(chan struct{})
		mtx = &sync.Mutex{}
		nThreads := 0
		for i, name := range names {
			email := emails[i]
			go updateFunc(ch, name, email)
			nThreads++
			for nThreads >= thrN {
				_ = <-ch
				nThreads--
				prc++
				if prc%20 == 0 {
					thrN = lib.GetThreadsNum(ctx)
				}
			}
		}
		for nThreads > 0 {
			_ = <-ch
			nThreads--
		}
	} else {
		for i, name := range names {
			email := emails[i]
			updateFunc(nil, name, email)
		}
	}
	lib.Printf("Updated %d/%d roles using %d CPUs\n", updated, nRoles, thrN)
}

// getGHAJSON - This is a work for single go routine - 1 hour of GHA data
// Usually such JSON conatin about 15000 - 60000 singe GHA events
// Boolean channel `ch` is used to synchronize go routines
func getGHAJSON(ch chan time.Time, ctx *lib.Ctx, dt time.Time, forg, frepo map[string]struct{}, orgRE, repoRE *regexp.Regexp, shas map[string]string, skipDates map[string]struct{}) {
	lib.Printf("Working on %v\n", dt)

	// Connect to Postgres DB
	con := lib.PgConn(ctx)
	defer func() { lib.FatalOnError(con.Close()) }()

	// Check skip GHA date config
	_, ok := skipDates[lib.ToYMDHDate(dt)]
	if ok {
		lib.Printf("Skipped %v\n", dt)
		markAsProcessed(con, ctx, dt)
		if ch != nil {
			ch <- dt
		}
		return
	}

	ghaURL := ctx.GHArchiveURL
	if ghaURL == "" {
		ghaURL = lib.GHArchiveURL
	}
	fn := fmt.Sprintf("%s%s.json.gz", ghaURL, lib.ToGHADate(dt))

	// Get gzipped JSON array via HTTP
	trials := 0
	var jsonsBytes []byte
	for {
		trials++
		if trials > 1 {
			lib.Printf("Retry(%d) %+v\n", trials, dt)
		}
		httpClient := &http.Client{Timeout: time.Minute * time.Duration(trials*ctx.HTTPTimeout)}
		response, err := httpClient.Get(fn)
		if err != nil {
			lib.Printf("%v: Error http.Get:\n%v\n", dt, err)
			if trials < ctx.HTTPRetry {
				time.Sleep(time.Duration((1+rand.Intn(20))*trials) * time.Second)
				continue
			}
			fmt.Fprintf(os.Stderr, "%v: Error http.Get:\n%v\n", dt, err)
		}
		lib.FatalOnError(err)

		// Decompress Gzipped response
		reader, err := gzip.NewReader(response.Body)
		//lib.FatalOnError(err)
		if err != nil {
			_ = response.Body.Close()
			lib.Printf("%v: No data yet, gzip reader:\n%v\n", dt, err)
			if trials < ctx.HTTPRetry {
				time.Sleep(time.Duration((1+rand.Intn(3))*trials) * time.Second)
				continue
			}
			fmt.Fprintf(os.Stderr, "%v: No data yet, gzip reader:\n%v\n", dt, err)
			if ch != nil {
				ch <- dt
			}
			lib.Printf("Gave up on %+v\n", dt)
			return
		}
		lib.Printf("Opened %s\n", fn)

		jsonsBytes, err = ioutil.ReadAll(reader)
		_ = reader.Close()
		_ = response.Body.Close()
		//lib.FatalOnError(err)
		if err != nil {
			lib.Printf("%v: Error (no data yet, ioutil readall):\n%v\n", dt, err)
			if trials < ctx.HTTPRetry {
				time.Sleep(time.Duration((1+rand.Intn(20))*trials) * time.Second)
				continue
			}
			fmt.Fprintf(os.Stderr, "%v: Error (no data yet, ioutil readall):\n%v\n", dt, err)
			if ch != nil {
				ch <- dt
			}
			lib.Printf("Gave up on %+v\n", dt)
			return
		}
		if trials > 1 {
			lib.Printf("Recovered(%d) & decompressed %s\n", trials, fn)
		} else {
			lib.Printf("Decompressed %s\n", fn)
		}
		break
	}

	// Split JSON array into separate JSONs
	jsonsArray := bytes.Split(jsonsBytes, []byte("\n"))
	lib.Printf("Split %s, %d JSONs\n", fn, len(jsonsArray))

	// Process JSONs one by one
	n, f, e := 0, 0, 0
	njsons := len(jsonsArray)
	for i, json := range jsonsArray {
		if len(json) < 1 {
			continue
		}
		fi, ei := parseJSON(con, ctx, i, njsons, json, dt, forg, frepo, orgRE, repoRE, shas)
		n++
		f += fi
		e += ei
	}
	lib.Printf(
		"Parsed: %s: %d JSONs, found %d matching, events %d\n",
		fn, n, f, e,
	)
	// Mark date as computed, to skip fetching this JSON again when it contains no events for a current project
	markAsProcessed(con, ctx, dt)
	if ch != nil {
		ch <- dt
	}
}

func getMemUsage() string {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return fmt.Sprintf("alloc:%dM heap-alloc:%dM(%dk objs) total:%dM sys:%dM #gc:%d", m.Alloc>>20, m.HeapAlloc>>20, m.HeapObjects>>10, m.TotalAlloc>>20, m.Sys>>20, m.NumGC)
}

func runGC() {
	lib.Printf("%s", getMemUsage()+"\n")
	runtime.GC()
	lib.Printf("%s", getMemUsage()+"\n")
}

// gha2db - main work horse
func gha2db(args []string) {
	// Environment context parse
	var (
		ctx      lib.Ctx
		err      error
		hourFrom int
		hourTo   int
		dFrom    time.Time
		dTo      time.Time
	)
	// Current date
	now := time.Now()
	// Init stuff
	debug.SetGCPercent(25)
	ctx.Init()
	lib.SetupTimeoutSignal(&ctx)
	rand.Seed(time.Now().UnixNano())

	if ctx.RefreshCommitRoles {
		defer func() { refreshCommitRoles(&ctx) }()
	} else {
		defer func() { updateCommitRoles(&ctx) }()
	}

	startD, startH, endD, endH := args[0], args[1], args[2], args[3]

	// Parse from day & hour
	if strings.ToLower(startH) == lib.Now {
		hourFrom = now.Hour()
	} else {
		hourFrom, err = strconv.Atoi(startH)
		lib.FatalOnError(err)
	}

	if strings.ToLower(startD) == lib.Today {
		dFrom = lib.DayStart(now).Add(time.Duration(hourFrom) * time.Hour)
	} else {
		dFrom, err = time.Parse(
			time.RFC3339,
			fmt.Sprintf("%sT%02d:00:00+00:00", startD, hourFrom),
		)
		lib.FatalOnError(err)
	}

	// Parse to day & hour
	var currNow time.Time
	dateToFunc := func() {
		currNow = time.Now()
		if strings.ToLower(endH) == lib.Now {
			hourTo = currNow.Hour()
		} else {
			hourTo, err = strconv.Atoi(endH)
			lib.FatalOnError(err)
		}

		if strings.ToLower(endD) == lib.Today {
			dTo = lib.DayStart(currNow).Add(time.Duration(hourTo) * time.Hour)
		} else {
			dTo, err = time.Parse(
				time.RFC3339,
				fmt.Sprintf("%sT%02d:00:00+00:00", endD, hourTo),
			)
			lib.FatalOnError(err)
		}
	}
	dateToFunc()

	// Strip function to be used by MapString
	stripFunc := func(x string) string { return strings.TrimSpace(x) }

	// Stripping whitespace from org and repo params
	var (
		org   map[string]struct{}
		orgRE *regexp.Regexp
	)
	if len(args) >= 5 {
		if strings.HasPrefix(args[4], "regexp:") {
			orgRE = regexp.MustCompile(args[4][7:])
		} else {
			org = lib.StringsMapToSet(
				stripFunc,
				strings.Split(args[4], ","),
			)
		}
	}

	var (
		repo   map[string]struct{}
		repoRE *regexp.Regexp
	)
	if len(args) >= 6 {
		if strings.HasPrefix(args[5], "regexp:") {
			repoRE = regexp.MustCompile(args[5][7:])
		} else {
			repo = lib.StringsMapToSet(
				stripFunc,
				strings.Split(args[5], ","),
			)
		}
	}

	// Get number of CPUs available
	thrN := lib.GetThreadsNum(&ctx)
	lib.Printf(
		"gha2db.go: Running (%v CPUs): %v - %v %v %v\n",
		thrN, dFrom, dTo,
		strings.Join(lib.StringsSetKeys(org), "+"),
		strings.Join(lib.StringsSetKeys(repo), "+"),
	)

	// GDPR data hiding
	shaMap := lib.GetHidden(&ctx, lib.HideCfgFile)

	// Skipping JSON dates
	dataPrefix := ctx.DataDir
	if ctx.Local {
		dataPrefix = "./"
	}

	// Read GHA dates to skip
	data, err := lib.ReadFile(&ctx, dataPrefix+ctx.SkipDatesYaml)
	if err != nil {
		lib.FatalOnError(err)
		return
	}

	// Read lista nd convert it to set
	var skipDatesList lib.SkipDatesList
	lib.FatalOnError(yaml.Unmarshal(data, &skipDatesList))
	skipDates := make(map[string]struct{})
	for _, date := range skipDatesList.Dates {
		skipDates[lib.ToYMDHDate(date)] = struct{}{}
	}

	igc := 0
	maybeGC := func() {
		igc++
		if igc%24 == 0 {
			runGC()
		}
	}

	dt := dFrom
	prc := 0
	if thrN > 1 {
		ch := make(chan time.Time)
		mp := make(map[time.Time]struct{})
		nThreads := 0
		for dt.Before(dTo) || dt.Equal(dTo) {
			dateToFunc()
			go getGHAJSON(ch, &ctx, dt, org, repo, orgRE, repoRE, shaMap, skipDates)
			mp[dt] = struct{}{}
			dt = dt.Add(time.Hour)
			nThreads++
			for nThreads >= thrN {
				prcdt := <-ch
				delete(mp, prcdt)
				nThreads--
				dateToFunc()
				maybeGC()
				prc++
				if prc%10 == 0 {
					thrN = lib.GetThreadsNum(&ctx)
				}
			}
		}
		lib.Printf("Final threads join (processed %d)\n", prc)
		for nThreads > 0 {
			if ctx.Debug >= 0 {
				dta := []string{}
				for k := range mp {
					dta = append(dta, lib.ToYMDHDate(k))
				}
				lib.Printf("%d remain: %v\n", nThreads, strings.Join(dta, ", "))
			}
			prcdt := <-ch
			delete(mp, prcdt)
			nThreads--
			dateToFunc()
			maybeGC()
		}
	} else {
		lib.Printf("Using single threaded version\n")
		for dt.Before(dTo) || dt.Equal(dTo) {
			dateToFunc()
			getGHAJSON(nil, &ctx, dt, org, repo, orgRE, repoRE, shaMap, skipDates)
			dt = dt.Add(time.Hour)
			maybeGC()
		}
	}
	// Finished
	lib.Printf("All done: %v\n", currNow.Sub(now))
}

func main() {
	dtStart := time.Now()
	// Required args
	if len(os.Args) < 5 {
		lib.Printf(
			"Arguments required: date_from_YYYY-MM-DD hour_from_HH date_to_YYYY-MM-DD hour_to_HH " +
				"['org1,org2,...,orgN' ['repo1,repo2,...,repoN']]\n",
		)
		os.Exit(1)
	}
	gha2db(os.Args[1:])
	dtEnd := time.Now()
	lib.Printf("Time: %v\n", dtEnd.Sub(dtStart))
}
