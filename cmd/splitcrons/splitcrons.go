package main

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	lib "github.com/cncf/devstatscode"
	"gopkg.in/yaml.v2"
)

type devstatsProject struct {
	Proj            string `yaml:"proj"`                      // kubernetes
	URL             string `yaml:"url"`                       // k8s
	DB              string `yaml:"db"`                        // gha
	Icon            string `yaml:"icon"`                      // 'k8s'
	Org             string `yaml:"org"`                       // 'Kubernetes'
	Repo            string `yaml:"repo"`                      // 'kubernetes/kubernetes'
	CronTest        string `yaml:"cronTest"`                  // '37 * * * *'
	CronProd        string `yaml:"cronProd"`                  // '7 * * * *'
	AffCronTest     string `yaml:"affCronTest"`               // '0 23 * * 0'
	AffCronProd     string `yaml:"affCronProd"`               // '0 11 * * 0'
	SuspendCronTest bool   `yaml:"suspendCronTest,omitempty"` // false
	SuspendCronProd bool   `yaml:"suspendCronProd,omitempty"` // false
	AffSkipTemp     string `yaml:"affSkipTemp"`               // '1'
	Disk            string `yaml:"disk"`                      // 50Gi
	Domains         [4]int `yaml:"domains,flow"`              // [1, 1, 0, 0]
	GA              string `yaml:"ga"`                        // 'UA-108085315-1' - warning - this is no longer supported by Google as it is < v4
	I               int    `yaml:"i"`                         // 0
	CertNum         int    `yaml:"certNum"`                   // 1
	MaxHist         int    `yaml:"maxHist,omitempty"`         // 8
	SkipAffsLock    int    `yaml:"skipAffsLock,omitempty"`    // 1
	AffsLockDB      string `yaml:"affsLockDB,omitempty"`      // gha, allprj
	NoDurable       int    `yaml:"noDurable,omitempty"`       // 1
	DurablePQ       int    `yaml:"durablePQ,omitempty"`       // 1
	MaxRunDuration  string `yaml:"maxRunDuration,omitempty"`  // annotations:1h:102,calc_metric:12h:102,columns:1h:102,get_repos:12h:102,gha2db:8h:102,ghapi2db:12h:102,structure:1h:102,tags:1h:102
	SkipGHAPI       int    `yaml:"skipGHAPI,omitempty"`       // skipGHAPI:1
	SkipGetRepos    int    `yaml:"skipGetRepos,omitempty"`    // skipGetRepos:1
	SkipUpdAffs     int    `yaml:"skipUpdAffs,omitempty"`     // skipUpdAffs:100 (percent)
	SkipImpAffs     int    `yaml:"skipImpAffs,omitempty"`     // skipImpAffs:100 (percent)
	Archived        bool   `yaml:"archived,omitempty"`        // false/true
}

type devstatsValues struct {
	SyncCPUs int               `yaml:"nSyncCPUs"`
	AffsCPUs int               `yaml:"nAffsCPUs"`
	Projects []devstatsProject `yaml:"projects"`
}

const (
	// cWeekHours - hours in week
	cWeekHours = 24.0 * 7.0
	// cWeekMinutes - minutes in week
	cWeekMinutes = 60.0 * 24.0 * 7.0
)

var (
	ctx           lib.Ctx
	gDebug        bool
	gPatched      int
	gAttempted    int
	gNever        bool
	gAlways       bool
	gOnlyEnv      bool
	gOnlySuspend  bool
	gSuspendAll   bool
	gNoSuspendA   bool
	gNoSuspendH   bool
	gMonthly      bool
	gSkipAffsEnv  bool
	gSkipSyncEnv  bool
	gOnlyProd     bool
	gOnlyTest     bool
	gOldAlgorithm bool
	gNoDBSizes    bool
	gPatchEnv     map[string]struct{}
	gName2Env     map[string]string
)

// getAliveCronjobs returns maps of cronjobs that actually exist in a given namespace:
// sync map: project -> true for 'devstats-<project>' CJs, affs map: project -> true for 'devstats-affiliations-<project>' CJs.
// Returns nil maps on kubectl failure (caller falls back to values.yaml eligibility for that env).
func getAliveCronjobs(namespace string) (sync, affs map[string]bool) {
	cmdAndArgs := []string{
		"kubectl",
		"get",
		"cronjob",
	}
	cmdAndArgs = append(cmdAndArgs, ctxArgsForNamespace(namespace)...)
	cmdAndArgs = append(cmdAndArgs,
		"-n",
		namespace,
		"-o",
		`jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}`,
	)
	res, err := lib.ExecCommand(
		&ctx,
		cmdAndArgs,
		nil,
	)
	if err != nil {
		fmt.Printf("getAliveCronjobs: -n %s: error: %+v (falling back to values.yaml based eligibility)\n", namespace, err)
		return nil, nil
	}
	sync = make(map[string]bool)
	affs = make(map[string]bool)
	for _, line := range strings.Split(res, "\n") {
		name := strings.TrimSpace(line)
		if name == "" {
			continue
		}
		if strings.HasPrefix(name, "devstats-affiliations-") {
			affs[name[len("devstats-affiliations-"):]] = true
			continue
		}
		if strings.HasPrefix(name, "devstats-") {
			sync[name[len("devstats-"):]] = true
		}
	}
	if gDebug {
		fmt.Printf("getAliveCronjobs: -n %s: %d sync, %d affs cronjobs found\n", namespace, len(sync), len(affs))
	}
	return sync, affs
}

// getDBSizes returns database name -> size in bytes probed from a given namespace's postgres pod.
// Returns nil on failure or when NO_DB_SIZES is set (caller falls back to even weights).
func getDBSizes(namespace string) map[string]float64 {
	if gNoDBSizes {
		return nil
	}
	pod := os.Getenv("SIZES_POD")
	if pod == "" {
		pod = "devstats-postgres-0"
	}
	cmdAndArgs := []string{
		"kubectl",
		"exec",
	}
	cmdAndArgs = append(cmdAndArgs, ctxArgsForNamespace(namespace)...)
	cmdAndArgs = append(cmdAndArgs,
		"-n",
		namespace,
		pod,
		"--",
		"psql",
		"-U",
		"postgres",
		"-At",
		"-F",
		" ",
		"-c",
		"select datname, pg_database_size(datname) from pg_database where not datistemplate",
	)
	res, err := lib.ExecCommand(
		&ctx,
		cmdAndArgs,
		nil,
	)
	if err != nil {
		fmt.Printf("getDBSizes: -n %s %s: error: %+v (falling back to even weights)\n", namespace, pod, err)
		return nil
	}
	sizes := make(map[string]float64)
	for _, line := range strings.Split(res, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		ary := strings.Split(line, " ")
		if len(ary) < 2 {
			continue
		}
		size, err := strconv.ParseFloat(ary[1], 64)
		if err != nil {
			continue
		}
		sizes[ary[0]] = size
	}
	if gDebug {
		fmt.Printf("getDBSizes: -n %s: got %d database sizes\n", namespace, len(sizes))
	}
	return sizes
}

// projWeight returns the scheduling weight of a project: sqrt of its DB size, so if project A's DB
// is R times bigger than project B's, A gets sqrt(R) times more time (geometric mean damping).
// Unknown DBs get the smallest known size, and when no sizes are available all weights are 1.0 (even split).
func projWeight(sizes map[string]float64, db string) float64 {
	if sizes == nil {
		return 1.0
	}
	size, ok := sizes[db]
	if !ok || size <= 1.0 {
		smallest := math.MaxFloat64
		for _, s := range sizes {
			if s > 1.0 && s < smallest {
				smallest = s
			}
		}
		if smallest == math.MaxFloat64 {
			return 1.0
		}
		size = smallest
	}
	return math.Sqrt(size)
}

// ctxArgsForNamespace returns kubectl '--context' args for a given namespace.
// Context defaults: 'test' for the test namespace, 'prod' for the prod one (matches in-cluster setups).
// Override with CTX_TEST / CTX_PROD (e.g. CTX_TEST=linode-test CTX_PROD=prod for cross-cluster check runs).
// Special value '-' means: use the current kubectl context (no --context argument).
func ctxArgsForNamespace(namespace string) []string {
	kctx, def := "", ""
	if strings.HasSuffix(namespace, "-test") {
		kctx, def = os.Getenv("CTX_TEST"), "test"
	} else if strings.HasSuffix(namespace, "-prod") {
		kctx, def = os.Getenv("CTX_PROD"), "prod"
	}
	if kctx == "" {
		kctx = def
	}
	if kctx == "-" || kctx == "" {
		return nil
	}
	return []string{"--context", kctx}
}

// kubectl patch cronjob -n devstats-test devstats-affiliations-rook -p '{"spec":{"jobTemplate":{"spec":{"template":{"spec":{"containers":[{"name":"devstats-affiliations-rook","env":[{"name":"USE_FLAGS","value":"1"}]}]}}}}}}'
func patchEnv(namespace, cronjob string, fields, patches []string) {
	if gDebug {
		fmt.Printf("patchEnv: -n %s %s: %v=%v\n", namespace, cronjob, fields, patches)
	}
	gAttempted++
	patchSpec := fmt.Sprintf(`{"spec":{"jobTemplate":{"spec":{"template":{"spec":{"containers":[{"name":"%s","env":[`, cronjob)
	n := len(fields)
	for i := range fields {
		field := fields[i]
		patch := patches[i]
		patchSpec += fmt.Sprintf(`{"name":"%s","value":"%s"}`, field, patch)
		if i < n-1 {
			patchSpec += ","
		}
	}
	patchSpec += `]}]}}}}}}`
	cmdAndArgs := []string{
		"kubectl",
		"patch",
		"cronjob",
	}
	cmdAndArgs = append(cmdAndArgs, ctxArgsForNamespace(namespace)...)
	cmdAndArgs = append(cmdAndArgs,
		"-n",
		namespace,
		cronjob,
		"-p",
		patchSpec,
	)
	_, err := lib.ExecCommand(
		&ctx,
		cmdAndArgs,
		nil,
	)
	//fmt.Printf("%+v:\n%s\n", cmdAndArgs, res)
	if err != nil {
		// fmt.Printf("%+v: error: %+v\n%s\n", cmdAndArgs, err, res)
		fmt.Printf("%+v: error: %+v\n", cmdAndArgs, err)
		return
	}
	gPatched++
}

// kubectl patch cronjob -n devstats-test devstats-affiliations-oras -p '{"spec":{"schedule": "40 4 * * 2"}}'
func patch(namespace, cronjob, field, patch string) {
	if gOnlyEnv {
		return
	}
	if gDebug {
		fmt.Printf("patch: -n %s %s: %s=%s\n", namespace, cronjob, field, patch)
	}
	gAttempted++
	patchSpec := fmt.Sprintf(`{"spec":{"%s":%s}}`, field, patch)
	cmdAndArgs := []string{
		"kubectl",
		"patch",
		"cronjob",
	}
	cmdAndArgs = append(cmdAndArgs, ctxArgsForNamespace(namespace)...)
	cmdAndArgs = append(cmdAndArgs,
		"-n",
		namespace,
		cronjob,
		"-p",
		patchSpec,
	)
	_, err := lib.ExecCommand(
		&ctx,
		cmdAndArgs,
		nil,
	)
	//fmt.Printf("%+v:\n%s\n", cmdAndArgs, res)
	if err != nil {
		// fmt.Printf("%+v: error: %+v\n%s\n", cmdAndArgs, err, res)
		fmt.Printf("%+v: error: %+v\n", cmdAndArgs, err)
		return
	}
	gPatched++
}

func considerPatchEnv(namespace, cronjob string, project *devstatsProject, nCPUs int, affs bool) {
	if gPatchEnv == nil {
		return
	}
	var (
		fields  []string
		patches []string
		envs    []string
	)
	if affs {
		if gSkipAffsEnv {
			return
		}
		envs = []string{"AffSkipTemp", "MaxHist", "SkipAffsLock", "AffsLockDB", "NoDurable", "DurablePQ", "MaxRunDuration", "SkipGHAPI", "SkipGetRepos", "NCPUs", "SkipImpAffs", "SkipUpdAffs"}
	} else {
		if gSkipSyncEnv {
			return
		}
		envs = []string{"MaxHist", "NoDurable", "DurablePQ", "MaxRunDuration", "NCPUs"}
	}
	for _, env := range envs {
		_, use := gPatchEnv[env]
		if !use {
			continue
		}
		field, _ := gName2Env[env]
		fields = append(fields, field)
		patch := ""
		switch env {
		case "NCPUs":
			patch = strconv.Itoa(nCPUs)
		case "AffSkipTemp":
			patch = project.AffSkipTemp
		case "MaxHist":
			patch = strconv.Itoa(project.MaxHist)
			if patch == "0" {
				patch = ""
			}
		case "SkipAffsLock":
			patch = strconv.Itoa(project.SkipAffsLock)
			if patch == "0" {
				patch = ""
			}
		case "AffsLockDB":
			patch = project.AffsLockDB
		case "NoDurable":
			patch = strconv.Itoa(project.NoDurable)
			if patch == "0" {
				patch = ""
			}
		case "DurablePQ":
			patch = strconv.Itoa(project.DurablePQ)
			if patch == "0" {
				patch = ""
			}
		case "MaxRunDuration":
			patch = project.MaxRunDuration
		case "SkipGHAPI":
			patch = strconv.Itoa(project.SkipGHAPI)
			if patch == "0" {
				patch = ""
			}
		case "SkipGetRepos":
			patch = strconv.Itoa(project.SkipGetRepos)
			if patch == "0" {
				patch = ""
			}
		case "SkipUpdAffs":
			patch = strconv.Itoa(project.SkipUpdAffs)
			if patch == "0" {
				patch = ""
			}
		case "SkipImpAffs":
			patch = strconv.Itoa(project.SkipImpAffs)
			if patch == "0" {
				patch = ""
			}
		}
		patches = append(patches, patch)
	}
	if len(fields) > 0 {
		patchEnv(namespace, cronjob, fields, patches)
	}
}

// weightedEntry - a single alive project entry in the weighted scheduler
type weightedEntry struct {
	idx    int     // index in values.Projects
	proj   string  // project name
	db     string  // project DB name
	weight float64 // sqrt(DB size)
	sizeGb float64 // DB size in Gb (for reporting)
}

// generateWeightedCronEntries computes and applies schedules for one env (test or prod) using the new algorithm:
// only projects actually alive in the cluster are scheduled, each gets time proportional to sqrt(its DB size)
// (geometric mean damping: DB size ratio R -> time ratio sqrt(R)), spread over the whole scheduling period.
// Sync crons run every syncHours with minute >= ghaOffset (GHA archives for hour H are available ~HH:04).
// Affs crons spread over the whole week (or 28 days in monthly mode), also with minute >= ghaOffset.
func generateWeightedCronEntries(values *devstatsValues, test bool, entries []weightedEntry, ghaOffset, syncHours float64) {
	env, namespace := "prod", "devstats-prod"
	if test {
		env, namespace = "test", "devstats-test"
	}
	periodDays := 7
	if gMonthly {
		periodDays = 28
	}
	almostHour := 60 - int(ghaOffset)
	syncSpace := int(syncHours) * almostHour
	affsSpace := periodDays * 24 * almostHour
	totalWeight := 0.0
	for _, e := range entries {
		totalWeight += e.weight
	}
	if totalWeight <= 0.0 {
		fmt.Printf("%s: no alive projects to schedule\n", env)
		return
	}
	// linear position -> sync cron: 'M h0,h0+syncHours,... * * *', minute M in [ghaOffset, 60)
	posToCronSync := func(pos int) string {
		hourS := pos / almostHour
		minuteS := (pos % almostHour) + int(ghaOffset)
		syncHrs := int(syncHours)
		if hourS >= syncHrs {
			hourS = 0
		}
		hoursS := ""
		for h := 0; h < 24; h++ {
			if h%syncHrs == hourS {
				hoursS += strconv.Itoa(h) + ","
			}
		}
		hoursS = hoursS[:len(hoursS)-1]
		return fmt.Sprintf("%d %s * * *", minuteS, hoursS)
	}
	// linear position -> affs cron: weekly 'M H * * D' or monthly 'M H D * *', minute M in [ghaOffset, 60)
	posToCronAffs := func(pos int) string {
		minuteA := (pos % almostHour) + int(ghaOffset)
		hourA := (pos / almostHour) % 24
		dayA := (pos / (almostHour * 24)) % periodDays
		if gMonthly {
			return fmt.Sprintf("%d %d %d * *", minuteA, hourA, dayA+1)
		}
		return fmt.Sprintf("%d %d * * %d", minuteA, hourA, dayA)
	}
	// distribute cumulative weighted positions, bump on collisions
	positions := func(space int) map[int]int {
		out := make(map[int]int)
		used := make(map[int]bool)
		cum := 0.0
		for _, e := range entries {
			pos := int((cum / totalWeight) * float64(space))
			if pos >= space {
				pos = space - 1
			}
			for used[pos] {
				pos = (pos + 1) % space
			}
			used[pos] = true
			out[e.idx] = pos
			cum += e.weight
		}
		return out
	}
	syncPos := positions(syncSpace)
	affsPos := positions(affsSpace)
	// gap = distance from a project's position to the next scheduled one (wraps around the space)
	gaps := func(pos map[int]int, space int) map[int]int {
		type pi struct{ pos, idx int }
		list := []pi{}
		for idx, p := range pos {
			list = append(list, pi{p, idx})
		}
		sort.Slice(list, func(i, j int) bool { return list[i].pos < list[j].pos })
		out := make(map[int]int)
		for i, it := range list {
			if i == len(list)-1 {
				out[it.idx] = space - it.pos + list[0].pos
			} else {
				out[it.idx] = list[i+1].pos - it.pos
			}
		}
		return out
	}
	syncGap := gaps(syncPos, syncSpace)
	affsGap := gaps(affsPos, affsSpace)
	fmt.Printf("%s: %d alive projects, weight = sqrt(DB size), sync space %d minutes (every %.0fh), affs space %d minutes (%d days):\n", env, len(entries), syncSpace, syncHours, affsSpace, periodDays)
	for _, e := range entries {
		cronS := posToCronSync(syncPos[e.idx])
		cronA := posToCronAffs(affsPos[e.idx])
		fmt.Printf("  %-24s db=%-16s size=%9.2fGb weight=%9.1f share=%5.1f%% sync='%s' gap=%dm affs='%s' gap=%.1fh\n", e.proj, e.db, e.sizeGb, e.weight, (e.weight/totalWeight)*100.0, cronS, syncGap[e.idx], cronA, float64(affsGap[e.idx])/60.0)
		if test {
			if gAlways || values.Projects[e.idx].AffCronTest != cronA {
				values.Projects[e.idx].AffCronTest = cronA
				if !gNever {
					patch(namespace, "devstats-affiliations-"+e.proj, "schedule", `"`+cronA+`"`)
				}
			}
			if gAlways || values.Projects[e.idx].CronTest != cronS {
				values.Projects[e.idx].CronTest = cronS
				if !gNever {
					patch(namespace, "devstats-"+e.proj, "schedule", `"`+cronS+`"`)
				}
			}
		} else {
			if gAlways || values.Projects[e.idx].AffCronProd != cronA {
				values.Projects[e.idx].AffCronProd = cronA
				if !gNever {
					patch(namespace, "devstats-affiliations-"+e.proj, "schedule", `"`+cronA+`"`)
				}
			}
			if gAlways || values.Projects[e.idx].CronProd != cronS {
				values.Projects[e.idx].CronProd = cronS
				if !gNever {
					patch(namespace, "devstats-"+e.proj, "schedule", `"`+cronS+`"`)
				}
			}
		}
		if !gNever {
			considerPatchEnv(namespace, "devstats-"+e.proj, &values.Projects[e.idx], values.SyncCPUs, false)
			considerPatchEnv(namespace, "devstats-affiliations-"+e.proj, &values.Projects[e.idx], values.AffsCPUs, true)
		}
	}
}

// newAlgorithmForEnv probes alive cronjobs and DB sizes for one env and builds its weighted entries list.
// Eligibility: values.yaml flags (suspend + domains) AND actual cronjob existence in the cluster
// (when the alive probe fails, values.yaml eligibility alone decides - with even weights on size probe failure).
// It also pushes values.yaml suspend states (or SUSPEND_ALL) to all alive cronjobs of eligible-domain projects.
func newAlgorithmForEnv(values *devstatsValues, test bool) []weightedEntry {
	namespace := "devstats-prod"
	if test {
		namespace = "devstats-test"
	}
	aliveSync, aliveAffs := getAliveCronjobs(namespace)
	sizes := getDBSizes(namespace)
	entries := []weightedEntry{}
	for i, project := range values.Projects {
		domainOK, suspended := false, false
		if test {
			domainOK, suspended = project.Domains[0] != 0, project.SuspendCronTest
		} else {
			domainOK, suspended = project.Domains[1] != 0 || project.Domains[2] != 0 || project.Domains[3] != 0, project.SuspendCronProd
		}
		if !domainOK {
			continue
		}
		syncAlive := aliveSync == nil || aliveSync[project.Proj]
		affsAlive := aliveAffs == nil || aliveAffs[project.Proj]
		if !gNever && !gOnlySuspend {
			suspend := fmt.Sprintf("%v", suspended)
			if gSuspendAll {
				suspend = "true"
			}
			if !gNoSuspendH && syncAlive {
				patch(namespace, "devstats-"+project.Proj, "suspend", suspend)
			}
			if !gNoSuspendA && affsAlive {
				patch(namespace, "devstats-affiliations-"+project.Proj, "suspend", suspend)
			}
		}
		if suspended || (!syncAlive && !affsAlive) {
			continue
		}
		sizeGb := 0.0
		if sizes != nil {
			sizeGb = sizes[project.DB] / (1024.0 * 1024.0 * 1024.0)
		}
		entries = append(entries, weightedEntry{idx: i, proj: project.Proj, db: project.DB, weight: projWeight(sizes, project.DB), sizeGb: sizeGb})
	}
	return entries
}

// idxt/idxp == -1 -> kubernetes
// idxt/idxp == -2 -> all cncf
func generateCronEntries(values *devstatsValues, idx int, test, prod bool, idxt, idxp, nt, np int, offsetHours, hours, kubernetesHours, allHours, intervalT, intervalP, minutes, ghaOffset, syncHours float64) {
	var minutesToCron func(int, int) (string, string)
	if gMonthly {
		minutesToCron = func(minA, minS int) (cronA, cronS string) {
			minutesA := minA % 60
			hoursA := (minA / 60) % 24
			dayA := ((minA / (60 * 24)) % 28) + 1
			cronA = fmt.Sprintf("%d %d %d * *", minutesA, hoursA, dayA)
			almostHour := 60 - int(ghaOffset)
			hourS := minS / almostHour
			minuteS := (minS % almostHour) + int(ghaOffset)
			hoursS := ""
			syncHrs := int(syncHours)
			if hourS >= syncHrs {
				fmt.Printf("warning: (minA,minS) = (%d,%d) generates hourS >= syncHrs: %d >= %d\n", minA, minS, hourS, syncHrs)
				hourS = 0
			}
			for h := 0; h < 24; h++ {
				if h%syncHrs == hourS {
					hoursS += strconv.Itoa(h) + ","
				}
			}
			hoursS = hoursS[:len(hoursS)-1]
			cronS = fmt.Sprintf("%d %s * * *", minuteS, hoursS)
			return
		}
	} else {
		minutesToCron = func(minA, minS int) (cronA, cronS string) {
			minutesA := minA % 60
			hoursA := (minA / 60) % 24
			dayA := (minA / (60 * 24)) % 7
			cronA = fmt.Sprintf("%d %d * * %d", minutesA, hoursA, dayA)
			almostHour := 60 - int(ghaOffset)
			hourS := minS / almostHour
			minuteS := (minS % almostHour) + int(ghaOffset)
			hoursS := ""
			syncHrs := int(syncHours)
			if hourS >= syncHrs {
				fmt.Printf("warning: (minA,minS) = (%d,%d) generates hourS >= syncHrs: %d >= %d\n", minA, minS, hourS, syncHrs)
				hourS = 0
			}
			for h := 0; h < 24; h++ {
				if h%syncHrs == hourS {
					hoursS += strconv.Itoa(h) + ","
				}
			}
			hoursS = hoursS[:len(hoursS)-1]
			cronS = fmt.Sprintf("%d %s * * *", minuteS, hoursS)
			return
		}
	}
	periodHours := int(cWeekHours)
	periodMinutes := int(cWeekMinutes)
	if gMonthly {
		periodHours <<= 2
		periodMinutes <<= 2
	}
	if test {
		minuteA, minuteS := -1, -1
		if idxt == -1 {
			//minuteA, minuteS = 0, int(ghaOffset)
			minuteA, minuteS = 0, 0
		} else if idxt == -2 {
			minuteA, minuteS = 60*int(float64(periodHours)-allHours), int(minutes/2.0)
		} else {
			minuteA, minuteS = int((kubernetesHours+intervalT*float64(idxt))*60.), int((float64(idxt)*minutes)/float64(nt))
		}
		minuteA += int(offsetHours * 60.0)
		// Offset for test
		// affiliations cronjob offset
		minuteA += periodHours * 30.0
		// hourly sync cronjob offset
		minuteS += int(minutes / 2.0)
		if minuteS >= int(minutes) {
			minuteS -= int(minutes)
		}
		if minuteA < 0 {
			minuteA += periodMinutes
		}
		if minuteA >= periodMinutes {
			minuteA -= periodMinutes
		}
		cronA, cronS := minutesToCron(minuteA, minuteS)
		// fmt.Printf("test: %d/%d: %s(#%d): %d,%d --> '%s','%s'\n", idxt, nt, values.Projects[idx].Proj, idx, minuteA, minuteS, cronA, cronS)
		if !gNever && (gAlways || values.Projects[idx].AffCronTest != cronA) {
			values.Projects[idx].AffCronTest = cronA
			patch("devstats-test", "devstats-affiliations-"+values.Projects[idx].Proj, "schedule", `"`+cronA+`"`)
		}
		if !gNever && (gAlways || values.Projects[idx].CronTest != cronS) {
			values.Projects[idx].CronTest = cronS
			patch("devstats-test", "devstats-"+values.Projects[idx].Proj, "schedule", `"`+cronS+`"`)
		}
		if !gNever {
			considerPatchEnv("devstats-test", "devstats-"+values.Projects[idx].Proj, &values.Projects[idx], values.SyncCPUs, false)
			considerPatchEnv("devstats-test", "devstats-affiliations-"+values.Projects[idx].Proj, &values.Projects[idx], values.AffsCPUs, true)
		}
	}
	if prod {
		minuteA, minuteS := -1, -1
		if idxp == -1 {
			//minuteA, minuteS = 0, int(ghaOffset)
			minuteA, minuteS = 0, 0
		} else if idxp == -2 {
			minuteA, minuteS = 60*int(float64(periodHours)-allHours), int(minutes/2.0)
		} else {
			minuteA, minuteS = int((kubernetesHours+intervalP*float64(idxp))*60.), int((float64(idxp)*minutes)/float64(np))
		}
		minuteA += int(offsetHours * 60.0)
		if minuteA < 0 {
			minuteA += periodMinutes
		}
		if minuteA >= periodMinutes {
			minuteA -= periodMinutes
		}
		cronA, cronS := minutesToCron(minuteA, minuteS)
		// fmt.Printf("prod: %d/%d: %s(#%d): %d,%d --> '%s','%s'\n", idxp, np, values.Projects[idx].Proj, idx, minuteA, minuteS, cronA, cronS)
		if !gNever && (gAlways || values.Projects[idx].AffCronProd != cronA) {
			values.Projects[idx].AffCronProd = cronA
			patch("devstats-prod", "devstats-affiliations-"+values.Projects[idx].Proj, "schedule", `"`+cronA+`"`)
		}
		if !gNever && (gAlways || values.Projects[idx].CronProd != cronS) {
			values.Projects[idx].CronProd = cronS
			patch("devstats-prod", "devstats-"+values.Projects[idx].Proj, "schedule", `"`+cronS+`"`)
		}
		if !gNever {
			considerPatchEnv("devstats-prod", "devstats-"+values.Projects[idx].Proj, &values.Projects[idx], values.SyncCPUs, false)
			considerPatchEnv("devstats-prod", "devstats-affiliations-"+values.Projects[idx].Proj, &values.Projects[idx], values.AffsCPUs, true)
		}
	}
}

func setPatchEnvMap() {
	data := os.Getenv("PATCH_ENV")
	if data == "" {
		return
	}
	ary := strings.Split(data, ",")
	gPatchEnv = make(map[string]struct{})
	for _, env := range ary {
		gPatchEnv[strings.TrimSpace(env)] = struct{}{}
	}
	gName2Env = map[string]string{
		"AffSkipTemp":    "SKIPTEMP",
		"MaxHist":        "GHA2DB_MAX_HIST",
		"SkipAffsLock":   "SKIP_AFFS_LOCK",
		"AffsLockDB":     "AFFS_LOCK_DB",
		"NoDurable":      "NO_DURABLE",
		"DurablePQ":      "DURABLE_PQ",
		"MaxRunDuration": "GHA2DB_MAX_RUN_DURATION",
		"SkipGHAPI":      "GHA2DB_GHAPISKIP",
		"SkipGetRepos":   "GHA2DB_GETREPOSSKIP",
		"NCPUs":          "GHA2DB_NCPUS",
		"SkipImpAffs":    "SKIP_IMP_AFFS",
		"SkipUpdAffs":    "SKIP_UPD_AFFS",
	}
}

func generateCronValues(inFile, outFile string) {
	ctx.Init()
	lib.SetupTimeoutSignal(&ctx)
	ctx.ExecFatal = false
	ctx.ExecOutput = true
	// ctx.ExecOutput = true

	data, err := ioutil.ReadFile(inFile)
	lib.FatalOnError(err)

	var values devstatsValues
	lib.FatalOnError(yaml.Unmarshal(data, &values))
	fmt.Printf("read %s\n", inFile)

	gDebug = os.Getenv("DEBUG") != ""
	gMonthly = os.Getenv("MONTHLY") != ""
	maxHours := 30
	if gMonthly {
		maxHours = 48
	}
	kubernetesHoursI := 24
	if gMonthly {
		kubernetesHoursI = 36
	}
	str := os.Getenv("KUBERNETES_HOURS")
	if str != "" {
		var err error
		kubernetesHoursI, err = strconv.Atoi(os.Getenv("KUBERNETES_HOURS"))
		lib.FatalOnError(err)
		if kubernetesHoursI < 3 || kubernetesHoursI > maxHours {
			lib.Fatalf("KUBERNETES_HOURS must be from [3,%d]", maxHours)
		}
	}
	kubernetesHours := float64(kubernetesHoursI)
	allHoursI := 20
	if gMonthly {
		allHoursI = 36
	}
	str = os.Getenv("ALL_HOURS")
	if str != "" {
		var err error
		allHoursI, err = strconv.Atoi(os.Getenv("ALL_HOURS"))
		lib.FatalOnError(err)
		if allHoursI < 3 || allHoursI > maxHours {
			lib.Fatalf("ALL_HOURS must be from [3,%d]", maxHours)
		}
	}
	allHours := float64(allHoursI)
	ghaOffsetI := 4
	str = os.Getenv("GHA_OFFSET")
	if str != "" {
		var err error
		ghaOffsetI, err = strconv.Atoi(os.Getenv("GHA_OFFSET"))
		lib.FatalOnError(err)
		if ghaOffsetI < 2 || ghaOffsetI > 10 {
			lib.Fatalf("GHA_OFFSET must be from [2,10]")
		}
	}
	ghaOffset := float64(ghaOffsetI)
	syncHoursI := 6
	str = os.Getenv("SYNC_HOURS")
	if str != "" {
		var err error
		syncHoursI, err = strconv.Atoi(os.Getenv("SYNC_HOURS"))
		lib.FatalOnError(err)
		if syncHoursI < 1 || syncHoursI > 6 {
			lib.Fatalf("SYNC_HOURS must be from 1 to 6")
		}
	}
	syncHours := float64(syncHoursI)
	offsetHoursI := -4
	str = os.Getenv("OFFSET_HOURS")
	if str != "" {
		var err error
		offsetHoursI, err = strconv.Atoi(os.Getenv("OFFSET_HOURS"))
		lib.FatalOnError(err)
		if offsetHoursI < -84 || offsetHoursI > 84 {
			lib.Fatalf("OFFSET_HOURS must be from [-84,84]")
		}
	}
	offsetHours := float64(offsetHoursI)
	gAlways = os.Getenv("ALWAYS_PATCH") != ""
	gNever = os.Getenv("NEVER_PATCH") != ""
	gOnlyEnv = os.Getenv("ONLY_ENV") != ""
	gOnlySuspend = os.Getenv("ONLY_SUSPEND") != ""
	gSuspendAll = os.Getenv("SUSPEND_ALL") != ""
	gNoSuspendH = os.Getenv("NO_SUSPEND_H") != ""
	gNoSuspendA = os.Getenv("NO_SUSPEND_A") != ""
	gSkipAffsEnv = os.Getenv("SKIP_AFFS_ENV") != ""
	gSkipSyncEnv = os.Getenv("SKIP_SYNC_ENV") != ""
	gOnlyProd = os.Getenv("ONLY_PROD") != ""
	gOnlyTest = os.Getenv("ONLY_TEST") != ""
	gOldAlgorithm = os.Getenv("OLD_ALGORITHM") != ""
	gNoDBSizes = os.Getenv("NO_DB_SIZES") != ""
	setPatchEnvMap()
	// New (default) algorithm: schedule only projects actually alive in each env's cluster,
	// give each time proportional to sqrt(its DB size), spread evenly over the whole period.
	// OLD_ALGORITHM=1 switches back to the legacy values.yaml based static split below.
	if !gOldAlgorithm {
		fmt.Printf("new algorithm: probing alive cronjobs & DB sizes (OLD_ALGORITHM=1 for legacy mode, NO_DB_SIZES=1 for even weights)\n")
		fmt.Printf("sync happens from HH:%02.0f, every %.0f hours; affs spread over %s\n", ghaOffset, syncHours, map[bool]string{true: "28 days", false: "7 days"}[gMonthly])
		if !gOnlyProd {
			entries := newAlgorithmForEnv(&values, true)
			if !gOnlySuspend {
				generateWeightedCronEntries(&values, true, entries, ghaOffset, syncHours)
			}
		}
		if !gOnlyTest {
			entries := newAlgorithmForEnv(&values, false)
			if !gOnlySuspend {
				generateWeightedCronEntries(&values, false, entries, ghaOffset, syncHours)
			}
		}
		fmt.Printf("patched %d/%d cronjobs\n", gPatched, gAttempted)
		yamlBytes, err := yaml.Marshal(values)
		lib.FatalOnError(err)
		lib.FatalOnError(ioutil.WriteFile(outFile, yamlBytes, 0644))
		fmt.Printf("written %s\n", outFile)
		return
	}
	minutes := syncHours * (60.0 - ghaOffset)
	hours := float64(cWeekHours)
	if gMonthly {
		hours *= 4.0
	}
	hours -= kubernetesHours + allHours
	kt, kp := 0, 0
	kubernetesIdx := -1
	allIdx := -1
	for i, project := range values.Projects {
		if project.DB == "gha" {
			kubernetesIdx = i
			continue
		}
		if project.DB == "allprj" {
			allIdx = i
			continue
		}
		if !project.SuspendCronTest && project.Domains[0] != 0 {
			kt++
		}
		if !project.SuspendCronProd && (project.Domains[1] != 0 || project.Domains[2] != 0 || project.Domains[3] != 0) {
			kp++
		}
	}
	intervalT := hours / float64(kt)
	intervalP := hours / float64(kp)
	intervalST := (60. * minutes) / float64(kt)
	intervalSP := (60. * minutes) / float64(kp)
	fmt.Printf("sync happens from HH:%02.0f, every %.0f hours, which gives %.0fmin for hourly syncs, middle of weekend offset is %.0fh\n", ghaOffset, syncHours, minutes, offsetHours)
	fmt.Printf("test: Kubernetes(#%d) needs %.0fh, All(#%d) needs %.0fh, %d others all have %.0fh, intervals are %.1fmin, %.1fs\n", kubernetesIdx, kubernetesHours, allIdx, allHours, kt, hours, intervalT*60., intervalST)
	fmt.Printf("prod: Kubernetes(#%d) needs %.0fh, All(#%d) needs %.0fh, %d others all have %.0fh, intervals are %.1fmin, %.1fs\n", kubernetesIdx, kubernetesHours, allIdx, allHours, kp, hours, intervalP*60., intervalSP)
	it, ip := 0, 0
	var suspend string
	if gSuspendAll {
		suspend = "true"
	}
	for i, project := range values.Projects {
		t := !project.SuspendCronTest && project.Domains[0] != 0
		p := !project.SuspendCronProd && (project.Domains[1] != 0 || project.Domains[2] != 0 || project.Domains[3] != 0)
		if gOnlyProd {
			t = false
		}
		if gOnlyTest {
			p = false
		}
		if !gOnlySuspend {
			switch project.DB {
			case "gha":
				// generateCronEntries(&values, i, true, true, -1, -1, kt, kp, offsetHours, hours, kubernetesHours, allHours, intervalT, intervalP, minutes, ghaOffset, syncHours)
				generateCronEntries(&values, i, t, p, -1, -1, kt, kp, offsetHours, hours, kubernetesHours, allHours, intervalT, intervalP, minutes, ghaOffset, syncHours)
			case "allprj":
				// generateCronEntries(&values, i, true, true, -2, -2, kt, kp, offsetHours, hours, kubernetesHours, allHours, intervalT, intervalP, minutes, ghaOffset, syncHours)
				generateCronEntries(&values, i, t, p, -2, -2, kt, kp, offsetHours, hours, kubernetesHours, allHours, intervalT, intervalP, minutes, ghaOffset, syncHours)
			default:
				generateCronEntries(&values, i, t, p, it, ip, kt, kp, offsetHours, hours, kubernetesHours, allHours, intervalT, intervalP, minutes, ghaOffset, syncHours)
				if t {
					it++
				}
				if p {
					ip++
				}
			}
		}
		if t && !gNever && project.Domains[0] != 0 {
			if !gSuspendAll {
				suspend = fmt.Sprintf("%v", values.Projects[i].SuspendCronTest)
			}
			if !gNoSuspendH {
				patch("devstats-test", "devstats-"+values.Projects[i].Proj, "suspend", suspend)
			}
			if !gNoSuspendA {
				patch("devstats-test", "devstats-affiliations-"+values.Projects[i].Proj, "suspend", suspend)
			}
		}
		if p && !gNever && (project.Domains[1] != 0 || project.Domains[2] != 0 || project.Domains[3] != 0) {
			if !gSuspendAll {
				suspend = fmt.Sprintf("%v", values.Projects[i].SuspendCronProd)
			}
			if !gNoSuspendH {
				patch("devstats-prod", "devstats-"+values.Projects[i].Proj, "suspend", suspend)
			}
			if !gNoSuspendA {
				patch("devstats-prod", "devstats-affiliations-"+values.Projects[i].Proj, "suspend", suspend)
			}
		}
	}
	fmt.Printf("patched %d/%d cronjobs\n", gPatched, gAttempted)
	yamlBytes, err := yaml.Marshal(values)
	lib.FatalOnError(err)
	lib.FatalOnError(ioutil.WriteFile(outFile, yamlBytes, 0644))
	fmt.Printf("written %s\n", outFile)
	return
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("usage: %s path/to/devstats-helm/values.yaml new-values.yaml\n", os.Args[0])
		return
	}
	generateCronValues(os.Args[1], os.Args[2])
	return
}
