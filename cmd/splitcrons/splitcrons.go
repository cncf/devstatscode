package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"

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
	GA              string `yaml:"ga"`                        // 'UA-108085315-1'
	I               int    `yaml:"i"`                         // 0
	CertNum         int    `yaml:"certNum"`                   // 1
	MaxHist         int    `yaml:"maxHist,omitempty"`         // 8
}

type devstatsValues struct {
	Projects []devstatsProject `yaml:"projects"`
}

const (
	// cWeekHours - hours in week
	cWeekHours = 24.0 * 7.0
	// cWeekMinutes - minutes in week
	cWeekMinutes = 60.0 * 24.0 * 7.0
)

var (
	ctx        lib.Ctx
	gPatched   int
	gAttempted int
	gNever     bool
	gAllways   bool
)

// kubectl patch cronjob -n devstats-test devstats-affiliations-oras -p '{"spec":{"schedule": "40 4 * * 2"}}'
func patch(namespace, cronjob, field, patch string) {
	gAttempted++
	patchSpec := fmt.Sprintf(`{"spec":{"%s":%s}}`, field, patch)
	cmdAndArgs := []string{
		"kubectl",
		"patch",
		"cronjob",
		"-n",
		namespace,
		cronjob,
		"-p",
		patchSpec,
	}
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

// idxt/idxp == -1 -> kubernetes
// idxt/idxp == -2 -> all cncf
func generateCronEntries(values *devstatsValues, idx int, test, prod bool, idxt, idxp, nt, np int, offsetHours, hours, kubernetesHours, allHours, intervalT, intervalP, minutes, ghaOffset, syncHours float64) {
	minutesToCron := func(minA, minS int) (cronA, cronS string) {
		minutesA := minA % 60
		hoursA := (minA / 60) % 24
		dayA := (minA / (60 * 24)) % 7
		cronA = fmt.Sprintf("%d %d * * %d", minutesA, hoursA, dayA)
		almostHour := 60 - int(ghaOffset)
		hourS := minS / almostHour
		minuteS := (minS % almostHour) + int(ghaOffset)
		hoursS := ""
		syncHrs := int(syncHours)
		for h := 0; h < 24; h++ {
			if h%syncHrs == hourS {
				hoursS += strconv.Itoa(h) + ","
			}
		}
		hoursS = hoursS[:len(hoursS)-1]
		cronS = fmt.Sprintf("%d %s * * *", minuteS, hoursS)
		return
	}
	if test {
		minuteA, minuteS := -1, -1
		if idxt == -1 {
			//minuteA, minuteS = 0, int(ghaOffset)
			minuteA, minuteS = 0, 0
		} else if idxt == -2 {
			minuteA, minuteS = 60*int(cWeekHours-allHours), int(minutes/2.0)
		} else {
			minuteA, minuteS = int((kubernetesHours+intervalT*float64(idxt))*60.), int((float64(idxt)*minutes)/float64(nt))
		}
		minuteA += int(offsetHours * 60.0)
		minuteA += cWeekHours * 30.0
		if minuteA < 0 {
			minuteA += cWeekMinutes
		}
		if minuteA > cWeekMinutes {
			minuteA -= cWeekMinutes
		}
		cronA, cronS := minutesToCron(minuteA, minuteS)
		// fmt.Printf("test: %d/%d: %s(#%d): %d,%d --> '%s','%s'\n", idxt, nt, values.Projects[idx].Proj, idx, minuteA, minuteS, cronA, cronS)
		if !gNever && (gAllways || values.Projects[idx].AffCronTest != cronA) {
			values.Projects[idx].AffCronTest = cronA
			patch("devstats-test", "devstats-affiliations-"+values.Projects[idx].Proj, "schedule", `"`+cronA+`"`)
		}
		if !gNever && (gAllways || values.Projects[idx].CronTest != cronS) {
			values.Projects[idx].CronTest = cronS
			patch("devstats-test", "devstats-"+values.Projects[idx].Proj, "schedule", `"`+cronS+`"`)
		}
	}
	if prod {
		minuteA, minuteS := -1, -1
		if idxp == -1 {
			//minuteA, minuteS = 0, int(ghaOffset)
			minuteA, minuteS = 0, 0
		} else if idxp == -2 {
			minuteA, minuteS = 60*int(cWeekHours-allHours), int(minutes/2.0)
		} else {
			minuteA, minuteS = int((kubernetesHours+intervalP*float64(idxp))*60.), int((float64(idxp)*minutes)/float64(np))
		}
		minuteA += int(offsetHours * 60.0)
		if minuteA < 0 {
			minuteA += cWeekMinutes
		}
		if minuteA > cWeekMinutes {
			minuteA -= cWeekMinutes
		}
		cronA, cronS := minutesToCron(minuteA, minuteS)
		// fmt.Printf("prod: %d/%d: %s(#%d): %d,%d --> '%s','%s'\n", idxp, np, values.Projects[idx].Proj, idx, minuteA, minuteS, cronA, cronS)
		if !gNever && (gAllways || values.Projects[idx].AffCronProd != cronA) {
			values.Projects[idx].AffCronProd = cronA
			patch("devstats-prod", "devstats-affiliations-"+values.Projects[idx].Proj, "schedule", `"`+cronA+`"`)
		}
		if !gNever && (gAllways || values.Projects[idx].CronProd != cronS) {
			values.Projects[idx].CronProd = cronS
			patch("devstats-prod", "devstats-"+values.Projects[idx].Proj, "schedule", `"`+cronS+`"`)
		}
	}
}

func generateCronValues(inFile, outFile string) {
	ctx.Init()
	ctx.ExecFatal = false
	// ctx.ExecOutput = true

	data, err := ioutil.ReadFile(inFile)
	lib.FatalOnError(err)

	var values devstatsValues
	lib.FatalOnError(yaml.Unmarshal(data, &values))
	fmt.Printf("read %s\n", inFile)

	kubernetesHoursI := 24
	str := os.Getenv("KUBERNETES_HOURS")
	if str != "" {
		var err error
		kubernetesHoursI, err = strconv.Atoi(os.Getenv("KUBERNETES_HOURS"))
		lib.FatalOnError(err)
		if kubernetesHoursI < 3 || kubernetesHoursI > 30 {
			lib.Fatalf("KUBERNETES_HOURS must be from [3,30]")
		}
	}
	kubernetesHours := float64(kubernetesHoursI)
	allHoursI := 20
	str = os.Getenv("ALL_HOURS")
	if str != "" {
		var err error
		allHoursI, err = strconv.Atoi(os.Getenv("ALL_HOURS"))
		lib.FatalOnError(err)
		if allHoursI < 3 || allHoursI > 30 {
			lib.Fatalf("ALL_HOURS must be from [3,30]")
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
	syncHoursI := 2
	str = os.Getenv("SYNC_HOURS")
	if str != "" {
		var err error
		syncHoursI, err = strconv.Atoi(os.Getenv("SYNC_HOURS"))
		lib.FatalOnError(err)
		if syncHoursI < 1 || syncHoursI > 3 {
			lib.Fatalf("SYNC_HOURS must be 1, 2 or 3")
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
	gAllways = os.Getenv("ALWAYS_PATCH") != ""
	gNever = os.Getenv("NEVER_PATCH") != ""
	minutes := syncHours * (60.0 - ghaOffset)
	hours := 7.0*24.0 - (kubernetesHours + allHours)
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
		if !project.SuspendCronProd && project.Domains[1] != 0 {
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
	for i, project := range values.Projects {
		if project.DB == "gha" {
			generateCronEntries(&values, i, true, true, -1, -1, kt, kp, offsetHours, hours, kubernetesHours, allHours, intervalT, intervalP, minutes, ghaOffset, syncHours)
			continue
		}
		if project.DB == "allprj" {
			generateCronEntries(&values, i, true, true, -2, -2, kt, kp, offsetHours, hours, kubernetesHours, allHours, intervalT, intervalP, minutes, ghaOffset, syncHours)
			continue
		}
		t := !project.SuspendCronTest && project.Domains[0] != 0
		p := !project.SuspendCronProd && project.Domains[1] != 0
		generateCronEntries(&values, i, t, p, it, ip, kt, kp, offsetHours, hours, kubernetesHours, allHours, intervalT, intervalP, minutes, ghaOffset, syncHours)
		if t {
			it++
		} else {
			if !gNever && (gAllways || values.Projects[i].SuspendCronTest != true) {
				patch("devstats-test", "devstats-"+values.Projects[i].Proj, "suspend", "true")
				patch("devstats-test", "devstats-affiliations-"+values.Projects[i].Proj, "suspend", "true")
			}
			values.Projects[i].SuspendCronTest = true
		}
		if p {
			ip++
		} else {
			if !gNever && (gAllways || values.Projects[i].SuspendCronProd != true) {
				patch("devstats-prod", "devstats-"+values.Projects[i].Proj, "suspend", "true")
				patch("devstats-prod", "devstats-affiliations-"+values.Projects[i].Proj, "suspend", "true")
			}
			values.Projects[i].SuspendCronProd = true
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
