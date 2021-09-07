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
	Proj            string `yaml:"proj"`            // kubernetes
	URL             string `yaml:"url"`             // k8s
	DB              string `yaml:"db"`              // gha
	Icon            string `yaml:"icon"`            // 'k8s'
	Org             string `yaml:"org"`             // 'Kubernetes'
	Repo            string `yaml:"repo"`            // 'kubernetes/kubernetes'
	CronTest        string `yaml:"cronTest"`        // '37 * * * *'
	CronProd        string `yaml:"cronProd"`        // '7 * * * *'
	AffCronTest     string `yaml:"affCronTest"`     // '0 23 * * 0'
	AffCronProd     string `yaml:"affCronProd"`     // '0 11 * * 0'
	SuspendCronTest bool   `yaml:"suspendCronTest"` // false
	SuspendCronProd bool   `yaml:"suspendCronProd"` // false
	AffSkipTemp     string `yaml:"affSkipTemp"`     // '1'
	Disk            string `yaml:"disk"`            // 50Gi
	Domains         [4]int `yaml:"domains"`         // [1, 1, 0, 0]
	GA              string `yaml:"ga"`              // 'UA-108085315-1'
	I               int    `yaml:"i"`               // 0
	CertNum         int    `yaml:"certNum"`         // 1
}

type devstatsValues struct {
	Projects []devstatsProject `yaml:"projects"`
}

const (
	// WeekHours - hours in week
	WeekHours = 24.0 * 7.0
	// WeekMinutes - minutes in week
	WeekMinutes = 60.0 * 24.0 * 7.0
)

// idxt/idxp == -1 -> kubernetes
// idxt/idxp == -2 -> all cncf
func generateCronEntries(values *devstatsValues, idx int, test, prod bool, idxt, idxp, nt, np int, hours, kubernetesHours, allHours, intervalT, intervalP, minutes, ghaOffset, syncHours float64) {
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
			minuteA, minuteS = 60*int(WeekHours-allHours), int(minutes/2.0)
		} else {
			minuteA, minuteS = int((kubernetesHours+intervalT*float64(idxt))*60.), int((float64(idxt)*minutes)/float64(nt))
		}
		minuteA += WeekHours * 30.0
		if minuteA > WeekMinutes {
			minuteA -= WeekMinutes
		}
		cronA, cronS := minutesToCron(minuteA, minuteS)
		fmt.Printf("test: %d/%d: %s(#%d): %d,%d --> '%s','%s'\n", idxt, nt, values.Projects[idx].Proj, idx, minuteA, minuteS, cronA, cronS)
	}
	if prod {
		minuteA, minuteS := -1, -1
		if idxp == -1 {
			//minuteA, minuteS = 0, int(ghaOffset)
			minuteA, minuteS = 0, 0
		} else if idxp == -2 {
			minuteA, minuteS = 60*int(WeekHours-allHours), int(minutes/2.0)
		} else {
			minuteA, minuteS = int((kubernetesHours+intervalP*float64(idxp))*60.), int((float64(idxp)*minutes)/float64(np))
		}
		cronA, cronS := minutesToCron(minuteA, minuteS)
		fmt.Printf("prod: %d/%d: %s(#%d): %d,%d --> '%s','%s'\n", idxp, np, values.Projects[idx].Proj, idx, minuteA, minuteS, cronA, cronS)
	}
}

func generateCronValues(inFile, outFile string) {
	data, err := ioutil.ReadFile(inFile)
	lib.FatalOnError(err)

	var values devstatsValues
	lib.FatalOnError(yaml.Unmarshal(data, &values))

	kubernetesHoursI := 18
	str := os.Getenv("KUBERNETES_HOURS")
	if str != "" {
		var err error
		kubernetesHoursI, err = strconv.Atoi(os.Getenv("KUBERNETES_HOURS"))
		lib.FatalOnError(err)
	}
	kubernetesHours := float64(kubernetesHoursI)
	allHoursI := 18
	str = os.Getenv("ALL_HOURS")
	if str != "" {
		var err error
		allHoursI, err = strconv.Atoi(os.Getenv("ALL_HOURS"))
		lib.FatalOnError(err)
	}
	allHours := float64(allHoursI)
	ghaOffsetI := 5
	str = os.Getenv("GHA_OFFSET")
	if str != "" {
		var err error
		ghaOffsetI, err = strconv.Atoi(os.Getenv("GHA_OFFSET"))
		lib.FatalOnError(err)
	}
	ghaOffset := float64(ghaOffsetI)
	syncHoursI := 2
	str = os.Getenv("SYNC_HOURS")
	if str != "" {
		var err error
		syncHoursI, err = strconv.Atoi(os.Getenv("SYNC_HOURS"))
		lib.FatalOnError(err)
	}
	syncHours := float64(syncHoursI)
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
		if !project.SuspendCronTest && project.Domains[1] != 0 {
			kt++
		}
		if !project.SuspendCronProd && project.Domains[0] != 0 {
			kp++
		}
	}
	intervalT := hours / float64(kt)
	intervalP := hours / float64(kp)
	fmt.Printf("sync happens from HH:%02.0f, every %.0f hours, which gives %.0fmin for hourly syncs\n", ghaOffset, syncHours, minutes)
	fmt.Printf("test: Kubernetes(#%d) needs %.0fh, All(#%d) needs %.0fh, %d others all have %.0fh, interval is %f.1min\n", kubernetesIdx, kubernetesHours, allIdx, allHours, kt, hours, intervalT*60.)
	fmt.Printf("prod: Kubernetes(#%d) needs %.0fh, All(#%d) needs %.0fh, %d others all have %.0fh, interval is %f.1min\n", kubernetesIdx, kubernetesHours, allIdx, allHours, kp, hours, intervalP*60.)
	it, ip := 0, 0
	for i, project := range values.Projects {
		if project.DB == "gha" {
			generateCronEntries(&values, i, true, true, -1, -1, kt, kp, hours, kubernetesHours, allHours, intervalT, intervalP, minutes, ghaOffset, syncHours)
			continue
		}
		if project.DB == "allprj" {
			generateCronEntries(&values, i, true, true, -2, -2, kt, kp, hours, kubernetesHours, allHours, intervalT, intervalP, minutes, ghaOffset, syncHours)
			continue
		}
		t := !project.SuspendCronTest && project.Domains[1] != 0
		p := !project.SuspendCronProd && project.Domains[0] != 0
		generateCronEntries(&values, i, t, p, it, ip, kt, kp, hours, kubernetesHours, allHours, intervalT, intervalP, minutes, ghaOffset, syncHours)
		if t {
			it++
		} else {
			project.SuspendCronTest = true
		}
		if p {
			ip++
		} else {
			project.SuspendCronProd = true
		}
	}

	/*
		  jsonBytes, err := jsoniter.Marshal(values)
		  lib.FatalOnError(err)
		  pretty := lib.PrettyPrintJSON(jsonBytes)
			fmt.Printf("%s\n", string(pretty))
	*/
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
