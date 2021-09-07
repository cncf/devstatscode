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
	Proj        string `yaml:"proj"`        // kubernetes
	URL         string `yaml:"url"`         // k8s
	DB          string `yaml:"db"`          // gha
	Icon        string `yaml:"icon"`        // 'k8s'
	Org         string `yaml:"org"`         // 'Kubernetes'
	Repo        string `yaml:"repo"`        // 'kubernetes/kubernetes'
	CronTest    string `yaml:"cronTest"`    // '37 * * * *'
	CronProd    string `yaml:"cronProd"`    // '7 * * * *'
	AffCronTest string `yaml:"affCronTest"` // '0 23 * * 0'
	AffCronProd string `yaml:"affCronProd"` // '0 11 * * 0'
	AffSkipTemp string `yaml:"affSkipTemp"` // '1'
	Disk        string `yaml:"disk"`        // 50Gi
	Domains     [4]int `yaml:"domains"`     // [1, 1, 0, 0]
	GA          string `yaml:"ga"`          // 'UA-108085315-1'
	I           int    `yaml:"i"`           // 0
	CertNum     int    `yaml:"certNum"`     // 1
}

type devstatsValues struct {
	Projects []devstatsProject `yaml:"projects"`
}

func generateCronValues(inFile, outFile string) {
	data, err := ioutil.ReadFile(inFile)
	lib.FatalOnError(err)

	var values devstatsValues
	lib.FatalOnError(yaml.Unmarshal(data, &values))

	kubernetesHours := 18
	str := os.Getenv("KUBERNETES_HOURS")
	if str != "" {
    var err error
		kubernetesHours, err = strconv.Atoi(os.Getenv("KUBERNETES_HOURS"))
		lib.FatalOnError(err)
	}
	allHours := 18
	str = os.Getenv("ALL_HOURS")
	if str != "" {
    var err error
		allHours, err = strconv.Atoi(os.Getenv("ALL_HOURS"))
		lib.FatalOnError(err)
	}
	ghaOffset := 5
	str = os.Getenv("GHA_OFFSET")
	if str != "" {
    var err error
		ghaOffset, err = strconv.Atoi(os.Getenv("GHA_OFFSET"))
		lib.FatalOnError(err)
	}
	syncHours := 2
	str = os.Getenv("SYNC_HOURS")
	if str != "" {
    var err error
		syncHours, err = strconv.Atoi(os.Getenv("SYNC_HOURS"))
		lib.FatalOnError(err)
	}

  minutes := syncHours*(60-ghaOffset)
	hours := float64(7*24 - (kubernetesHours + allHours))
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
    if project.Domains[0] != 0 {
		  kp++
    }
    if project.Domains[1] != 0 {
		  kt++
    }
	}
	intervalT := hours / float64(kt)
	intervalP := hours / float64(kp)
  fmt.Printf("sync happens from HH:%02d, every %d hours, which gives %dmin for hourly syncs\n", ghaOffset, syncHours, minutes)
  fmt.Printf("test: Kubernetes(#%d) needs %dh, All(#%d) needs %dh, %d others all have %.0fh, interval is %f.1min\n", kubernetesIdx, kubernetesHours, allIdx, allHours, kt, hours, intervalT*60.)
  fmt.Printf("prod: Kubernetes(#%d) needs %dh, All(#%d) needs %dh, %d others all have %.0fh, interval is %f.1min\n", kubernetesIdx, kubernetesHours, allIdx, allHours, kp, hours, intervalP*60.)

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
