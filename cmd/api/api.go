package main

import (
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

func returnError(w http.ResponseWriter, err error) {
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

func apiHealth(w http.ResponseWriter, payload map[string]interface{}) {
	if len(payload) == 0 {
		returnError(w, fmt.Errorf("API 'health' 'payload' section empty or missing"))
		return
	}
	iproject, ok := payload["project"]
	if !ok {
		returnError(w, fmt.Errorf("API 'health' missing 'project' field in 'payload' section"))
		return
	}
  project, ok := iproject.(string)
	if !ok {
		returnError(w, fmt.Errorf("API 'health' 'payload' 'project' field '%+v' is not a string", iproject))
		return
	}
	db, err := nameToDB(project)
	if err != nil {
		returnError(w, err)
		return
	}
	fmt.Printf("project:%s: db:%s\n", project, db)
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
	case "health":
		apiHealth(w, pl.Payload)
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
