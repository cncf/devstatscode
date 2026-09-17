package devstatscode

import (
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"

	yaml "gopkg.in/yaml.v2"
)

// ProjectFilter - the ingestion rules of a project: what its own gha2db accepts (see RepoHit, ActorHit).
// Used by the tools that re-check events against the project's org/repo/actor rules (reconcile_dbs, the
// ghapi2db repository events feed pass). A zero Name means "no project": nothing is filtered.
type ProjectFilter struct {
	Name      string // project name, "" - no project: nothing is filtered
	Detail    string // why nothing is filtered (when Name is "")
	Hist      bool   // historical mode: the project's `hist_command_line` rules were requested
	HistRules bool   // historical mode and the project defines `hist_command_line` (else `command_line` is used)
	Forg      map[string]struct{}
	Frepo     map[string]struct{}
	OrgRE     *regexp.Regexp
	RepoRE    *regexp.Regexp
	Ctx       *Ctx // context initialized with the project's `env` applied (exclude list, exact mode, actor filters)
}

// ProjectsPath - path of projects.yaml: GHA2DB_PROJECTS_YAML in the data directory (or ./ in local mode)
func ProjectsPath(ctx *Ctx) string {
	dataPrefix := ctx.DataDir
	if ctx.Local {
		dataPrefix = "./"
	}
	return dataPrefix + ctx.ProjectsYaml
}

// ReadProjects - projects.yaml (GHA2DB_PROJECTS_YAML) from the data directory (or ./ in local mode)
func ReadProjects(ctx *Ctx) (*AllProjects, string) {
	path := ProjectsPath(ctx)
	data, err := ioutil.ReadFile(path)
	FatalOnError(err)
	var projects AllProjects
	FatalOnError(yaml.Unmarshal(data, &projects))
	return &projects, path
}

// ReadProjectsIfPresent - like ReadProjects, but a missing file is not an error (nil projects)
func ReadProjectsIfPresent(ctx *Ctx) (*AllProjects, string) {
	path := ProjectsPath(ctx)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil, path
		}
		FatalOnError(err)
	}
	return ReadProjects(ctx)
}

// ParseFilterArg - one `command_line` item of a project the way gha2db gets it from gha2db_sync (comma split,
// trimmed, re-joined) and parses it: `regexp:` prefix - a regexp, else a set of names (empty - no restriction)
func ParseFilterArg(arg string) (map[string]struct{}, *regexp.Regexp) {
	stripFunc := func(x string) string { return strings.TrimSpace(x) }
	joined := strings.Join(StringsMapToArray(stripFunc, strings.Split(arg, ",")), ",")
	if strings.HasPrefix(joined, "regexp:") {
		return nil, regexp.MustCompile(joined[7:])
	}
	return StringsMapToSet(stripFunc, strings.Split(joined, ",")), nil
}

// ProjectForDB - the project of the target database: GHA2DB_PROJECT when it names an enabled project with
// that database, else the first (order, name) enabled project with `psql_db` = target
func ProjectForDB(ctx *Ctx, projects *AllProjects, target string) (string, *Project) {
	if ctx.Project != "" {
		if project, ok := projects.Projects[ctx.Project]; ok && !IsProjectDisabled(ctx, ctx.Project, project.Disabled) {
			if strings.TrimSpace(project.PDB) == target {
				return ctx.Project, &project
			}
		}
	}
	bestName := ""
	var best *Project
	for name, project := range projects.Projects {
		if IsProjectDisabled(ctx, name, project.Disabled) {
			continue
		}
		if strings.TrimSpace(project.PDB) != target {
			continue
		}
		if best == nil || project.Order < best.Order || (project.Order == best.Order && name < bestName) {
			p := project
			best = &p
			bestName = name
		}
	}
	return bestName, best
}

// NewProjectFilter - the ingestion rules of the target database's project (none when projects is nil or no
// enabled project uses the database); the project's `env` is applied like gha2db_sync does (unless ENV_SET).
// hist - historical mode: use the project's `hist_command_line` (the biggest scope it ever had) when defined,
// else its `command_line` like the daily runs do.
func NewProjectFilter(ctx *Ctx, projects *AllProjects, target, path string, hist bool) ProjectFilter {
	if projects == nil {
		return ProjectFilter{Detail: "no " + path, Hist: hist}
	}
	name, project := ProjectForDB(ctx, projects, target)
	if project == nil {
		return ProjectFilter{Detail: fmt.Sprintf("no enabled project uses this database in %s", path), Hist: hist}
	}
	if os.Getenv("ENV_SET") == "" {
		for envK, envV := range project.Env {
			FatalOnError(os.Setenv(envK, envV))
		}
	}
	var fctx Ctx
	fctx.Init()
	filter := ProjectFilter{Name: name, Ctx: &fctx, Hist: hist}
	commandLine := project.CommandLine
	if hist && len(project.HistCommandLine) > 0 {
		commandLine = project.HistCommandLine
		filter.HistRules = true
	}
	orgArg, repoArg := "", ""
	if len(commandLine) > 0 {
		orgArg = commandLine[0]
	}
	if len(commandLine) > 1 {
		repoArg = commandLine[1]
	}
	filter.Forg, filter.OrgRE = ParseFilterArg(orgArg)
	filter.Frepo, filter.RepoRE = ParseFilterArg(repoArg)
	return filter
}

// Active - is there a project whose rules are applied?
func (f *ProjectFilter) Active() bool {
	return f.Name != ""
}

// Hit - would the project's gha2db ingest an event of this repository by this actor?
func (f *ProjectFilter) Hit(repoName, actorLogin string) bool {
	return f.RepoHit(repoName) && f.ActorHit(actorLogin)
}

// RepoHit - does this repository (full name) pass the project's org/repo rules?
func (f *ProjectFilter) RepoHit(repoName string) bool {
	if f.Name == "" {
		return true
	}
	return RepoHit(f.Ctx, repoName, f.Forg, f.Frepo, f.OrgRE, f.RepoRE)
}

// ActorHit - does this actor pass the project's actor rules (GHA2DB_ACTORS_FILTER/ALLOW/FORBID)?
func (f *ProjectFilter) ActorHit(actorLogin string) bool {
	if f.Name == "" {
		return true
	}
	return ActorHit(f.Ctx, actorLogin)
}

// namesInfo - "any org" / "3 org(s)" / "org regexp '...'"
func namesInfo(kind string, names map[string]struct{}, re *regexp.Regexp) string {
	if re != nil {
		return fmt.Sprintf("%s regexp '%s'", kind, re.String())
	}
	if len(names) == 0 {
		return "any " + kind
	}
	return fmt.Sprintf("%d %s(s)", len(names), kind)
}

// Info - human readable filter description
func (f *ProjectFilter) Info() string {
	if f.Name == "" {
		return fmt.Sprintf("none (%s)", f.Detail)
	}
	mode := ""
	if f.HistRules {
		mode = " (historical rules)"
	} else if f.Hist {
		mode = " (historical rules: none, using command_line)"
	}
	return fmt.Sprintf(
		"project '%s'%s: %s, %s, %d excluded repo(s), exact %v, actors filter %v",
		f.Name, mode, namesInfo("org", f.Forg, f.OrgRE), namesInfo("repo", f.Frepo, f.RepoRE), len(f.Ctx.ExcludeRepos), f.Ctx.Exact, f.Ctx.ActorsFilter,
	)
}
