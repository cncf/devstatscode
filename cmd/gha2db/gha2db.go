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
	// gUseCache - use gEmailName2LoginIDCache or not
	gUseCache = true
	// gCacheMtx - cache access mutex
	gCacheMtx = &sync.RWMutex{}
	// gEmailName2LoginIDCache - cache found actors (login, ID) pairs for (name, email) pairs
	gEmailName2LoginIDCache = make(map[[2]string][2]string)
	// gGitTrailerPattern - message trailer pattern
	gGitTrailerPattern = regexp.MustCompile(`^(?P<name>[a-zA-z0-9\-]+)\:[ \t]+(?P<value>.+)$`)
	// gGitAllowedTrailers - allowed commit trailer flags (lowercase/case insensitive -> correct case)
	gGitAllowedTrailers = map[string][]string{
		"about-fscking-timed-by":                 {"Reviewed-by"},
		"accked-by":                              {"Reviewed-by"},
		"aced-by":                                {"Reviewed-by"},
		"ack":                                    {"Reviewed-by"},
		"ack-by":                                 {"Reviewed-by"},
		"ackde-by":                               {"Reviewed-by"},
		"acked":                                  {"Reviewed-by"},
		"acked-and-reviewed":                     {"Reviewed-by"},
		"acked-and-reviewed-by":                  {"Reviewed-by"},
		"acked-and-tested-by":                    {"Reviewed-by", "Tested-by"},
		"acked-b":                                {"Reviewed-by"},
		"acked-by":                               {"Reviewed-by"},
		"acked-by-stale-maintainer":              {"Reviewed-by"},
		"acked-by-with-comments":                 {"Reviewed-by"},
		"acked-by-without-testing":               {"Reviewed-by"},
		"acked-for-mfd-by":                       {"Reviewed-by"},
		"acked-for-now-by":                       {"Reviewed-by"},
		"acked-off-by":                           {"Reviewed-by"},
		"acked-the-net-bits-by":                  {"Reviewed-by"},
		"acked-the-tulip-bit-by":                 {"Reviewed-by"},
		"acked-with-apologies-by":                {"Reviewed-by"},
		"acked_by":                               {"Reviewed-by"},
		"ackedby":                                {"Reviewed-by"},
		"ackeded-by":                             {"Reviewed-by"},
		"acknowledged-by":                        {"Reviewed-by"},
		"acted-by":                               {"Reviewed-by"},
		"actually-written-by":                    {"Co-authored-by"},
		"additional-author":                      {"Co-authored-by"},
		"all-the-fault-of":                       {"Informed-by"},
		"also-analyzed-by":                       {"Reviewed-by"},
		"also-fixed-by":                          {"Co-authored-by"},
		"also-posted-by":                         {"Reported-by"},
		"also-reported-and-tested-by":            {"Reported-by", "Tested-by"},
		"also-reported-by":                       {"Reported-by"},
		"also-spotted-by":                        {"Reported-by"},
		"also-suggested-by":                      {"Reviewed-by"},
		"also-written-by":                        {"Co-authored-by"},
		"analysed-by":                            {"Reviewed-by"},
		"analyzed-by":                            {"Reviewed-by"},
		"aoled-by":                               {"Reviewed-by"},
		"apology-from":                           {"Informed-by"},
		"appreciated-by":                         {"Informed-by"},
		"approved":                               {"Approved-by"},
		"approved-by":                            {"Approved-by"},
		"architected-by":                         {"Influenced-by"},
		"assisted-by":                            {"Co-authored-by"},
		"badly-reviewed-by ":                     {"Reviewed-by"},
		"based-in-part-on-patch-by":              {"Influenced-by"},
		"based-on":                               {"Influenced-by"},
		"based-on-a-patch-by":                    {"Influenced-by"},
		"based-on-code-by":                       {"Influenced-by"},
		"based-on-code-from":                     {"Influenced-by"},
		"based-on-comments-by":                   {"Influenced-by"},
		"based-on-idea-by":                       {"Influenced-by"},
		"based-on-original-patch-by":             {"Influenced-by"},
		"based-on-patch-by":                      {"Influenced-by"},
		"based-on-patch-from":                    {"Influenced-by"},
		"based-on-patches-by":                    {"Influenced-by"},
		"based-on-similar-patches-by":            {"Influenced-by"},
		"based-on-suggestion-from":               {"Influenced-by"},
		"based-on-text-by":                       {"Influenced-by"},
		"based-on-the-original-screenplay-by":    {"Influenced-by"},
		"based-on-the-true-story-by":             {"Influenced-by"},
		"based-on-work-by":                       {"Influenced-by"},
		"based-on-work-from":                     {"Influenced-by"},
		"belatedly-acked-by":                     {"Reviewed-by"},
		"bisected-and-acked-by":                  {"Reviewed-by"},
		"bisected-and-analyzed-by":               {"Reviewed-by"},
		"bisected-and-reported-by":               {"Reported-by"},
		"bisected-and-tested-by":                 {"Reported-by", "Tested-by"},
		"bisected-by":                            {"Reviewed-by"},
		"bisected-reported-and-tested-by":        {"Reviewed-by", "Tested-by"},
		"bitten-by-and-tested-by":                {"Reviewed-by", "Tested-by"},
		"bitterly-acked-by":                      {"Reviewed-by"},
		"blame-taken-by":                         {"Informed-by"},
		"bonus-points-awarded-by":                {"Reviewed-by"},
		"boot-tested-by":                         {"Tested-by"},
		"brainstormed-with":                      {"Influenced-by"},
		"broken-by":                              {"Informed-by"},
		"bug-actually-spotted-by":                {"Reported-by"},
		"bug-fixed-by":                           {"Resolved-by"},
		"bug-found-by":                           {"Reported-by"},
		"bug-identified-by":                      {"Reported-by"},
		"bug-reported-by":                        {"Reported-by"},
		"bug-spotted-by":                         {"Reported-by"},
		"build-fixes-from":                       {"Resolved-by"},
		"build-tested-by":                        {"Tested-by"},
		"build-testing-by":                       {"Tested-by"},
		"catched-by-and-rightfully-ranted-at-by": {"Reported-by"},
		"caught-by":                              {"Reported-by"},
		"cause-discovered-by":                    {"Reported-by"},
		"cautiously-acked-by":                    {"Reviewed-by"},
		"cc":                                     {"Informed-by"},
		"celebrated-by":                          {"Reviewed-by"},
		"changelog-cribbed-from":                 {"Influenced-by"},
		"changelog-heavily-inspired-by":          {"Influenced-by"},
		"chucked-on-by":                          {"Reviewed-by"},
		"cked-by":                                {"Reviewed-by"},
		"cleaned-up-by":                          {"Co-authored-by"},
		"cleanups-from":                          {"Co-authored-by"},
		"co-author":                              {"Co-authored-by"},
		"co-authored":                            {"Co-authored-by"},
		"co-authored-by":                         {"Co-authored-by"},
		"co-debugged-by":                         {"Co-authored-by"},
		"co-developed-by":                        {"Co-authored-by"},
		"co-developed-with":                      {"Co-authored-by"},
		"committed":                              {"Committed-by"},
		"committed-by":                           {"Co-authored-by", "Committed-by"},
		"compile-tested-by":                      {"Tested-by"},
		"compiled-by":                            {"Tested-by"},
		"compiled-tested-by":                     {"Tested-by"},
		"complained-about-by":                    {"Reported-by"},
		"conceptually-acked-by":                  {"Reviewed-by"},
		"confirmed-by":                           {"Reviewed-by"},
		"confirms-rustys-story-ends-the-same-by": {"Reviewed-by"},
		"contributors":                           {"Co-authored-by"},
		"credit":                                 {"Co-authored-by"},
		"credit-to":                              {"Co-authored-by"},
		"credits-by":                             {"Reviewed-by"},
		"csigned-off-by":                         {"Co-authored-by"},
		"cut-and-paste-bug-by":                   {"Reported-by"},
		"debuged-by":                             {"Tested-by"},
		"debugged-and-acked-by":                  {"Reviewed-by"},
		"debugged-and-analyzed-by":               {"Reviewed-by", "Tested-by"},
		"debugged-and-tested-by":                 {"Reviewed-by", "Tested-by"},
		"debugged-by":                            {"Tested-by"},
		"deciphered-by":                          {"Tested-by"},
		"decoded-by":                             {"Tested-by"},
		"delightedly-acked-by":                   {"Reviewed-by"},
		"demanded-by":                            {"Reported-by"},
		"derived-from-code-by":                   {"Co-authored-by"},
		"designed-by":                            {"Influenced-by"},
		"diagnoised-by":                          {"Tested-by"},
		"diagnosed-and-reported-by":              {"Reported-by"},
		"diagnosed-by":                           {"Tested-by"},
		"discovered-and-analyzed-by":             {"Reported-by"},
		"discovered-by":                          {"Reported-by"},
		"discussed-with":                         {"Co-authored-by"},
		"earlier-version-tested-by":              {"Tested-by"},
		"embarrassingly-acked-by":                {"Reviewed-by"},
		"emphatically-acked-by":                  {"Reviewed-by"},
		"encouraged-by":                          {"Influenced-by"},
		"enthusiastically-acked-by":              {"Reviewed-by"},
		"enthusiastically-supported-by":          {"Reviewed-by"},
		"evaluated-by":                           {"Tested-by"},
		"eventually-typed-in-by":                 {"Reported-by"},
		"eviewed-by":                             {"Reviewed-by"},
		"explained-by":                           {"Influenced-by"},
		"fairly-blamed-by":                       {"Reported-by"},
		"fine-by-me":                             {"Reviewed-by"},
		"finished-by":                            {"Co-authored-by"},
		"fix-creation-mandated-by":               {"Resolved-by"},
		"fix-proposed-by":                        {"Resolved-by"},
		"fix-suggested-by":                       {"Resolved-by"},
		"fixed-by":                               {"Resolved-by"},
		"fixes-from":                             {"Resolved-by"},
		"forwarded-by":                           {"Informed-by"},
		"found-by":                               {"Reported-by"},
		"found-ok-by":                            {"Tested-by"},
		"from":                                   {"Informed-by"},
		"grudgingly-acked-by":                    {"Reviewed-by"},
		"grumpily-reviewed-by":                   {"Reviewed-by"},
		"guess-its-ok-by":                        {"Reviewed-by"},
		"hella-acked-by":                         {"Reviewed-by"},
		"helped-by":                              {"Co-authored-by"},
		"helped-out-by":                          {"Co-authored-by"},
		"hinted-by":                              {"Influenced-by"},
		"historical-research-by":                 {"Co-authored-by"},
		"humbly-acked-by":                        {"Reviewed-by"},
		"i-dont-see-any-problems-with-it":        {"Reviewed-by"},
		"idea-by":                                {"Influenced-by"},
		"idea-from":                              {"Influenced-by"},
		"identified-by":                          {"Reported-by"},
		"improved-by":                            {"Influenced-by"},
		"improvements-by":                        {"Influenced-by"},
		"includes-changes-by":                    {"Influenced-by"},
		"initial-analysis-by":                    {"Co-authored-by"},
		"initial-author":                         {"Co-authored-by"},
		"initial-fix-by":                         {"Resolved-by"},
		"initial-patch-by":                       {"Co-authored-by"},
		"initial-work-by":                        {"Co-authored-by"},
		"inspired-by":                            {"Influenced-by"},
		"inspired-by-patch-from":                 {"Influenced-by"},
		"intermittently-reported-by":             {"Reported-by"},
		"investigated-by":                        {"Tested-by"},
		"lightly-tested-by":                      {"Tested-by"},
		"liked-by":                               {"Reviewed-by"},
		"list-usage-fixed-by":                    {"Resolved-by"},
		"looked-over-by":                         {"Reviewed-by"},
		"looks-good-to":                          {"Reviewed-by"},
		"looks-great-to":                         {"Reviewed-by"},
		"looks-ok-by":                            {"Reviewed-by"},
		"looks-okay-to":                          {"Reviewed-by"},
		"looks-reasonable-to":                    {"Reviewed-by"},
		"makes-sense-to":                         {"Reviewed-by"},
		"makes-sparse-happy":                     {"Reviewed-by"},
		"maybe-reported-by":                      {"Reported-by"},
		"mentored-by":                            {"Influenced-by"},
		"modified-and-reviewed-by":               {"Reviewed-by"},
		"modified-by":                            {"Co-authored-by"},
		"more-or-less-tested-by":                 {"Tested-by"},
		"most-definitely-acked-by":               {"Reviewed-by"},
		"mostly-acked-by":                        {"Reviewed-by"},
		"much-requested-by":                      {"Reported-by"},
		"nacked-by":                              {"Reviewed-by"},
		"naked-by":                               {"Reviewed-by"},
		"narrowed-down-by":                       {"Reviewed-by"},
		"niced-by":                               {"Reviewed-by"},
		"no-objection-from-me-by":                {"Reviewed-by"},
		"no-problems-with":                       {"Reviewed-by"},
		"not-nacked-by":                          {"Reviewed-by"},
		"noted-by":                               {"Reviewed-by"},
		"noticed-and-acked-by":                   {"Reviewed-by"},
		"noticed-by":                             {"Reviewed-by"},
		"okay-ished-by":                          {"Reviewed-by"},
		"oked-to-go-through-tracing-tree-by":     {"Reviewed-by"},
		"once-upon-a-time-reviewed-by":           {"Reviewed-by"},
		"original-author":                        {"Co-authored-by"},
		"original-by":                            {"Co-authored-by"},
		"original-from":                          {"Co-authored-by"},
		"original-idea-and-signed-off-by":        {"Co-authored-by"},
		"original-idea-by":                       {"Influenced-by"},
		"original-patch-acked-by":                {"Reviewed-by"},
		"original-patch-by":                      {"Co-authored-by"},
		"original-signed-off-by":                 {"Co-authored-by"},
		"original-version-by":                    {"Co-authored-by"},
		"originalauthor":                         {"Co-authored-by"},
		"originally-by":                          {"Co-authored-by"},
		"originally-from":                        {"Co-authored-by"},
		"originally-suggested-by":                {"Influenced-by"},
		"originally-written-by":                  {"Co-authored-by"},
		"origionally-authored-by":                {"Co-authored-by"},
		"origionally-signed-off-by":              {"Co-authored-by"},
		"partially-reviewed-by":                  {"Reviewed-by"},
		"partially-tested-by":                    {"Tested-by"},
		"partly-suggested-by":                    {"Co-authored-by"},
		"patch-by":                               {"Co-authored-by"},
		"patch-fixed-up-by":                      {"Resolved-by"},
		"patch-from":                             {"Co-authored-by"},
		"patch-inspired-by":                      {"Influenced-by"},
		"patch-originally-by":                    {"Co-authored-by"},
		"patch-updated-by":                       {"Co-authored-by"},
		"patiently-pointed-out-by":               {"Reported-by"},
		"pattern-pointed-out-by":                 {"Influenced-by"},
		"performance-tested-by":                  {"Tested-by"},
		"pinpointed-by":                          {"Reported-by"},
		"pointed-at-by":                          {"Reported-by"},
		"pointed-out-and-tested-by":              {"Reported-by", "Tested-by"},
		"proposed-by":                            {"Reported-by"},
		"pushed-by":                              {"Co-authored-by"},
		"ranted-by":                              {"Reported-by"},
		"re-reported-by":                         {"Reported-by"},
		"reasoning-sounds-sane-to":               {"Reviewed-by"},
		"recalls-having-tested-once-upon-a-time-by": {"Tested-by"},
		"received-from":                                  {"Informed-by"},
		"recommended-by":                                 {"Reviewed-by"},
		"reivewed-by":                                    {"Reviewed-by"},
		"reluctantly-acked-by":                           {"Reviewed-by"},
		"repored-and-bisected-by":                        {"Reported-by"},
		"reporetd-by":                                    {"Reported-by"},
		"reporeted-and-tested-by":                        {"Reported-by", "Tested-by"},
		"report-by":                                      {"Reported-by"},
		"reportded-by":                                   {"Reported-by"},
		"reported":                                       {"Reported-by"},
		"reported--and-debugged-by":                      {"Reported-by", "Tested-by"},
		"reported-acked-and-tested-by":                   {"Reported-by", "Tested-by"},
		"reported-analyzed-and-tested-by":                {"Reported-by"},
		"reported-and-acked-by":                          {"Reviewed-by"},
		"reported-and-bisected-and-tested-by":            {"Reviewed-by", "Tested-by"},
		"reported-and-bisected-by":                       {"Reported-by"},
		"reported-and-reviewed-and-tested-by":            {"Reviewed-by", "Tested-by"},
		"reported-and-root-caused-by":                    {"Reported-by"},
		"reported-and-suggested-by":                      {"Reported-by"},
		"reported-and-test-by":                           {"Reported-by"},
		"reported-and-tested-by":                         {"Tested-by"},
		"reported-any-tested-by":                         {"Tested-by"},
		"reported-bisected-and-tested-by":                {"Reported-by", "Tested-by"},
		"reported-bisected-and-tested-by-the-invaluable": {"Reported-by", "Tested-by"},
		"reported-bisected-tested-by":                    {"Reported-by", "Tested-by"},
		"reported-bistected-and-tested-by":               {"Reported-by", "Tested-by"},
		"reported-by":                                    {"Reported-by"},
		"reported-by-and-tested-by":                      {"Reported-by", "Tested-by"},
		"reported-by-tested-by":                          {"Tested-by"},
		"reported-by-with-patch":                         {"Reported-by"},
		"reported-debuged-tested-acked-by":               {"Tested-by"},
		"reported-off-by":                                {"Reported-by"},
		"reported-requested-and-tested-by":               {"Reported-by", "Tested-by"},
		"reported-reviewed-and-acked-by":                 {"Reviewed-by"},
		"reported-tested-and-acked-by":                   {"Reviewed-by", "Tested-by"},
		"reported-tested-and-bisected-by":                {"Reported-by", "Tested-by"},
		"reported-tested-and-fixed-by":                   {"Co-authored-by", "Reported-by", "Tested-by"},
		"reported-tested-by":                             {"Tested-by"},
		"reported_by":                                    {"Reported-by"},
		"reportedy-and-tested-by":                        {"Reported-by", "Tested-by"},
		"reproduced-by":                                  {"Tested-by"},
		"requested-and-acked-by":                         {"Reviewed-by"},
		"requested-and-tested-by":                        {"Tested-by"},
		"requested-by":                                   {"Reported-by"},
		"researched-with":                                {"Co-authored-by"},
		"reveiewed-by":                                   {"Reviewed-by"},
		"review-by":                                      {"Reviewed-by"},
		"reviewd-by":                                     {"Reviewed-by"},
		"reviewed":                                       {"Reviewed-by"},
		"reviewed-and-tested-by":                         {"Reviewed-by", "Tested-by"},
		"reviewed-and-wanted-by":                         {"Reviewed-by"},
		"reviewed-by":                                    {"Reviewed-by"},
		"reviewed-off-by":                                {"Reviewed-by"},
		"reviewed–by":                                    {"Reviewed-by"},
		"reviewer":                                       {"Reviewed-by"},
		"reviewws-by":                                    {"Reviewed-by"},
		"root-cause-analysis-by":                         {"Reported-by"},
		"root-cause-found-by":                            {"Reported-by"},
		"seconded-by":                                    {"Reviewed-by"},
		"seems-ok":                                       {"Reviewed-by"},
		"seems-reasonable-to":                            {"Reviewed-by"},
		"sefltests-acked-by":                             {"Reviewed-by"},
		"sent-by":                                        {"Informed-by"},
		"serial-parts-acked-by":                          {"Reviewed-by"},
		"siged-off-by":                                   {"Co-authored-by"},
		"sighed-off-by":                                  {"Co-authored-by"},
		"signed":                                         {"Signed-off-by"},
		"signed-by":                                      {"Signed-off-by"},
		"signed-off":                                     {"Signed-off-by"},
		"signed-off-by":                                  {"Signed-off-by"},
		"singend-off-by":                                 {"Signed-off-by"},
		"slightly-grumpily-acked-by":                     {"Reviewed-by"},
		"smoke-tested-by":                                {"Tested-by"},
		"some-suggestions-by":                            {"Influenced-by"},
		"spotted-by":                                     {"Reported-by"},
		"submitted-by":                                   {"Co-authored-by"},
		"suggested-and-acked-by":                         {"Reviewed-by"},
		"suggested-and-reviewed-by":                      {"Reviewed-by"},
		"suggested-and-tested-by":                        {"Reviewed-by", "Tested-by"},
		"suggested-by":                                   {"Reviewed-by"},
		"tested":                                         {"Tested-by"},
		"tested-and-acked-by":                            {"Tested-by"},
		"tested-and-bugfixed-by":                         {"Resolved-by", "Tested-by"},
		"tested-and-reported-by":                         {"Reported-by", "Tested-by"},
		"tested-by":                                      {"Tested-by"},
		"tested-off":                                     {"Tested-by"},
		"thanks-to":                                      {"Influenced-by", "Informed-by"},
		"to":                                             {"Informed-by"},
		"tracked-by":                                     {"Tested-by"},
		"tracked-down-by":                                {"Tested-by"},
		"was-acked-by":                                   {"Reviewed-by"},
		"weak-reviewed-by":                               {"Reviewed-by"},
		"workflow-found-ok-by":                           {"Reviewed-by"},
		"written-by":                                     {"Reported-by"},
	}
)

// Inserts single GHA Actor
func ghaActor(con *sql.Tx, ctx *lib.Ctx, actor *lib.Actor, maybeHide func(string) string) {
	// gha_actors
	// {"id:Fixnum"=>48592, "login:String"=>48592, "display_login:String"=>48592,
	// "gravatar_id:String"=>48592, "url:String"=>48592, "avatar_url:String"=>48592}
	// {"id"=>8, "login"=>34, "display_login"=>34, "gravatar_id"=>0, "url"=>63, "avatar_url"=>49}
	lib.ExecSQLTxWithErr(
		con,
		ctx,
		lib.InsertIgnore("into gha_actors(id, login, name) "+lib.NValues(3)),
		lib.AnyArray{actor.ID, maybeHide(actor.Login), ""}...,
	)
}

// Inserts single GHA Repo
func ghaRepo(db *sql.DB, ctx *lib.Ctx, repo *lib.Repo, orgID, orgLogin interface{}) {
	// gha_repos
	// {"id:Fixnum"=>48592, "name:String"=>48592, "url:String"=>48592}
	// {"id"=>8, "name"=>111, "url"=>140}
	lib.ExecSQLWithErr(
		db,
		ctx,
		lib.InsertIgnore("into gha_repos(id, name, org_id, org_login) "+lib.NValues(4)),
		lib.AnyArray{repo.ID, repo.Name, orgID, orgLogin}...,
	)
}

// Inserts single GHA Org
func ghaOrg(db *sql.DB, ctx *lib.Ctx, org *lib.Org) {
	// gha_orgs
	// {"id:Fixnum"=>18494, "login:String"=>18494, "gravatar_id:String"=>18494,
	// "url:String"=>18494, "avatar_url:String"=>18494}
	// {"id"=>8, "login"=>38, "gravatar_id"=>0, "url"=>66, "avatar_url"=>49}
	if org != nil {
		lib.ExecSQLWithErr(
			db,
			ctx,
			lib.InsertIgnore("into gha_orgs(id, login) "+lib.NValues(2)),
			lib.AnyArray{org.ID, org.Login}...,
		)
	}
}

// Inserts single GHA Milestone
func ghaMilestone(con *sql.Tx, ctx *lib.Ctx, eid string, milestone *lib.Milestone, ev *lib.Event, maybeHide func(string) string) {
	// creator
	if milestone.Creator != nil {
		ghaActor(con, ctx, milestone.Creator, maybeHide)
	}

	// gha_milestones
	lib.ExecSQLTxWithErr(
		con,
		ctx,
		"insert into gha_milestones("+
			"id, event_id, closed_at, closed_issues, created_at, creator_id, "+
			"description, due_on, number, open_issues, state, title, updated_at, "+
			"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
			"dupn_creator_login) "+lib.NValues(20),
		lib.AnyArray{
			milestone.ID,
			eid,
			lib.TimeOrNil(milestone.ClosedAt),
			milestone.ClosedIssues,
			milestone.CreatedAt,
			lib.ActorIDOrNil(milestone.Creator),
			lib.TruncStringOrNil(milestone.Description, 0xffff),
			lib.TimeOrNil(milestone.DueOn),
			milestone.Number,
			milestone.OpenIssues,
			milestone.State,
			lib.TruncToBytes(milestone.Title, 200),
			milestone.UpdatedAt,
			ev.Actor.ID,
			maybeHide(ev.Actor.Login),
			ev.Repo.ID,
			ev.Repo.Name,
			ev.Type,
			ev.CreatedAt,
			lib.ActorLoginOrNil(milestone.Creator, maybeHide),
		}...,
	)
}

// Inserts single GHA Forkee (old format < 2015)
func ghaForkeeOld(con *sql.Tx, ctx *lib.Ctx, eid string, forkee *lib.ForkeeOld, actor *lib.Actor, repo *lib.Repo, ev *lib.EventOld, maybeHide func(string) string) {

	// Lookup author by GitHub login
	aid := lookupActorTx(con, ctx, forkee.Owner, maybeHide)

	// Owner
	owner := lib.Actor{ID: aid, Login: forkee.Owner}
	ghaActor(con, ctx, &owner, maybeHide)

	// gha_forkees
	// Table details and analysis in `analysis/analysis.txt` and `analysis/forkee_*.json`
	lib.ExecSQLTxWithErr(
		con,
		ctx,
		"insert into gha_forkees("+
			"id, event_id, name, full_name, owner_id, description, fork, "+
			"created_at, updated_at, pushed_at, homepage, size, language, organization, "+
			"stargazers_count, has_issues, has_projects, has_downloads, "+
			"has_wiki, has_pages, forks, default_branch, open_issues, watchers, public, "+
			"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
			"dup_owner_login) "+lib.NValues(32),
		lib.AnyArray{
			forkee.ID,
			eid,
			lib.TruncToBytes(forkee.Name, 80),
			lib.TruncToBytes(forkee.Name, 200), // ForkeeOld has no FullName
			owner.ID,
			lib.TruncStringOrNil(forkee.Description, 0xffff),
			forkee.Fork,
			forkee.CreatedAt,
			forkee.CreatedAt, // ForkeeOld has no UpdatedAt
			lib.TimeOrNil(forkee.PushedAt),
			lib.StringOrNil(forkee.Homepage),
			forkee.Size,
			lib.StringOrNil(forkee.Language),
			lib.StringOrNil(forkee.Organization),
			forkee.Stargazers,
			forkee.HasIssues,
			nil,
			forkee.HasDownloads,
			forkee.HasWiki,
			nil,
			forkee.Forks,
			lib.TruncToBytes(forkee.DefaultBranch, 200),
			forkee.OpenIssues,
			forkee.Watchers,
			lib.NegatedBoolOrNil(forkee.Private),
			actor.ID,
			maybeHide(actor.Login),
			repo.ID,
			repo.Name,
			ev.Type,
			ev.CreatedAt,
			maybeHide(owner.Login),
		}...,
	)
}

// Inserts single GHA Forkee
func ghaForkee(con *sql.Tx, ctx *lib.Ctx, eid string, forkee *lib.Forkee, ev *lib.Event, maybeHide func(string) string) {
	// owner
	ghaActor(con, ctx, &forkee.Owner, maybeHide)

	// gha_forkees
	// Table details and analysis in `analysis/analysis.txt` and `analysis/forkee_*.json`
	lib.ExecSQLTxWithErr(
		con,
		ctx,
		"insert into gha_forkees("+
			"id, event_id, name, full_name, owner_id, description, fork, "+
			"created_at, updated_at, pushed_at, homepage, size, language, organization, "+
			"stargazers_count, has_issues, has_projects, has_downloads, "+
			"has_wiki, has_pages, forks, default_branch, open_issues, watchers, public, "+
			"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
			"dup_owner_login) "+lib.NValues(32),
		lib.AnyArray{
			forkee.ID,
			eid,
			lib.TruncToBytes(forkee.Name, 80),
			lib.TruncToBytes(forkee.FullName, 200),
			forkee.Owner.ID,
			lib.TruncStringOrNil(forkee.Description, 0xffff),
			forkee.Fork,
			forkee.CreatedAt,
			forkee.UpdatedAt,
			lib.TimeOrNil(forkee.PushedAt),
			lib.StringOrNil(forkee.Homepage),
			forkee.Size,
			nil,
			nil,
			forkee.StargazersCount,
			forkee.HasIssues,
			lib.BoolOrNil(forkee.HasProjects),
			forkee.HasDownloads,
			forkee.HasWiki,
			lib.BoolOrNil(forkee.HasPages),
			forkee.Forks,
			lib.TruncToBytes(forkee.DefaultBranch, 200),
			forkee.OpenIssues,
			forkee.Watchers,
			lib.BoolOrNil(forkee.Public),
			ev.Actor.ID,
			maybeHide(ev.Actor.Login),
			ev.Repo.ID,
			ev.Repo.Name,
			ev.Type,
			ev.CreatedAt,
			maybeHide(forkee.Owner.Login),
		}...,
	)
}

// Inserts single GHA Branch
func ghaBranch(con *sql.Tx, ctx *lib.Ctx, eid string, branch *lib.Branch, ev *lib.Event, skipIDs []int, maybeHide func(string) string) {
	// user
	if branch.User != nil {
		ghaActor(con, ctx, branch.User, maybeHide)
	}

	// repo
	if branch.Repo != nil {
		rid := branch.Repo.ID
		insert := true
		for _, skipID := range skipIDs {
			if rid == skipID {
				insert = false
				break
			}
		}
		if insert {
			ghaForkee(con, ctx, eid, branch.Repo, ev, maybeHide)
		}
	}

	// gha_branches
	lib.ExecSQLTxWithErr(
		con,
		ctx,
		"insert into gha_branches("+
			"sha, event_id, user_id, repo_id, label, ref, "+
			"dup_type, dup_created_at, dupn_user_login, dupn_forkee_name"+
			") "+lib.NValues(10),
		lib.AnyArray{
			branch.SHA,
			eid,
			lib.ActorIDOrNil(branch.User),
			lib.ForkeeIDOrNil(branch.Repo), // GitHub uses JSON "repo" but it conatins Forkee
			lib.TruncToBytes(branch.Label, 200),
			lib.TruncToBytes(branch.Ref, 200),
			ev.Type,
			ev.CreatedAt,
			lib.ActorLoginOrNil(branch.User, maybeHide),
			lib.ForkeeNameOrNil(branch.Repo),
		}...,
	)
}

// Search for given label using name & color
// If not found, return hash as its ID
func lookupLabel(con *sql.Tx, ctx *lib.Ctx, name string, color string) int {
	rows := lib.QuerySQLTxWithErr(
		con,
		ctx,
		fmt.Sprintf(
			"select id from gha_labels where name=%s and color=%s",
			lib.NValue(1),
			lib.NValue(2),
		),
		name,
		color,
	)
	defer func() { lib.FatalOnError(rows.Close()) }()
	lid := 0
	for rows.Next() {
		lib.FatalOnError(rows.Scan(&lid))
	}
	lib.FatalOnError(rows.Err())
	if lid == 0 {
		lid = lib.HashStrings([]string{name, color})
	}
	return lid
}

// Search for given actor using his/her login
// If not found, return hash as its ID
func lookupActor(db *sql.DB, ctx *lib.Ctx, login string, maybeHide func(string) string) int {
	hlogin := maybeHide(login)
	rows := lib.QuerySQLWithErr(
		db,
		ctx,
		fmt.Sprintf("select id from gha_actors where login=%s order by id desc limit 1", lib.NValue(1)),
		hlogin,
	)
	defer func() { lib.FatalOnError(rows.Close()) }()
	aid := 0
	for rows.Next() {
		lib.FatalOnError(rows.Scan(&aid))
	}
	lib.FatalOnError(rows.Err())
	if aid == 0 {
		aid = lib.HashStrings([]string{login})
	}
	return aid
}

// Search for given actor using his/her login
// If not found, return hash as its ID
func lookupActorTx(con *sql.Tx, ctx *lib.Ctx, login string, maybeHide func(string) string) int {
	hlogin := maybeHide(login)
	rows := lib.QuerySQLTxWithErr(
		con,
		ctx,
		fmt.Sprintf("select id from gha_actors where login=%s order by id desc limit 1", lib.NValue(1)),
		hlogin,
	)
	defer func() { lib.FatalOnError(rows.Close()) }()
	aid := 0
	for rows.Next() {
		lib.FatalOnError(rows.Scan(&aid))
	}
	lib.FatalOnError(rows.Err())
	if aid == 0 {
		aid = lib.HashStrings([]string{login})
	}
	return aid
}

// Search for given actor using his/her name and email
// If not found, return hash as its ID
// Uses DB object, not TX
func lookupActorNameEmail(con *sql.DB, ctx *lib.Ctx, name, email string, maybeHide func(string) string) (int, string) {
	if gUseCache {
		gCacheMtx.RLock()
		data, ok := gEmailName2LoginIDCache[[2]string{email, name}]
		gCacheMtx.RUnlock()
		if ok {
			id, _ := strconv.Atoi(data[0])
			// fmt.Printf("cache success: (%s,%s) -> (%d,%s)\n", email, name, id, data[1])
			return id, data[1]
		}
	}
	// By email
	hemail := maybeHide(email)
	erows := lib.QuerySQLWithErr(
		con,
		ctx,
		fmt.Sprintf("select a.id, a.login from gha_actors a, gha_actors_emails ae where a.id = ae.actor_id and ae.email=%s order by a.id desc limit 1", lib.NValue(1)),
		hemail,
	)
	defer func() { lib.FatalOnError(erows.Close()) }()
	eaid := 0
	elogin := ""
	for erows.Next() {
		lib.FatalOnError(erows.Scan(&eaid, &elogin))
	}
	lib.FatalOnError(erows.Err())
	if eaid != 0 {
		if gUseCache {
			gCacheMtx.Lock()
			gEmailName2LoginIDCache[[2]string{email, name}] = [2]string{strconv.Itoa(eaid), elogin}
			gCacheMtx.Unlock()
		}
		return eaid, elogin
	}

	// By name from actors names table
	hname := maybeHide(name)
	nrows := lib.QuerySQLWithErr(
		con,
		ctx,
		fmt.Sprintf("select a.id, a.login from gha_actors a, gha_actors_names an where a.id = an.actor_id and an.name=%s order by a.id desc limit 1", lib.NValue(1)),
		hname,
	)
	defer func() { lib.FatalOnError(nrows.Close()) }()
	naid := 0
	nlogin := ""
	for nrows.Next() {
		lib.FatalOnError(nrows.Scan(&naid, &nlogin))
	}
	lib.FatalOnError(nrows.Err())
	if naid != 0 {
		if gUseCache {
			gCacheMtx.Lock()
			gEmailName2LoginIDCache[[2]string{email, name}] = [2]string{strconv.Itoa(naid), nlogin}
			gCacheMtx.Unlock()
		}
		return naid, nlogin
	}

	// By name from actors table
	n2rows := lib.QuerySQLWithErr(
		con,
		ctx,
		fmt.Sprintf("select id, login from gha_actors where name=%s order by id desc limit 1", lib.NValue(1)),
		hname,
	)
	defer func() { lib.FatalOnError(n2rows.Close()) }()
	n2aid := 0
	n2login := ""
	for n2rows.Next() {
		lib.FatalOnError(n2rows.Scan(&n2aid, &n2login))
	}
	lib.FatalOnError(n2rows.Err())
	if n2aid != 0 {
		if gUseCache {
			gCacheMtx.Lock()
			gEmailName2LoginIDCache[[2]string{email, name}] = [2]string{strconv.Itoa(n2aid), n2login}
			gCacheMtx.Unlock()
		}
		return n2aid, n2login
	}
	return 0, ""
}

// Search for given actor using his/her name and email
// If not found, return hash as its ID
// Uses TX object not DB
func lookupActorNameEmailTx(con *sql.Tx, ctx *lib.Ctx, name, email string, maybeHide func(string) string) (int, string) {
	if gUseCache {
		gCacheMtx.RLock()
		data, ok := gEmailName2LoginIDCache[[2]string{email, name}]
		gCacheMtx.RUnlock()
		if ok {
			id, _ := strconv.Atoi(data[0])
			// fmt.Printf("cache success: (%s,%s) -> (%d,%s)\n", email, name, id, data[1])
			return id, data[1]
		}
	}
	// By email
	hemail := maybeHide(email)
	erows := lib.QuerySQLTxWithErr(
		con,
		ctx,
		fmt.Sprintf("select a.id, a.login from gha_actors a, gha_actors_emails ae where a.id = ae.actor_id and ae.email=%s order by a.id desc limit 1", lib.NValue(1)),
		hemail,
	)
	defer func() { lib.FatalOnError(erows.Close()) }()
	eaid := 0
	elogin := ""
	for erows.Next() {
		lib.FatalOnError(erows.Scan(&eaid, &elogin))
	}
	lib.FatalOnError(erows.Err())
	if eaid != 0 {
		if gUseCache {
			gCacheMtx.Lock()
			gEmailName2LoginIDCache[[2]string{email, name}] = [2]string{strconv.Itoa(eaid), elogin}
			gCacheMtx.Unlock()
		}
		return eaid, elogin
	}

	// By name from actors names table
	hname := maybeHide(name)
	nrows := lib.QuerySQLTxWithErr(
		con,
		ctx,
		fmt.Sprintf("select a.id, a.login from gha_actors a, gha_actors_names an where a.id = an.actor_id and an.name=%s order by a.id desc limit 1", lib.NValue(1)),
		hname,
	)
	defer func() { lib.FatalOnError(nrows.Close()) }()
	naid := 0
	nlogin := ""
	for nrows.Next() {
		lib.FatalOnError(nrows.Scan(&naid, &nlogin))
	}
	lib.FatalOnError(nrows.Err())
	if naid != 0 {
		if gUseCache {
			gCacheMtx.Lock()
			gEmailName2LoginIDCache[[2]string{email, name}] = [2]string{strconv.Itoa(naid), nlogin}
			gCacheMtx.Unlock()
		}
		return naid, nlogin
	}

	// By name from actors table
	n2rows := lib.QuerySQLTxWithErr(
		con,
		ctx,
		fmt.Sprintf("select id, login from gha_actors where name=%s order by id desc limit 1", lib.NValue(1)),
		hname,
	)
	defer func() { lib.FatalOnError(n2rows.Close()) }()
	n2aid := 0
	n2login := ""
	for n2rows.Next() {
		lib.FatalOnError(n2rows.Scan(&n2aid, &n2login))
	}
	lib.FatalOnError(n2rows.Err())
	if n2aid != 0 {
		if gUseCache {
			gCacheMtx.Lock()
			gEmailName2LoginIDCache[[2]string{email, name}] = [2]string{strconv.Itoa(n2aid), n2login}
			gCacheMtx.Unlock()
		}
		return n2aid, n2login
	}
	return 0, ""
}

// Try to find Repo by name and Organization
func findRepoFromNameAndOrg(db *sql.DB, ctx *lib.Ctx, repoName string, orgID *int) (int, bool) {
	var rows *sql.Rows
	if orgID != nil {
		rows = lib.QuerySQLWithErr(
			db,
			ctx,
			fmt.Sprintf(
				"select id from gha_repos where name=%s and org_id=%s",
				lib.NValue(1),
				lib.NValue(2),
			),
			repoName,
			orgID,
		)
	} else {
		rows = lib.QuerySQLWithErr(
			db,
			ctx,
			fmt.Sprintf(
				"select id from gha_repos where name=%s and org_id is null",
				lib.NValue(1),
			),
			repoName,
		)
	}
	defer func() { lib.FatalOnError(rows.Close()) }()
	exists := false
	rid := 0
	for rows.Next() {
		lib.FatalOnError(rows.Scan(&rid))
		exists = true
	}
	lib.FatalOnError(rows.Err())
	return rid, exists
}

// Try to find OrgID for given OrgLogin (returns nil for nil)
func findOrgIDOrNil(db *sql.DB, ctx *lib.Ctx, orgLogin *string) *int {
	var orgID int
	if orgLogin == nil {
		return nil
	}
	rows := lib.QuerySQLWithErr(
		db,
		ctx,
		fmt.Sprintf(
			"select id from gha_orgs where login=%s",
			lib.NValue(1),
		),
		*orgLogin,
	)
	defer func() { lib.FatalOnError(rows.Close()) }()
	for rows.Next() {
		lib.FatalOnError(rows.Scan(&orgID))
		return &orgID
	}
	lib.FatalOnError(rows.Err())
	return nil
}

// Check if given event existis (given by ID)
func eventExists(db *sql.DB, ctx *lib.Ctx, eventID string) bool {
	rows := lib.QuerySQLWithErr(db, ctx, fmt.Sprintf("select 1 from gha_events where id=%s", lib.NValue(1)), eventID)
	defer func() { lib.FatalOnError(rows.Close()) }()
	exists := false
	for rows.Next() {
		exists = true
	}
	return exists
}

// matchGroups - return regular expression matching groups as a map
func matchGroups(re *regexp.Regexp, arg string) (result map[string]string) {
	match := re.FindStringSubmatch(arg)
	result = make(map[string]string)
	for i, name := range re.SubexpNames() {
		if i > 0 && i <= len(match) {
			result[name] = match[i]
		}
	}
	return
}

// Process commit message trailers
func ghaCommitsRoles(con *sql.Tx, ctx *lib.Ctx, msg, sha, eventID string, repoID int, repoName string, evCreatedAt time.Time, maybeHide func(string) string) {
	// fmt.Printf("got here: sha=%s, created=%v\nmsg:\n%s\n", sha, evCreatedAt, msg)
	msg = strings.Replace(msg, "\r", "\n", -1)
	lines := strings.Split(msg, "\n")
	for _, line := range lines {
		line := strings.TrimSpace(line)
		if line == "" {
			continue
		}
		m := matchGroups(gGitTrailerPattern, line)
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
		id, login := lookupActorNameEmailTx(con, ctx, name, email, maybeHide)
		// fmt.Printf("got trailer(s) '%s': %+v -> ('%s', '%s', %d, '%s')\n", line, trailers, name, email, id, login)
		for _, role := range trailers {
			lib.ExecSQLTxWithErr(
				con,
				ctx,
				lib.InsertIgnore(
					"into gha_commits_roles("+
						"sha, event_id, role, actor_id, actor_login, actor_name, actor_email, "+
						"dup_repo_id, dup_repo_name, dup_created_at"+
						") "+lib.NValues(10)),
				lib.AnyArray{
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
				}...,
			)
		}
	}
	// fmt.Printf("out of here: sha=%s, created=%v\n", sha, evCreatedAt)
}

// Process GHA pages
// gha_pages
// {"page_name:String"=>370, "title:String"=>370, "summary:NilClass"=>370,
// "action:String"=>370, "sha:String"=>370, "html_url:String"=>370}
// {"page_name"=>65, "title"=>65, "summary"=>0, "action"=>7, "sha"=>40, "html_url"=>130}
// 370
func ghaPages(con *sql.Tx, ctx *lib.Ctx, payloadPages *[]lib.Page, eventID string, actor *lib.Actor, repo *lib.Repo, eType string, eCreatedAt time.Time, maybeHide func(string) string) {
	pages := []lib.Page{}
	if payloadPages != nil {
		pages = *payloadPages
	}
	for _, page := range pages {
		sha := page.SHA
		lib.ExecSQLTxWithErr(
			con,
			ctx,
			lib.InsertIgnore(
				"into gha_pages(sha, event_id, action, title, "+
					"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at"+
					") "+lib.NValues(10)),
			lib.AnyArray{
				sha,
				eventID,
				page.Action,
				lib.TruncToBytes(page.Title, 300),
				actor.ID,
				maybeHide(actor.Login),
				repo.ID,
				repo.Name,
				eType,
				eCreatedAt,
			}...,
		)
	}
}

// gha_comments
// Table details and analysis in `analysis/analysis.txt` and `analysis/comment_*.json`
func ghaComment(con *sql.Tx, ctx *lib.Ctx, payloadComment *lib.Comment, eventID string, actor *lib.Actor, repo *lib.Repo, eType string, eCreatedAt time.Time, maybeHide func(string) string) {
	if payloadComment == nil {
		return
	}
	comment := *payloadComment

	// user
	ghaActor(con, ctx, &comment.User, maybeHide)

	// comment
	cid := comment.ID
	lib.ExecSQLTxWithErr(
		con,
		ctx,
		lib.InsertIgnore(
			"into gha_comments("+
				"id, event_id, body, created_at, updated_at, user_id, "+
				"commit_id, original_commit_id, diff_hunk, position, "+
				"original_position, path, pull_request_review_id, line, "+
				"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
				"dup_user_login) "+lib.NValues(21),
		),
		lib.AnyArray{
			cid,
			eventID,
			lib.TruncToBytes(comment.Body, 0xffff),
			comment.CreatedAt,
			comment.UpdatedAt,
			comment.User.ID,
			lib.StringOrNil(comment.CommitID),
			lib.StringOrNil(comment.OriginalCommitID),
			lib.StringOrNil(comment.DiffHunk),
			lib.IntOrNil(comment.Position),
			lib.IntOrNil(comment.OriginalPosition),
			lib.StringOrNil(comment.Path),
			lib.IntOrNil(comment.PullRequestReviewID),
			lib.IntOrNil(comment.Line),
			actor.ID,
			maybeHide(actor.Login),
			repo.ID,
			repo.Name,
			eType,
			eCreatedAt,
			maybeHide(comment.User.Login),
		}...,
	)
}

// gha_reviews
// Table details and analysis in `analysis/analysis.txt` and `analysis/*review_*.json`
func ghaReview(con *sql.Tx, ctx *lib.Ctx, payloadReview *lib.Review, eventID string, actor *lib.Actor, repo *lib.Repo, eType string, eCreatedAt time.Time, maybeHide func(string) string) {
	if payloadReview == nil {
		return
	}
	review := *payloadReview

	// user
	ghaActor(con, ctx, &review.User, maybeHide)

	// review
	rid := review.ID
	lib.ExecSQLTxWithErr(
		con,
		ctx,
		lib.InsertIgnore(
			"into gha_reviews("+
				"id, event_id, state, author_association, submitted_at, user_id, commit_id, body, "+
				"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
				"dup_user_login) "+lib.NValues(15),
		),
		lib.AnyArray{
			rid,
			eventID,
			review.State,
			review.AuthorAssociation,
			review.SubmittedAt,
			review.User.ID,
			review.CommitID,
			lib.TruncStringOrNil(review.Body, 0xffff),
			actor.ID,
			maybeHide(actor.Login),
			repo.ID,
			repo.Name,
			eType,
			eCreatedAt,
			maybeHide(review.User.Login),
		}...,
	)
}

// gha_releases
// Table details and analysis in `analysis/analysis.txt` and `analysis/release_*.json`
func ghaRelease(con *sql.Tx, ctx *lib.Ctx, payloadRelease *lib.Release, eventID string, actor *lib.Actor, repo *lib.Repo, eType string, eCreatedAt time.Time, maybeHide func(string) string) {
	if payloadRelease == nil {
		return
	}
	release := *payloadRelease

	// author
	ghaActor(con, ctx, &release.Author, maybeHide)

	// release
	rid := release.ID
	lib.ExecSQLTxWithErr(
		con,
		ctx,
		"insert into gha_releases("+
			"id, event_id, tag_name, target_commitish, name, draft, "+
			"author_id, prerelease, created_at, published_at, body, "+
			"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
			"dup_author_login) "+lib.NValues(18),
		lib.AnyArray{
			rid,
			eventID,
			lib.TruncToBytes(release.TagName, 200),
			lib.TruncToBytes(release.TargetCommitish, 200),
			lib.TruncStringOrNil(release.Name, 200),
			release.Draft,
			release.Author.ID,
			release.Prerelease,
			release.CreatedAt,
			lib.TimeOrNil(release.PublishedAt),
			lib.TruncStringOrNil(release.Body, 0xffff),
			actor.ID,
			maybeHide(actor.Login),
			repo.ID,
			repo.Name,
			eType,
			eCreatedAt,
			maybeHide(release.Author.Login),
		}...,
	)

	// Assets
	for _, asset := range release.Assets {
		// uploader
		ghaActor(con, ctx, &asset.Uploader, maybeHide)

		// asset
		aid := asset.ID
		lib.ExecSQLTxWithErr(
			con,
			ctx,
			"insert into gha_assets("+
				"id, event_id, name, label, uploader_id, content_type, "+
				"state, size, download_count, created_at, updated_at, "+
				"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
				"dup_uploader_login) "+lib.NValues(18),
			lib.AnyArray{
				aid,
				eventID,
				lib.TruncToBytes(asset.Name, 200),
				lib.TruncStringOrNil(asset.Label, 120),
				asset.Uploader.ID,
				asset.ContentType,
				asset.State,
				asset.Size,
				asset.DownloadCount,
				asset.CreatedAt,
				asset.UpdatedAt,
				actor.ID,
				maybeHide(actor.Login),
				repo.ID,
				repo.Name,
				eType,
				eCreatedAt,
				maybeHide(asset.Uploader.Login),
			}...,
		)

		// release-asset connection
		lib.ExecSQLTxWithErr(
			con,
			ctx,
			"insert into gha_releases_assets(release_id, event_id, asset_id) "+lib.NValues(3),
			lib.AnyArray{rid, eventID, aid}...,
		)
	}
}

// gha_pull_requests
// Table details and analysis in `analysis/analysis.txt` and `analysis/pull_request_*.json`
func ghaPullRequest(con *sql.Tx, ctx *lib.Ctx, payloadPullRequest *lib.PullRequest, eventID string, actor *lib.Actor, repo *lib.Repo, eType string, eCreatedAt time.Time, forkeeIDsToSkip []int, maybeHide func(string) string) {
	if payloadPullRequest == nil {
		return
	}

	// PR object
	pr := *payloadPullRequest

	// user
	ghaActor(con, ctx, &pr.User, maybeHide)

	baseSHA := pr.Base.SHA
	headSHA := pr.Head.SHA
	baseRepoID := lib.ForkeeIDOrNil(pr.Base.Repo)

	// Create Event
	ev := lib.Event{Actor: *actor, Repo: *repo, Type: eType, CreatedAt: eCreatedAt}

	// base
	ghaBranch(con, ctx, eventID, &pr.Base, &ev, forkeeIDsToSkip, maybeHide)

	// head (if different, and skip its repo if defined and the same as base repo)
	if baseSHA != headSHA {
		if baseRepoID != nil {
			forkeeIDsToSkip = append(forkeeIDsToSkip, baseRepoID.(int))
		}
		ghaBranch(con, ctx, eventID, &pr.Head, &ev, forkeeIDsToSkip, maybeHide)
	}

	// merged_by
	if pr.MergedBy != nil {
		ghaActor(con, ctx, pr.MergedBy, maybeHide)
	}

	// assignee
	if pr.Assignee != nil {
		ghaActor(con, ctx, pr.Assignee, maybeHide)
	}

	// milestone
	if pr.Milestone != nil {
		ghaMilestone(con, ctx, eventID, pr.Milestone, &ev, maybeHide)
	}

	// pull_request
	prid := pr.ID
	lib.ExecSQLTxWithErr(
		con,
		ctx,
		"insert into gha_pull_requests("+
			"id, event_id, user_id, base_sha, head_sha, merged_by_id, assignee_id, milestone_id, "+
			"number, state, locked, title, body, created_at, updated_at, closed_at, merged_at, "+
			"merge_commit_sha, merged, mergeable, rebaseable, mergeable_state, comments, "+
			"review_comments, maintainer_can_modify, commits, additions, deletions, changed_files, "+
			"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
			"dup_user_login, dupn_assignee_login, dupn_merged_by_login) "+lib.NValues(38),
		lib.AnyArray{
			prid,
			eventID,
			pr.User.ID,
			baseSHA,
			headSHA,
			lib.ActorIDOrNil(pr.MergedBy),
			lib.ActorIDOrNil(pr.Assignee),
			lib.MilestoneIDOrNil(pr.Milestone),
			pr.Number,
			pr.State,
			lib.BoolOrNil(pr.Locked),
			lib.CleanUTF8(pr.Title),
			lib.TruncStringOrNil(pr.Body, 0xffff),
			pr.CreatedAt,
			pr.UpdatedAt,
			lib.TimeOrNil(pr.ClosedAt),
			lib.TimeOrNil(pr.MergedAt),
			lib.StringOrNil(pr.MergeCommitSHA),
			lib.BoolOrNil(pr.Merged),
			lib.BoolOrNil(pr.Mergeable),
			lib.BoolOrNil(pr.Rebaseable),
			lib.StringOrNil(pr.MergeableState),
			lib.IntOrNil(pr.Comments),
			lib.IntOrNil(pr.ReviewComments),
			lib.BoolOrNil(pr.MaintainerCanModify),
			lib.IntOrNil(pr.Commits),
			lib.IntOrNil(pr.Additions),
			lib.IntOrNil(pr.Deletions),
			lib.IntOrNil(pr.ChangedFiles),
			actor.ID,
			maybeHide(actor.Login),
			repo.ID,
			repo.Name,
			eType,
			eCreatedAt,
			maybeHide(pr.User.Login),
			lib.ActorLoginOrNil(pr.Assignee, maybeHide),
			lib.ActorLoginOrNil(pr.MergedBy, maybeHide),
		}...,
	)

	// Arrays: actors: assignees, requested_reviewers
	// assignees
	var assignees []lib.Actor

	prAid := lib.ActorIDOrNil(pr.Assignee)
	if pr.Assignee != nil {
		assignees = append(assignees, *pr.Assignee)
	}

	if pr.Assignees != nil {
		for _, assignee := range *pr.Assignees {
			aid := assignee.ID
			if aid == prAid {
				continue
			}
			assignees = append(assignees, assignee)
		}
	}

	for _, assignee := range assignees {
		// assignee
		ghaActor(con, ctx, &assignee, maybeHide)

		// pull_request-assignee connection
		lib.ExecSQLTxWithErr(
			con,
			ctx,
			"insert into gha_pull_requests_assignees(pull_request_id, event_id, assignee_id) "+lib.NValues(3),
			lib.AnyArray{prid, eventID, assignee.ID}...,
		)
	}

	// requested_reviewers
	if pr.RequestedReviewers != nil {
		for _, reviewer := range *pr.RequestedReviewers {
			// reviewer
			ghaActor(con, ctx, &reviewer, maybeHide)

			// pull_request-requested_reviewer connection
			lib.ExecSQLTxWithErr(
				con,
				ctx,
				"insert into gha_pull_requests_requested_reviewers(pull_request_id, event_id, requested_reviewer_id) "+lib.NValues(3),
				lib.AnyArray{prid, eventID, reviewer.ID}...,
			)
		}
	}
}

// gha_teams
func ghaTeam(con *sql.Tx, ctx *lib.Ctx, payloadTeam *lib.Team, payloadRepo *lib.Forkee, eventID string, actor *lib.Actor, repo *lib.Repo, eType string, eCreatedAt time.Time, maybeHide func(string) string) {
	if payloadTeam == nil {
		return
	}
	team := *payloadTeam

	// team
	tid := team.ID
	lib.ExecSQLTxWithErr(
		con,
		ctx,
		"insert into gha_teams("+
			"id, event_id, name, slug, permission, "+
			"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at"+
			") "+lib.NValues(11),
		lib.AnyArray{
			tid,
			eventID,
			lib.TruncToBytes(team.Name, 120),
			lib.TruncToBytes(team.Slug, 100),
			lib.TruncToBytes(team.Permission, 20),
			actor.ID,
			maybeHide(actor.Login),
			repo.ID,
			repo.Name,
			eType,
			eCreatedAt,
		}...,
	)

	// team-repository connection
	if payloadRepo != nil {
		lib.ExecSQLTxWithErr(
			con,
			ctx,
			"insert into gha_teams_repositories(team_id, event_id, repository_id) "+lib.NValues(3),
			lib.AnyArray{tid, eventID, payloadRepo.ID}...,
		)
	}
}

// Write GHA entire event (in old pre 2015 format) into Postgres DB
func writeToDBOldFmt(db *sql.DB, ctx *lib.Ctx, eventID string, ev *lib.EventOld, shas map[string]string) int {
	if eventExists(db, ctx, eventID) {
		return 0
	}

	// To handle GDPR
	maybeHide := lib.MaybeHideFunc(shas)

	// Lookup author by GitHub login
	aid := lookupActor(db, ctx, ev.Actor, maybeHide)
	actor := lib.Actor{ID: aid, Login: ev.Actor}

	// Repository
	repository := ev.Repository

	// Find Org ID from Repository.Organization
	oid := findOrgIDOrNil(db, ctx, repository.Organization)

	// Find Repo ID from Repository (this is a ForkeeOld before 2015).
	rid, ok := findRepoFromNameAndOrg(db, ctx, repository.Name, oid)
	if !ok {
		rid = repository.ID
	}

	// We defer transaction create until we're inserting data that can be shared between different events
	lib.ExecSQLWithErr(
		db,
		ctx,
		"insert into gha_events("+
			"id, type, actor_id, repo_id, public, created_at, "+
			"dup_actor_login, dup_repo_name, org_id, forkee_id) "+lib.NValues(10),
		lib.AnyArray{
			eventID,
			ev.Type,
			aid,
			rid,
			ev.Public,
			ev.CreatedAt,
			maybeHide(ev.Actor),
			ev.Repository.Name,
			oid,
			ev.Repository.ID,
		}...,
	)

	// Organization
	if repository.Organization != nil {
		if oid == nil {
			h := lib.HashStrings([]string{*repository.Organization})
			oid = &h
		}
		ghaOrg(db, ctx, &lib.Org{ID: *oid, Login: *repository.Organization})
	}

	// Add Repository
	repo := lib.Repo{ID: rid, Name: repository.Name}
	ghaRepo(db, ctx, &repo, oid, repository.Organization)

	// Pre 2015 Payload
	pl := ev.Payload
	if pl == nil {
		return 0
	}

	iid := lib.FirstIntOrNil([]*int{pl.Issue, pl.IssueID})
	cid := lib.CommentIDOrNil(pl.Comment)
	if cid == nil {
		cid = lib.IntOrNil(pl.CommentID)
	}

	lib.ExecSQLWithErr(
		db,
		ctx,
		"insert into gha_payloads("+
			"event_id, push_id, size, ref, head, befor, action, "+
			"issue_id, pull_request_id, comment_id, ref_type, master_branch, commit, "+
			"description, number, forkee_id, release_id, member_id, "+
			"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at"+
			") "+lib.NValues(24),
		lib.AnyArray{
			eventID,
			nil,
			lib.IntOrNil(pl.Size),
			lib.TruncStringOrNil(pl.Ref, 200),
			lib.StringOrNil(pl.Head),
			nil,
			lib.StringOrNil(pl.Action),
			iid,
			lib.PullRequestIDOrNil(pl.PullRequest),
			cid,
			lib.StringOrNil(pl.RefType),
			lib.TruncStringOrNil(pl.MasterBranch, 200),
			lib.StringOrNil(pl.Commit),
			lib.TruncStringOrNil(pl.Description, 0xffff),
			lib.IntOrNil(pl.Number),
			lib.ForkeeIDOrNil(pl.Repository),
			lib.ReleaseIDOrNil(pl.Release),
			lib.ActorIDOrNil(pl.Member),
			actor.ID,
			maybeHide(actor.Login),
			repo.ID,
			repo.Name,
			ev.Type,
			ev.CreatedAt,
		}...,
	)

	// Start transaction for data possibly shared between events
	con, err := db.Begin()
	lib.FatalOnError(err)

	// gha_actors
	ghaActor(con, ctx, &actor, maybeHide)

	// Payload's Forkee (it uses new structure, so I'm giving it precedence over
	// Event's Forkee (which uses older structure)
	if pl.Repository != nil {
		// Reposotory is actually a Forkee (non old in this case!)
		// Artificial event is only used to allow duplicating EventOld's data
		// (passed as Event to avoid code duplication)
		artificialEv := lib.Event{Actor: actor, Repo: repo, Type: ev.Type, CreatedAt: ev.CreatedAt}
		ghaForkee(con, ctx, eventID, pl.Repository, &artificialEv, maybeHide)
	}

	// Add Forkee in old mode if we didn't added it from payload or if it is a different Forkee
	if pl.Repository == nil || pl.Repository.ID != ev.Repository.ID {
		ghaForkeeOld(con, ctx, eventID, &ev.Repository, &actor, &repo, ev, maybeHide)
	}

	// SHAs - commits
	if pl.SHAs != nil {
		commits := *pl.SHAs
		for _, comm := range commits {
			commit, ok := comm.([]interface{})
			if !ok {
				lib.Fatalf("comm is not []interface{}: %+v", comm)
			}
			sha, ok := commit[0].(string)
			if !ok {
				lib.Fatalf("commit[0] is not string: %+v", commit[0])
			}
			lib.ExecSQLTxWithErr(
				con,
				ctx,
				"insert into gha_commits("+
					"sha, event_id, author_name, encrypted_email, message, is_distinct, "+
					"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at"+
					") "+lib.NValues(12),
				lib.AnyArray{
					sha,
					eventID,
					maybeHide(lib.TruncToBytes(commit[3].(string), 160)),
					lib.TruncToBytes(commit[1].(string), 160),
					lib.TruncToBytes(commit[2].(string), 0xffff),
					commit[4].(bool),
					actor.ID,
					maybeHide(actor.Login),
					repo.ID,
					repo.Name,
					ev.Type,
					ev.CreatedAt,
				}...,
			)
			// Commit Roles
			ghaCommitsRoles(con, ctx, commit[2].(string), sha, eventID, repo.ID, repo.Name, ev.CreatedAt, maybeHide)
		}
	}

	// Pages
	ghaPages(con, ctx, pl.Pages, eventID, &actor, &repo, ev.Type, ev.CreatedAt, maybeHide)

	// Member
	if pl.Member != nil {
		ghaActor(con, ctx, pl.Member, maybeHide)
	}

	// Comment
	ghaComment(con, ctx, pl.Comment, eventID, &actor, &repo, ev.Type, ev.CreatedAt, maybeHide)

	// Release & assets
	ghaRelease(con, ctx, pl.Release, eventID, &actor, &repo, ev.Type, ev.CreatedAt, maybeHide)

	// Team & Repo connection
	ghaTeam(con, ctx, pl.Team, pl.Repository, eventID, &actor, &repo, ev.Type, ev.CreatedAt, maybeHide)

	// Pull Request
	forkeeIDsToSkip := []int{ev.Repository.ID}
	if pl.Repository != nil {
		forkeeIDsToSkip = append(forkeeIDsToSkip, pl.Repository.ID)
	}
	ghaPullRequest(con, ctx, pl.PullRequest, eventID, &actor, &repo, ev.Type, ev.CreatedAt, forkeeIDsToSkip, maybeHide)

	// We need artificial issue
	// gha_issues
	// Table details and analysis in `analysis/analysis.txt` and `analysis/issue_*.json`
	if pl.PullRequest != nil {
		pr := *pl.PullRequest

		// issue
		iid = -pr.ID
		isPR := true
		comments := 0
		locked := false
		if pr.Comments != nil {
			comments = *pr.Comments
		}
		if pr.Locked != nil {
			locked = *pr.Locked
		}
		lib.ExecSQLTxWithErr(
			con,
			ctx,
			"insert into gha_issues("+
				"id, event_id, assignee_id, body, closed_at, comments, created_at, "+
				"locked, milestone_id, number, state, title, updated_at, user_id, "+
				"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
				"dup_user_login, dupn_assignee_login, is_pull_request) "+lib.NValues(23),
			lib.AnyArray{
				iid,
				eventID,
				lib.ActorIDOrNil(pr.Assignee),
				lib.TruncStringOrNil(pr.Body, 0xffff),
				lib.TimeOrNil(pr.ClosedAt),
				comments,
				pr.CreatedAt,
				locked,
				lib.MilestoneIDOrNil(pr.Milestone),
				pr.Number,
				pr.State,
				lib.CleanUTF8(pr.Title),
				pr.UpdatedAt,
				pr.User.ID,
				actor.ID,
				maybeHide(actor.Login),
				repo.ID,
				repo.Name,
				ev.Type,
				ev.CreatedAt,
				maybeHide(pr.User.Login),
				lib.ActorLoginOrNil(pr.Assignee, maybeHide),
				isPR,
			}...,
		)

		var assignees []lib.Actor

		prAid := lib.ActorIDOrNil(pr.Assignee)
		if pr.Assignee != nil {
			assignees = append(assignees, *pr.Assignee)
		}

		if pr.Assignees != nil {
			for _, assignee := range *pr.Assignees {
				aid := assignee.ID
				if aid == prAid {
					continue
				}
				assignees = append(assignees, assignee)
			}
		}

		for _, assignee := range assignees {
			// pull_request-assignee connection
			lib.ExecSQLTxWithErr(
				con,
				ctx,
				"insert into gha_issues_assignees(issue_id, event_id, assignee_id) "+lib.NValues(3),
				lib.AnyArray{iid, eventID, assignee.ID}...,
			)
		}
	}

	// Final commit
	lib.FatalOnError(con.Commit())
	return 1
}

// Write entire GHA event (in a new 2015+ format) into Postgres DB
func writeToDB(db *sql.DB, ctx *lib.Ctx, ev *lib.Event, shas map[string]string) int {
	eventID := ev.ID
	if eventExists(db, ctx, eventID) {
		return 0
	}

	// To handle GDPR
	maybeHide := lib.MaybeHideFunc(shas)

	// We defer transaction create until we're inserting data that can be shared between different events
	// gha_events
	// {"id:String"=>48592, "type:String"=>48592, "actor:Hash"=>48592, "repo:Hash"=>48592,
	// "payload:Hash"=>48592, "public:TrueClass"=>48592, "created_at:String"=>48592,
	// "org:Hash"=>19451}
	// {"id"=>10, "type"=>29, "actor"=>278, "repo"=>290, "payload"=>216017, "public"=>4,
	// "created_at"=>20, "org"=>230}
	// Fields dup_actor_login, dup_repo_name are copied from (gha_actors and gha_repos) to save
	// joins on complex queries (MySQL has no hash joins and is very slow on big tables joins)
	lib.ExecSQLWithErr(
		db,
		ctx,
		"insert into gha_events("+
			"id, type, actor_id, repo_id, public, created_at, "+
			"dup_actor_login, dup_repo_name, org_id, forkee_id) "+lib.NValues(10),
		lib.AnyArray{
			eventID,
			ev.Type,
			ev.Actor.ID,
			ev.Repo.ID,
			ev.Public,
			ev.CreatedAt,
			maybeHide(ev.Actor.Login),
			ev.Repo.Name,
			lib.OrgIDOrNil(ev.Org),
			nil,
		}...,
	)

	// Repository
	repo := ev.Repo
	org := ev.Org
	ghaRepo(db, ctx, &repo, lib.OrgIDOrNil(org), lib.OrgLoginOrNil(org))

	// Organization
	if org != nil {
		ghaOrg(db, ctx, org)
	}

	// gha_payloads
	// {"push_id:Fixnum"=>24636, "size:Fixnum"=>24636, "distinct_size:Fixnum"=>24636,
	// "ref:String"=>30522, "head:String"=>24636, "before:String"=>24636, "commits:Array"=>24636,
	// "action:String"=>14317, "issue:Hash"=>6446, "comment:Hash"=>6055, "ref_type:String"=>8010,
	// "master_branch:String"=>6724, "description:String"=>3701, "pusher_type:String"=>8010,
	// "pull_request:Hash"=>4475, "ref:NilClass"=>2124, "description:NilClass"=>3023,
	// "number:Fixnum"=>2992, "forkee:Hash"=>1211, "pages:Array"=>370, "release:Hash"=>156,
	// "member:Hash"=>219}
	// {"push_id"=>10, "size"=>4, "distinct_size"=>4, "ref"=>110, "head"=>40, "before"=>40,
	// "commits"=>33215, "action"=>9, "issue"=>87776, "comment"=>177917, "ref_type"=>10,
	// "master_branch"=>34, "description"=>3222, "pusher_type"=>4, "pull_request"=>70565,
	// "number"=>5, "forkee"=>6880, "pages"=>855, "release"=>31206, "member"=>1040}
	// 48746
	// using exec_stmt (without select), because payload are per event_id.
	// Columns duplicated from gha_events starts with "dup_"
	pl := ev.Payload
	lib.ExecSQLWithErr(
		db,
		ctx,
		"insert into gha_payloads("+
			"event_id, push_id, size, ref, head, befor, action, "+
			"issue_id, pull_request_id, comment_id, ref_type, master_branch, commit, "+
			"description, number, forkee_id, release_id, member_id, "+
			"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at"+
			") "+lib.NValues(24),
		lib.AnyArray{
			eventID,
			lib.IntOrNil(pl.PushID),
			lib.IntOrNil(pl.Size),
			lib.TruncStringOrNil(pl.Ref, 200),
			lib.StringOrNil(pl.Head),
			lib.StringOrNil(pl.Before),
			lib.StringOrNil(pl.Action),
			lib.IssueIDOrNil(pl.Issue),
			lib.PullRequestIDOrNil(pl.PullRequest),
			lib.CommentIDOrNil(pl.Comment),
			lib.StringOrNil(pl.RefType),
			lib.TruncStringOrNil(pl.MasterBranch, 200),
			nil,
			lib.TruncStringOrNil(pl.Description, 0xffff),
			lib.IntOrNil(pl.Number),
			lib.ForkeeIDOrNil(pl.Forkee),
			lib.ReleaseIDOrNil(pl.Release),
			lib.ActorIDOrNil(pl.Member),
			ev.Actor.ID,
			maybeHide(ev.Actor.Login),
			ev.Repo.ID,
			ev.Repo.Name,
			ev.Type,
			ev.CreatedAt,
		}...,
	)

	// Start transaction for data possibly shared between events
	con, err := db.Begin()
	lib.FatalOnError(err)

	// gha_actors
	ghaActor(con, ctx, &ev.Actor, maybeHide)

	// Make sure that entry is gha_actors is most up-to-date
	/*
		lib.ExecSQLWithErr(
			db,
			ctx,
			fmt.Sprintf(
				"update gha_actors set login=%s where id=%s"+
					lib.NValue(1),
				  lib.NValue(2),
			),
			lib.AnyArray{
				maybeHide(ev.Actor.Login),
				ev.Actor.ID,
			}...,
		)
	*/

	// gha_commits
	// {"sha:String"=>23265, "author:Hash"=>23265, "message:String"=>23265,
	// "distinct:TrueClass"=>21789, "url:String"=>23265, "distinct:FalseClass"=>1476}
	// {"sha"=>40, "author"=>177, "message"=>19005, "distinct"=>5, "url"=>191}
	// author: {"name:String"=>23265, "email:String"=>23265} (only git username/email)
	// author: {"name"=>96, "email"=>95}
	// 23265
	commits := []lib.Commit{}
	if pl.Commits != nil {
		commits = *pl.Commits
	}
	for _, commit := range commits {
		sha := commit.SHA
		lib.ExecSQLTxWithErr(
			con,
			ctx,
			"insert into gha_commits("+
				"sha, event_id, author_name, encrypted_email, message, is_distinct, "+
				"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at"+
				") "+lib.NValues(12),
			lib.AnyArray{
				sha,
				eventID,
				maybeHide(lib.TruncToBytes(commit.Author.Name, 160)),
				lib.TruncToBytes(commit.Author.Email, 160),
				lib.TruncToBytes(commit.Message, 0xffff),
				commit.Distinct,
				ev.Actor.ID,
				maybeHide(ev.Actor.Login),
				ev.Repo.ID,
				ev.Repo.Name,
				ev.Type,
				ev.CreatedAt,
			}...,
		)
		// Commit Roles
		ghaCommitsRoles(con, ctx, commit.Message, sha, eventID, ev.Repo.ID, ev.Repo.Name, ev.CreatedAt, maybeHide)
	}

	// Pages
	ghaPages(con, ctx, pl.Pages, eventID, &ev.Actor, &ev.Repo, ev.Type, ev.CreatedAt, maybeHide)

	// Member
	if pl.Member != nil {
		ghaActor(con, ctx, pl.Member, maybeHide)
	}

	// Comment
	ghaComment(con, ctx, pl.Comment, eventID, &ev.Actor, &ev.Repo, ev.Type, ev.CreatedAt, maybeHide)

	// gha_issues
	// Table details and analysis in `analysis/analysis.txt` and `analysis/issue_*.json`
	if pl.Issue != nil {
		issue := *pl.Issue

		// user, assignee
		ghaActor(con, ctx, &issue.User, maybeHide)
		if issue.Assignee != nil {
			ghaActor(con, ctx, issue.Assignee, maybeHide)
		}

		// issue
		iid := issue.ID
		isPR := false
		if issue.PullRequest != nil {
			isPR = true
		}
		lib.ExecSQLTxWithErr(
			con,
			ctx,
			"insert into gha_issues("+
				"id, event_id, assignee_id, body, closed_at, comments, created_at, "+
				"locked, milestone_id, number, state, title, updated_at, user_id, "+
				"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
				"dup_user_login, dupn_assignee_login, is_pull_request) "+lib.NValues(23),
			lib.AnyArray{
				iid,
				eventID,
				lib.ActorIDOrNil(issue.Assignee),
				lib.TruncStringOrNil(issue.Body, 0xffff),
				lib.TimeOrNil(issue.ClosedAt),
				issue.Comments,
				issue.CreatedAt,
				issue.Locked,
				lib.MilestoneIDOrNil(issue.Milestone),
				issue.Number,
				issue.State,
				lib.CleanUTF8(issue.Title),
				issue.UpdatedAt,
				issue.User.ID,
				ev.Actor.ID,
				maybeHide(ev.Actor.Login),
				ev.Repo.ID,
				ev.Repo.Name,
				ev.Type,
				ev.CreatedAt,
				maybeHide(issue.User.Login),
				lib.ActorLoginOrNil(issue.Assignee, maybeHide),
				isPR,
			}...,
		)

		// milestone
		if issue.Milestone != nil {
			ghaMilestone(con, ctx, eventID, issue.Milestone, ev, maybeHide)
		}

		pAid := lib.ActorIDOrNil(issue.Assignee)
		for _, assignee := range issue.Assignees {
			aid := assignee.ID
			if aid == pAid {
				continue
			}

			// assignee
			ghaActor(con, ctx, &assignee, maybeHide)

			// issue-assignee connection
			lib.ExecSQLTxWithErr(
				con,
				ctx,
				"insert into gha_issues_assignees(issue_id, event_id, assignee_id) "+lib.NValues(3),
				lib.AnyArray{iid, eventID, aid}...,
			)
		}

		// labels
		for _, label := range issue.Labels {
			lid := lib.IntOrNil(label.ID)
			if lid == nil {
				lid = lookupLabel(con, ctx, lib.TruncToBytes(label.Name, 160), label.Color)
			}

			// label
			lib.ExecSQLTxWithErr(
				con,
				ctx,
				lib.InsertIgnore("into gha_labels(id, name, color, is_default) "+lib.NValues(4)),
				lib.AnyArray{lid, lib.TruncToBytes(label.Name, 160), label.Color, lib.BoolOrNil(label.Default)}...,
			)

			// issue-label connection
			lib.ExecSQLTxWithErr(
				con,
				ctx,
				lib.InsertIgnore(
					"into gha_issues_labels(issue_id, event_id, label_id, "+
						"dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, "+
						"dup_issue_number, dup_label_name"+
						") "+lib.NValues(11)),
				lib.AnyArray{
					iid,
					eventID,
					lid,
					ev.Actor.ID,
					maybeHide(ev.Actor.Login),
					ev.Repo.ID,
					ev.Repo.Name,
					ev.Type,
					ev.CreatedAt,
					issue.Number,
					label.Name,
				}...,
			)
		}
	}

	// gha_forkees
	if pl.Forkee != nil {
		ghaForkee(con, ctx, eventID, pl.Forkee, ev, maybeHide)
	}

	// Release & assets
	ghaRelease(con, ctx, pl.Release, eventID, &ev.Actor, &ev.Repo, ev.Type, ev.CreatedAt, maybeHide)

	// Pull Request
	ghaPullRequest(con, ctx, pl.PullRequest, eventID, &ev.Actor, &ev.Repo, ev.Type, ev.CreatedAt, []int{}, maybeHide)

	// Review
	ghaReview(con, ctx, pl.Review, eventID, &ev.Actor, &ev.Repo, ev.Type, ev.CreatedAt, maybeHide)

	// Final commit
	lib.FatalOnError(con.Commit())
	return 1
}

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
				e = writeToDBOldFmt(con, ctx, eid, &hOld, shas)
			} else {
				e = writeToDB(con, ctx, &h, shas)
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
			m := matchGroups(gGitTrailerPattern, line)
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
			id, login := lookupActorNameEmail(con, ctx, name, email, maybeHide)
			// fmt.Printf("got trailer(s) '%s': %+v -> ('%s', '%s', %d, '%s')\n", line, trailers, name, email, id, login)
			for _, role := range trailers {
				ky := kyRoot + role
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
		gCacheMtx.RLock()
		nCache := len(gEmailName2LoginIDCache)
		gCacheMtx.RUnlock()
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
				if nThreads == thrN {
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
			if nThreads == thrN {
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
		id, login := lookupActorNameEmail(con, ctx, name, email, maybeHide)
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
			if nThreads == thrN {
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

	fn := fmt.Sprintf("http://data.gharchive.org/%s.json.gz", lib.ToGHADate(dt))

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
	lib.Printf(getMemUsage() + "\n")
	runtime.GC()
	lib.Printf(getMemUsage() + "\n")
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
			if nThreads == thrN {
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
