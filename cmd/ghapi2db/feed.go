package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	lib "github.com/cncf/devstatscode"
	"github.com/google/go-github/v38/github"
	jsoniter "github.com/json-iterator/go"
)

// Repository events feed pass: GH Archive is built from the very same
// `GET /repos/{owner}/{repo}/events` objects, so every event the feed still
// shows (at most 300 per repository, 3 pages of 100, 90 days back) is written
// with the gha2db writer under its native id - GH Archive's own copy, when it
// arrives, is a duplicate and skipped by either side.

// feedPerPage - events per page, feedMaxPages - GitHub returns 422 for page 4
const (
	feedPerPage  = 100
	feedMaxPages = 3
)

// feedPage - one page of the repository events feed as raw JSON objects
func feedPage(gctx context.Context, gc *ghClients, org, repo string, page int) ([]json.RawMessage, *github.Response, error) {
	var events []json.RawMessage
	resp, err := gc.do(func(cl *github.Client) (*github.Response, error) {
		req, err := cl.NewRequest("GET", fmt.Sprintf("repos/%s/%s/events?per_page=%d&page=%d", org, repo, feedPerPage, page), nil)
		if err != nil {
			return nil, err
		}
		events = nil
		return cl.Do(gctx, req, &events)
	})
	if err != nil {
		return nil, resp, err
	}
	return events, resp, nil
}

// restoreRepoEventsRepo - write the events of one repository's feed with the gha2db writer
func restoreRepoEventsRepo(gctx context.Context, gc *ghClients, c *sql.DB, ctx *lib.Ctx, filter *lib.ProjectFilter, org, repo, orgRepo string, repoID int64, shas map[string]string, stats *restoreStats) {
	name := passRepoEvents.label()
	// bug 75: gha_repos also holds repositories the project's gha2db never ingests (renamed into another
	// organization, historical scope, another project sharing the database) - their feeds are not written either
	if !filter.RepoHit(orgRepo) {
		stats.filteredRepos++
		if ctx.Debug > 0 {
			lib.Printf("%s: %s: outside project '%s' org/repo rules, skipping the feed\n", name, orgRepo, filter.Name)
		}
		return
	}
	for page := 1; page <= feedMaxPages; page++ {
		var (
			events []json.RawMessage
			got    bool
		)
		info := fmt.Sprintf("%s: %s events page %d", name, orgRepo, page)
		more := apiPage(ctx, info, func() (*github.Response, bool, error) {
			evs, resp, err := feedPage(gctx, gc, org, repo, page)
			if err != nil {
				return resp, false, err
			}
			events, got = evs, true
			return resp, resp.NextPage > 0, nil
		})
		if !got {
			return
		}
		stats.pages++
		var oldest time.Time
		for _, raw := range events {
			var ev lib.Event
			if err := jsoniter.Unmarshal(raw, &ev); err != nil {
				lib.Printf("WARNING: %s: %s: cannot unmarshal a feed event: %v, skipping the feed\n", name, orgRepo, err)
				return
			}
			if ev.Repo.ID == 0 && ev.Repo.Name == "" {
				// GitHub blanks the repository object of some events (a fork into a private
				// repository for example) - they are still this repository's events
				ev.Repo.ID, ev.Repo.Name = int(repoID), orgRepo
				if ctx.Debug > 0 {
					lib.Printf("%s: %s: %s %s has no repository object, attributed to the feed's repository\n", name, orgRepo, ev.Type, ev.ID)
				}
			}
			if int64(ev.Repo.ID) != repoID && !trackedRepo(c, ctx, orgRepo, int64(ev.Repo.ID)) {
				lib.Printf("WARNING: %s: %s: the feed belongs to %s (id %d) which is not tracked, skipping\n", name, orgRepo, ev.Repo.Name, ev.Repo.ID)
				return
			}
			stats.checked++
			if oldest.IsZero() || ev.CreatedAt.Before(oldest) {
				oldest = ev.CreatedAt
			}
			// the same actor filters gha2db applies to the archives
			if !lib.ActorHit(ctx, ev.Actor.Login) {
				continue
			}
			// and the project's org/repo/actor rules (bug 75)
			if !filter.Hit(ev.Repo.Name, ev.Actor.Login) {
				stats.filteredEvents++
				if ctx.Debug > 0 {
					lib.Printf("%s: %s: %s %s by %s is outside project '%s' org/repo/actor rules, skipping\n", name, orgRepo, ev.Type, ev.ID, ev.Actor.Login, filter.Name)
				}
				continue
			}
			if lib.WriteToDB(c, ctx, &ev, shas) == 0 {
				continue
			}
			stats.restored++
			stats.addType(ev.Type)
			stats.mark(ev.CreatedAt)
			if eid, err := strconv.ParseInt(ev.ID, 10, 64); err == nil {
				// the stored (banded) id, see lib.NativeEventID - the targeted postprocess selects by it
				stats.eids = append(stats.eids, lib.NativeEventID(eid, ev.Type, ev.CreatedAt))
			}
			if ctx.Debug > 0 {
				lib.Printf("%s: %s: restored %s %s (%v)\n", name, orgRepo, ev.Type, ev.ID, ev.CreatedAt)
			}
		}
		if ctx.Debug > 0 {
			lib.Printf("%s: %s: page %d: %d events, oldest %v, restored so far %d\n", name, orgRepo, page, len(events), oldest, stats.restored)
		}
		// only the `Link: next` header decides: GitHub filters the feed after paginating it
		// (a "full" page holds 84-96 events while more pages follow) and orders it by id,
		// which is no longer monotonic in time, so neither a short page nor an old event
		// on the page means the remaining pages are old
		if !more {
			return
		}
	}
}

// syncRepoEvents - repository events feed pass
func syncRepoEvents(ctx *lib.Ctx) restoreStats {
	shas := lib.GetHidden(ctx, lib.HideCfgFile)
	name := passRepoEvents.label()
	// the feed writes native events with the gha2db writer, so it accepts exactly what the project's own
	// gha2db accepts from the archives: its projects.yaml `command_line` org/repo rules and actor filters
	// (bug 75) - no rules when there is no projects.yaml or no enabled project uses this database
	projects, path := lib.ReadProjectsIfPresent(ctx)
	filter := lib.NewProjectFilter(ctx, projects, ctx.PgDB, path, false)
	if filter.Active() || ctx.Debug > 0 {
		lib.Printf("%s: filter: %s\n", name, filter.Info())
	}
	stats := restorePass(ctx, passRepoEvents, func(gctx context.Context, gc *ghClients, c *sql.DB, ctx *lib.Ctx, org, repo, orgRepo string, repoID int64, orgID interface{}, recentDt time.Time, maybeHide func(string) string, stats *restoreStats) {
		restoreRepoEventsRepo(gctx, gc, c, ctx, &filter, org, repo, orgRepo, repoID, shas, stats)
	})
	if filter.Active() {
		lib.Printf("%s: filtered out %d repo(s) and %d event(s) outside project '%s' org/repo/actor rules\n", name, stats.filteredRepos, stats.filteredEvents, filter.Name)
	}
	return stats
}
