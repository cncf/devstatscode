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
func feedPage(gctx context.Context, gc *github.Client, org, repo string, page int) ([]json.RawMessage, *github.Response, error) {
	req, err := gc.NewRequest("GET", fmt.Sprintf("repos/%s/%s/events?per_page=%d&page=%d", org, repo, feedPerPage, page), nil)
	if err != nil {
		return nil, nil, err
	}
	var events []json.RawMessage
	resp, err := gc.Do(gctx, req, &events)
	if err != nil {
		return nil, resp, err
	}
	return events, resp, nil
}

// restoreRepoEventsRepo - write the events of one repository's feed with the gha2db writer
func restoreRepoEventsRepo(gctx context.Context, gc *github.Client, c *sql.DB, ctx *lib.Ctx, org, repo, orgRepo string, repoID int64, shas map[string]string, stats *restoreStats) {
	name := passRepoEvents.label()
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
			if lib.WriteToDB(c, ctx, &ev, shas) == 0 {
				continue
			}
			stats.restored++
			stats.addType(ev.Type)
			stats.mark(ev.CreatedAt)
			if eid, err := strconv.ParseInt(ev.ID, 10, 64); err == nil {
				stats.eids = append(stats.eids, eid)
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
	return restorePass(ctx, passRepoEvents, func(gctx context.Context, gc *github.Client, c *sql.DB, ctx *lib.Ctx, org, repo, orgRepo string, repoID int64, orgID interface{}, recentDt time.Time, maybeHide func(string) string, stats *restoreStats) {
		restoreRepoEventsRepo(gctx, gc, c, ctx, org, repo, orgRepo, repoID, shas, stats)
	})
}
