//! Repository events feed pass (Go `cmd/ghapi2db/feed.go`): GH Archive is
//! built from the very same `GET /repos/{owner}/{repo}/events` objects, so
//! every event the feed still shows (at most 300 per repository, 3 pages of
//! 100, 90 days back) is written with the gha2db writer under its native id -
//! GH Archive's own copy, when it arrives, is a duplicate and skipped by
//! either side.

use chrono::{DateTime, Utc};
use devstatscode::eventid::native_event_id;
use devstatscode::gha::{actor_hit, zero_time, Event};
use devstatscode::ghawriter::write_to_db;
use devstatscode::gofmt;
use devstatscode::project_filter::{new_project_filter, read_projects_if_present, ProjectFilter};
use devstatscode::{printf, Ctx};

use crate::heartbeat::{tracked_repo, ApiPass};
use crate::restore::{api_page, page_failed, restore_pass, RepoJob, RestoreStats};

/// Events per page; GitHub returns 422 for page 4.
const FEED_PER_PAGE: i64 = 100;
const FEED_MAX_PAGES: i64 = 3;

/// Go `%v` of the page's oldest event time (the zero time when the page is empty).
fn oldest_string(oldest: Option<DateTime<Utc>>) -> String {
    match oldest {
        Some(t) => gofmt::time(t),
        None => zero_time().to_string(),
    }
}

/// Go `restoreRepoEventsRepo`: write the events of one repository's feed.
fn restore_repo_events_repo(job: &RepoJob<'_>, filter: &ProjectFilter, stats: &mut RestoreStats) {
    let (gc, c, ctx) = (job.gc, job.c, job.ctx);
    let name = ApiPass::RepoEvents.label();
    // bug 75: gha_repos also holds repositories the project's gha2db never ingests (renamed into another
    // organization, historical scope, another project sharing the database) - their feeds are not written either
    if !filter.repo_hit(job.org_repo) {
        stats.filtered_repos += 1;
        if ctx.debug > 0 {
            printf!(
                "{}: {}: outside project '{}' org/repo rules, skipping the feed\n",
                name,
                job.org_repo,
                filter.name
            );
        }
        return;
    }
    for page in 1..=FEED_MAX_PAGES {
        let mut events: Option<Vec<Box<serde_json::value::RawValue>>> = None;
        let info = format!("{}: {} events page {}", name, job.org_repo, page);
        let more = api_page(ctx, &info, &mut || {
            let r = gc.call(|cl| {
                cl.activity_list_repository_events_raw(job.org, job.repo, FEED_PER_PAGE, page)
            });
            if page_failed(&r) {
                return (r.response, false, r.error);
            }
            events = Some(r.value.unwrap_or_default());
            let resp = r.response.expect("checked above");
            let more = resp.next_page > 0;
            (Some(resp), more, None)
        });
        let events = match events {
            Some(evs) => evs,
            None => return,
        };
        stats.pages += 1;
        let mut oldest: Option<DateTime<Utc>> = None;
        for raw in &events {
            let mut ev: Event = match serde_json::from_str(raw.get()) {
                Ok(ev) => ev,
                Err(e) => {
                    printf!(
                        "WARNING: {}: {}: cannot unmarshal a feed event: {}, skipping the feed\n",
                        name,
                        job.org_repo,
                        e
                    );
                    return;
                }
            };
            if ev.repo.id == 0 && ev.repo.name.is_empty() {
                // GitHub blanks the repository object of some events (a fork into a private
                // repository for example) - they are still this repository's events
                ev.repo.id = job.repo_id;
                ev.repo.name = job.org_repo.to_string();
                if ctx.debug > 0 {
                    printf!(
                        "{}: {}: {} {} has no repository object, attributed to the feed's repository\n",
                        name,
                        job.org_repo,
                        ev.type_,
                        ev.id
                    );
                }
            }
            if ev.repo.id != job.repo_id && !tracked_repo(c, ctx, job.org_repo, ev.repo.id) {
                printf!(
                    "WARNING: {}: {}: the feed belongs to {} (id {}) which is not tracked, skipping\n",
                    name,
                    job.org_repo,
                    ev.repo.name,
                    ev.repo.id
                );
                return;
            }
            stats.checked += 1;
            let created_at = ev.created_at.with_timezone(&Utc);
            if oldest.map(|o| created_at < o).unwrap_or(true) {
                oldest = Some(created_at);
            }
            // the same actor filters gha2db applies to the archives
            if !actor_hit(ctx, &ev.actor.login) {
                continue;
            }
            // and the project's org/repo/actor rules (bug 75)
            if !filter.hit(&ev.repo.name, &ev.actor.login) {
                stats.filtered_events += 1;
                if ctx.debug > 0 {
                    printf!(
                        "{}: {}: {} {} by {} is outside project '{}' org/repo/actor rules, skipping\n",
                        name,
                        job.org_repo,
                        ev.type_,
                        ev.id,
                        ev.actor.login,
                        filter.name
                    );
                }
                continue;
            }
            if write_to_db(c, ctx, &ev, job.maybe_hide) == 0 {
                continue;
            }
            stats.restored += 1;
            stats.add_type(&ev.type_);
            stats.mark(created_at);
            if let Ok(eid) = ev.id.parse::<i64>() {
                // the stored (banded) id, see `eventid::native_event_id` - the targeted
                // postprocess selects by it
                stats.eids.push(native_event_id(eid, &ev.type_, created_at));
            }
            if ctx.debug > 0 {
                printf!(
                    "{}: {}: restored {} {} ({})\n",
                    name,
                    job.org_repo,
                    ev.type_,
                    ev.id,
                    ev.created_at
                );
            }
        }
        if ctx.debug > 0 {
            printf!(
                "{}: {}: page {}: {} events, oldest {}, restored so far {}\n",
                name,
                job.org_repo,
                page,
                events.len(),
                oldest_string(oldest),
                stats.restored
            );
        }
        // only the `Link: next` header decides: GitHub filters the feed after paginating it
        // (a "full" page holds 84-96 events while more pages follow) and orders it by id,
        // which is no longer monotonic in time, so neither a short page nor an old event
        // on the page means the remaining pages are old
        if !more {
            return;
        }
    }
}

/// Go `syncRepoEvents`: the repository events feed pass.
pub fn sync_repo_events(ctx: &mut Ctx) -> RestoreStats {
    let name = ApiPass::RepoEvents.label();
    // the feed writes native events with the gha2db writer, so it accepts exactly what the project's own
    // gha2db accepts from the archives: its projects.yaml `command_line` org/repo rules and actor filters
    // (bug 75) - no rules when there is no projects.yaml or no enabled project uses this database
    let (projects, path) = read_projects_if_present(ctx);
    let filter = new_project_filter(ctx, projects.as_ref(), &ctx.pg_db, &path, false);
    if filter.active() || ctx.debug > 0 {
        printf!("{}: filter: {}\n", name, filter.info());
    }
    let stats = restore_pass(ctx, ApiPass::RepoEvents, &|job, stats| {
        restore_repo_events_repo(job, &filter, stats)
    });
    if filter.active() {
        printf!(
            "{}: filtered out {} repo(s) and {} event(s) outside project '{}' org/repo/actor rules\n",
            name,
            stats.filtered_repos,
            stats.filtered_events,
            filter.name
        );
    }
    stats
}
