//! Repository events feed pass (Go `cmd/ghapi2db/feed.go`): GH Archive is
//! built from the very same `GET /repos/{owner}/{repo}/events` objects, so
//! every event the feed still shows (at most 300 per repository, 3 pages of
//! 100, 90 days back) is written with the gha2db writer under its native id -
//! GH Archive's own copy, when it arrives, is a duplicate and skipped by
//! either side.

use chrono::{DateTime, Utc};
use devstatscode::eventid::native_event_id;
use devstatscode::gha::{actor_hit, zero_time, Event, Org};
use devstatscode::ghapi::repo_name_at;
use devstatscode::ghawriter::write_to_db;
use devstatscode::gofmt;
use devstatscode::pg::SqlArg;
use devstatscode::project_filter::ProjectFilter;
use devstatscode::{printf, Ctx};

use crate::heartbeat::{get_project_rules, repo_owner, scope_untracked, tracked_repo, ApiPass};
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
    // organization, historical scope, another project sharing the database) - their feeds are not written either.
    // A repository renamed out of the rules while a former name is inside (the heartbeat warned about it) is
    // the project's: its feed is written until the configuration covers or excludes the new name
    let untracked = scope_untracked(job.org_repo);
    if !filter.repo_hit(job.org_repo) && !untracked {
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
            // the feed carries the repository's current name on every event, also on those older than a rename:
            // the event gets the name (and organization) of its time, like its GH Archive copy has
            let ev_org_id = match &ev.org {
                Some(org) => SqlArg::Int(org.id),
                None => SqlArg::Null,
            };
            let (at_name, at_org_id, renamed) =
                repo_name_at(c, ctx, ev.repo.id, &ev.repo.name, &ev_org_id, created_at);
            if renamed {
                ev.org = match at_org_id {
                    SqlArg::Int(id) => Some(Org {
                        id,
                        login: repo_owner(&at_name).to_string(),
                    }),
                    _ => None,
                };
                ev.repo.name = at_name;
            }
            // and the project's org/repo/actor rules (bug 75). An event of the feed's own repository also passes
            // the org/repo rules under a name taken from the archived events (the project ingested that name then)
            // and when the repository was renamed out of the rules - unless the name is explicitly excluded
            let repo_hit = filter.repo_hit(&ev.repo.name)
                || ((renamed || untracked)
                    && ev.repo.id == job.repo_id
                    && !filter.repo_excluded(&ev.repo.name));
            if !repo_hit || !filter.actor_hit(&ev.actor.login) {
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
    let filter = &get_project_rules(ctx).filter;
    if filter.active() || ctx.debug > 0 {
        printf!("{}: filter: {}\n", name, filter.info());
    }
    let stats = restore_pass(ctx, ApiPass::RepoEvents, &|job, stats| {
        restore_repo_events_repo(job, filter, stats)
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
