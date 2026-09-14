# Filling the GH Archive gaps from the GitHub API — `ghapi2db` / `get_repos` research (2026-09-14)

Status: **research / proposal only — no code changed yet.** Everything below was measured on 2026-09-13/14
against the production kubernetes DB (`gha`, Patroni replica), raw `data.gharchive.org` hour files, the
OpenDigger mirror and live GitHub REST/GraphQL probes (49 prod tokens, read-only).

Constraints given by the project owner (apply to every proposal in this document):

* **no DB schema changes** — only fill existing tables (`gha_events`, `gha_payloads`, `gha_issues`,
  `gha_pull_requests`, `gha_forkees`, `gha_commits`, `gha_actors*`, `gha_*labels/assignees/…`);
* **no schedule changes** — `ghapi2db` keeps its daily per-project run, `get_repos` its current cadence; the
  extra work has to fit into those runs;
* **Rust only** — `rust/cmd/ghapi2db`, `rust/cmd/get_repos`, `rust/devstatscode` (Go is left as is except
  for genuine bug fixes, which are fixed in both per the project rules);
* `gha2db` stays GHA-driven (only a config-level option is mentioned for it, §7.0).

---

## 0. TL;DR

1. **GH Archive is no longer a usable primary source.** Since GitHub's 2025-10-07 Events-API change the
   payloads are stripped (no push commits, PR objects are 6-field stubs, no `author_association`), and since
   ~2026-06 the unmaintained gharchive.org scraper only captures a fraction of the firehose (hour files fell
   from 85–98 MB to 7.8 MB; 2026-09-09 15:00 UTC: **7,414 events vs 624,300** in the OpenDigger mirror).
   Ground truth for the kubernetes orgs: DevStats sees **48–65 %** of issues/PRs opened in 2026-06…08 (100 %
   in 2024-08).
2. **The GitHub API still has almost everything** — with actors, ids and timestamps — and our budget is huge
   (49 tokens × 5,000 REST req/h + 49 × 5,000 GraphQL points/h; conditional `304`s are free). Measured costs:
   heartbeat of 100 repos = 3 GraphQL points, 50 full PR objects = 2 points, one branch history page = 1
   point, `/repos/{o}/{r}/events` = 3 REST requests per repo per run.
3. **What is lost for good:** individual star/watch actors older than ~a day (stargazer lists are restricted
   since 2026-06-30; only the per-repo events feed still shows fresh `WatchEvent`s), and `author_association`.
4. **Today `ghapi2db` only repairs what GHA already told it about** (issues/PRs with an issue-event in the
   window, comments/reviews/forks/releases of *recent* repos), and its repo scope is itself derived from GHA
   (`gha_events` of the last 2 days) — 15 of the 80 kubernetes repos active in the last 2 days (19 %) are
   invisible to every pass. Nothing recreates `IssuesEvent`/`PullRequestEvent` (opened/closed/merged) or
   `gha_forkees` snapshots (0 rows for k/k since 2025-11 → the "stars & forks by repository" dashboards
   `watchers.sql` are frozen).
5. **`get_repos` orphan-commit restore misses 34 % of k/k master commits and 100 % of release-branch
   commits** (630 master commits in 30 days → 413 in DB; 73/73 `release-*` commits missing) because it
   filters `git log` by *commit date* while commits *land* on the branch days later via a merge, and it scans
   the default ref only. Its restored rows are also attributed to the oldest alias
   (`GoogleCloudPlatform/kubernetes`) — bug candidate.
6. **Proposal (§7):** four `ghapi2db` passes and two `get_repos` changes, all writing existing tables with the
   existing artificial-id scheme, all behind `GHA2DB_GHAPISKIP*`-style flags, all deduplicating against whatever
   GHA delivers later. Estimated per-run cost for `allprj` (11.4k repos): ≈ 40k REST requests + ≈ 2k GraphQL
   points, i.e. < 1/6 of one hour of the token pool.

---

## 1. What is happening to GH Archive (measured)

### 1.1 Payload fields (raw hour files, `~/g2r-logs/gha_payload_keys_diff.txt`)

GitHub changelog 2025-08-08 "Upcoming changes to GitHub Events API payloads" (effective 2025-10-07):
<https://github.blog/changelog/2025-08-08-upcoming-changes-to-github-events-api-payloads/>

| Event / object | 2023-06 | 2025-09 | 2025-10-15 onwards |
|---|---|---|---|
| `PushEvent.payload` | `push_id, size, distinct_size, ref, head, before, commits[]` (≤ 20 commits with sha/author/message/distinct) | same | **`push_id, ref, head, before, repository_id` only** |
| `PullRequest*Event.payload.pull_request` | full object (title, body, user, state, created/updated/closed/merged_at, merged_by, additions, deletions, changed_files, commits, comments, review_comments, mergeable*, head/base with full repo objects, labels, milestone, assignees, requested_reviewers) | same | **stub: `id, number, url, head{ref,sha,repo{id,name,url}}, base{…}`** |
| `IssuesEvent` / `IssueCommentEvent.issue` | full | full | full (still complete) |
| `PullRequestReviewCommentEvent.comment` | incl. `line, side, original_line…` | same | line/side removed |
| `ForkEvent.forkee` | full repo incl. `public` | same | `public` removed |
| `*.author_association` | present | present | **removed everywhere** |
| new PR `action`s | – | – | `labeled/unlabeled/assigned/unassigned` (2025-10), `merged` (2025-12) |

Consequences in the DB (kubernetes): `gha_pull_requests` rows written from stubs have `created_at =
updated_at = 0001-01-01`, `title = ''`, `user_id = 0`; PRs whose *only* rows are stubs: 26–36 % (2026-01…05),
14 % (06), 4.3 % (07), 2.2 % (08), 29.7 % (09 so far). The `new_prs`/`issues_opened`-style metrics use
`gha_pull_requests.created_at` → those PRs are invisible to them. PR payload fields were 100 % present until
2025-09, 10.9 % in 2025-10, 0 % since 2025-11.

### 1.2 Volume collapse of gharchive.org (weekday 15:00 UTC hour files)

| hour file | events | file size | notes |
|---|---|---|---|
| 2024-09-11-15 | 228k | 92 MB | healthy |
| 2025-09-10-15 | 166k | 85 MB | |
| 2025-11-12-15 | 145k | 35 MB | pushes stubbed |
| 2026-03-11-15 | 158k | 21 MB | PR 3.3k, issues 1.2k (non-push events already −75 %) |
| 2026-06-10-15 | 128k | 19 MB | PR 646 |
| 2026-07-15-15 | 165k | 17 MB | PR 268 |
| 2026-08-26-15 | 85k | 12 MB | |
| 2026-09-02-15 | **1,797** | | |
| 2026-09-09-15 | **7,414** (PushEvent 0) | 7.8 MB | same hour in the OpenDigger mirror: **624,300** events, 361 MB |

Root cause (gharchive.org is unmaintained): the scraper polls only page 1 (100 events) every 0.75 s —
<https://github.com/igrigorik/gharchive.org/issues/312>, #310, #320, #294; unmerged fix PR #317. The
**OpenDigger mirror** (<https://github.com/igrigorik/gharchive.org/issues/323>,
`https://gharchive.open-digger.cn/YYYY-MM-DD-H.json.gz` + `.manifest.json`, since 2026-09-06) polls all 3
pages adaptively; its 2026-09-09-15 file contains 325k stubbed PushEvents, 107k PR events, 40k IssuesEvents, 51k
issue comments, 19k WatchEvents, keyed by observation time (256k created in hour 15, 187k in hour 14, 61k in
hour 13, tail from the previous day); 92.6 % of gharchive.org's events are included; kubernetes+sigs: 538
events/h vs 84 in 2025-era GHA.

### 1.3 What the kubernetes DB actually receives now (`gha` DB, GHA-native = `0 < id < 2^48`)

| month 2026 | IssuesEvent | PullRequestEvent opened | PushEvent | WatchEvent | restored comments | restored reviews | orphan commits |
|---|---|---|---|---|---|---|---|
| 01 | 2,634 | 1,505 | 2,161 | 1,263 | | | 835 |
| 05 | | | | 295 | | | |
| 06 | | | | 109 | | | |
| 07 | | | | 32 (+661 restored, last time stars restore worked) | | | |
| 08 | **46** | **40** | **341** | 37 | 41k | 18.7k | 5,008 |
| 09 (13 days) | | | 47 | 413 | | | |

Ground truth (GitHub search API, kubernetes orgs) vs DB distinct ids by `created_at`:
2024-08 issues 1031/1044, PRs 4488/4443 (~100 %) · 2025-08 95 % / 89 % · **2026-06 51 % / 48 % · 2026-07
55 % / 54 % · 2026-08 65 % / 65 %**.

`gha_forkees` (source of `metrics/shared/watchers.sql` = "Stars and forks by repository"): rows for
`kubernetes/kubernetes` per month: 2,365 (2025-06) … 641 (2025-10) → **0 since 2025-11**; distinct tracked
repos with rows: 2,217 → 51 (2026-06). The metric takes `max(watchers/forks/open_issues)` per repo alias inside
each period → no rows = no data points.

`gha_commits` for k/k (repo id 20580498), by `dup_created_at`:

| month | GHA-native | orphan-restored | real master commits (git) |
|---|---|---|---|
| 2026-05 | 269 | 171 | ~650 |
| 2026-06 | 258 | 256 | ~650 |
| 2026-07 | 258 | 526 | ~650 |
| 2026-08 | 11 | 176 | ~650 |
| 2026-09 (13 d) | 4 | 223 | |

### 1.4 Stars

Individual star events are not recoverable any more except from the fresh per-repo events feed:
`GET /repos/{o}/{r}/stargazers` and `/subscribers` → **404**, GraphQL `stargazers{…}` → empty (while
`stargazerCount` works). Official: "Upcoming access restrictions to public API endpoints and UI views"
<https://github.blog/changelog/2026-06-30-upcoming-access-restrictions-to-public-api-endpoints-and-ui-views/>
(stargazer/watcher lists limited to admins/collaborators, community discussion #201209). Prod stars restore:
**0 restored / 0 checked in all 1,867 runs of the last 7 days**, silently. kubernetes WatchEvents/month:
1,964 (2025-06) → 37 (2026-08).

---

## 2. What `ghapi2db` does today (Rust `rust/cmd/ghapi2db`, 1:1 with Go)

All passes share `get_api_params()` → `get_recent_repos()` = `select distinct repo_id, dup_repo_name from
gha_events where created_at > now() - GHA2DB_RECENT_REPOS_RANGE` (prod: 2 days), window
`GHA2DB_RECENT_RANGE` (prod: 26 hours). Order in `main()`:

| pass | flag to skip | source | writes | limits |
|---|---|---|---|---|
| `sync_licenses` | `GHA2DB_GHAPISKIPLICENSES` | `GET /repos/{o}/{r}/license` | `gha_repos.license_*` | fine |
| `sync_langs` | `GHA2DB_GHAPISKIPLANGS` | `GET /repos/{o}/{r}/languages` | `gha_repos_langs` | fine |
| `sync_events` | `GHA2DB_GHAPISKIPEVENTS` | `GET /repos/{o}/{r}/issues/events` paged until older than `recent_dt`; one `GET /pulls/{n}` per PR seen | artificial events `2^48 + rest_event_id` of types `labeled, unlabeled, closed, reopened, merged, assigned, …` (`EVENT_TYPES`) + full `gha_issues` / `gha_pull_requests` rows via `sync_issues_state` | only issues/PRs that had an *issue event*; artificial event types are not GHA types → not counted by contribution metrics (correct); does not recreate `IssuesEvent`/`PullRequestEvent` |
| `sync_commits` | `GHA2DB_GHAPISKIPCOMMITS` | `GET /repos/{o}/{r}/commits?since&until` (default branch) | enriches existing `gha_commits` rows (author/committer id+login) | default branch only, only rows already present |
| `sync_comments` / `sync_reviews` | `GHA2DB_GHAPISKIPCOMMENTS` / `…REVIEWS` | `/issues/comments?since`, `/pulls/comments?since`, `/pulls/{n}/reviews` | restored `IssueCommentEvent`, `PullRequestReviewCommentEvent`, `PullRequestReviewEvent` (ids ≥ 2^48) + `gha_comments`/`gha_reviews` | recent repos only; a restored comment does **not** create `gha_issues`/`gha_pull_requests` rows |
| `sync_forks` / `sync_releases` | `…FORKS` / `…RELEASES` | `/forks?sort=newest`, `/releases` | restored `ForkEvent` (+`gha_forkees` for the fork), `ReleaseEvent` | recent repos only |
| `sync_stars` | `GHA2DB_GHAPISKIPSTARS` | `/stargazers` (REST) then GraphQL | negative-id `WatchEvent`s | **dead since 2026-06-30, logs `restored 0 / checked 0` as if fine** |
| `run_event_ids_postprocess` | | | `gha_events_commits_files`, series | keep using for new passes |

Scope feedback loop measured (GraphQL heartbeat over all 385 live kubernetes `gha_repos`): 80 repos had a
push / issue / PR update in the last 2 days, **65 are in `get_recent_repos`, 15 are not** (e.g.
`kubernetes-sigs/cri-tools`, `kubernetes-csi/livenessprobe`, `kubernetes/steering`) → no restore pass ever
looks at them until GHA happens to catch one of their events.

## 3. What `get_repos` does today (Rust `rust/cmd/get_repos`, per-project PVC clones)

| step | flag | what | limits found |
|---|---|---|---|
| clone / fetch | `GHA2DB_PROCESS_REPOS` | `git clone` / `git fetch` every repo of every project under `GHA2DB_REPOS_DIR`, **also under every historical name** | fine |
| `process_commits` | `GHA2DB_PROCESS_COMMITS` | for every sha referenced anywhere (`util_sql/list_unprocessed_commits_files.sql`): `gha_commits_files` + `loc_added/loc_removed/files_changed` | works for restored commits too (Aug k/k: 182/187 have LOC) |
| `backfill_push_event_commits` | `GHA2DB_FETCH_COMMITS_MODE` (prod 1) | for stubbed GHA `PushEvent`s: `git rev-list before..head` → `gha_commits` rows attached to the real event | only as good as the PushEvents GHA delivers (k/k Sep 2026: 47) |
| `restore_orphan_commits` | `GHA2DB_RESTORE_ORPHAN_COMMITS` (prod 1), `GHA2DB_ORPHAN_COMMITS_RANGE` (prod 26 h) | `git log <default ref> --since=YYYY-MM-DD` → for shas not in `gha_commits`: one negative-id artificial `PushEvent` per commit (`created_at` = author date, actor = author via name/email → `gha_actors_emails`), `gha_payloads.action='restored_orphan_commit'`, `gha_commits`, `gha_commits_roles` (Author/Committer/trailers) | see §3.1 |

### 3.1 Measured `restore_orphan_commits` gaps (kubernetes/kubernetes, 2026-08-14 → 09-13)

* `master`: **630** commits reachable (279 merge commits) → **413 in `gha_commits`** (411 restored, 6 GHA-native).
  216 of the 217 missing are **non-merge commits**; every merge commit is present. Mechanism: prow merges PRs
  with a merge commit whose committer date is the landing time, but the PR's own commits keep their original
  committer dates (days/weeks earlier). `git log --since` filters by committer date, so anything committed
  more than ~26–50 h before it was merged never enters the window. The restored ones are dated by author date,
  i.e. *before* they existed on the branch (GHA's `PushEvent` was dated at push time).
* `release-1.34/35/36/37`: **73 commits in 30 days, 0 in the DB** (default ref only; "`--all` would also pick
  upstream history reachable in fork clones" — true, but branches of `origin` are safe).
* All k/k restored rows have `dup_repo_name = 'GoogleCloudPlatform/kubernetes'` (2014 alias; 399/399 since
  2026-08): the pass iterates every historical clone name in parallel and the DB-wide `claimed_shas` set lets
  the alphabetically first alias win. `repo_id` is correct, so repo-group metrics are fine, but per-repo
  views (`dup_repo_name`, `gha_payloads.dup_repo_name`, `gha_commits_roles.dup_repo_name`) show a name
  that stopped existing 11 years ago. **Bug candidate (Go + Rust), fix: prefer the clone whose name is the
  repo's current name (latest `gha_events.dup_repo_name` for that `repo_id`, or `git remote get-url origin`).**

---

## 4. What the GitHub API still offers (live probes, costs)

REST: 5,000 requests/h/token; GraphQL: 5,000 points/h/token; 49 tokens; `If-None-Match` → `304` does not
consume the REST quota (verified).

| need | endpoint / query | measured |
|---|---|---|
| Recent public events of a repo, GHA-shaped, **same ids as GHA**, incl. `WatchEvent` + `ForkEvent` + `IssuesEvent` + `PullRequestEvent` + `IssueCommentEvent` + stubbed `PushEvent` | `GET /repos/{o}/{r}/events?per_page=100&page=1..3` (page 4 → 422) | k/k: 289 events reach back ~14 h (58 WatchEvent, 115 PullRequestEvent, 71 IssueCommentEvent, 16 IssuesEvent, 9 Push, 2 Fork, 9+9 review events); `kubernetes-sigs/kueue`: 3 pages ≈ 36 h; `kubernetes/dashboard`: 9 events reach back to 08-19. `X-Poll-Interval: 60`, ETag present |
| Org-wide events | `GET /orgs/{org}/events` | 300 events ≈ 3 h for kubernetes-sigs — too short for a daily run, not proposed |
| Issues **and PRs** updated since T, full objects (`state_reason`, `closed_by`, `reactions`, labels, milestone, assignees, `pull_request` marker) | `GET /repos/{o}/{r}/issues?state=all&since=T&sort=updated&direction=asc&per_page=100` | 1 request per 100 objects, no time window limit; conditional requests free |
| PR-only fields (`additions, deletions, changed_files, commits, comments, review_comments, merged, merged_by, mergeable, mergeable_state, maintainer_can_modify, rebaseable`, full `head.repo`/`base.repo`) | `GET /pulls/{n}` (1 req/PR) or GraphQL `pullRequests(first:50 …){additions deletions changedFiles commits{totalCount} … mergedBy{login databaseId} baseRepository{…}}` | GraphQL: **25 PRs = 1 point, 50 = 2 points**; 100 full PR bodies → GitHub 502/truncated (payload ≈ 0.5 MB) → use 50; stats-only fields for 100 PRs = 1 point |
| Issues updated since T with labels/milestone/assignees | GraphQL `issues(first:100, orderBy:UPDATED_AT, filterBy:{since})` | 2 points / 100 |
| Branch tips | GraphQL `refs(refPrefix:"refs/heads/", first:100){nodes{name target{... on Commit{oid committedDate}}}}` | 1 point (k/k: 62 branches, 5 with tips < 30 d) |
| Commits of one branch since T, with `author.user{login databaseId}`, `additions/deletions/changedFilesIfAvailable`, `associatedPullRequests` | GraphQL `ref(qualifiedName){target{... on Commit{history(since, first:100)}}}` | **1 point per page**; whole `refs{…history}` in one query = 101 points (avoid) |
| Cheap change detection for many repos | aliased GraphQL `repository(owner,name){databaseId isArchived pushedAt stargazerCount forkCount watchers{totalCount} issues(last:1 orderBy:UPDATED_AT){nodes{updatedAt}} pullRequests(last:1 …) releases(last:1 …)}` | **100 repos = 3 points** (385 kubernetes repos = 11 points; follows renames, dedupe by `databaseId`) |
| Repo counters for `gha_forkees` | `GET /repos/{o}/{r}` (`stargazers_count, forks_count, open_issues_count, subscribers_count, watchers_count, updated_at`) or the heartbeat above | 1 request / 0.03 point |
| Commit → GitHub user | `GET /repos/{o}/{r}/commits/{sha}` (`author.id/login`, `committer.id/login`, `stats`, `files`) | 1 request per commit (k/k ≈ 650/month) |
| Per-issue history | `GET /issues/{n}/timeline` | 1+ request per issue — too expensive as a sweep, only for spot repairs |
| Star / watcher **lists** | `/stargazers`, `/subscribers`, GraphQL `stargazers` | **gone** (404 / empty) |

---

## 5. Gap map

| lost in GHA | metric impact (../devstats `metrics/shared`) | today | proposed source (§7) | existing tables written |
|---|---|---|---|---|
| `IssuesEvent` / `PullRequestEvent` (opened, closed, reopened) for the ~35–50 % of objects GHA never reports | `issues_opened`, `new_prs`, `prs_merged`, contributions/contributors (`gha_events.type in (IssuesEvent, PullRequestEvent, …)` + `actor_id`), `pr_time_to_*`, `open_issues_*` | `sync_events` repairs only objects that already had an issue event | **P1-B DONE** (events feed, native ids) + P1-C issues/PR sweep (artificial `IssuesEvent`/`PullRequestEvent` with `action` opened/closed, GHA-shaped) | `gha_events`, `gha_payloads`, `gha_issues`, `gha_pull_requests`, `gha_issues_labels/assignees`, `gha_pull_requests_*`, `gha_actors`, `gha_labels`, `gha_milestones` |
| full PR objects (stub rows with `created_at = 0001-01-01`) | everything keyed on `gha_pull_requests.created_at/merged_at/merged_by_id/additions…` | rows fixed only when `sync_events` sees an issue event | P1-C: `/issues?since` + GraphQL 50-batch, `update` of stub rows, insert of missing ones | `gha_pull_requests`, `gha_forkees` (base/head repo) |
| `gha_forkees` snapshots of tracked repos | `watchers.sql` ("Stars and forks by repository") | none since 2025-11 | **P1-D DONE**: one counters row per repo per run (`ghapi2db repo stats`, 0 requests from the heartbeat); P1-C still to add the PR base/head repo objects | `gha_forkees` |
| `WatchEvent` (stars) | `project_stats.sql`, `countries*.sql`, `bus_factor*.sql` (event counts by type), `Stargazers/Watchers` series | dead restore, silent 0 | **P1-B DONE**: the events feed gives real `WatchEvent`s (actor, id, time) for repos with < 300 events/day and the last ~14 h of k/k; counts via P1-D (DONE); `sync_stars` logs honestly (bug 62) | `gha_events`, `gha_payloads`, `gha_actors` |
| `ForkEvent`, `ReleaseEvent`, `CreateEvent`, `DeleteEvent`, `MemberEvent`, `GollumEvent`, `PublicEvent`, `CommitCommentEvent` | forks/releases metrics, contributions (`CommitCommentEvent`) | forks/releases restored for recent repos | **P1-B DONE**: events feed (all types, native ids); existing restores kept as backstop | as gha2db |
| `PushEvent.commits[]` | `commits`, `committers`, contributions, `gha_commits_roles` | `backfill_push_event_commits` (needs the stub event), `restore_orphan_commits` (default ref, commit-date window) | P2-A landing-window + P2-B all `origin/*` branches + P2-C grouping; P3-A GraphQL fallback for repos without clone | `gha_events`, `gha_payloads`, `gha_commits`, `gha_commits_roles`, `gha_commits_files` |
| commit → actor id/login for restored commits | affiliations, company metrics | name/email lookup in `gha_actors_emails` | `sync_commits` already enriches default-branch rows; extend to all branches (P3-B) | `gha_commits.author_id/committer_id/dup_*_login` |
| repos silently out of scope | all of the above | `get_recent_repos` (GHA feedback loop) | P1-A: scope = all `gha_repos` (dedupe by id, current name), heartbeat decides who has work — **DONE 2026-09-14** | – |
| `author_association` | (not used by shared metrics) | – | not recoverable, ignore | – |

---

## 6. Design rules for the new passes

* **GHA-shaped events for GHA-typed data.** Where GHA would have produced an `IssuesEvent`/`PullRequestEvent`/
  `WatchEvent`, the synthesized row uses that exact `type`, the real actor (`gha_actors` upsert with login+id from
  the API), `created_at` = the object's real timestamp (`created_at` for opened, `closed_at` for closed,
  `merged_at` for merged, event time for feed events), and a `gha_payloads` row with the matching `action`
  (`opened/closed/reopened`, `started`, …) — this is what the contribution/contributor metrics count.
* **Ids.** Events taken from `/repos/{o}/{r}/events` keep their **native id** (< 2^48): gha2db's
  `event_exists_collision` skips duplicates by id either way, so it does not matter which process wins.
  Synthesized events use deterministic artificial ids with the existing helpers: `hash::negative_artificial_id`
  (negative, like stars/orphan commits) — deterministic → re-runs are idempotent (`on conflict do nothing` +
  the existing `orphan_event_conflict` identity check).
* **Never overwrite real data with synthesized data.** `gha_issues`/`gha_pull_requests` rows are keyed
  `(id, event_id)`; the sweep inserts a row for the artificial event and *updates only zero/empty columns* of
  existing stub rows (`created_at = '0001-01-01'`, `user_id = 0`, `title = ''`). Later real GHA rows are
  untouched.
* **Idempotent and dedup-safe against a healthier GHA** (OpenDigger mirror, GHA fix): every insert is
  conditional on the object/event not existing; the id-postprocess (`run_event_ids_postprocess`) runs once per
  batch as today.
* **Budget aware**: the existing `PassRate`/`get_rate_limits` machinery, `GHA2DB_MIN_GHAPI_POINTS`,
  `GHA2DB_MAX_GHAPI_WAIT`, `GHA2DB_GHAPI_ERROR_FATAL` semantics; new passes are ordered after the existing
  ones and each is skippable with its own `GHA2DB_GHAPISKIP<NAME>` flag; `REPO=` single-repo mode kept.
* **Provenance is visible without schema changes**: the id range (≥ 2^48 = ghapi2db artificial, negative =
  deterministic hash ids) identifies synthesized rows for audits; `get_repos` additionally marks its rows with
  `gha_payloads.action = 'restored_orphan_commit'`, while the API restores keep GHA-native actions (`created`,
  `published`, `started`) — the new passes do the same (GHA-shaped `opened/closed/reopened/started`; no shared
  metric filters on `gha_payloads.action`).

---

## 7. Proposal

### 7.0 (config only, optional, gha2db) — point `GHA2DB_GHARCHIVE_URL` at the OpenDigger mirror

Already supported by Go and Rust (`GHA2DB_GHARCHIVE_URL`, default `http://data.gharchive.org/`). Trade-off:
360 MB/h files (vs 8 MB) downloaded once per project per hour (~100 projects) → a local caching proxy
(nginx `proxy_cache` on the cluster) would make this a single 360 MB/h download. Mirror exists only since
2026-09-06, is a volunteer effort, and its files are keyed by observation time (harmless for gha2db, which
filters by `created_at`). This is the only thing that brings *everything* back (still with stripped
payloads) — the API passes below remain necessary for the fields and for the past.

### 7.1 Phase 1 — `ghapi2db` (Rust)

**P1-A Scope = every tracked repo, not "recent" repos — IMPLEMENTED 2026-09-14 (Go + Rust).** Default scope is
now every `gha_repos` row: lib `GetTrackedRepos`/`get_tracked_repos` joins each `(id, name)` with the newest
*native* event of that pair (`0 < id < 2^48`, lateral query) and keeps **one current name per id** (newest event
wins, ties → larger event id, ids without events → alphabetically first name); the other names are *historical*
and skipped (`ghapi2db scope: %d repos from gha_repos (%d ids), %d historical names skipped`; `GHA2DB_DEBUG`
lists both sets). A name may be tracked under several ids (`kubernetes-csi/external-attacher` ×2 in prod).
Opt-out: `GHA2DB_GHAPI_RECENT_REPOS_ONLY` (ctx `GHAPIAllRepos=false`) = the legacy `GetRecentRepos` scope with the
legacy lines; the compat harness sets it for the 81 pre-existing scenarios. **Heartbeat** (`cmd/ghapi2db/
heartbeat.go` / `heartbeat.rs`, computed once per process, `GetThreadsNum` workers): GraphQL batches of **50**
repos (100 hit `RESOURCE_LIMITS_EXCEEDED`; 50 = 2 points, ~3.7 s) asking `databaseId nameWithOwner isArchived
pushedAt stargazerCount forkCount watchers openIssues openPRs` plus the newest issue/PR `updatedAt`, release
`createdAt`/`publishedAt` and fork `createdAt`; tokens round-robin through the shared `ghGraphQLPost`/
`gh_graphql_post` transport (also used by the stars restore now); `RESOURCE_LIMITS_EXCEEDED` → batch split in
halves; a batch failing on every token → its repos are **unknown = processed by every pass** (fail open,
`WARNING: ghapi2db heartbeat: N repos unknown (...)`), no token → every repo processed. Classification: found
(archived counted, `nameWithOwner` differs with the same id → rename, processed under our name), not found
(alias `null`; malformed names such as prod's `kubernetes/` are not even queried), **moved** (`databaseId` not
among the tracked ids → `WARNING: %s: resolves to %s (id %d) but is tracked as id %d, skipping`). Gates per
pass (`since recentDt`): events `issueAt||prAt`, commits `pushedAt`, comments `issueAt||prAt||pushedAt`,
reviews `prAt`, forks `forkAt`, releases `max(createdAt, publishedAt)`, stars `stargazerCount > 0 &&
stargazerCount != newest gha_forkees.stargazers_count snapshot at or before recentDt` (no snapshot → active).
Bypasses (no heartbeat): `REPO=` (any tracked repo now works, not only recently active ones) and
`DTFROM`/`DTTO` for events/commits. Lines: `ghapi2db heartbeat: %d repos in %d GraphQL queries: %d found, %d not
found, %d moved, %d unknown, %d archived; active since %v: pushes %d, issues %d, PRs %d, forks %d, releases %d,
stars %d`; count lines gain ` (heartbeat: %d skipped)`; per-repo debug `…: skipped by heartbeat (no <gate>
since %v)`. Licenses/langs already used every repo and are untouched. Side fix (bug 63): the restore passes
resolved `repo_id`/`org_id` only from `gha_events` and skipped repos GHA never delivered an event for — they now
fall back to `gha_repos`. Tests: 12 `scope_*` scenarios in `rust/cmd/ghapi2db/tests/compat.rs` (93 total, Go and
Rust byte-identical). Cost: kubernetes 419 ids → 9 queries ≈ 18 points; allprj 11,443 repos → 229 queries
≈ 460 points, ~2 min with 8 workers (the "100 repos per query, 345 points" estimate below was measured wrong).

**P1-B Events feed gap filler (`ghapi2db repo events`) — DONE 2026-09-14 (Go + Rust).** A new pass that
runs right after the licenses/languages passes and *before* every other API pass (so the issue events,
comments, reviews and forks passes see the feed's native events before synthesizing artificial ones). For
every repository the heartbeat shows *any* activity for since `recent_dt` (issue or PR update, push, fork,
release, or a star count differing from the snapshot; unknown heartbeat → processed; not found / moved →
skipped): `GET /repos/{owner}/{repo}/events?per_page=100&page=N` for N = 1..3 (GitHub returns 422 for
page 4). Every element is decoded with the gha2db `Event` type (Go `jsoniter` / Rust `serde`) and written
with the shared gha2db writer (`lib.WriteToDB` / `devstatscode::ghawriter::write_to_db`, moved out of
`cmd/gha2db` for this — no behaviour change for gha2db, its 60 compat scenarios stayed green) under the
event's **native id, actor and time stamp**, hide.csv anonymisation and the gha2db actor filters (`GHA2DB_ACTORS_FILTER`/`ALLOW`/`FORBID`) included; an id that already exists
(GH Archive delivered it) is skipped, a different event under the same id logs the writer's
`event id collision` line. All types are written — `PushEvent` stubs (no commits, they feed
`backfill_push_event_commits`) and the 5-field PR stubs included. Paging stops after the last page, after a
short page, after page 3, or once a page reaches back before `recent_dt` (everything on a fetched page is
written regardless of age). A feed whose events carry another repository id (`org/repo` now redirects
elsewhere) is skipped with a `WARNING` when that id is not tracked; 404/410 are silent. Restored event ids
go to the targeted postprocess (`gha_texts`/labels/issue-PR links immediately). Summary lines:
`ghapi2db repo events: processed N repos, P pages, checked C, restored R` and
`ghapi2db repo events: restored events by type: ForkEvent 1, IssueCommentEvent 35, …` (sorted). Flag:
`GHA2DB_GHAPISKIPREPOEVENTS`. Cost: ≤ 3 requests per active repo per run. Coverage: complete for every repo
with < 300 events between two runs (~all but a handful in allprj), the newest ~300 events (3.5–14 h) for
k/k — the schedule is fixed, so k/k keeps relying on P1-C for the rest. Tests: 8 `repo_events_*` scenarios
in `rust/cmd/ghapi2db/tests/compat.rs` (109 total, Go and Rust byte-identical, database contents included).

**P1-C Issues + PRs sweep (`sync_issues_prs`).** For every repo with issue/PR activity since `recent_dt`:
`GET /repos/{o}/{r}/issues?state=all&since=<recent_dt>&sort=updated&direction=asc&per_page=100` (issues and PRs,
full objects). For PR numbers, GraphQL `pullRequests` by number in batches of 50 (2 points) — or `GET /pulls/{n}`
(1 request) when fewer than ~10 — for `additions, deletions, changedFiles, commits, comments, reviewThreads,
merged, mergedAt, mergedBy, mergeable, maintainerCanModify, headRepository/baseRepository` and label/milestone/
assignee/requested-reviewer lists. Then per object:
1. upsert `gha_actors` (user, closed_by, merged_by, assignees) exactly like gha2db does;
2. if no `gha_events` row of type `IssuesEvent`/`PullRequestEvent` with `action='opened'` exists for the
   object (any id range) → artificial `IssuesEvent`/`PullRequestEvent` `opened` at `created_at`;
   likewise `closed` at `closed_at` (and `reopened` when `state=open` but a `closed` row exists);
   `merged_at` goes into the `gha_pull_requests` row (metrics use `merged_at`, not an event);
3. insert the full `gha_issues`/`gha_pull_requests` (+ `_labels`, `_assignees`, `_requested_reviewers`,
   `gha_milestones`, `gha_labels`) rows for those artificial events, and **upgrade existing stub rows**
   (`update … set created_at, updated_at, closed_at, merged_at, title, body, user_id, state, … where id = $1
   and created_at = '0001-01-01'`);
4. write `gha_forkees` rows for `base.repo` / `head.repo` of every synthesized PR event — this alone revives
   `watchers.sql` for every repo with PR traffic.
The existing `sync_events` stays (it produces the `labeled/closed/merged/…` issue-event history the
`sync_issues_state` machinery needs). Cost: allprj ≈ 1 request per 100 updated objects + 2 points per 50 PRs
(k/k busiest day ≈ 400 updated objects → 4 requests + 8 points). Flag: `GHA2DB_GHAPISKIPISSUESPRS`.

**P1-D Repo counters → `gha_forkees` (`sync_repo_stats`) — DONE 2026-09-14 (Go + Rust).** A new last pass
(`ghapi2db repo stats`, after the stars pass, before the event-id post-processing; flag
`GHA2DB_GHAPISKIPREPOSTATS`) writes **one `gha_forkees` snapshot per tracked repository per run**. The
counters come from the heartbeat (the fragment now also asks `owner { login … databaseId }`), so the pass
costs **zero REST requests** in the normal `gha_repos` scope: `id = databaseId`, `name`/`full_name` from
`nameWithOwner`, `owner_id`, `stargazers_count = stargazerCount`, `forks = forkCount`,
`open_issues = openIssues + openPRs` (the REST `open_issues_count` semantics: PRs included) and
`watchers = stargazers_count` (GH Archive/REST legacy semantics — all 736k k/k rows on prod have
`watchers == stargazers_count`). Repositories without a heartbeat (single-repo `REPO=` mode, the legacy
`GHA2DB_GHAPI_RECENT_REPOS_ONLY` scope, heartbeat *unknown*) fall back to one `GET /repos/{o}/{r}` each; the
returned id must be tracked (`WARNING: ghapi2db repo stats: org/repo: resolves to x/y (id N) which is not
tracked, skipping`) and 404s count as *unavailable*. No event is invented: the row is attached to the
**newest `gha_events` row of the repository** (by `created_at, id`; first under the tracked name, then under
any name for renamed repositories), `dup_actor_id`/`dup_created_at` copy the event, `dup_repo_name` is the
tracked name and `updated_at = now` (wall clock, seconds). A repository without events is skipped
(`without events`). The write is `insert … on conflict (id, event_id) do update` — repeated runs refresh the
same anchor row (`snapshots: N inserted, M refreshed`) and the **counter-less stub rows GH Archive writes
since 2024-09** (`0` everywhere, `updated_at = 0001-01-01`, `full_name = ''`) are upgraded in place.
Side effect: the stars heartbeat gate (`star_snapshot`) now picks the last snapshot by `updated_at` (what
`watchers_by_alias.sql` uses), so those stub rows can no longer hide a real snapshot. Verified on prod
(kubernetes, Go and Rust one-off Jobs): see §9.

**P1-E Stars honesty — DONE 2026-09-14 (Go + Rust, = bug 62).** The stargazers GraphQL query also asks
`stargazerCount`; a first page with no edges while `stargazerCount > 0` marks the repo *unavailable*
(`org/repo: stargazer list unavailable (N stars), skipping` with `GHA2DB_DEBUG`) and the pass ends with one
summary line: `ghapi2db stars restore: stargazer lists unavailable for U/P repos (GitHub restricted
stargazer/watcher lists to repository admins on 2026-06-30), star events cannot be restored`. A repo nobody
starred (`stargazerCount = 0`) is not flagged. Behaviour unchanged otherwise — same requests, no rows (real
`WatchEvent`s now arrive through P1-B). Verified live: k/k `stargazerCount=127691`, `edges=[]`, no error.

**P1-F Conditional requests.** In-process ETag map for the paged endpoints (repeated pages inside one run)
and `If-None-Match` for `/repos/{o}/{r}/events` — `304`s are free. Persisting ETags across the daily runs
would need a file on a PVC (ghapi2db pods have no repo PVC) → not in v1.

### 7.2 Phase 2 — `get_repos` (Go + Rust) — **IMPLEMENTED 2026-09-14** (P2-A/B/C; P2-D = bug 61, fixed earlier)

Implementation notes (what actually landed; both languages identical, compared by the get_repos compat suite —
34 orphan scenarios incl. 8 new ones: `orphan_landing_window`, `orphan_no_grouping_commit_dates`,
`orphan_legacy_event_reuse`, `orphan_web_flow_committer`, `orphan_all_branches`, `orphan_default_branch_only`,
`orphan_stale_branch_skipped`, `orphan_branches_shared_history`):

* env / ctx: `GHA2DB_ORPHAN_COMMITS_DEFAULT_BRANCH_ONLY` (set → `OrphanCommitsAllBranches=false`) and
  `GHA2DB_ORPHAN_COMMITS_NO_GROUPING` (set → `OrphanCommitsGroup=false`, the legacy shape). Defaults = the new
  behaviour; the flag names proposed below (`…_ALL_BRANCHES`, `…_GROUP=0`) became these opt-outs so that an unset
  environment (the CronJobs) gets the fix without any Helm change.
* the young-repo fallback is not `--since` but the whole reachable history (`git log <ref>` when the boundary is
  empty) — the `--since` listing would miss the merged old-dated commits exactly like before;
* the range is listed with `git log --format='%H %P %ct' <boundary>..<ref>` and grouped in memory (first-parent
  chain from the tip = steps; each commit belongs to the oldest step it is reachable from = `prev..step`);
  payload `size` = the full group size even when some of its commits are already in the DB / claimed by another
  branch of the same clone (first branch wins, the default branch first; `refs/remotes/origin/HEAD` is skipped);
* event actor = the step's **committer**, falling back to the author when the committer is GitHub's web-flow
  identity (`noreply@github.com`, UI merges/edits); the legacy mode keeps the author. Metrics were checked:
  `metrics/shared/*.sql` committer metrics union `(dup_actor, author, committer)`, so a pusher-shaped actor is safe;
* `orphanEventCheck` (replaces `orphanEventConflict`): an existing event with the same id is **reused** when it is
  a PushEvent of the same repo with the same payload head (its `created_at` is kept and the new commits join it —
  this is what happens when a legacy single-commit event for a merge commit meets the grouped run), anything else
  is a conflict and skipped as before (`created_at` is no longer part of the identity: legacy rows carry the author
  date, push-shaped rows the landing time);
* payload `ref` = `refs/heads/<branch>` for remote branches, the scanned ref itself for the `HEAD` fallback;
  `befor` = the first parent ("" for a root commit).

Validation on real data (2026-09-14, images `devstats-minimal-{prod,test}-rust:latest` 10:33 UTC / Go `:latest`
10:44 UTC, one-off Jobs with `GHA2DB_FETCH_COMMITS_MODE=0`, 26 h window, debug on): devstats-test `zephyr`
(Rust) — `zephyrproject-rtos/zephyr: scanning 14 branches`, 13 backport-branch commits restored, `zephyr-testing:
integration/main-2026-09-13: 69 commits in 40 pushes` → 99 restored with grouped pushes (`push afed7e8a…: restoring 4
of 5`; DB row `size=5 ref=refs/heads/integration/main-2026-09-13 befor=<first parent>`, actor = head committer),
`processed 7 repos, checked 284 commits, restored 113` in 7.8 s. devstats-prod `kubernetes` — Rust `processed 23
repos, checked 70 commits, restored 32` in 45 s, Go run 3 min later `checked 70, restored 0` (all reused), Go/Rust
branch/push listings md5-identical. `kubernetes/kubernetes` reported `no commits found` correctly (GitHub shows zero
master commits between Sat 09-12 20:05 and Mon 09-14 03:07 UTC; the clone is refreshed by the 03:04 daily run). The
prod `gha` DB has **no native GHA PushEvent for kubernetes/kubernetes in the last 10 days** — all 148 recent ones are
restored (`id < 0`) rows, i.e. the orphan restore is now the only commit source for k/k.

Original proposal:

**P2-A Landing window instead of commit-date window.** For the default ref: boundary = `git rev-list -1
--first-parent --before=<since> <ref>` (the tip as of `since`), then commits = `git rev-list
<boundary>..<ref>` — every commit that *became reachable* since, regardless of its own dates (this is exactly
what GHA's `PushEvent(before, head)` encoded). Fallback to today's `--since` listing when the boundary is
empty (young repos). Removes the 34 % master gap.

**P2-B All `origin/*` branches.** `git for-each-ref --format='%(refname) %(committerdate:unix)'
refs/remotes/origin/` → for every branch whose tip is newer than `since`, apply P2-A per branch; shas already
claimed on another branch (or present in `gha_commits`) are skipped (existing DB-wide dedupe), so a commit that
lands on `release-1.37` and later on `master` is counted once. `--all` is still avoided (fork clones / upstream
refs). Flag: `GHA2DB_ORPHAN_COMMITS_ALL_BRANCHES` (default on in Rust). k/k: +73 commits/30 d.

**P2-C Push-shaped grouping and timestamps.** Group the new commits by the first-parent step that brought
them in (`git rev-list --first-parent <boundary>..<ref>` → for each step `p1..step`): one artificial
`PushEvent` per step with `created_at` = that step's committer date (landing time), actor = its committer
(prow bot for k/k, as GHA had it), `gha_payloads.size` = number of commits, `ref` = the branch, `head` = step
sha, `befor` = its first parent; `gha_commits.dup_created_at` = landing time, `author_*` unchanged. Keeps the
existing one-event-per-commit shape as fallback (`GHA2DB_ORPHAN_COMMITS_GROUP=0`) so the compat scenarios stay
valid.

**P2-D Alias attribution fix (bug candidate 61, Go + Rust).** Resolve each `repo_id` to its current name before
restoring; skip historical-alias clones when the current-name clone exists. **DONE** (see §10, bug 61).

### 7.3 Phase 3 — later / optional

* **P3-A** `ghapi2db` GraphQL all-branch commit restore for repos without a PVC clone (same shape as P2, 1
  point per branch page) — only if projects exist where `get_repos` cannot clone.
* **P3-B** extend `sync_commits` enrichment to all branches / to restored rows (`author.user.databaseId`
  from GraphQL history is cheaper than one REST call per commit).
* **P3-C** `gha_comments`/`gha_reviews` restore already exists; add `gha_issues` row creation when a restored
  comment references an issue that has no `gha_issues` row (cheap: the issue object is in the P1-C sweep).
* **P3-D** timeline API only as a manual repair tool (`REPO=` + issue number).

---

## 8. Budget (per daily run)

| pass | kubernetes (385 repos, ~80 active) | allprj (11,443 repos, ~640 active/2 d, 2,850/60 d) |
|---|---|---|
| P1-A heartbeat (implemented: 50 repos/query, 2 points each) | 9 GraphQL queries, ≈ 18 points | 229 queries, ≈ 460 points |
| P1-B events feed (DONE) | ≤ 240 requests | ≤ 8,550 requests (3 × active repos) |
| P1-C issues/PR sweep | ≈ 30 requests + ≈ 40 points | ≈ 1,500 requests + ≈ 1,000 points |
| P1-D counters (DONE) | 0 requests (heartbeat), 1 per repo without heartbeat | 0 requests (heartbeat) |
| existing passes | unchanged | unchanged |

Pool: 49 tokens × 5,000 = 245,000 REST requests/h and 245,000 GraphQL points/h; `304`s are free. The
additions are < 10 % of one hour of the pool even for `allprj`; the passes reuse the existing rate-limit
waiting logic so the run only stretches when the pool is exhausted by other projects.

## 9. Risks and open points

* **Metric semantics.** Synthesized `IssuesEvent`/`PullRequestEvent` rows *are* counted as contributions — that
  is the intent (they represent real activity GHA dropped), and it is how 2021–2023 data looked. Audits can
  separate them via the id range / `gha_payloads.action`.
* **Partial windows for the busiest repos.** The events feed covers ~14 h of k/k per daily run; P1-C covers
  issues/PRs fully; stars for k/k stay partial (only GitHub can fix that). Document it.
* **DB growth**: P1-D refreshes the same `(id, event_id)` row until the repository gets a new event, so it adds
  at most one `gha_forkees` row per repo per *event*, not per day; + the events GHA should have delivered anyway
  — neutral compared with 2023 volumes.
* **P1-D prod verification (2026-09-14, kubernetes)**: see the "P1-D" entries of `~/devstats-go2rust.md` §6 —
  Go and Rust one-off Jobs with all other passes skipped, the same counters (modulo GitHub drift between the
  runs), the second run reports `refreshed` for every repo; `watchers_by_alias.sql`-style query returns the
  new snapshots.
* **Compat tests**: the Go⇄Rust scenarios must keep running with the new passes disabled (they compare against
  Go); new Rust-only tests use the fake GitHub server (`rust/compat/src/github.rs`) with recorded fixtures for
  `/events`, `/issues?since`, GraphQL batches, and git fixtures with merge-based histories and release branches
  for P2 (`rust/cmd/get_repos/tests`).
* **Writer refactor** (gha2db writer → lib) touches gha2db; it must stay byte-for-byte compatible (existing
  gha2db compat scenarios cover it).
* **`gha_repos` hygiene**: 27 of 538 kubernetes names no longer resolve (renamed/deleted) — P1-A must tolerate
  404/redirects and log, not fail.
* **When GHA recovers** (mirror or upstream fix) nothing has to be switched off: all inserts are dedup-safe.

## 10. Bugs found while researching (approved 2026-09-14; fixed in Go + Rust as the implementation proceeds)

| # | where | what | proposed fix |
|---|---|---|---|
| 61 | `get_repos` `restore_orphan_repo` (Go + Rust) | restored commits of renamed repos attributed to the oldest alias (`GoogleCloudPlatform/kubernetes` for all 399 k/k rows since 2026-08) | **FIXED 2026-09-14**: current name per `repo_id` is *derived from data* (the name with the newest native GHA event, `0 < id < 2^48`) — NOT from `gha_repos.alias`, which is set once at project init and is stale project-wide (e.g. `kubernetes-sigs/kubespray` → dead `kubernetes-incubator/kubespray`); historical clones are skipped when the current clone exists, otherwise attributed to the current name; ambiguous names (current for one id, historical for another — `repo_groups.sql` hack `downloadkubernetes → kubernetes/kubernetes`) are ignored. Existing rows repaired on prod (96,420 orphan + 825,444 ghapi2db + 22,554 star events renamed, 621 duplicate star events deleted) and test. P1-A must use the same derived rule. |
| 62 | `ghapi2db` `restore_stars_repo` (Go + Rust) | reported `restored 0 / checked 0` although the lists are gone (404/empty) — silent data loss since 2026-07 | **FIXED 2026-09-14**: detect via `stargazerCount > 0` + empty page and log "unavailable" (P1-E) |
| – | `get_repos` orphan window semantics | design limitation, not a bug (P2-A/B) | Rust-only extension |

## 11. Evidence / reproduction

* Raw GHA hour files and OpenDigger file: `/tmp/g2r/gha/*.json.gz` (volatile), key diff:
  `~/g2r-logs/gha_payload_keys_diff.txt`.
* DB queries (replica `devstats-postgres-1`, `-d gha`): provenance by id range
  (`id > 0 and id < 281474976710656` = GHA-native, `id < 0` = restored stars/orphan commits, `id >= 2^48` =
  ghapi2db artificial), `gha_forkees` per month, `gha_commits` per month by `event_id` sign.
* k/k git ground truth: GraphQL `history(since:"2026-08-14T00:00:00Z")` of `master` (630) and `release-1.34…37`
  (86, 73 not on master) vs `select sha from gha_commits where sha in (…)`; per-day missing split by
  `parents.totalCount` (216 non-merge / 1 merge missing).
* Heartbeat vs scope: `select distinct repo_id from gha_events where created_at > now() - '2 days'` (77) vs
  GraphQL heartbeat of all `gha_repos` names (385 live ids, 80 active, 15 out of scope).
* API probes: `/repos/kubernetes/kubernetes/events` pages 1–3 (289 events, 14 h, type mix above), `/orgs/…/events`,
  `/issues?since`, `If-None-Match` → 304 with unchanged `X-RateLimit-Remaining`, GraphQL `rateLimit{cost}` for
  every query shape listed in §4.
