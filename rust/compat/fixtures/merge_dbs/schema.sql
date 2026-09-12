-- Schema used by the merge_dbs compatibility tests (rust/cmd/merge_dbs/tests/compat.rs).
-- A reduced, type-diverse version of the DevStats tables merge_dbs copies:
-- the table names and the columns its MERGE_DT_FROM filter relies on
-- (dup_created_at / created_at / dt / event_id) are the real ones, everything
-- else is trimmed down. Like in the real schema gha_issues_events_labels and
-- gha_texts have no primary key (merging them twice duplicates rows) and
-- gha_pages is wide (70 columns) so USE_BATCH has to cap its batch size.
-- gha_companies is not merged (kept to prove untouched tables stay empty).
create table gha_actors(id bigint not null, login varchar(120) not null, name varchar(120), country_id varchar(2), sex varchar(1), sex_prob double precision, tz varchar(40), tz_offset int, country_name text, age int, primary key(id, login));
create table gha_assets(id bigint not null, event_id bigint not null, dup_created_at timestamp not null, name text not null, size bigint, primary key(id, event_id));
create table gha_branches(sha varchar(40) not null, event_id bigint not null, dup_created_at timestamp not null, name text, primary key(sha, event_id));
create table gha_comments(id bigint not null, event_id bigint not null, body text not null, dup_created_at timestamp not null, position int, primary key(id, event_id));
create table gha_reviews(id bigint not null, event_id bigint not null, state varchar(40), dup_created_at timestamp not null, primary key(id, event_id));
create table gha_commits(sha varchar(40) not null, event_id bigint not null, message text not null, dup_created_at timestamp not null, loc int, files_changed int, primary key(sha, event_id));
create table gha_commits_files(sha varchar(40) not null, path text not null, size bigint not null, dt timestamp not null, primary key(sha, path));
create table gha_commits_roles(sha varchar(40) not null, event_id bigint not null, role varchar(40) not null, actor_login varchar(120) not null, dup_created_at timestamp not null, primary key(sha, event_id, role, actor_login));
create table gha_events(id bigint not null primary key, type varchar(40) not null, actor_id bigint not null, repo_id bigint not null, created_at timestamp not null, org_id bigint, public boolean not null default true, dup_actor_login varchar(120) not null, dup_repo_name varchar(160) not null);
create table gha_forkees(id bigint not null, event_id bigint not null, name varchar(80) not null, dup_created_at timestamp not null, size int, fork boolean, primary key(id, event_id));
create table gha_issues(id bigint not null, event_id bigint not null, title text not null, dup_created_at timestamp not null, closed_at timestamp, is_pull_request boolean not null, primary key(id, event_id));
create table gha_issues_assignees(issue_id bigint not null, event_id bigint not null, assignee_id bigint not null, primary key(issue_id, event_id, assignee_id));
create table gha_issues_events_labels(issue_id bigint not null, event_id bigint not null, label_id bigint not null, label_name varchar(160) not null, created_at timestamp not null, actor_login varchar(120) not null);
create table gha_issues_labels(issue_id bigint not null, event_id bigint not null, label_id bigint not null, dup_created_at timestamp not null, primary key(issue_id, event_id, label_id));
create table gha_issues_pull_requests(issue_id bigint not null, pull_request_id bigint not null, number int not null, repo_id bigint not null, repo_name varchar(160) not null, created_at timestamp not null, primary key(issue_id, pull_request_id));
create table gha_labels(id bigint not null primary key, name varchar(160) not null, color varchar(8) not null, is_default boolean);
create table gha_milestones(id bigint not null, event_id bigint not null, title varchar(200) not null, dup_created_at timestamp not null, state varchar(20) not null, primary key(id, event_id));
create table gha_orgs(id bigint not null primary key, login varchar(100) not null);
create table gha_pages(sha varchar(40) not null, event_id bigint not null, action varchar(20) not null, title varchar(300) not null, dup_created_at timestamp not null, c1 smallint, c2 smallint, c3 smallint, c4 smallint, c5 smallint, c6 smallint, c7 smallint, c8 smallint, c9 smallint, c10 smallint, c11 smallint, c12 smallint, c13 smallint, c14 smallint, c15 smallint, c16 smallint, c17 smallint, c18 smallint, c19 smallint, c20 smallint, c21 smallint, c22 smallint, c23 smallint, c24 smallint, c25 smallint, c26 smallint, c27 smallint, c28 smallint, c29 smallint, c30 smallint, c31 smallint, c32 smallint, c33 smallint, c34 smallint, c35 smallint, c36 smallint, c37 smallint, c38 smallint, c39 smallint, c40 smallint, c41 smallint, c42 smallint, c43 smallint, c44 smallint, c45 smallint, c46 smallint, c47 smallint, c48 smallint, c49 smallint, c50 smallint, c51 smallint, c52 smallint, c53 smallint, c54 smallint, c55 smallint, c56 smallint, c57 smallint, c58 smallint, c59 smallint, c60 smallint, c61 smallint, c62 smallint, c63 smallint, c64 smallint, c65 smallint, primary key(sha, event_id, action, title));
create table gha_payloads(event_id bigint not null primary key, push_id bigint, size int, ref varchar(200), head varchar(40), action varchar(20), issue_id bigint, comment_id bigint, dup_created_at timestamp not null, number int, score double precision);
create table gha_pull_requests(id bigint not null, event_id bigint not null, title text not null, dup_created_at timestamp not null, merged boolean, additions int, primary key(id, event_id));
create table gha_pull_requests_assignees(pull_request_id bigint not null, event_id bigint not null, assignee_id bigint not null, primary key(pull_request_id, event_id, assignee_id));
create table gha_pull_requests_requested_reviewers(pull_request_id bigint not null, event_id bigint not null, requested_reviewer_id bigint not null, primary key(pull_request_id, event_id, requested_reviewer_id));
create table gha_releases(id bigint not null, event_id bigint not null, tag_name varchar(200) not null, dup_created_at timestamp not null, draft boolean not null, primary key(id, event_id));
create table gha_releases_assets(release_id bigint not null, event_id bigint not null, asset_id bigint not null, primary key(release_id, event_id, asset_id));
create table gha_repos(id bigint not null, name varchar(160) not null, org_id bigint, org_login varchar(100), repo_group varchar(80), alias varchar(160), primary key(id, name));
create table gha_repo_groups(id bigint not null, name varchar(160) not null, repo_group varchar(80), primary key(id, name));
create table gha_repos_langs(repo_id bigint not null, repo_name varchar(160) not null, lang_name varchar(60) not null, lang_loc int not null, lang_perc double precision not null, dt timestamp not null, primary key(repo_name, lang_name));
create table gha_skip_commits(sha varchar(40) not null primary key, dt timestamp not null);
create table gha_teams(id bigint not null, event_id bigint not null, name varchar(120) not null, dup_created_at timestamp not null, primary key(id, event_id));
create table gha_teams_repositories(team_id bigint not null, event_id bigint not null, repository_id bigint not null, primary key(team_id, event_id, repository_id));
create table gha_texts(event_id bigint not null, body text not null, created_at timestamp not null, repo_id bigint not null, actor_login varchar(120) not null, type varchar(40) not null, score numeric(10,3), tags text[], meta jsonb);
create table gha_companies(name varchar(160) not null, primary key(name));
