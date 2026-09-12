-- Minimal project database for the `website_data` Go⇄Rust compatibility tests:
-- the four tables the tool queries, with the column types `structure` creates
-- (only the columns `website_data` touches plus what makes rows insertable).
create table gha_commits(
  sha varchar(40) not null,
  event_id bigint not null,
  dup_actor_login varchar(120) not null,
  dup_created_at timestamp not null,
  primary key(sha, event_id)
);
create table gha_texts(
  event_id bigint,
  created_at timestamp not null,
  actor_login varchar(120) not null
);
create table gha_forkees(
  id bigint not null,
  event_id bigint not null,
  full_name varchar(200) not null,
  stargazers_count int not null,
  dup_repo_name varchar(160) not null,
  dup_created_at timestamp not null,
  primary key(id, event_id)
);
create table gha_issues(
  id bigint not null,
  event_id bigint not null,
  closed_at timestamp,
  updated_at timestamp not null,
  is_pull_request boolean not null,
  primary key(id, event_id)
);
