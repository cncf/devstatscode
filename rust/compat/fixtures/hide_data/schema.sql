-- Schema for the `hide_data` compatibility tests: every table/column the tool
-- anonymizes (types as `structure` creates them), plus pgcrypto for `digest`.
create extension if not exists pgcrypto;
create table gha_actors(id serial primary key, login varchar(120) not null, name varchar(120));
create table gha_actors_emails(id serial primary key, email varchar(120) not null);
create table gha_actors_names(id serial primary key, name varchar(120) not null);
create table gha_actors_affiliations(id serial primary key, company_name varchar(160) not null, original_company_name varchar(160) not null);
create table gha_companies(id serial primary key, name varchar(160) not null);
create table gha_events(id serial primary key, dup_actor_login varchar(120) not null);
create table gha_payloads(id serial primary key, dup_actor_login varchar(120) not null);
create table gha_commits(id serial primary key, dup_actor_login varchar(120) not null, dup_author_login varchar(120) not null, dup_committer_login varchar(120) not null, author_name varchar(160) not null, author_email varchar(160) not null, committer_name varchar(160) not null, committer_email varchar(160) not null);
create table gha_commits_roles(id serial primary key, actor_login varchar(120) not null, actor_name varchar(160) not null, actor_email varchar(160) not null);
create table gha_pages(id serial primary key, dup_actor_login varchar(120) not null);
create table gha_comments(id serial primary key, dup_actor_login varchar(120) not null, dup_user_login varchar(120) not null);
create table gha_reviews(id serial primary key, dup_actor_login varchar(120) not null, dup_user_login varchar(120) not null);
create table gha_issues(id serial primary key, dup_actor_login varchar(120) not null, dup_user_login varchar(120) not null);
create table gha_milestones(id serial primary key, dup_actor_login varchar(120) not null, dupn_creator_login varchar(120));
create table gha_issues_labels(id serial primary key, dup_actor_login varchar(120) not null);
create table gha_releases(id serial primary key, dup_actor_login varchar(120) not null, dup_author_login varchar(120) not null);
create table gha_assets(id serial primary key, dup_actor_login varchar(120) not null, dup_uploader_login varchar(120) not null);
create table gha_pull_requests(id serial primary key, dup_actor_login varchar(120) not null, dup_user_login varchar(120) not null);
create table gha_teams(id serial primary key, dup_actor_login varchar(120) not null);
create table gha_texts(id serial primary key, actor_login varchar(120) not null);
create table gha_issues_events_labels(id serial primary key, actor_login varchar(120) not null);
