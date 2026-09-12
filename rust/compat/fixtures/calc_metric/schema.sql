-- Schema for the calc_metric compatibility tests: the two bookkeeping tables
-- calc_metric writes (structure.go definitions), a small deterministic event
-- log the metric SQL fixtures query and the quick ranges tags table the
-- annotations tool fills (`annotations_ranges`).
create table gha_computed(metric text not null, dt timestamp not null, primary key(metric, dt));
create index computed_metric_idx on gha_computed(metric);
create index computed_dt_idx on gha_computed(dt);
create table gha_last_computed(metric text not null, dt timestamp not null, start_dt timestamp, took bigint, took_as_str text, command text, primary key(metric));
create table gha_actors(id bigint primary key, login text not null, name text);
insert into gha_actors(id, login, name) values
(1, 'alice', 'Alice A'),
(2, 'bob', 'Bob B'),
(3, 'carol', 'Carol C'),
(4, 'dependabot[bot]', null),
(5, 'k8s-ci-robot', 'K8s CI'),
(6, 'dave', 'Dave Ünicode');
create table gha_events(id bigint primary key, type text not null, actor_id bigint not null, repo_id bigint not null, created_at timestamp not null, dup_actor_login text not null, dup_repo_name text not null);
create index events_created_at_idx on gha_events(created_at);
insert into gha_events(id, type, actor_id, repo_id, created_at, dup_actor_login, dup_repo_name) values
(1000, 'PushEvent', 1, 11, '2015-08-01 00:00:00', 'alice', 'org/repo1'),
(1001, 'PullRequestEvent', 2, 12, '2015-08-01 05:00:00', 'bob', 'org/Repo.Two'),
(1002, 'IssuesEvent', 3, 13, '2015-08-01 10:00:00', 'carol', 'other/repo3'),
(1003, 'WatchEvent', 4, 11, '2015-08-01 15:00:00', 'dependabot[bot]', 'org/repo1'),
(1004, 'ForkEvent', 5, 12, '2015-08-01 20:00:00', 'k8s-ci-robot', 'org/Repo.Two'),
(1005, 'PushEvent', 6, 13, '2015-08-02 01:00:00', 'dave', 'other/repo3'),
(1006, 'PullRequestEvent', 1, 11, '2015-08-02 06:00:00', 'alice', 'org/repo1'),
(1007, 'IssuesEvent', 2, 12, '2015-08-02 11:00:00', 'bob', 'org/Repo.Two'),
(1008, 'WatchEvent', 3, 13, '2015-08-02 16:00:00', 'carol', 'other/repo3'),
(1009, 'ForkEvent', 4, 11, '2015-08-02 21:00:00', 'dependabot[bot]', 'org/repo1'),
(1010, 'PushEvent', 5, 12, '2015-08-03 02:00:00', 'k8s-ci-robot', 'org/Repo.Two'),
(1011, 'PullRequestEvent', 6, 13, '2015-08-03 07:00:00', 'dave', 'other/repo3'),
(1012, 'IssuesEvent', 1, 11, '2015-08-03 12:00:00', 'alice', 'org/repo1'),
(1013, 'WatchEvent', 2, 12, '2015-08-03 17:00:00', 'bob', 'org/Repo.Two'),
(1014, 'ForkEvent', 3, 13, '2015-08-03 22:00:00', 'carol', 'other/repo3'),
(1015, 'PushEvent', 4, 11, '2015-08-04 03:00:00', 'dependabot[bot]', 'org/repo1'),
(1016, 'PullRequestEvent', 5, 12, '2015-08-04 08:00:00', 'k8s-ci-robot', 'org/Repo.Two'),
(1017, 'IssuesEvent', 6, 13, '2015-08-04 13:00:00', 'dave', 'other/repo3'),
(1018, 'WatchEvent', 1, 11, '2015-08-04 18:00:00', 'alice', 'org/repo1'),
(1019, 'ForkEvent', 2, 12, '2015-08-04 23:00:00', 'bob', 'org/Repo.Two'),
(1020, 'PushEvent', 3, 13, '2015-08-05 04:00:00', 'carol', 'other/repo3'),
(1021, 'PullRequestEvent', 4, 11, '2015-08-05 09:00:00', 'dependabot[bot]', 'org/repo1'),
(1022, 'IssuesEvent', 5, 12, '2015-08-05 14:00:00', 'k8s-ci-robot', 'org/Repo.Two'),
(1023, 'WatchEvent', 6, 13, '2015-08-05 19:00:00', 'dave', 'other/repo3'),
(1024, 'ForkEvent', 1, 11, '2015-08-06 00:00:00', 'alice', 'org/repo1'),
(1025, 'PushEvent', 2, 12, '2015-08-06 05:00:00', 'bob', 'org/Repo.Two'),
(1026, 'PullRequestEvent', 3, 13, '2015-08-06 10:00:00', 'carol', 'other/repo3'),
(1027, 'IssuesEvent', 4, 11, '2015-08-06 15:00:00', 'dependabot[bot]', 'org/repo1'),
(1028, 'WatchEvent', 5, 12, '2015-08-06 20:00:00', 'k8s-ci-robot', 'org/Repo.Two'),
(1029, 'ForkEvent', 6, 13, '2015-08-07 01:00:00', 'dave', 'other/repo3'),
(1030, 'PushEvent', 1, 11, '2015-08-07 06:00:00', 'alice', 'org/repo1'),
(1031, 'PullRequestEvent', 2, 12, '2015-08-07 11:00:00', 'bob', 'org/Repo.Two'),
(1032, 'IssuesEvent', 3, 13, '2015-08-07 16:00:00', 'carol', 'other/repo3'),
(1033, 'WatchEvent', 4, 11, '2015-08-07 21:00:00', 'dependabot[bot]', 'org/repo1'),
(1034, 'ForkEvent', 5, 12, '2015-08-08 02:00:00', 'k8s-ci-robot', 'org/Repo.Two'),
(1035, 'PushEvent', 6, 13, '2015-08-08 07:00:00', 'dave', 'other/repo3'),
(1036, 'PullRequestEvent', 1, 11, '2015-08-08 12:00:00', 'alice', 'org/repo1'),
(1037, 'IssuesEvent', 2, 12, '2015-08-08 17:00:00', 'bob', 'org/Repo.Two'),
(1038, 'WatchEvent', 3, 13, '2015-08-08 22:00:00', 'carol', 'other/repo3'),
(1039, 'ForkEvent', 4, 11, '2015-08-09 03:00:00', 'dependabot[bot]', 'org/repo1'),
(1040, 'PushEvent', 5, 12, '2015-08-09 08:00:00', 'k8s-ci-robot', 'org/Repo.Two'),
(1041, 'PullRequestEvent', 6, 13, '2015-08-09 13:00:00', 'dave', 'other/repo3'),
(1042, 'IssuesEvent', 1, 11, '2015-08-09 18:00:00', 'alice', 'org/repo1'),
(1043, 'WatchEvent', 2, 12, '2015-08-09 23:00:00', 'bob', 'org/Repo.Two'),
(1044, 'ForkEvent', 3, 13, '2015-08-10 04:00:00', 'carol', 'other/repo3'),
(1045, 'PushEvent', 4, 11, '2015-08-10 09:00:00', 'dependabot[bot]', 'org/repo1'),
(1046, 'PullRequestEvent', 5, 12, '2015-08-10 14:00:00', 'k8s-ci-robot', 'org/Repo.Two'),
(1047, 'IssuesEvent', 6, 13, '2015-08-10 19:00:00', 'dave', 'other/repo3'),
(1048, 'WatchEvent', 1, 11, '2015-08-11 00:00:00', 'alice', 'org/repo1'),
(1049, 'ForkEvent', 2, 12, '2015-08-11 05:00:00', 'bob', 'org/Repo.Two'),
(1050, 'PushEvent', 3, 13, '2015-08-11 10:00:00', 'carol', 'other/repo3'),
(1051, 'PullRequestEvent', 4, 11, '2015-08-11 15:00:00', 'dependabot[bot]', 'org/repo1'),
(1052, 'IssuesEvent', 5, 12, '2015-08-11 20:00:00', 'k8s-ci-robot', 'org/Repo.Two'),
(1053, 'WatchEvent', 6, 13, '2015-08-12 01:00:00', 'dave', 'other/repo3'),
(1054, 'ForkEvent', 1, 11, '2015-08-12 06:00:00', 'alice', 'org/repo1'),
(1055, 'PushEvent', 2, 12, '2015-08-12 11:00:00', 'bob', 'org/Repo.Two'),
(1056, 'PullRequestEvent', 3, 13, '2015-08-12 16:00:00', 'carol', 'other/repo3'),
(1057, 'IssuesEvent', 4, 11, '2015-08-12 21:00:00', 'dependabot[bot]', 'org/repo1'),
(1058, 'WatchEvent', 5, 12, '2015-08-13 02:00:00', 'k8s-ci-robot', 'org/Repo.Two'),
(1059, 'ForkEvent', 6, 13, '2015-08-13 07:00:00', 'dave', 'other/repo3');
insert into gha_events(id, type, actor_id, repo_id, created_at, dup_actor_login, dup_repo_name) values
(2001, 'PushEvent', 1, 11, now() - '1 hour'::interval, 'alice', 'org/repo1'),
(2002, 'IssuesEvent', 2, 12, now() - '3 days'::interval, 'bob', 'org/Repo.Two'),
(2003, 'PushEvent', 3, 13, now() - '20 days'::interval, 'carol', 'other/repo3'),
(2004, 'WatchEvent', 4, 11, now() - '100 days'::interval, 'dependabot[bot]', 'org/repo1'),
(2005, 'PullRequestEvent', 6, 12, now() - '2 years'::interval, 'dave', 'org/Repo.Two');
create table tquick_ranges(time timestamp primary key, quick_ranges_suffix text, quick_ranges_name text, quick_ranges_data text);
insert into tquick_ranges(time, quick_ranges_suffix, quick_ranges_name, quick_ranges_data) values
('2012-07-01 00:00:00', 'd', 'Last day', 'd;1 day;;'),
('2012-06-30 23:00:00', 'w', 'Last week', 'w;1 week;;'),
('2012-06-30 22:00:00', 'd10', 'Last 10 days', 'd10;10 days;;'),
('2012-06-30 21:00:00', 'm', 'Last month', 'm;1 month;;'),
('2012-06-30 20:00:00', 'q', 'Last quarter', 'q;3 months;;'),
('2012-06-30 19:00:00', 'y', 'Last year', 'y;1 year;;'),
('2012-06-30 18:00:00', 'y10', 'Last decade', 'y10;10 years;;'),
('2012-06-30 17:00:00', 'a_0_1', 'v0.1 - v0.2', 'a_0_1;;2015-08-03 00:00:00;2015-08-06 00:00:00'),
('2012-06-30 16:00:00', 'a_1_2', 'v0.2 - v0.3', 'a_1_2;;2015-08-06 00:00:00;2015-08-10 00:00:00'),
('2012-06-30 15:00:00', 'c_n', 'v0.3 - now', 'c_n;;2015-08-01 00:00:00;2099-01-01 00:00:00');
