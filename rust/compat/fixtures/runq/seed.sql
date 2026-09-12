-- Extra seed data for the runq compatibility tests, loaded after
-- structure/full_structure.sql and tags/seed.sql.

-- gha_logs: for util_sql/get_keywords.sql ({{msg}} error / warning); messages
-- with '%' (Printf verb), unicode, quotes, commas and newlines.
insert into gha_logs(dt, prog, proj, run_dt, msg) values
  ('2026-09-01 10:00:00.123456', 'gha2db_sync', 'kubernetes', '2026-09-01 10:00:00', 'Error: something failed 100% of the time'),
  ('2026-09-01 11:00:00', 'calc_metric', 'prometheus', '2026-09-01 10:59:59.5', 'Warning: slow query, took 5s'),
  ('2026-09-02 12:30:00.5', 'tags', 'envoy', '2026-09-02 12:00:00', E'error in "tag" \'x\', line1\nline2'),
  ('2026-09-03 08:00:00', 'structure', 'all', '2026-09-03 08:00:00', 'Zażółć gęślą jaźń: ERROR %s %d %%'),
  ('2026-09-04 09:00:00', 'devstats', 'devstats', '2026-09-04 09:00:00', 'all good');

-- gha_countries + actors with country codes: util_sql/update_country_names.sql
insert into gha_countries(code, name) values
  ('pl', 'Poland'), ('us', 'United States'), ('de', 'Germany'), ('jp', 'Japan'), ('fr', 'France')
on conflict do nothing;
-- wrong/missing names to be fixed by the update
update gha_actors set country_name = 'Polska' where id = 100;
update gha_actors set country_name = null where id = 105;
insert into gha_actors(id, login, name, country_id, country_name) values
  (107, 'frank', 'Frank', 'fr', ''),
  (108, 'grace', 'Grace', 'xx', 'Nowhere');

-- gha_postprocess_scripts: util_sql/default_postprocess_scripts.sql
-- ("on conflict do nothing": one of the defaults is already there)
insert into gha_postprocess_scripts(ord, path) values
  (1, 'util_sql/postprocess_texts.sql'),
  (9, 'util_sql/custom.sql');

-- gha_issues_pull_requests with fully duplicated rows: util_sql/remove_dups.sql
insert into gha_issues_pull_requests(issue_id, pull_request_id, number, repo_id, repo_name, created_at) values
  (1000, 2000, 1, 1, 'kubernetes/kubernetes', '2026-01-01 00:00:00'),
  (1000, 2000, 1, 1, 'kubernetes/kubernetes', '2026-01-01 00:00:00'),
  (1000, 2000, 1, 1, 'kubernetes/kubernetes', '2026-01-01 00:00:00'),
  (1001, 2001, 2, 1, 'kubernetes/kubernetes', '2026-01-02 00:00:00'),
  (1001, 2001, 2, 1, 'kubernetes/kubernetes', '2026-01-02 00:00:01'),
  (1002, 2002, 3, 2, 'kubernetes/website', '2026-01-03 00:00:00');

-- artificial events (id > 2^48): util_sql/delete_artificial.sql
insert into gha_events(id, type, actor_id, repo_id, created_at, org_id, dup_actor_login, dup_repo_name) values
  (281474976710657, 'IssuesEvent', 100, 1, '2026-08-01 00:00:00', 10, 'alice', 'kubernetes/kubernetes'),
  (281474976710658, 'PullRequestEvent', 101, 1, '2026-08-02 00:00:00', 10, 'bob', 'kubernetes/kubernetes'),
  (281474976710656, 'IssuesEvent', 102, 1, '2026-08-03 00:00:00', 10, 'carol', 'kubernetes/kubernetes');
insert into gha_issues(id, event_id, comments, created_at, locked, number, state, title, updated_at, user_id, is_pull_request, dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_user_login) values
  (5000, 281474976710657, 0, '2026-08-01 00:00:00', false, 1, 'open', 'artificial issue', '2026-08-01 00:00:00', 100, false, 100, 'alice', 1, 'kubernetes/kubernetes', 'IssuesEvent', '2026-08-01 00:00:00', 'alice'),
  (5001, 281474976710656, 0, '2026-08-03 00:00:00', false, 2, 'open', 'real issue', '2026-08-03 00:00:00', 102, false, 102, 'carol', 1, 'kubernetes/kubernetes', 'IssuesEvent', '2026-08-03 00:00:00', 'carol');
insert into gha_issues_labels(issue_id, event_id, label_id, dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_issue_number, dup_label_name) values
  (5000, 281474976710657, 1, 100, 'alice', 1, 'kubernetes/kubernetes', 'IssuesEvent', '2026-08-01 00:00:00', 1, 'kind/bug'),
  (5001, 281474976710656, 1, 102, 'carol', 1, 'kubernetes/kubernetes', 'IssuesEvent', '2026-08-03 00:00:00', 2, 'kind/bug');
insert into gha_texts(event_id, body, created_at, actor_id, actor_login, repo_id, repo_name, type) values
  (281474976710657, 'artificial text', '2026-08-01 00:00:00', 100, 'alice', 1, 'kubernetes/kubernetes', 'IssuesEvent'),
  (281474976710656, 'real text', '2026-08-03 00:00:00', 102, 'carol', 1, 'kubernetes/kubernetes', 'IssuesEvent');
insert into gha_payloads(event_id, issue_id, number, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at) values
  (281474976710657, 5000, 1, 'alice', 1, 'kubernetes/kubernetes', 'IssuesEvent', '2026-08-01 00:00:00'),
  (281474976710656, 5001, 2, 'carol', 1, 'kubernetes/kubernetes', 'IssuesEvent', '2026-08-03 00:00:00');
insert into gha_issues_events_labels(issue_id, event_id, label_id, label_name, created_at, actor_id, actor_login, repo_id, repo_name, type) values
  (5000, 281474976710657, 1, 'kind/bug', '2026-08-01 00:00:00', 100, 'alice', 1, 'kubernetes/kubernetes', 'IssuesEvent');

-- gha_comments: metrics/shared/hist_commenters.sql
insert into gha_comments(id, event_id, body, created_at, updated_at, user_id, dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_user_login) values
  (1, 1, 'first', '2026-08-10 10:00:00', '2026-08-10 10:00:00', 100, 100, 'alice', 1, 'kubernetes/kubernetes', 'IssueCommentEvent', '2026-08-10 10:00:00', 'alice'),
  (2, 2, 'second', '2026-08-11 10:00:00', '2026-08-11 10:00:00', 100, 100, 'alice', 1, 'kubernetes/kubernetes', 'IssueCommentEvent', '2026-08-11 10:00:00', 'alice'),
  (3, 3, 'third', '2026-08-12 10:00:00', '2026-08-12 10:00:00', 101, 101, 'bob', 2, 'kubernetes/website', 'IssueCommentEvent', '2026-08-12 10:00:00', 'bob'),
  (4, 4, 'bot', '2026-08-13 10:00:00', '2026-08-13 10:00:00', 103, 103, 'k8s-ci-robot', 1, 'kubernetes/kubernetes', 'IssueCommentEvent', '2026-08-13 10:00:00', 'k8s-ci-robot'),
  (5, 5, 'bot2', '2026-08-14 10:00:00', '2026-08-14 10:00:00', 104, 104, 'dependabot[bot]', 3, 'prometheus/prometheus', 'IssueCommentEvent', '2026-08-14 10:00:00', 'dependabot[bot]'),
  (6, 6, 'review', '2026-08-15 10:00:00', '2026-08-15 10:00:00', 105, 105, 'dave', 3, 'prometheus/prometheus', 'PullRequestReviewCommentEvent', '2026-08-15 10:00:00', 'dave'),
  (7, 7, 'old', '2019-01-01 10:00:00', '2019-01-01 10:00:00', 106, 106, 'eve', 1, 'kubernetes/kubernetes', 'IssueCommentEvent', '2019-01-01 10:00:00', 'eve'),
  (8, 8, 'unknown repo', '2026-08-16 10:00:00', '2026-08-16 10:00:00', 106, 106, 'eve', 999, 'nobody/nothing', 'IssueCommentEvent', '2026-08-16 10:00:00', 'eve');

-- a table with every column type runq may meet (sql/types.sql)
create table runq_types(
  id int primary key,
  i2 smallint, i4 int, i8 bigint, num numeric(12,4), f4 real, f8 double precision,
  b boolean, t text, vc varchar(20), ch char(6), c1 "char", nm name,
  d date, ts timestamp, tstz timestamptz, tm time, tmtz timetz, iv interval,
  by bytea, j json, jb jsonb, u uuid, ip inet, ia int[], ta text[], oid_ oid
);
insert into runq_types values
  (1, 32767, 2147483647, 9223372036854775807, 12345678.1234, 0.1, 1e6, true, 'plain', 'var', 'pad', 'x', 'a_name',
   '2012-07-01', '2012-07-01 12:34:56.123456', '2012-07-01 12:34:56.5+00', '12:34:56.789', '12:34:56+02', '1 year 2 mons 3 days 04:05:06.5',
   E'\\xDEADBEEF', '{"a": 1, "b": [1, 2]}', '{"b": [1, 2], "a": 1}', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '192.168.0.1/24', '{1,2,3}', '{"a,b","c\"d",NULL,""}', 42),
  (2, -32768, -2147483648, -9223372036854775808, -0.0001, -3.4028235e38, -1.7976931348623157e308, false, E'tab\there', '', 'ab', 'ż', 'n',
   '0001-01-01', '9999-12-31 23:59:59.999999', '1970-01-01 00:00:00+00', '00:00:00', '23:59:59.999999-12', '-1 days -02:03:04',
   E'\\x00ff10', '[]', '[]', '00000000-0000-0000-0000-000000000000', '::1', '{}', '{}', 0),
  (3, 0, 0, 0, 0, 1.5, 123456789.125, null, 'Zażółć gęślą jaźń 日本語', 'ünï', 'ł', '%', 'x',
   '2000-02-29', '2000-02-29 00:00:00', '2026-03-29 02:30:00+00', '23:59:59', '00:00:00+00', '00:00:00',
   E'\\x', '"str"', 'null', null, null, '{NULL}', '{"a b"}', 4294967295),
  (4, null, null, null, 'NaN', 'NaN', 'Infinity', true, E'line1\nline2\r\nline3', ' lead', ' sp', ' ', 'x y',
   'infinity', '-infinity', 'infinity', null, null, null,
   null, null, null, null, null, null, null, null),
  (5, 1, 2, 3, 0.5, 1e-7, 1e21, false, 'a%b%%c', '100%', '%s', '"', 'quote"name',
   '1999-12-31', '2012-07-01 00:00:00', '2012-12-31 23:59:59.999+05:30', '01:02:03.000001', '01:02:03+00', '1 mon',
   E'\\x25', '{"z": "ż"}', '{"z": "ż"}', 'ffffffff-ffff-ffff-ffff-ffffffffffff', '10.0.0.0/8', '{{1,2},{3,4}}', '{"x\\y"}', 1);

-- a table with more than 100 columns (sql/wide.sql)
create table runq_wide as
select
  s as id, s * 2 as c2, s * 3 as c3, s * 4 as c4, s * 5 as c5, s * 6 as c6, s * 7 as c7, s * 8 as c8, s * 9 as c9, s * 10 as c10,
  s * 11 as c11, s * 12 as c12, s * 13 as c13, s * 14 as c14, s * 15 as c15, s * 16 as c16, s * 17 as c17, s * 18 as c18, s * 19 as c19, s * 20 as c20,
  s * 21 as c21, s * 22 as c22, s * 23 as c23, s * 24 as c24, s * 25 as c25, s * 26 as c26, s * 27 as c27, s * 28 as c28, s * 29 as c29, s * 30 as c30,
  s * 31 as c31, s * 32 as c32, s * 33 as c33, s * 34 as c34, s * 35 as c35, s * 36 as c36, s * 37 as c37, s * 38 as c38, s * 39 as c39, s * 40 as c40,
  s * 41 as c41, s * 42 as c42, s * 43 as c43, s * 44 as c44, s * 45 as c45, s * 46 as c46, s * 47 as c47, s * 48 as c48, s * 49 as c49, s * 50 as c50,
  s * 51 as c51, s * 52 as c52, s * 53 as c53, s * 54 as c54, s * 55 as c55, s * 56 as c56, s * 57 as c57, s * 58 as c58, s * 59 as c59, s * 60 as c60,
  s * 61 as c61, s * 62 as c62, s * 63 as c63, s * 64 as c64, s * 65 as c65, s * 66 as c66, s * 67 as c67, s * 68 as c68, s * 69 as c69, s * 70 as c70,
  s * 71 as c71, s * 72 as c72, s * 73 as c73, s * 74 as c74, s * 75 as c75, s * 76 as c76, s * 77 as c77, s * 78 as c78, s * 79 as c79, s * 80 as c80,
  s * 81 as c81, s * 82 as c82, s * 83 as c83, s * 84 as c84, s * 85 as c85, s * 86 as c86, s * 87 as c87, s * 88 as c88, s * 89 as c89, s * 90 as c90,
  s * 91 as c91, s * 92 as c92, s * 93 as c93, s * 94 as c94, s * 95 as c95, s * 96 as c96, s * 97 as c97, s * 98 as c98, s * 99 as c99, s * 100 as c100,
  s * 101 as c101, s * 102 as c102, 'last' as c103
from generate_series(1, 3) s;
