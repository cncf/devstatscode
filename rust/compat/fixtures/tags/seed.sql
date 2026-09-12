-- Seed data for the `tags` compatibility tests.
-- Applied after compat/fixtures/structure/full_structure.sql (the real DevStats
-- tables); small but covering every real metrics/shared/*_tags.sql query,
-- including NULLs, unicode, the bot exclusion and the `{{lim}}` cut-off.

insert into gha_repos(id, name, org_id, org_login, repo_group, alias, license_key, license_name, license_prob) values
  (1, 'kubernetes/kubernetes', 10, 'kubernetes', 'Kubernetes', 'kubernetes/kubernetes', 'apache-2.0', 'Apache License 2.0', 99.5),
  (2, 'kubernetes/website', 10, 'kubernetes', 'Docs', 'k8s website', 'cc-by-4.0', 'Creative Commons Attribution 4.0', 98),
  (3, 'prometheus/prometheus', 20, 'prometheus', 'Prometheus', null, 'apache-2.0', 'Apache License 2.0', 100),
  (4, 'envoyproxy/envoy', 30, 'envoyproxy', 'Envoy', 'envoy', null, 'Not found', null),
  (5, 'cncf/devstats', 40, 'cncf', null, 'devstats', null, '', null),
  (6, 'zé/ünïcode-ok', 50, 'zé', 'Ünïcode Group', 'ünïcode', 'mit', 'MIT License', 50);

insert into gha_repo_groups(id, name, repo_group, org_id, org_login, alias) values
  (1, 'kubernetes/kubernetes', 'Kubernetes', 10, 'kubernetes', 'kubernetes/kubernetes'),
  (2, 'kubernetes/website', 'Docs', 10, 'kubernetes', 'k8s website'),
  (3, 'prometheus/prometheus', 'Prometheus', 20, 'prometheus', null),
  (4, 'envoyproxy/envoy', 'Envoy', 30, 'envoyproxy', 'envoy'),
  (6, 'zé/ünïcode-ok', 'Ünïcode Group', 50, 'zé', 'ünïcode');

insert into gha_actors(id, login, name, country_id, country_name) values
  (100, 'alice', 'Alice', 'pl', 'Poland'),
  (101, 'bob', 'Bob', 'us', 'United States'),
  (102, 'carol', 'Carol', 'de', 'Germany'),
  (103, 'k8s-ci-robot', 'CI', null, null),
  (104, 'dependabot[bot]', null, null, ''),
  (105, 'dave', 'Dave', 'pl', 'Poland'),
  (106, 'eve', 'Eve', 'jp', 'Japan');

insert into gha_companies(name) values
  ('Google'), ('Red Hat'), ('Microsoft'), ('(Unknown)'), ('NotFound'), ('Ünïcode Ltd.'), ('');

insert into gha_actors_affiliations(actor_id, company_name, original_company_name, dt_from, dt_to, source) values
  (100, 'Google', 'Google', '1900-01-01', '2100-01-01', ''),
  (101, 'Red Hat', 'Red Hat', '1900-01-01', '2100-01-01', ''),
  (102, 'Microsoft', 'Microsoft', '1900-01-01', '2100-01-01', ''),
  (103, 'Google', 'Google', '1900-01-01', '2100-01-01', ''),
  (105, '(Unknown)', '(Unknown)', '1900-01-01', '2100-01-01', ''),
  (106, 'Ünïcode Ltd.', 'Ünïcode Ltd.', '1900-01-01', '2100-01-01', '');

-- events: recent (relative to now()) so the "last N months/years" filters keep them
insert into gha_events(id, type, actor_id, repo_id, created_at, org_id, dup_actor_login, dup_repo_name) values
  (1, 'PushEvent', 100, 1, now() - '10 days'::interval, 10, 'alice', 'kubernetes/kubernetes'),
  (2, 'PullRequestEvent', 100, 1, now() - '9 days'::interval, 10, 'alice', 'kubernetes/kubernetes'),
  (3, 'IssuesEvent', 101, 1, now() - '8 days'::interval, 10, 'bob', 'kubernetes/kubernetes'),
  (4, 'IssueCommentEvent', 101, 2, now() - '7 days'::interval, 10, 'bob', 'kubernetes/website'),
  (5, 'PullRequestReviewCommentEvent', 102, 3, now() - '6 days'::interval, 20, 'carol', 'prometheus/prometheus'),
  (6, 'PushEvent', 103, 1, now() - '5 days'::interval, 10, 'k8s-ci-robot', 'kubernetes/kubernetes'),
  (7, 'IssueCommentEvent', 104, 4, now() - '4 days'::interval, 30, 'dependabot[bot]', 'envoyproxy/envoy'),
  (8, 'WatchEvent', 105, 5, now() - '3 days'::interval, 40, 'dave', 'cncf/devstats'),
  (9, 'PullRequestReviewEvent', 106, 6, now() - '2 days'::interval, 50, 'eve', 'zé/ünïcode-ok'),
  (10, 'PushEvent', 100, 3, now() - '1 days'::interval, 20, 'alice', 'prometheus/prometheus'),
  (11, 'ForkEvent', 105, 1, now() - '12 hours'::interval, 10, 'dave', 'kubernetes/kubernetes'),
  (12, 'IssueCommentEvent', 102, 1, now() - '5 years'::interval, 10, 'carol', 'kubernetes/kubernetes');

insert into gha_issues_labels(issue_id, event_id, label_id, dup_actor_id, dup_actor_login, dup_repo_id, dup_repo_name, dup_type, dup_created_at, dup_issue_number, dup_label_name) values
  (1000, 3, 1, 101, 'bob', 1, 'kubernetes/kubernetes', 'IssuesEvent', now() - '8 days'::interval, 1, 'priority/critical-urgent'),
  (1000, 3, 2, 101, 'bob', 1, 'kubernetes/kubernetes', 'IssuesEvent', now() - '8 days'::interval, 1, 'kind/bug'),
  (1001, 4, 3, 101, 'bob', 2, 'kubernetes/website', 'IssueCommentEvent', now() - '7 days'::interval, 2, 'Priority/P1'),
  (1002, 7, 4, 104, 'dependabot[bot]', 4, 'envoyproxy/envoy', 'IssueCommentEvent', now() - '4 days'::interval, 3, 'priority/critical-urgent'),
  (1003, 12, 5, 102, 'carol', 1, 'kubernetes/kubernetes', 'IssueCommentEvent', now() - '5 years'::interval, 4, 'priority/backlog');

insert into gha_repos_langs(repo_name, lang_name, lang_loc, lang_perc) values
  ('kubernetes/kubernetes', 'Go', 1000000, 95.5),
  ('kubernetes/kubernetes', 'Shell', 20000, 2.5),
  ('kubernetes/website', 'HTML', 5000, 60),
  ('kubernetes/website', 'unknown', 1, 0.1),
  ('prometheus/prometheus', 'Go', 300000, 99),
  ('cncf/devstats', '', 1, 0),
  ('zé/ünïcode-ok', 'C++', 10, 100);

insert into gha_texts(event_id, body, created_at, actor_id, actor_login, repo_id, repo_name, type) values
  (4, E'looks good\n/lgtm\n', now() - '7 days'::interval, 101, 'bob', 2, 'kubernetes/website', 'IssueCommentEvent'),
  (7, '/lgtm', now() - '4 days'::interval, 104, 'dependabot[bot]', 4, 'envoyproxy/envoy', 'IssueCommentEvent'),
  (8, 'nothing to see', now() - '3 days'::interval, 105, 'dave', 5, 'cncf/devstats', 'IssueCommentEvent');

insert into gha_issues_events_labels(issue_id, event_id, label_id, label_name, created_at, actor_id, actor_login, repo_id, repo_name, type) values
  (1000, 3, 10, 'lgtm', now() - '8 days'::interval, 101, 'bob', 1, 'kubernetes/kubernetes', 'IssuesEvent'),
  (1000, 11, 10, 'lgtm', now() - '12 hours'::interval, 105, 'dave', 1, 'kubernetes/kubernetes', 'ForkEvent');
