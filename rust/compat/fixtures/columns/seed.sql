-- Seed data for the `columns` compatibility tests (a tiny TSDB: tag tables
-- `t*` as written by `tags`, series tables `s*` as written by `calc_metric`).
-- Every scratch database gets this whole file; the yamls under
-- data/metrics/* pick the tables they touch.

-- ---------------------------------------------------------------------------
-- Tag tables: time + <name>/<value> text columns (real shape, see WriteTSPoints)
-- ---------------------------------------------------------------------------
create table trepo_groups(time timestamp primary key, repo_group_name text, repo_group_value text);
insert into trepo_groups values
  ('2012-07-01 00:00:00', 'Kubernetes', 'kubernetes'),
  ('2012-07-01 01:00:00', 'Docs', 'docs'),
  ('2012-07-01 02:00:00', 'Prometheus', 'prometheus'),
  ('2012-07-01 03:00:00', 'Envoy', 'envoy'),
  ('2012-07-01 04:00:00', 'Ünïcode Group', 'unicode_group');

create table tall_repo_groups(time timestamp primary key, all_repo_group_name text, all_repo_group_value text);
insert into tall_repo_groups values
  ('2012-07-01 00:00:00', 'All', 'all'),
  ('2012-07-01 01:00:00', 'Kubernetes', 'kubernetes'),
  ('2012-07-01 02:00:00', 'Docs', 'docs');

create table tcompanies(time timestamp primary key, companies_name text, companies_value text);
insert into tcompanies values
  ('2012-07-01 00:00:00', 'Google', 'google'),
  ('2012-07-01 01:00:00', 'Red Hat', 'red_hat'),
  ('2012-07-01 02:00:00', 'Microsoft', 'microsoft'),
  ('2012-07-01 03:00:00', '(Unknown)', 'unknown'),
  ('2012-07-01 04:00:00', 'Ünïcode Ltd.', 'unicode_ltd');

-- no values at all: "Warning: no tag values for (users_name, tusers)"
create table tusers(time timestamp primary key, users_name text, users_value text);

create table treviewers(time timestamp primary key, reviewers_name text, reviewers_value text);
insert into treviewers values
  ('2012-07-01 00:00:00', 'alice', 'alice'),
  ('2012-07-01 01:00:00', 'bob', 'bob'),
  ('2012-07-01 02:00:00', 'carol', 'carol');

create table tevent_types(time timestamp primary key, event_type_name text, event_type_value text);
insert into tevent_types values
  ('2012-07-01 00:00:00', 'PushEvent', 'pushevent'),
  ('2012-07-01 01:00:00', 'PullRequestEvent', 'pullrequestevent'),
  ('2012-07-01 02:00:00', 'IssuesEvent', 'issuesevent');

create table tcountries(time timestamp primary key, country_name text, country_value text);
insert into tcountries values
  ('2012-07-01 00:00:00', 'Poland', 'poland'),
  ('2012-07-01 01:00:00', 'United States', 'united_states'),
  ('2012-07-01 02:00:00', 'Germany', 'germany'),
  ('2012-07-01 03:00:00', 'Japan', 'japan'),
  ('2012-07-01 04:00:00', 'Côte d''Ivoire', 'cote_d_ivoire');

-- kubernetes/columns.yaml tags
create table tsig_mentions_labels(time timestamp primary key, sig_mentions_labels_name text, sig_mentions_labels_value text);
insert into tsig_mentions_labels values
  ('2012-07-01 00:00:00', 'sig/api-machinery', 'sig_api_machinery'),
  ('2012-07-01 01:00:00', 'sig/node', 'sig_node');

create table ttop_repo_names(time timestamp primary key, top_repo_names_name text, top_repo_names_value text);
insert into ttop_repo_names values
  ('2012-07-01 00:00:00', 'kubernetes/kubernetes', 'kubernetes_kubernetes'),
  ('2012-07-01 01:00:00', 'kubernetes/website', 'kubernetes_website');

create table tbot_commands(time timestamp primary key, bot_command_name text, bot_command_value text);
insert into tbot_commands values
  ('2012-07-01 00:00:00', '/lgtm', 'lgtm'),
  ('2012-07-01 01:00:00', '/approve', 'approve'),
  ('2012-07-01 02:00:00', '/retest', 'retest');

create table tsig_mentions_texts(time timestamp primary key, sig_mentions_texts_name text, sig_mentions_texts_value text);
insert into tsig_mentions_texts values
  ('2012-07-01 00:00:00', 'sig-node', 'sig_node'),
  ('2012-07-01 01:00:00', 'sig-api-machinery', 'sig_api_machinery');

-- edge cases
create table tnulls(time timestamp primary key, nulls_name text, nulls_value text);
insert into tnulls values
  ('2012-07-01 00:00:00', 'ok', 'ok'),
  ('2012-07-01 01:00:00', null, 'null');

create table tquotes(time timestamp primary key, quotes_name text, quotes_value text);
insert into tquotes values
  ('2012-07-01 00:00:00', 'Plain', 'plain'),
  ('2012-07-01 01:00:00', 'O"Reilly', 'o_reilly'),
  ('2012-07-01 02:00:00', 'Also plain', 'also_plain');

-- the existing c001.. columns of the "row is too big" tables plus 4 new values
-- (see swide/sgiveup/snarrow): the c* columns are needed, so only the 4 new
-- ones are added and mass-updated
create table twide(time timestamp primary key, wide_name text, wide_value text);
insert into twide values
  ('2012-07-01 00:00:00'::timestamp + interval '0 hours', 'c001', 'c001'),
  ('2012-07-01 00:00:00'::timestamp + interval '1 hours', 'c002', 'c002'),
  ('2012-07-01 00:00:00'::timestamp + interval '2 hours', 'c003', 'c003'),
  ('2012-07-01 00:00:00'::timestamp + interval '3 hours', 'c004', 'c004'),
  ('2012-07-01 00:00:00'::timestamp + interval '4 hours', 'c005', 'c005'),
  ('2012-07-01 00:00:00'::timestamp + interval '5 hours', 'c006', 'c006'),
  ('2012-07-01 00:00:00'::timestamp + interval '6 hours', 'c007', 'c007'),
  ('2012-07-01 00:00:00'::timestamp + interval '7 hours', 'c008', 'c008'),
  ('2012-07-01 00:00:00'::timestamp + interval '8 hours', 'c009', 'c009'),
  ('2012-07-01 00:00:00'::timestamp + interval '9 hours', 'c010', 'c010'),
  ('2012-07-01 00:00:00'::timestamp + interval '10 hours', 'c011', 'c011'),
  ('2012-07-01 00:00:00'::timestamp + interval '11 hours', 'c012', 'c012'),
  ('2012-07-01 00:00:00'::timestamp + interval '12 hours', 'c013', 'c013'),
  ('2012-07-01 00:00:00'::timestamp + interval '13 hours', 'c014', 'c014'),
  ('2012-07-01 00:00:00'::timestamp + interval '14 hours', 'c015', 'c015'),
  ('2012-07-01 00:00:00'::timestamp + interval '15 hours', 'c016', 'c016'),
  ('2012-07-01 00:00:00'::timestamp + interval '16 hours', 'c017', 'c017'),
  ('2012-07-01 00:00:00'::timestamp + interval '17 hours', 'c018', 'c018'),
  ('2012-07-01 00:00:00'::timestamp + interval '18 hours', 'c019', 'c019'),
  ('2012-07-01 00:00:00'::timestamp + interval '19 hours', 'c020', 'c020'),
  ('2012-07-01 00:00:00'::timestamp + interval '20 hours', 'c021', 'c021'),
  ('2012-07-01 00:00:00'::timestamp + interval '21 hours', 'c022', 'c022'),
  ('2012-07-01 00:00:00'::timestamp + interval '22 hours', 'c023', 'c023'),
  ('2012-07-01 00:00:00'::timestamp + interval '23 hours', 'c024', 'c024'),
  ('2012-07-01 00:00:00'::timestamp + interval '24 hours', 'c025', 'c025'),
  ('2012-07-01 00:00:00'::timestamp + interval '25 hours', 'c026', 'c026'),
  ('2012-07-01 00:00:00'::timestamp + interval '26 hours', 'c027', 'c027'),
  ('2012-07-01 00:00:00'::timestamp + interval '27 hours', 'c028', 'c028'),
  ('2012-07-01 00:00:00'::timestamp + interval '28 hours', 'c029', 'c029'),
  ('2012-07-01 00:00:00'::timestamp + interval '29 hours', 'c030', 'c030'),
  ('2012-07-01 00:00:00'::timestamp + interval '30 hours', 'c031', 'c031'),
  ('2012-07-01 00:00:00'::timestamp + interval '31 hours', 'c032', 'c032'),
  ('2012-07-01 00:00:00'::timestamp + interval '32 hours', 'c033', 'c033'),
  ('2012-07-01 00:00:00'::timestamp + interval '33 hours', 'c034', 'c034'),
  ('2012-07-01 00:00:00'::timestamp + interval '34 hours', 'c035', 'c035'),
  ('2012-07-01 00:00:00'::timestamp + interval '35 hours', 'c036', 'c036'),
  ('2012-07-01 00:00:00'::timestamp + interval '36 hours', 'c037', 'c037'),
  ('2012-07-01 00:00:00'::timestamp + interval '37 hours', 'c038', 'c038'),
  ('2012-07-01 00:00:00'::timestamp + interval '38 hours', 'c039', 'c039'),
  ('2012-07-01 00:00:00'::timestamp + interval '39 hours', 'c040', 'c040'),
  ('2012-07-01 00:00:00'::timestamp + interval '40 hours', 'c041', 'c041'),
  ('2012-07-01 00:00:00'::timestamp + interval '41 hours', 'c042', 'c042'),
  ('2012-07-01 00:00:00'::timestamp + interval '42 hours', 'c043', 'c043'),
  ('2012-07-01 00:00:00'::timestamp + interval '43 hours', 'c044', 'c044'),
  ('2012-07-01 00:00:00'::timestamp + interval '44 hours', 'c045', 'c045'),
  ('2012-07-01 00:00:00'::timestamp + interval '45 hours', 'c046', 'c046'),
  ('2012-07-01 00:00:00'::timestamp + interval '46 hours', 'c047', 'c047'),
  ('2012-07-01 00:00:00'::timestamp + interval '47 hours', 'c048', 'c048'),
  ('2012-07-01 00:00:00'::timestamp + interval '48 hours', 'c049', 'c049'),
  ('2012-07-01 00:00:00'::timestamp + interval '49 hours', 'c050', 'c050'),
  ('2012-07-01 00:00:00'::timestamp + interval '50 hours', 'c051', 'c051'),
  ('2012-07-01 00:00:00'::timestamp + interval '51 hours', 'c052', 'c052'),
  ('2012-07-01 00:00:00'::timestamp + interval '52 hours', 'c053', 'c053'),
  ('2012-07-01 00:00:00'::timestamp + interval '53 hours', 'c054', 'c054'),
  ('2012-07-01 00:00:00'::timestamp + interval '54 hours', 'c055', 'c055'),
  ('2012-07-01 00:00:00'::timestamp + interval '55 hours', 'c056', 'c056'),
  ('2012-07-01 00:00:00'::timestamp + interval '56 hours', 'c057', 'c057'),
  ('2012-07-01 00:00:00'::timestamp + interval '57 hours', 'c058', 'c058'),
  ('2012-07-01 00:00:00'::timestamp + interval '58 hours', 'c059', 'c059'),
  ('2012-07-01 00:00:00'::timestamp + interval '59 hours', 'c060', 'c060'),
  ('2012-07-01 00:00:00'::timestamp + interval '60 hours', 'c061', 'c061'),
  ('2012-07-01 00:00:00'::timestamp + interval '61 hours', 'c062', 'c062'),
  ('2012-07-01 00:00:00'::timestamp + interval '62 hours', 'c063', 'c063'),
  ('2012-07-01 00:00:00'::timestamp + interval '63 hours', 'c064', 'c064'),
  ('2012-07-01 00:00:00'::timestamp + interval '64 hours', 'c065', 'c065'),
  ('2012-07-01 00:00:00'::timestamp + interval '65 hours', 'c066', 'c066'),
  ('2012-07-01 00:00:00'::timestamp + interval '66 hours', 'c067', 'c067'),
  ('2012-07-01 00:00:00'::timestamp + interval '67 hours', 'c068', 'c068'),
  ('2012-07-01 00:00:00'::timestamp + interval '68 hours', 'c069', 'c069'),
  ('2012-07-01 00:00:00'::timestamp + interval '69 hours', 'c070', 'c070'),
  ('2012-07-01 00:00:00'::timestamp + interval '70 hours', 'c071', 'c071'),
  ('2012-07-01 00:00:00'::timestamp + interval '71 hours', 'c072', 'c072'),
  ('2012-07-01 00:00:00'::timestamp + interval '72 hours', 'c073', 'c073'),
  ('2012-07-01 00:00:00'::timestamp + interval '73 hours', 'c074', 'c074'),
  ('2012-07-01 00:00:00'::timestamp + interval '74 hours', 'c075', 'c075'),
  ('2012-07-01 00:00:00'::timestamp + interval '75 hours', 'c076', 'c076'),
  ('2012-07-01 00:00:00'::timestamp + interval '76 hours', 'c077', 'c077'),
  ('2012-07-01 00:00:00'::timestamp + interval '77 hours', 'c078', 'c078'),
  ('2012-07-01 00:00:00'::timestamp + interval '78 hours', 'c079', 'c079'),
  ('2012-07-01 00:00:00'::timestamp + interval '79 hours', 'c080', 'c080'),
  ('2012-07-01 00:00:00'::timestamp + interval '80 hours', 'c081', 'c081'),
  ('2012-07-01 00:00:00'::timestamp + interval '81 hours', 'c082', 'c082'),
  ('2012-07-01 00:00:00'::timestamp + interval '82 hours', 'c083', 'c083'),
  ('2012-07-01 00:00:00'::timestamp + interval '83 hours', 'c084', 'c084'),
  ('2012-07-01 00:00:00'::timestamp + interval '84 hours', 'c085', 'c085'),
  ('2012-07-01 00:00:00'::timestamp + interval '85 hours', 'c086', 'c086'),
  ('2012-07-01 00:00:00'::timestamp + interval '86 hours', 'c087', 'c087'),
  ('2012-07-01 00:00:00'::timestamp + interval '87 hours', 'c088', 'c088'),
  ('2012-07-01 00:00:00'::timestamp + interval '88 hours', 'c089', 'c089'),
  ('2012-07-01 00:00:00'::timestamp + interval '89 hours', 'c090', 'c090'),
  ('2012-07-01 00:00:00'::timestamp + interval '90 hours', 'c091', 'c091'),
  ('2012-07-01 00:00:00'::timestamp + interval '91 hours', 'c092', 'c092'),
  ('2012-07-01 00:00:00'::timestamp + interval '92 hours', 'c093', 'c093'),
  ('2012-07-01 00:00:00'::timestamp + interval '93 hours', 'c094', 'c094'),
  ('2012-07-01 00:00:00'::timestamp + interval '94 hours', 'c095', 'c095'),
  ('2012-07-01 00:00:00'::timestamp + interval '95 hours', 'c096', 'c096'),
  ('2012-07-01 00:00:00'::timestamp + interval '96 hours', 'c097', 'c097'),
  ('2012-07-01 00:00:00'::timestamp + interval '97 hours', 'c098', 'c098'),
  ('2012-07-01 00:00:00'::timestamp + interval '98 hours', 'c099', 'c099'),
  ('2012-07-01 00:00:00'::timestamp + interval '99 hours', 'c100', 'c100'),
  ('2012-07-01 00:00:00'::timestamp + interval '100 hours', 'Docs', 'docs'),
  ('2012-07-01 00:00:00'::timestamp + interval '101 hours', 'Envoy', 'envoy'),
  ('2012-07-01 00:00:00'::timestamp + interval '102 hours', 'Kubernetes', 'kubernetes'),
  ('2012-07-01 00:00:00'::timestamp + interval '103 hours', 'Prometheus', 'prometheus');

create table tnarrow(time timestamp primary key, narrow_name text, narrow_value text);
insert into tnarrow values
  ('2012-07-01 00:00:00'::timestamp + interval '0 hours', 'c001', 'c001'),
  ('2012-07-01 00:00:00'::timestamp + interval '1 hours', 'c002', 'c002'),
  ('2012-07-01 00:00:00'::timestamp + interval '2 hours', 'c003', 'c003'),
  ('2012-07-01 00:00:00'::timestamp + interval '3 hours', 'c004', 'c004'),
  ('2012-07-01 00:00:00'::timestamp + interval '4 hours', 'c005', 'c005'),
  ('2012-07-01 00:00:00'::timestamp + interval '5 hours', 'c006', 'c006'),
  ('2012-07-01 00:00:00'::timestamp + interval '6 hours', 'c007', 'c007'),
  ('2012-07-01 00:00:00'::timestamp + interval '7 hours', 'c008', 'c008'),
  ('2012-07-01 00:00:00'::timestamp + interval '8 hours', 'c009', 'c009'),
  ('2012-07-01 00:00:00'::timestamp + interval '9 hours', 'c010', 'c010'),
  ('2012-07-01 00:00:00'::timestamp + interval '10 hours', 'Docs', 'docs'),
  ('2012-07-01 00:00:00'::timestamp + interval '11 hours', 'Envoy', 'envoy'),
  ('2012-07-01 00:00:00'::timestamp + interval '12 hours', 'Kubernetes', 'kubernetes'),
  ('2012-07-01 00:00:00'::timestamp + interval '13 hours', 'Prometheus', 'prometheus');

-- ---------------------------------------------------------------------------
-- Series tables: time + period + double precision columns (real shape)
-- ---------------------------------------------------------------------------
-- has a stale column (dropped), the protected all/None (kept) and an
-- already present tag column (kept, not re-added)
create table sact(time timestamp not null, period text not null default '', "Kubernetes" double precision not null default 0.0, "Stale" double precision not null default 0.0, "all" double precision not null default 0.0, "None" double precision not null default 0.0, primary key(time, period));
insert into sact values
  ('2020-01-01', 'd', 1.5, 2.5, 3.5, 4.5),
  ('2020-01-02', 'd', 10, 20, 30, 40),
  ('2020-01-01', 'w', 100, 200, 300, 400);

create table scommits(time timestamp not null, period text not null default '', primary key(time, period));
insert into scommits values ('2020-01-01', 'd'), ('2020-01-02', 'd');

-- no rows at all, one tag column already there
create table sgrp_pr_merg(time timestamp not null, period text not null default '', "Docs" double precision not null default 0.0, primary key(time, period));

create table scompany_activity(time timestamp not null, period text not null default '', "Google" double precision not null default 0.0, "Old Corp" double precision not null default 0.0, primary key(time, period));
insert into scompany_activity values ('2020-01-01', 'd', 7, 8), ('2020-01-01', 'm', 70, 80);

create table scompany_activity_repos(time timestamp not null, period text not null default '', primary key(time, period));
insert into scompany_activity_repos values ('2020-01-01', 'd');

-- its tag (tusers) has no values: left alone
create table suser_activity(time timestamp not null, period text not null default '', "Whatever" double precision not null default 0.0, primary key(time, period));
insert into suser_activity values ('2020-01-01', 'd', 1);

create table suser_reviews(time timestamp not null, period text not null default '', primary key(time, period));
insert into suser_reviews values ('2020-01-01', 'd');

create table siopened(time timestamp not null, period text not null default '', primary key(time, period));
insert into siopened values ('2020-01-01', 'd'), ('2020-01-02', 'd');
create table siclosed(time timestamp not null, period text not null default '', "gone" double precision not null default 0.0, primary key(time, period));
insert into siclosed values ('2020-01-01', 'd', 5);

-- '^spr_appr' is a prefix match
create table spr_appr(time timestamp not null, period text not null default '', primary key(time, period));
insert into spr_appr values ('2020-01-01', 'd');
create table spr_appr_by_group(time timestamp not null, period text not null default '', primary key(time, period));
insert into spr_appr_by_group values ('2020-01-01', 'd');

create table sevent_types(time timestamp not null, period text not null default '', primary key(time, period));
insert into sevent_types values ('2020-01-01', 'd');

create table scountries(time timestamp not null, period text not null default '', primary key(time, period));
insert into scountries values ('2020-01-01', 'd');
create table scountries_cum(time timestamp not null, period text not null default '', primary key(time, period));

-- all/columns.yaml `hll: true` target (needs the hll extension to succeed)
create table sprjcntr(time timestamp not null, series text not null default '', period text not null default '', primary key(time, series, period));
insert into sprjcntr values ('2020-01-01', 'prjcntr', 'd');
create table shll(time timestamp not null, period text not null default '', primary key(time, period));
insert into shll values ('2020-01-01', 'd');

-- kubernetes/columns.yaml targets
create table ssig_pr_wl(time timestamp not null, period text not null default '', primary key(time, period));
insert into ssig_pr_wl values ('2020-01-01', 'd');
create table sawaiting_prs(time timestamp not null, period text not null default '', "sig/node" double precision not null default 0.0, "sig/old" double precision not null default 0.0, primary key(time, period));
insert into sawaiting_prs values ('2020-01-01', 'd', 1, 2);
create table sgh_stats_rgrp(time timestamp not null, period text not null default '', primary key(time, period));
create table sgh_stats_r(time timestamp not null, period text not null default '', primary key(time, period));
insert into sgh_stats_r values ('2020-01-01', 'd');
create table sbot_commands(time timestamp not null, period text not null default '', primary key(time, period));
insert into sbot_commands values ('2020-01-01', 'd');
create table sbot_commands_repos(time timestamp not null, period text not null default '', primary key(time, period));
create table sbot_commands_other(time timestamp not null, period text not null default '', primary key(time, period));
create table ssigm_txt(time timestamp not null, period text not null default '', primary key(time, period));
insert into ssigm_txt values ('2020-01-01', 'd');
create table sprblck_all(time timestamp not null, period text not null default '', primary key(time, period));
insert into sprblck_all values ('2020-01-01', 'd');

-- matched by nothing
create table sother(time timestamp not null, period text not null default '', "Kubernetes" double precision not null default 0.0, primary key(time, period));
insert into sother values ('2020-01-01', 'd', 1);

-- tquotes target: the O"Reilly value cannot be used as an identifier
create table squotes(time timestamp not null, period text not null default '', primary key(time, period));
insert into squotes values ('2020-01-01', 'd');

-- tnulls target (never reached: the NULL tag value is a fatal scan error)
create table snulls(time timestamp not null, period text not null default '', primary key(time, period));

-- ---------------------------------------------------------------------------
-- "row is too big" (PostgreSQL 54000) tables: a plain-storage `series` text
-- keeps the single row just below the 8160-byte heap tuple limit so that
-- setting the 4 new twide columns to 0.0 overflows it and
-- HandleRowIsTooBig/DropLeastUsedCol kick in (`series` is a protected name).
-- swide:   8136-byte row, 100 candidate columns -> 2 drop rounds, then the
--          mass update succeeds on the 3rd trial.
-- sgiveup: 8152-byte row -> 3 drop rounds, "Give up 'mass add columns'",
--          then the not-null alter fails on the still-NULL columns.
-- snarrow: 8152-byte row but only 10 candidate columns (< 80): nothing is
--          dropped, no retry, the alter fails the same way.
-- ---------------------------------------------------------------------------
create table swide(time timestamp not null, series text not null default '', period text not null default '', "c001" double precision not null default 0.0, "c002" double precision not null default 0.0, "c003" double precision not null default 0.0, "c004" double precision not null default 0.0, "c005" double precision not null default 0.0, "c006" double precision not null default 0.0, "c007" double precision not null default 0.0, "c008" double precision not null default 0.0, "c009" double precision not null default 0.0, "c010" double precision not null default 0.0, "c011" double precision not null default 0.0, "c012" double precision not null default 0.0, "c013" double precision not null default 0.0, "c014" double precision not null default 0.0, "c015" double precision not null default 0.0, "c016" double precision not null default 0.0, "c017" double precision not null default 0.0, "c018" double precision not null default 0.0, "c019" double precision not null default 0.0, "c020" double precision not null default 0.0, "c021" double precision not null default 0.0, "c022" double precision not null default 0.0, "c023" double precision not null default 0.0, "c024" double precision not null default 0.0, "c025" double precision not null default 0.0, "c026" double precision not null default 0.0, "c027" double precision not null default 0.0, "c028" double precision not null default 0.0, "c029" double precision not null default 0.0, "c030" double precision not null default 0.0, "c031" double precision not null default 0.0, "c032" double precision not null default 0.0, "c033" double precision not null default 0.0, "c034" double precision not null default 0.0, "c035" double precision not null default 0.0, "c036" double precision not null default 0.0, "c037" double precision not null default 0.0, "c038" double precision not null default 0.0, "c039" double precision not null default 0.0, "c040" double precision not null default 0.0, "c041" double precision not null default 0.0, "c042" double precision not null default 0.0, "c043" double precision not null default 0.0, "c044" double precision not null default 0.0, "c045" double precision not null default 0.0, "c046" double precision not null default 0.0, "c047" double precision not null default 0.0, "c048" double precision not null default 0.0, "c049" double precision not null default 0.0, "c050" double precision not null default 0.0, "c051" double precision not null default 0.0, "c052" double precision not null default 0.0, "c053" double precision not null default 0.0, "c054" double precision not null default 0.0, "c055" double precision not null default 0.0, "c056" double precision not null default 0.0, "c057" double precision not null default 0.0, "c058" double precision not null default 0.0, "c059" double precision not null default 0.0, "c060" double precision not null default 0.0, "c061" double precision not null default 0.0, "c062" double precision not null default 0.0, "c063" double precision not null default 0.0, "c064" double precision not null default 0.0, "c065" double precision not null default 0.0, "c066" double precision not null default 0.0, "c067" double precision not null default 0.0, "c068" double precision not null default 0.0, "c069" double precision not null default 0.0, "c070" double precision not null default 0.0, "c071" double precision not null default 0.0, "c072" double precision not null default 0.0, "c073" double precision not null default 0.0, "c074" double precision not null default 0.0, "c075" double precision not null default 0.0, "c076" double precision not null default 0.0, "c077" double precision not null default 0.0, "c078" double precision not null default 0.0, "c079" double precision not null default 0.0, "c080" double precision not null default 0.0, "c081" double precision not null default 0.0, "c082" double precision not null default 0.0, "c083" double precision not null default 0.0, "c084" double precision not null default 0.0, "c085" double precision not null default 0.0, "c086" double precision not null default 0.0, "c087" double precision not null default 0.0, "c088" double precision not null default 0.0, "c089" double precision not null default 0.0, "c090" double precision not null default 0.0, "c091" double precision not null default 0.0, "c092" double precision not null default 0.0, "c093" double precision not null default 0.0, "c094" double precision not null default 0.0, "c095" double precision not null default 0.0, "c096" double precision not null default 0.0, "c097" double precision not null default 0.0, "c098" double precision not null default 0.0, "c099" double precision not null default 0.0, "c100" double precision not null default 0.0, primary key(time, period));
alter table swide alter column series set storage plain;
insert into swide values ('2020-01-01', repeat('x', 7296), 'd', 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0, 40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0, 48.0, 49.0, 50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0, 60.0, 61.0, 62.0, 63.0, 64.0, 65.0, 66.0, 67.0, 68.0, 69.0, 70.0, 71.0, 72.0, 73.0, 74.0, 75.0, 76.0, 77.0, 78.0, 79.0, 80.0, 81.0, 82.0, 83.0, 84.0, 85.0, 86.0, 87.0, 88.0, 89.0, 90.0, 91.0, 92.0, 93.0, 94.0, 95.0, 96.0, 97.0, 98.0, 99.0, 100.0);
create table sgiveup(time timestamp not null, series text not null default '', period text not null default '', "c001" double precision not null default 0.0, "c002" double precision not null default 0.0, "c003" double precision not null default 0.0, "c004" double precision not null default 0.0, "c005" double precision not null default 0.0, "c006" double precision not null default 0.0, "c007" double precision not null default 0.0, "c008" double precision not null default 0.0, "c009" double precision not null default 0.0, "c010" double precision not null default 0.0, "c011" double precision not null default 0.0, "c012" double precision not null default 0.0, "c013" double precision not null default 0.0, "c014" double precision not null default 0.0, "c015" double precision not null default 0.0, "c016" double precision not null default 0.0, "c017" double precision not null default 0.0, "c018" double precision not null default 0.0, "c019" double precision not null default 0.0, "c020" double precision not null default 0.0, "c021" double precision not null default 0.0, "c022" double precision not null default 0.0, "c023" double precision not null default 0.0, "c024" double precision not null default 0.0, "c025" double precision not null default 0.0, "c026" double precision not null default 0.0, "c027" double precision not null default 0.0, "c028" double precision not null default 0.0, "c029" double precision not null default 0.0, "c030" double precision not null default 0.0, "c031" double precision not null default 0.0, "c032" double precision not null default 0.0, "c033" double precision not null default 0.0, "c034" double precision not null default 0.0, "c035" double precision not null default 0.0, "c036" double precision not null default 0.0, "c037" double precision not null default 0.0, "c038" double precision not null default 0.0, "c039" double precision not null default 0.0, "c040" double precision not null default 0.0, "c041" double precision not null default 0.0, "c042" double precision not null default 0.0, "c043" double precision not null default 0.0, "c044" double precision not null default 0.0, "c045" double precision not null default 0.0, "c046" double precision not null default 0.0, "c047" double precision not null default 0.0, "c048" double precision not null default 0.0, "c049" double precision not null default 0.0, "c050" double precision not null default 0.0, "c051" double precision not null default 0.0, "c052" double precision not null default 0.0, "c053" double precision not null default 0.0, "c054" double precision not null default 0.0, "c055" double precision not null default 0.0, "c056" double precision not null default 0.0, "c057" double precision not null default 0.0, "c058" double precision not null default 0.0, "c059" double precision not null default 0.0, "c060" double precision not null default 0.0, "c061" double precision not null default 0.0, "c062" double precision not null default 0.0, "c063" double precision not null default 0.0, "c064" double precision not null default 0.0, "c065" double precision not null default 0.0, "c066" double precision not null default 0.0, "c067" double precision not null default 0.0, "c068" double precision not null default 0.0, "c069" double precision not null default 0.0, "c070" double precision not null default 0.0, "c071" double precision not null default 0.0, "c072" double precision not null default 0.0, "c073" double precision not null default 0.0, "c074" double precision not null default 0.0, "c075" double precision not null default 0.0, "c076" double precision not null default 0.0, "c077" double precision not null default 0.0, "c078" double precision not null default 0.0, "c079" double precision not null default 0.0, "c080" double precision not null default 0.0, "c081" double precision not null default 0.0, "c082" double precision not null default 0.0, "c083" double precision not null default 0.0, "c084" double precision not null default 0.0, "c085" double precision not null default 0.0, "c086" double precision not null default 0.0, "c087" double precision not null default 0.0, "c088" double precision not null default 0.0, "c089" double precision not null default 0.0, "c090" double precision not null default 0.0, "c091" double precision not null default 0.0, "c092" double precision not null default 0.0, "c093" double precision not null default 0.0, "c094" double precision not null default 0.0, "c095" double precision not null default 0.0, "c096" double precision not null default 0.0, "c097" double precision not null default 0.0, "c098" double precision not null default 0.0, "c099" double precision not null default 0.0, "c100" double precision not null default 0.0, primary key(time, period));
alter table sgiveup alter column series set storage plain;
insert into sgiveup values ('2020-01-01', repeat('x', 7312), 'd', 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0, 40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0, 48.0, 49.0, 50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0, 60.0, 61.0, 62.0, 63.0, 64.0, 65.0, 66.0, 67.0, 68.0, 69.0, 70.0, 71.0, 72.0, 73.0, 74.0, 75.0, 76.0, 77.0, 78.0, 79.0, 80.0, 81.0, 82.0, 83.0, 84.0, 85.0, 86.0, 87.0, 88.0, 89.0, 90.0, 91.0, 92.0, 93.0, 94.0, 95.0, 96.0, 97.0, 98.0, 99.0, 100.0);
create table snarrow(time timestamp not null, series text not null default '', period text not null default '', "c001" double precision not null default 0.0, "c002" double precision not null default 0.0, "c003" double precision not null default 0.0, "c004" double precision not null default 0.0, "c005" double precision not null default 0.0, "c006" double precision not null default 0.0, "c007" double precision not null default 0.0, "c008" double precision not null default 0.0, "c009" double precision not null default 0.0, "c010" double precision not null default 0.0, primary key(time, period));
alter table snarrow alter column series set storage plain;
insert into snarrow values ('2020-01-01', repeat('x', 8032), 'd', 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0);
