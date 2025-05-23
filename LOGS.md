# Project Metrics API logs

To check logs for http://cncf.io/project-metrics:
- `` k exec -itn devstats-prod devstats-postgres-0 -- psql devstats ``.
- Then: `` select * from gha_logs where prog = 'api' and lower(msg) like '%cached values%' order by dt desc limit 10; ``.
- Then: `` select dt, run_dt, prog, msg from gha_logs where proj = 'all' and lower(msg) like '%contributors_and_orgs_count%' order by dt desc limit 10; ``.
- Metric executions: `` select dt, run_dt, msg from gha_logs where proj = 'all' and prog = 'gha2db_sync' and msg like '%Contributors and organizations%' order by dt desc limit 10; ``.
- What exactly got executed: `` select dt, run_dt, prog, msg from gha_logs where proj = 'all' and dt >= '2025-05-22 20:22:38.966613' and dt <= '2025-05-22 20:23:21.965162' order by dt; ``.


# Columns additions/deletions

To check which columns were added/deleted recently:
- `` k exec -itn devstats-prod devstats-postgres-0 -- psql devstats ``.
- Additions: `` select dt, run_dt, proj, msg from gha_logs where prog = 'columns' and msg like '%Added column%' order by dt desc limit 100; ``.
- Deletions: `` select dt, run_dt, proj, msg from gha_logs where prog = 'columns' and msg like '%Need to delete columns%' order by dt desc limit 100; ``.
