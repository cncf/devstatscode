# `sqlitedb` fixtures

* `schema.sql` — the `dashboard`/`dashboard_tag` tables (and their indexes) as
  found in a real DevStats Grafana SQLite database
  (`https://devstats.cncf.io/backups/grafana.<project>.db`, Grafana 6-era
  schema, the one `sqlitedb` inserts into).
* `dashboards/*.json` — four real dashboards of the `prometheus` project from
  `cncf/devstats` (`grafana/dashboards/prometheus/`), already `jq -S` sorted
  like the repository keeps them.
