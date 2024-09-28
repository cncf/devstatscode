#!/bin/bash
# on k8s node-0: 'k exec -n devstats-prod devstats-postgres-0 -- pg_dump tuf > tuf.dump'
# sftp root@node-0: 'mget tuf.dump'
# here: 'PGPASSWORD=[redacted] createdb -Ugha_admin -hlocalhost -p5432 tuf'
# here: 'PGPASSWORD=[redacted] psql -Ugha_admin -hlocalhost -p5432 tuf < tuf.dump'
if [ -z "${PG_PASS}" ]
then
  echo "$0: you must specify PG_PASS=..."
  exit 1
fi
make fmt && make calc_metric || exit 2
cd ../devstats || exit 3
# GHA2DB_ENABLE_METRICS_DROP=1 PG_DB=tuf GHA2DB_PROJECT=tuf GHA2DB_QOUT=1 GHA2DB_DEBUG=2 GHA2DB_SQLDEBUG=1 GHA2DB_LOCAL=1 GHA2DB_ST=1 GHA2DB_NCPUS=1 ../devstatscode/calc_metric events_hll ./events_hll.sql '2024-01-01 0' '2024-10-01 0' w 'skip_past,hll'
# GHA2DB_ENABLE_METRICS_DROP=1 PG_DB=tuf GHA2DB_PROJECT=tuf GHA2DB_QOUT=1 GHA2DB_DEBUG=2 GHA2DB_SQLDEBUG=1 GHA2DB_LOCAL=1 ../devstatscode/calc_metric multi_row_multi_column ./metrics/shared/project_countries.sql '2024-01-01 0' '2024-01-01 0' q 'multivalue,hll,merge_series:prjcntr,drop:sprjcntr' > ../devstatscode/out @>&1
# GHA2DB_ENABLE_METRICS_DROP=1 PG_DB=tuf GHA2DB_PROJECT=tuf GHA2DB_QOUT=1 GHA2DB_DEBUG=2 GHA2DB_SQLDEBUG=1 GHA2DB_LOCAL=1 GHA2DB_ST=1 GHA2DB_NCPUS=1 ../devstatscode/calc_metric multi_row_multi_column ./metrics/shared/project_countries.sql '2024-01-01 0' '2024-10-01 0' q 'multivalue,hll,merge_series:prjcntr,drop:sprjcntr'
# GHA2DB_ENABLE_METRICS_DROP=1 PG_DB=tuf GHA2DB_PROJECT=tuf GHA2DB_LOCAL=1 ../devstatscode/calc_metric multi_row_multi_column ./metrics/shared/project_countries.sql '2024-01-01 0' '2024-10-01 0' m 'multivalue,hll,merge_series:prjcntr,drop:sprjcntr'
# GHA2DB_ENABLE_METRICS_DROP=1 PG_DB=tuf GHA2DB_PROJECT=tuf GHA2DB_LOCAL=1 ../devstatscode/calc_metric multi_row_multi_column ./metrics/shared/project_countries_commiters.sql '2014-01-01 0' '2024-10-01 0' m 'hll,merge_series:prjcntr,drop:sprjcntr,skip_escape_series_name'
GHA2DB_ENABLE_METRICS_DROP=1 PG_DB=tuf GHA2DB_PROJECT=tuf GHA2DB_LOCAL=1 ../devstatscode/calc_metric multi_row_multi_column ./metrics/shared/project_countries_commiters.sql '2014-01-01 0' '2024-10-01 0' m 'multivalue,hll,skip_escape_series_name,merge_series:prjcntr,drop:sprjcntr'
