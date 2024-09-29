#!/bin/bash
if [ -z "${PG_PASS}" ]
then
  echo "$0: you must specify PG_PASS=..."
  exit 1
fi
make fmt && make columns || exit 2
cd ../devstats || exit 3
PG_DB=tuf GHA2DB_PROJECT=tuf GHA2DB_LOCAL=1 GHA2DB_DEBUG=2 GHA2DB_COLUMNS_YAML=devel/test_columns.yaml ../devstatscode/columns
