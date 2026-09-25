package devstatscode

import (
	"database/sql"
	"strings"
	"time"
)

// PeriodComputedKey - 'gha_computed' key of a single 'calc_metric' run: "<proj>/<file>.sql <series_name_or_func> <period>"
// the SQL file path is reduced to its last two components, so the key does not depend on the data directory
func PeriodComputedKey(seriesNameOrFunc, sqlFile, period string) string {
	ary := strings.Split(sqlFile, "/")
	if len(ary) > 2 {
		ary = ary[len(ary)-2:]
	}
	return strings.Join(ary, "/") + " " + seriesNameOrFunc + " " + period
}

// IsPeriodComputed - true when 'gha_computed' has a marker of 'key' written by a sync between the start of the period
// containing 'to' (see PeriodStartAt, 'rangeStart' is the start of a histogram quick range, zero otherwise) and 'to'
// always true for periods without a calendar period
func IsPeriodComputed(con *sql.DB, ctx *Ctx, key, period string, rangeStart, to time.Time) bool {
	dt, ok := PeriodStartAt(ctx, period, rangeStart, to)
	if !ok {
		return true
	}
	rows := QuerySQLWithErr(
		con,
		ctx,
		"select 1 from gha_computed where metric = "+NValue(1)+" and dt >= "+NValue(2)+" and dt <= "+NValue(3)+" limit 1",
		key,
		dt,
		to,
	)
	defer func() { FatalOnError(rows.Close()) }()
	i := 0
	for rows.Next() {
		FatalOnError(rows.Scan(&i))
	}
	FatalOnError(rows.Err())
	return i > 0
}

// SetPeriodComputed - marks 'key' as successfully computed by the sync ending at 'to' (hour precision)
func SetPeriodComputed(con *sql.DB, ctx *Ctx, key string, to time.Time) {
	ExecSQLWithErr(con, ctx, InsertIgnore("into gha_computed(metric, dt) "+NValues(2)), key, HourStart(to))
}
