package devstatscode

import (
	"database/sql"
	"testing"
	"time"

	lib "github.com/cncf/devstatscode"
	testlib "github.com/cncf/devstatscode/test"
)

// markerRows returns the 'gha_computed' dates stored for a given key
func markerRows(c *sql.DB, ctx *lib.Ctx, key string) []time.Time {
	rows := lib.QuerySQLWithErr(c, ctx, "select dt from gha_computed where metric = "+lib.NValue(1)+" order by dt", key)
	defer func() { lib.FatalOnError(rows.Close()) }()
	var dts []time.Time
	var dt time.Time
	for rows.Next() {
		lib.FatalOnError(rows.Scan(&dt))
		dts = append(dts, dt)
	}
	lib.FatalOnError(rows.Err())
	return dts
}

func TestPeriodComputedMarkers(t *testing.T) {
	var ctx lib.Ctx
	ctx.Init()
	ctx.TestMode = true

	// Do not allow to run tests in "gha" database
	if ctx.PgDB != "dbtest" {
		t.Errorf("tests can only be run on \"dbtest\" database")
		return
	}
	lib.DropDatabaseIfExists(&ctx)
	if !lib.CreateDatabaseIfNeeded(&ctx) {
		t.Errorf("failed to create database \"%s\"", ctx.PgDB)
	}
	defer func() {
		lib.DropDatabaseIfExists(&ctx)
	}()
	c := lib.PgConn(&ctx)
	defer func() { lib.FatalOnError(c.Close()) }()

	// The same 'gha_computed' definition as in structure.go
	lib.ExecSQLWithErr(c, &ctx, lib.CreateTable("gha_computed(metric text not null, dt {{ts}} not null, primary key(metric, dt))"))

	ft := testlib.YMDHMS
	var zero time.Time
	sqlFile := "/etc/gha2db/metrics/kubernetes/reviewers.sql"
	wKey := lib.PeriodComputedKey("reviewers", sqlFile, "w")
	dKey := lib.PeriodComputedKey("reviewers", sqlFile, "d")
	hKey := lib.PeriodComputedKey("reviewers", sqlFile, "h")
	otherKey := lib.PeriodComputedKey("multi_row_multi_column", sqlFile, "w")
	histFile := "/etc/gha2db/metrics/kubernetes/hist_reviewers.sql"
	aKey := lib.PeriodComputedKey("hist_reviewers", histFile, "a_0_1")
	nKey := lib.PeriodComputedKey("hist_reviewers", histFile, "a_3_n")

	// Nothing computed yet
	if lib.IsPeriodComputed(c, &ctx, wKey, "w", zero, ft(2026, 9, 25, 10)) {
		t.Errorf("empty table: week must not be reported as computed")
	}

	// Markers are the hours of the syncs which computed the metric
	// Weekly metric computed by the Monday 2026-09-21 09:00 sync, then again on Wednesday (twice in the same hour)
	lib.SetPeriodComputed(c, &ctx, wKey, ft(2026, 9, 21, 9))
	lib.SetPeriodComputed(c, &ctx, wKey, ft(2026, 9, 23, 15, 30))
	lib.SetPeriodComputed(c, &ctx, wKey, ft(2026, 9, 23, 15, 45))
	// Daily metric computed by the 2026-09-25 03:04 sync, hourly by the 10:00 one
	lib.SetPeriodComputed(c, &ctx, dKey, ft(2026, 9, 25, 3, 4))
	lib.SetPeriodComputed(c, &ctx, hKey, ft(2026, 9, 25, 10))
	// Histogram quick ranges get markers like any other period
	lib.SetPeriodComputed(c, &ctx, aKey, ft(2026, 9, 25, 3, 4))
	lib.SetPeriodComputed(c, &ctx, nKey, ft(2026, 9, 25, 3, 4))

	var storedCases = []struct {
		key      string
		expected []time.Time
	}{
		{key: wKey, expected: []time.Time{ft(2026, 9, 21, 9), ft(2026, 9, 23, 15)}},
		{key: dKey, expected: []time.Time{ft(2026, 9, 25, 3)}},
		{key: hKey, expected: []time.Time{ft(2026, 9, 25, 10)}},
		{key: aKey, expected: []time.Time{ft(2026, 9, 25, 3)}},
		{key: nKey, expected: []time.Time{ft(2026, 9, 25, 3)}},
		{key: otherKey, expected: nil},
	}
	for index, test := range storedCases {
		got := markerRows(c, &ctx, test.key)
		if len(got) != len(test.expected) {
			t.Errorf("stored test number %d, key '%s': expected %v, got %v", index+1, test.key, test.expected, got)
			continue
		}
		for i := range got {
			if !got[i].Equal(test.expected[i]) {
				t.Errorf("stored test number %d, key '%s': expected %v, got %v", index+1, test.key, test.expected, got)
			}
		}
	}

	var testCases = []struct {
		key        string
		period     string
		rangeStart time.Time
		tmOffset   int
		to         time.Time
		expected   bool
	}{
		// the week 2026-09-21 - 2026-09-27 is covered by its markers from the Monday sync on
		{key: wKey, period: "w", to: ft(2026, 9, 21, 9), expected: true},
		{key: wKey, period: "w", to: ft(2026, 9, 21, 10), expected: true},
		{key: wKey, period: "w", to: ft(2026, 9, 25, 10, 58), expected: true},
		{key: wKey, period: "w", to: ft(2026, 9, 27, 23, 59, 59), expected: true},
		// a sync ending before the first marker of the week does not see it
		{key: wKey, period: "w", to: ft(2026, 9, 21, 8, 59), expected: false},
		// previous and next weeks are not covered
		{key: wKey, period: "w", to: ft(2026, 9, 20, 23, 59, 59), expected: false},
		{key: wKey, period: "w", to: ft(2026, 9, 28), expected: false},
		// another metric using the same SQL file has its own key
		{key: otherKey, period: "w", to: ft(2026, 9, 25, 10), expected: false},
		// daily: the same day, from the marker hour on, hour precision 'to' as passed to calc_metric
		{key: dKey, period: "d", to: ft(2026, 9, 25, 3), expected: true},
		{key: dKey, period: "d", to: ft(2026, 9, 25, 2, 59), expected: false},
		{key: dKey, period: "d", to: ft(2026, 9, 25, 23), expected: true},
		{key: dKey, period: "d", to: ft(2026, 9, 26), expected: false},
		{key: dKey, period: "d", to: ft(2026, 9, 24, 23), expected: false},
		// GHA2DB_TMOFFSET moves the day boundary: 2026-09-25 22:00 + 2h is already 09-26
		{key: dKey, period: "d", tmOffset: 2, to: ft(2026, 9, 25, 21, 59), expected: true},
		{key: dKey, period: "d", tmOffset: 2, to: ft(2026, 9, 25, 22), expected: false},
		{key: dKey, period: "d", tmOffset: 2, to: ft(2026, 9, 24, 23), expected: false},
		// hourly: the same hour only
		{key: hKey, period: "h", to: ft(2026, 9, 25, 10), expected: true},
		{key: hKey, period: "h", to: ft(2026, 9, 25, 10, 59, 59), expected: true},
		{key: hKey, period: "h", to: ft(2026, 9, 25, 11), expected: false},
		{key: hKey, period: "h", to: ft(2026, 9, 25, 9, 59), expected: false},
		// past quick range: daily, whatever its start
		{key: aKey, period: "a_0_1", to: ft(2026, 9, 25, 10), expected: true},
		{key: aKey, period: "a_0_1", rangeStart: ft(2015, 1, 1), to: ft(2026, 9, 25, 10), expected: true},
		{key: aKey, period: "a_0_1", rangeStart: ft(2015, 1, 1), to: ft(2026, 9, 26), expected: false},
		// quick range ending now: by its length
		{key: nKey, period: "a_3_n", rangeStart: ft(2026, 9, 15), to: ft(2026, 9, 25, 10), expected: true},
		{key: nKey, period: "a_3_n", rangeStart: ft(2026, 9, 15), to: ft(2026, 9, 26), expected: false},
		{key: nKey, period: "a_3_n", rangeStart: ft(2026, 8, 1), to: ft(2026, 9, 26), expected: true},
		{key: nKey, period: "a_3_n", rangeStart: ft(2026, 8, 1), to: ft(2026, 9, 30, 23), expected: true},
		{key: nKey, period: "a_3_n", rangeStart: ft(2026, 8, 1), to: ft(2026, 10, 1), expected: false},
		{key: nKey, period: "a_3_n", rangeStart: ft(2020, 3, 1), to: ft(2026, 12, 31, 23), expected: true},
		{key: nKey, period: "a_3_n", rangeStart: ft(2020, 3, 1), to: ft(2027, 1, 1), expected: false},
		{key: nKey, period: "a_3_n", to: ft(2026, 9, 26), expected: false},
		// periods without a calendar period are always reported as computed
		{key: "unknown", period: "range:2020-01-01,2020-02-01", to: ft(2026, 9, 25, 10), expected: true},
		{key: "unknown", period: "", to: ft(2026, 9, 25, 10), expected: true},
	}
	for index, test := range testCases {
		ctx.TmOffset = test.tmOffset
		got := lib.IsPeriodComputed(c, &ctx, test.key, test.period, test.rangeStart, test.to)
		if got != test.expected {
			t.Errorf(
				"test number %d, expected '%v' for key '%s', period '%s', range start '%v', to '%v', offset %d, got '%v'",
				index+1, test.expected, test.key, test.period, test.rangeStart, test.to, test.tmOffset, got,
			)
		}
	}
	ctx.TmOffset = 0

	// Markers do not depend on the offset: one written with offset 14 is found with any offset
	ctx.TmOffset = 14
	lib.SetPeriodComputed(c, &ctx, dKey, ft(2026, 9, 26, 10, 58))
	if !lib.IsPeriodComputed(c, &ctx, dKey, "d", zero, ft(2026, 9, 26, 11)) {
		t.Errorf("offset 14: marker written at 2026-09-26 10:00 must be found for 2026-09-26 11:00")
	}
	ctx.TmOffset = 0
	if !lib.IsPeriodComputed(c, &ctx, dKey, "d", zero, ft(2026, 9, 26, 11)) {
		t.Errorf("offset 0: marker written at 2026-09-26 10:00 must be found for 2026-09-26 11:00")
	}
	if lib.IsPeriodComputed(c, &ctx, dKey, "d", zero, ft(2026, 9, 26, 9, 59)) {
		t.Errorf("offset 0: 2026-09-26 09:59 is before the marker")
	}
	got := markerRows(c, &ctx, dKey)
	expected := []time.Time{ft(2026, 9, 25, 3), ft(2026, 9, 26, 10)}
	if len(got) != 2 || !got[0].Equal(expected[0]) || !got[1].Equal(expected[1]) {
		t.Errorf("expected daily markers %v, got %v", expected, got)
	}
}
