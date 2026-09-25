package devstatscode

import (
	"reflect"
	"testing"
	"time"

	lib "github.com/cncf/devstatscode"
	testlib "github.com/cncf/devstatscode/test"
)

func TestIntervalHours(t *testing.T) {
	// Test cases
	var testCases = []struct {
		period   string
		expected string
	}{
		{period: "", expected: "0"},
		{period: "h", expected: "1.000000"},
		{period: "  1 h ", expected: "1.000000"},
		{period: "1.00 h and whatever else", expected: "1.000000"},
		{period: "2 hrs", expected: "2.000000"},
		{period: "3  hour", expected: "3.000000"},
		{period: "4.5 hours", expected: "4.500000"},
		{period: "1 day", expected: "24.000000"},
		{period: "1 week", expected: "168.000000"},
		{period: "10 days", expected: "240.000000"},
		{period: "1 month", expected: "730.500000"},
		{period: "3 months", expected: "2191.500000"},
		{period: "1 quarter", expected: "2191.500000"},
		{period: "1 year", expected: "8766.000000"},
		{period: "10 years", expected: "87660.000000"},
		{period: "100 years", expected: "876600.000000"},
		{period: "15 minutes", expected: "0.250000"},
		{period: "20 mins", expected: "0.333333"},
		{period: "180 sec", expected: "0.050000"},
		{period: "-10 days", expected: "0.000000"},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.IntervalHours(test.period)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestRangeHours(t *testing.T) {
	// Test cases
	ft := testlib.YMDHMS
	var testCases = []struct {
		from     time.Time
		to       time.Time
		expected string
	}{
		{
			from:     ft(2017, 8, 29, 12, 29, 3),
			to:       ft(2017, 8, 29, 14, 29, 3),
			expected: "2.000000",
		},
		{
			from:     ft(2017, 8, 29, 14, 29, 3),
			to:       ft(2017, 8, 29, 12, 29, 3),
			expected: "0",
		},
		{
			from:     ft(2020, 3, 13, 12, 0, 0),
			to:       ft(2020, 3, 13, 12, 0, 1),
			expected: "0.000278",
		},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.RangeHours(test.from, test.to)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestComputePeriodAtThisDate(t *testing.T) {
	// Rules (independent of the sync frequency, no time-of-day, no randomness):
	// h*: always
	// d*: first sync after a day boundary (previous sync ended at 'from', this one at 'to')
	// w*: first sync after a week boundary (weeks start on Monday)
	// m*, q*, y*: first sync after a month/quarter/year boundary
	// The same rules apply to histograms; their quick ranges ending now (_n) follow the class given by
	// their length (d/m/q/y), the other quick ranges (fully in the past) are daily
	// 2026-09-20 is a Sunday, 2026-09-21 a Monday, 2026-10-01 a Thursday, 2026-11-01 a Sunday,
	// 2027-01-01 a Friday, 2029-01-01 a Monday
	ft := testlib.YMDHMS
	var zero time.Time
	// Periods of each class, quick ranges ending now start 'days' before 'to' (0: unknown start)
	type classPeriod struct {
		period string
		days   int
	}
	periodsByClass := map[string][]classPeriod{
		"h": {{"h", 0}, {"h24", 0}},
		"d": {{"d", 0}, {"d7", 0}, {"d10", 0}, {"a_3_n", 10}, {"c_n", 30}, {"a_0_1", 400}, {"c_b", 0}, {"c_j_i", 0}, {"a_5_n", 0}},
		"w": {{"w", 0}, {"w2", 0}},
		"m": {{"m", 0}, {"m6", 0}, {"a_2_n", 31}, {"c_i_n", 90}},
		"q": {{"q", 0}, {"q2", 0}, {"a_1_n", 92}, {"c_g_n", 365}},
		"y": {{"y", 0}, {"y2", 0}, {"y10", 0}, {"y100", 0}, {"a_0_n", 366}, {"c_n", 3650}},
	}
	classes := []string{"h", "d", "w", "m", "q", "y"}
	var scenarios = []struct {
		name     string
		tmOffset int
		from     time.Time
		to       time.Time
		expected map[string]bool
	}{
		{name: "hourly sync, same day", from: ft(2026, 9, 21, 9), to: ft(2026, 9, 21, 10, 4), expected: map[string]bool{"h": true, "d": false, "w": false, "m": false, "q": false, "y": false}},
		{name: "hourly sync, Wednesday midnight", from: ft(2026, 9, 22, 23), to: ft(2026, 9, 23, 0, 4), expected: map[string]bool{"h": true, "d": true, "w": false, "m": false, "q": false, "y": false}},
		{name: "hourly sync, Monday midnight", from: ft(2026, 9, 20, 23), to: ft(2026, 9, 21, 0, 4), expected: map[string]bool{"h": true, "d": true, "w": true, "m": false, "q": false, "y": false}},
		{name: "hourly sync, Oct 1st midnight (Thursday)", from: ft(2026, 9, 30, 23), to: ft(2026, 10, 1, 0, 4), expected: map[string]bool{"h": true, "d": true, "w": false, "m": true, "q": true, "y": false}},
		{name: "hourly sync, Nov 1st midnight (Sunday, weeks start on Monday)", from: ft(2026, 10, 31, 23), to: ft(2026, 11, 1, 0, 4), expected: map[string]bool{"h": true, "d": true, "w": false, "m": true, "q": false, "y": false}},
		{name: "hourly sync, Jan 1st 2027 midnight (Friday)", from: ft(2026, 12, 31, 23), to: ft(2027, 1, 1, 0, 4), expected: map[string]bool{"h": true, "d": true, "w": false, "m": true, "q": true, "y": true}},
		{name: "hourly sync, Jan 1st 2029 midnight (Monday)", from: ft(2028, 12, 31, 23), to: ft(2029, 1, 1, 0, 4), expected: map[string]bool{"h": true, "d": true, "w": true, "m": true, "q": true, "y": true}},
		{name: "daily sync, Tuesday", from: ft(2026, 9, 21, 9), to: ft(2026, 9, 22, 9, 58), expected: map[string]bool{"h": true, "d": true, "w": false, "m": false, "q": false, "y": false}},
		{name: "daily sync, Monday", from: ft(2026, 9, 20, 9), to: ft(2026, 9, 21, 9, 58), expected: map[string]bool{"h": true, "d": true, "w": true, "m": false, "q": false, "y": false}},
		{name: "daily sync, Oct 1st late evening", from: ft(2026, 9, 30, 22), to: ft(2026, 10, 1, 22, 35), expected: map[string]bool{"h": true, "d": true, "w": false, "m": true, "q": true, "y": false}},
		{name: "daily sync, Monday sync missed, Tuesday still concludes the week", from: ft(2026, 9, 20, 9), to: ft(2026, 9, 22, 9, 58), expected: map[string]bool{"h": true, "d": true, "w": true, "m": false, "q": false, "y": false}},
		{name: "daily sync, 5 days gap over a week and a month boundary", from: ft(2026, 9, 26, 9), to: ft(2026, 10, 1, 9, 58), expected: map[string]bool{"h": true, "d": true, "w": true, "m": true, "q": true, "y": false}},
		{name: "4 syncs/day, 1st sync on Monday", from: ft(2026, 9, 20, 21), to: ft(2026, 9, 21, 3, 4), expected: map[string]bool{"h": true, "d": true, "w": true, "m": false, "q": false, "y": false}},
		{name: "4 syncs/day, 2nd sync on Monday", from: ft(2026, 9, 21, 3), to: ft(2026, 9, 21, 9, 4), expected: map[string]bool{"h": true, "d": false, "w": false, "m": false, "q": false, "y": false}},
		{name: "4 syncs/day, 1st sync on Oct 1st", from: ft(2026, 9, 30, 21), to: ft(2026, 10, 1, 3, 4), expected: map[string]bool{"h": true, "d": true, "w": false, "m": true, "q": true, "y": false}},
		{name: "4 syncs/day, 2nd sync on Oct 1st", from: ft(2026, 10, 1, 3), to: ft(2026, 10, 1, 9, 4), expected: map[string]bool{"h": true, "d": false, "w": false, "m": false, "q": false, "y": false}},
		{name: "4 syncs/day, 4th sync on Oct 1st", from: ft(2026, 10, 1, 15), to: ft(2026, 10, 1, 21, 4), expected: map[string]bool{"h": true, "d": false, "w": false, "m": false, "q": false, "y": false}},
		{name: "reset TSDB (from = default start date)", from: ft(2012, 7, 1), to: ft(2026, 9, 25, 10), expected: map[string]bool{"h": true, "d": true, "w": true, "m": true, "q": true, "y": true}},
		{name: "same hour", from: ft(2026, 9, 25, 10), to: ft(2026, 9, 25, 10, 30), expected: map[string]bool{"h": true, "d": false, "w": false, "m": false, "q": false, "y": false}},
		{name: "from after to", from: ft(2026, 9, 26, 11), to: ft(2026, 9, 25, 10), expected: map[string]bool{"h": true, "d": false, "w": false, "m": false, "q": false, "y": false}},
		{name: "tz offset -6 moves the day boundary: 2nd Monday sync becomes the 1st", tmOffset: -6, from: ft(2026, 9, 21, 3), to: ft(2026, 9, 21, 9, 4), expected: map[string]bool{"h": true, "d": true, "w": true, "m": false, "q": false, "y": false}},
		{name: "tz offset +2 moves the month boundary before midnight UTC", tmOffset: 2, from: ft(2026, 9, 30, 21), to: ft(2026, 9, 30, 23, 4), expected: map[string]bool{"h": true, "d": true, "w": false, "m": true, "q": true, "y": false}},
		{name: "tz offset +2, boundary already crossed by the previous sync", tmOffset: 2, from: ft(2026, 9, 30, 23), to: ft(2026, 10, 1, 1, 4), expected: map[string]bool{"h": true, "d": false, "w": false, "m": false, "q": false, "y": false}},
	}

	// Environment context parse
	var ctx lib.Ctx
	ctx.Init()
	ctx.TestMode = true

	// Execute scenarios: every period of a class, histogram or not (quick ranges are histograms only)
	checked := 0
	for index, scenario := range scenarios {
		ctx.TmOffset = scenario.tmOffset
		ctx.ComputeAll = false
		ctx.ComputePeriods = nil
		for _, class := range classes {
			for _, cp := range periodsByClass[class] {
				rangeStart := zero
				if cp.days > 0 {
					rangeStart = scenario.to.AddDate(0, 0, -cp.days)
				}
				hists := []bool{false, true}
				if cp.period[0:1] == "a" || cp.period[0:1] == "c" {
					hists = []bool{true}
				}
				for _, hist := range hists {
					expected := scenario.expected[class]
					got := lib.ComputePeriodAtThisDate(&ctx, cp.period, rangeStart, scenario.from, scenario.to, hist)
					if got != expected {
						t.Errorf(
							"scenario %d '%s', expected '%v' from period '%v', range start '%v', hist '%v' for from '%v', to '%v', got '%v'",
							index+1, scenario.name, expected, cp.period, rangeStart, hist, scenario.from, scenario.to, got,
						)
					}
					checked++
				}
			}
		}
	}
	if checked != 23*(15*2+12) {
		t.Errorf("expected %d scenario checks, got %d", 23*(15*2+12), checked)
	}

	// Overrides and histogram quick ranges
	var testCases = []struct {
		tmOffset       int
		period         string
		rangeStart     time.Time
		from           time.Time
		to             time.Time
		hist           bool
		expected       bool
		computeAll     bool
		computePeriods map[string]map[bool]struct{}
	}{
		// GHA2DB_COMPUTE_ALL: everything
		{hist: false, period: "y", from: ft(2026, 9, 25, 10), to: ft(2026, 9, 25, 10, 30), computeAll: true, expected: true},
		{hist: true, period: "d", from: ft(2026, 9, 25, 10), to: ft(2026, 9, 25, 10, 30), computeAll: true, expected: true},
		{hist: true, period: "a_13_n", from: ft(2026, 9, 25, 10), to: ft(2026, 9, 25, 10, 30), computeAll: true, expected: true},
		{hist: true, period: "a_0_1", from: ft(2026, 9, 25, 10), to: ft(2026, 9, 25, 10, 30), computeAll: true, expected: true},
		// GHA2DB_FORCE_PERIODS: only the listed period/hist combinations, keyed by the period name
		{hist: false, period: "y", from: ft(2026, 9, 25, 10), to: ft(2026, 9, 25, 10, 30), computePeriods: map[string]map[bool]struct{}{"y": {false: {}}}, expected: true},
		{hist: false, period: "y", from: ft(2026, 9, 25, 10), to: ft(2026, 9, 25, 10, 30), computePeriods: map[string]map[bool]struct{}{"y": {true: {}}}, expected: false},
		{hist: false, period: "y", from: ft(2026, 9, 25, 10), to: ft(2026, 9, 25, 10, 30), computePeriods: map[string]map[bool]struct{}{"m": {false: {}}}, expected: false},
		{hist: true, period: "y", from: ft(2026, 9, 25, 10), to: ft(2026, 9, 25, 10, 30), computePeriods: map[string]map[bool]struct{}{"y": {false: {}}}, expected: false},
		{hist: true, period: "y", from: ft(2026, 9, 25, 10), to: ft(2026, 9, 25, 10, 30), computePeriods: map[string]map[bool]struct{}{"y": {true: {}}}, expected: true},
		{hist: false, period: "d", from: ft(2012, 7, 1), to: ft(2026, 9, 25, 10), computePeriods: map[string]map[bool]struct{}{"d": {true: {}}}, expected: false},
		{hist: true, period: "d", from: ft(2012, 7, 1), to: ft(2026, 9, 25, 10), computePeriods: map[string]map[bool]struct{}{"d": {true: {}}}, expected: true},
		{hist: false, period: "h", from: ft(2012, 7, 1), to: ft(2026, 9, 25, 10), computePeriods: map[string]map[bool]struct{}{"d": {false: {}}}, expected: false},
		{hist: true, period: "a_0_1", from: ft(2012, 7, 1), to: ft(2026, 9, 25, 10), computePeriods: map[string]map[bool]struct{}{"a_0_1": {true: {}}}, expected: true},
		{hist: true, period: "a_0_n", rangeStart: ft(2012, 7, 1), from: ft(2012, 7, 1), to: ft(2026, 9, 25, 10), computePeriods: map[string]map[bool]struct{}{"a_0_1": {true: {}}}, expected: false},
		// quick ranges fully in the past (a_i_j, c_b, c_j_i, c_i_g, c_j_g): daily, their start date does not matter
		{hist: true, period: "a_0_1", rangeStart: ft(2015, 1, 1), from: ft(2026, 9, 25, 9), to: ft(2026, 9, 25, 10, 4), expected: false},
		{hist: true, period: "a_0_1", rangeStart: ft(2015, 1, 1), from: ft(2026, 9, 24, 23), to: ft(2026, 9, 25, 0, 4), expected: true},
		{hist: true, period: "a_12_13", from: ft(2026, 9, 24, 23), to: ft(2026, 9, 25, 0, 4), expected: true},
		{hist: true, period: "c_b", from: ft(2026, 9, 25, 9), to: ft(2026, 9, 25, 10, 4), expected: false},
		{hist: true, period: "c_b", from: ft(2026, 9, 24, 23), to: ft(2026, 9, 25, 0, 4), expected: true},
		{hist: true, period: "c_j_i", rangeStart: ft(2016, 1, 1), from: ft(2026, 9, 24, 23), to: ft(2026, 9, 25, 0, 4), expected: true},
		{hist: true, period: "c_i_g", rangeStart: ft(2016, 1, 1), from: ft(2026, 9, 25, 9), to: ft(2026, 9, 25, 10, 4), expected: false},
		{hist: true, period: "c_j_g", rangeStart: ft(2016, 1, 1), from: ft(2026, 9, 24, 23), to: ft(2026, 9, 25, 0, 4), expected: true},
		// quick ranges ending now with an unknown start: daily
		{hist: true, period: "a_5_n", from: ft(2026, 9, 25, 9), to: ft(2026, 9, 25, 10, 4), expected: false},
		{hist: true, period: "a_5_n", from: ft(2026, 9, 24, 23), to: ft(2026, 9, 25, 0, 4), expected: true},
		{hist: true, period: "c_n", from: ft(2026, 9, 24, 23), to: ft(2026, 9, 25, 0, 4), expected: true},
		// quick ranges ending now, up to a month long: daily
		{hist: true, period: "a_3_n", rangeStart: ft(2026, 9, 15), from: ft(2026, 9, 25, 9), to: ft(2026, 9, 25, 10, 4), expected: false},
		{hist: true, period: "a_3_n", rangeStart: ft(2026, 9, 15), from: ft(2026, 9, 24, 23), to: ft(2026, 9, 25, 0, 4), expected: true},
		{hist: true, period: "a_3_n", rangeStart: ft(2026, 9, 15), from: ft(2026, 9, 21, 3), to: ft(2026, 9, 21, 9, 4), expected: false},
		{hist: true, period: "a_3_n", rangeStart: ft(2026, 9, 15), tmOffset: -6, from: ft(2026, 9, 21, 3), to: ft(2026, 9, 21, 9, 4), expected: true},
		// exactly a month (730.5h up to the hour of 'to'): daily, a minute longer: monthly
		{hist: true, period: "a_1_n", rangeStart: ft(2026, 8, 25, 13, 30), from: ft(2026, 9, 24, 23), to: ft(2026, 9, 25, 0, 4), expected: true},
		{hist: true, period: "a_1_n", rangeStart: ft(2026, 8, 25, 13, 29), from: ft(2026, 9, 24, 23), to: ft(2026, 9, 25, 0, 4), expected: false},
		// quick ranges ending now, up to a quarter long: monthly
		{hist: true, period: "a_2_n", rangeStart: ft(2026, 8, 1), from: ft(2026, 9, 24, 23), to: ft(2026, 9, 25, 0, 4), expected: false},
		{hist: true, period: "a_2_n", rangeStart: ft(2026, 8, 1), from: ft(2026, 8, 31, 23), to: ft(2026, 9, 1, 0, 4), expected: true},
		{hist: true, period: "c_n", rangeStart: ft(2026, 7, 10), from: ft(2026, 9, 20, 23), to: ft(2026, 9, 21, 0, 4), expected: false},
		{hist: true, period: "c_n", rangeStart: ft(2026, 7, 10), from: ft(2026, 9, 30, 23), to: ft(2026, 10, 1, 0, 4), expected: true},
		// quick ranges ending now, up to a year long: quarterly
		{hist: true, period: "c_i_n", rangeStart: ft(2026, 1, 1), from: ft(2026, 8, 31, 23), to: ft(2026, 9, 1, 0, 4), expected: false},
		{hist: true, period: "c_i_n", rangeStart: ft(2026, 1, 1), from: ft(2026, 6, 30, 23), to: ft(2026, 7, 1, 0, 4), expected: true},
		{hist: true, period: "a_4_n", rangeStart: ft(2025, 12, 1), from: ft(2026, 9, 30, 23), to: ft(2026, 10, 1, 0, 4), expected: true},
		// quick ranges ending now, longer than a year: yearly
		{hist: true, period: "c_g_n", rangeStart: ft(2020, 3, 1), from: ft(2026, 9, 30, 23), to: ft(2026, 10, 1, 0, 4), expected: false},
		{hist: true, period: "c_g_n", rangeStart: ft(2020, 3, 1), from: ft(2025, 12, 31, 23), to: ft(2026, 1, 1, 0, 4), expected: true},
		{hist: true, period: "a_0_n", rangeStart: ft(2012, 7, 1), from: ft(2012, 7, 1), to: ft(2026, 9, 25, 10), expected: true},
		{hist: true, period: "a_0_n", rangeStart: ft(2012, 7, 1), from: ft(2026, 9, 24, 23), to: ft(2026, 9, 25, 0, 4), expected: false},
		{hist: true, period: "a_0_n", rangeStart: ft(2012, 7, 1), tmOffset: 3, from: ft(2026, 12, 31, 20), to: ft(2026, 12, 31, 21, 4), expected: true},
	}
	for index, test := range testCases {
		ctx.TmOffset = test.tmOffset
		ctx.ComputeAll = test.computeAll
		ctx.ComputePeriods = test.computePeriods
		got := lib.ComputePeriodAtThisDate(&ctx, test.period, test.rangeStart, test.from, test.to, test.hist)
		if got != test.expected {
			t.Errorf(
				"test number %d, expected '%v' from period '%v', range start '%v', hist '%v' for from '%v', to '%v', got '%v'",
				index+1, test.expected, test.period, test.rangeStart, test.hist, test.from, test.to, got,
			)
		}
	}
}

func TestPeriodClass(t *testing.T) {
	ft := testlib.YMDHMS
	var zero time.Time
	// Friday 10:58:33, lengths are measured up to the hour start: 10:00
	to := ft(2026, 9, 25, 10, 58, 33)
	var testCases = []struct {
		period     string
		rangeStart time.Time
		to         time.Time
		expected   string
	}{
		// calendar periods: their first letter, multiples follow their base period
		{period: "h", to: to, expected: "h"},
		{period: "h24", to: to, expected: "h"},
		{period: "d", to: to, expected: "d"},
		{period: "d7", to: to, expected: "d"},
		{period: "d10", to: to, expected: "d"},
		{period: "w", to: to, expected: "w"},
		{period: "w2", to: to, expected: "w"},
		{period: "m", to: to, expected: "m"},
		{period: "m6", to: to, expected: "m"},
		{period: "q", to: to, expected: "q"},
		{period: "q2", to: to, expected: "q"},
		{period: "y", to: to, expected: "y"},
		{period: "y2", to: to, expected: "y"},
		{period: "y10", to: to, expected: "y"},
		{period: "y100", to: to, expected: "y"},
		{period: "d", rangeStart: ft(2012, 7, 1), to: to, expected: "d"},
		// quick ranges fully in the past: daily whatever their start
		{period: "a_0_1", to: to, expected: "d"},
		{period: "a_0_1", rangeStart: ft(2015, 1, 1), to: to, expected: "d"},
		{period: "a_12_13", rangeStart: ft(2026, 9, 20), to: to, expected: "d"},
		{period: "c_b", to: to, expected: "d"},
		{period: "c_j_i", rangeStart: ft(2016, 1, 1), to: to, expected: "d"},
		{period: "c_i_g", rangeStart: ft(2016, 1, 1), to: to, expected: "d"},
		{period: "c_j_g", rangeStart: ft(2016, 1, 1), to: to, expected: "d"},
		// quick ranges ending now: by their length, daily when the start is unknown
		{period: "a_3_n", to: to, expected: "d"},
		{period: "c_n", to: to, expected: "d"},
		{period: "a_3_n", rangeStart: ft(2026, 9, 25, 9, 59), to: to, expected: "d"},
		{period: "a_3_n", rangeStart: ft(2026, 9, 25, 10, 30), to: to, expected: "d"},
		{period: "a_3_n", rangeStart: ft(2026, 9, 26), to: to, expected: "d"},
		// exactly a month (730.5h) and a minute more
		{period: "a_3_n", rangeStart: ft(2026, 8, 25, 23, 30), to: to, expected: "d"},
		{period: "a_3_n", rangeStart: ft(2026, 8, 25, 23, 29), to: to, expected: "m"},
		// minutes of 'to' are ignored: 10:58:33 - 08-25 23:45 is longer than a month, 10:00 - 23:45 is not
		{period: "a_3_n", rangeStart: ft(2026, 8, 25, 23, 45), to: to, expected: "d"},
		{period: "a_3_n", rangeStart: ft(2026, 8, 25, 23, 45), to: ft(2026, 9, 25, 11), expected: "m"},
		// exactly a quarter (2191.5h) and a minute more
		{period: "c_n", rangeStart: ft(2026, 6, 26, 2, 30), to: to, expected: "m"},
		{period: "c_n", rangeStart: ft(2026, 6, 26, 2, 29), to: to, expected: "q"},
		// exactly a year (8766h) and a minute more
		{period: "c_i_n", rangeStart: ft(2025, 9, 25, 4), to: to, expected: "q"},
		{period: "c_i_n", rangeStart: ft(2025, 9, 25, 3, 59), to: to, expected: "y"},
		{period: "c_g_n", rangeStart: ft(2016, 3, 10), to: to, expected: "y"},
		{period: "a_0_n", rangeStart: ft(2012, 7, 1), to: to, expected: "y"},
		{period: "a_0_n", rangeStart: ft(2012, 7, 1), to: ft(2012, 7, 20), expected: "d"},
		// no calendar period
		{period: "", to: to, expected: ""},
		{period: "x", to: to, expected: ""},
		{period: "D", to: to, expected: ""},
		{period: "H", to: to, expected: ""},
		{period: "range:2020-01-01,2020-02-01", to: to, expected: ""},
		{period: "range:2020-01-01,2020-02-01", rangeStart: ft(2020, 1, 1), to: to, expected: ""},
	}
	for index, test := range testCases {
		got := lib.PeriodClass(test.period, test.rangeStart, test.to)
		if got != test.expected {
			t.Errorf(
				"test number %d, expected '%s' for period '%s', range start '%v', to '%v', got '%s'",
				index+1, test.expected, test.period, test.rangeStart, test.to, got,
			)
		}
	}
	_ = zero
}

func TestQuickRangeStarts(t *testing.T) {
	ft := testlib.YMDHMS
	// 'quick_ranges_data' tag values as written by 'annotations': suffix;period;from;to
	data := []string{
		"d;1 day;;",
		"d7;7 days;;",
		"w;1 week;;",
		"y100;100 years;;",
		"a_0_1;;2019-01-01 00:00:00;2019-06-01 00:00:00",
		"a_1_n;;2019-06-01 12:30:45;2026-09-26 00:00:00",
		"c_b;;2012-07-01 00:00:00;2018-03-01 00:00:00",
		"c_n;;2018-03-01 00:00:00;2026-09-26 00:00:00",
		"",
		"garbage",
		"x;;",
		"a_2_n;;;2026-09-26 00:00:00",
		"a_3_n;d;2019-01-01 00:00:00;2026-09-26 00:00:00",
	}
	expected := map[string]time.Time{
		"a_0_1": ft(2019, 1, 1),
		"a_1_n": ft(2019, 6, 1, 12, 30, 45),
		"c_b":   ft(2012, 7, 1),
		"c_n":   ft(2018, 3, 1),
	}
	got := lib.QuickRangeStarts(data)
	if len(got) != len(expected) {
		t.Errorf("expected %d quick range starts, got %d: %v", len(expected), len(got), got)
	}
	for sfx, dt := range expected {
		gotDt, ok := got[sfx]
		if !ok {
			t.Errorf("missing quick range start for '%s'", sfx)
			continue
		}
		if !gotDt.Equal(dt) {
			t.Errorf("expected '%v' for '%s', got '%v'", dt, sfx, gotDt)
		}
	}
	if len(lib.QuickRangeStarts(nil)) != 0 {
		t.Errorf("expected no quick range starts for no data")
	}
}

func TestDayBoundaryCrossed(t *testing.T) {
	ft := testlib.YMDHMS
	var testCases = []struct {
		tmOffset int
		from     time.Time
		to       time.Time
		expected bool
	}{
		{from: ft(2026, 9, 21, 9), to: ft(2026, 9, 21, 10, 4), expected: false},
		{from: ft(2026, 9, 20, 23), to: ft(2026, 9, 21, 0, 4), expected: true},
		{from: ft(2026, 9, 20, 21), to: ft(2026, 9, 21, 3, 4), expected: true},
		{from: ft(2026, 9, 21, 3), to: ft(2026, 9, 21, 9, 4), expected: false},
		{from: ft(2026, 9, 21, 9), to: ft(2026, 9, 22, 9, 58), expected: true},
		{from: ft(2026, 9, 21, 9), to: ft(2026, 9, 25, 9, 58), expected: true},
		{from: ft(2012, 7, 1), to: ft(2026, 9, 25, 10), expected: true},
		{from: ft(2026, 9, 25, 10), to: ft(2026, 9, 25, 10), expected: false},
		{from: ft(2026, 9, 26, 11), to: ft(2026, 9, 25, 10), expected: false},
		{tmOffset: -6, from: ft(2026, 9, 21, 3), to: ft(2026, 9, 21, 9, 4), expected: true},
		{tmOffset: 2, from: ft(2026, 9, 30, 21), to: ft(2026, 9, 30, 23, 4), expected: true},
		{tmOffset: 2, from: ft(2026, 9, 30, 23), to: ft(2026, 10, 1, 1, 4), expected: false},
	}
	var ctx lib.Ctx
	ctx.Init()
	ctx.TestMode = true
	for index, test := range testCases {
		ctx.TmOffset = test.tmOffset
		got := lib.DayBoundaryCrossed(&ctx, test.from, test.to)
		if got != test.expected {
			t.Errorf(
				"test number %d, expected '%v' for from '%v', to '%v', got '%v'",
				index+1, test.expected, test.from, test.to, got,
			)
		}
	}
}

func TestPeriodStartAt(t *testing.T) {
	ft := testlib.YMDHMS
	// Friday
	dt := ft(2026, 9, 25, 10, 58, 33)
	var testCases = []struct {
		period     string
		rangeStart time.Time
		tmOffset   int
		dt         time.Time
		expected   time.Time
		ok         bool
	}{
		{period: "h", dt: dt, expected: ft(2026, 9, 25, 10), ok: true},
		{period: "h24", dt: dt, expected: ft(2026, 9, 25, 10), ok: true},
		{period: "d", dt: dt, expected: ft(2026, 9, 25), ok: true},
		{period: "d7", dt: dt, expected: ft(2026, 9, 25), ok: true},
		{period: "d10", dt: dt, expected: ft(2026, 9, 25), ok: true},
		{period: "w", dt: dt, expected: ft(2026, 9, 21), ok: true},
		{period: "w2", dt: dt, expected: ft(2026, 9, 21), ok: true},
		{period: "m", dt: dt, expected: ft(2026, 9, 1), ok: true},
		{period: "m6", dt: dt, expected: ft(2026, 9, 1), ok: true},
		{period: "q", dt: dt, expected: ft(2026, 7, 1), ok: true},
		{period: "q2", dt: dt, expected: ft(2026, 7, 1), ok: true},
		{period: "y", dt: dt, expected: ft(2026, 1, 1), ok: true},
		{period: "y2", dt: dt, expected: ft(2026, 1, 1), ok: true},
		{period: "y10", dt: dt, expected: ft(2026, 1, 1), ok: true},
		{period: "y100", dt: dt, expected: ft(2026, 1, 1), ok: true},
		// weeks start on Monday: Sunday belongs to the previous Monday's week, Monday 00:00 starts a new one
		{period: "w", dt: ft(2026, 9, 20, 23, 59, 59), expected: ft(2026, 9, 14), ok: true},
		{period: "w", dt: ft(2026, 9, 21), expected: ft(2026, 9, 21), ok: true},
		{period: "q", dt: ft(2026, 10, 1), expected: ft(2026, 10, 1), ok: true},
		{period: "y", dt: ft(2026, 12, 31, 23, 59, 59), expected: ft(2026, 1, 1), ok: true},
		// hour-precision 'to' as passed by gha2db_sync to calc_metric gives the same period start
		{period: "d", dt: ft(2026, 9, 25, 10), expected: ft(2026, 9, 25), ok: true},
		{period: "h", dt: ft(2026, 9, 25, 10), expected: ft(2026, 9, 25, 10), ok: true},
		// GHA2DB_TMOFFSET: the boundary is found on the shifted clock, the result is the instant it happened
		{period: "h", tmOffset: 2, dt: dt, expected: ft(2026, 9, 25, 10), ok: true},
		{period: "d", tmOffset: 14, dt: dt, expected: ft(2026, 9, 25, 10), ok: true},
		{period: "d", tmOffset: -11, dt: dt, expected: ft(2026, 9, 24, 11), ok: true},
		{period: "d", tmOffset: 2, dt: ft(2026, 9, 25, 21, 59), expected: ft(2026, 9, 24, 22), ok: true},
		{period: "d", tmOffset: 2, dt: ft(2026, 9, 25, 22), expected: ft(2026, 9, 25, 22), ok: true},
		{period: "w", tmOffset: -11, dt: ft(2026, 9, 21, 3), expected: ft(2026, 9, 14, 11), ok: true},
		{period: "m", tmOffset: -11, dt: ft(2026, 10, 1, 3), expected: ft(2026, 9, 1, 11), ok: true},
		{period: "y", tmOffset: 3, dt: ft(2026, 12, 31, 22), expected: ft(2026, 12, 31, 21), ok: true},
		// histogram quick ranges: past ranges and ranges with an unknown start are daily
		{period: "a_0_1", dt: dt, expected: ft(2026, 9, 25), ok: true},
		{period: "a_0_1", rangeStart: ft(2015, 1, 1), dt: dt, expected: ft(2026, 9, 25), ok: true},
		{period: "c_b", dt: dt, expected: ft(2026, 9, 25), ok: true},
		{period: "c_j_g", rangeStart: ft(2016, 1, 1), dt: dt, expected: ft(2026, 9, 25), ok: true},
		{period: "a_5_n", dt: dt, expected: ft(2026, 9, 25), ok: true},
		// ranges ending now by their length
		{period: "a_3_n", rangeStart: ft(2026, 9, 15), dt: dt, expected: ft(2026, 9, 25), ok: true},
		{period: "a_2_n", rangeStart: ft(2026, 8, 1), dt: dt, expected: ft(2026, 9, 1), ok: true},
		{period: "c_i_n", rangeStart: ft(2026, 1, 1), dt: dt, expected: ft(2026, 7, 1), ok: true},
		{period: "c_g_n", rangeStart: ft(2020, 3, 1), dt: dt, expected: ft(2026, 1, 1), ok: true},
		{period: "c_n", rangeStart: ft(2012, 7, 1), dt: dt, expected: ft(2026, 1, 1), ok: true},
		{period: "a_2_n", rangeStart: ft(2026, 8, 1), tmOffset: 2, dt: ft(2026, 9, 30, 23, 30), expected: ft(2026, 9, 30, 22), ok: true},
		// no calendar period
		{period: "range:2020-01-01,2020-02-01", dt: dt, ok: false},
		{period: "range:2020-01-01,2020-02-01", rangeStart: ft(2020, 1, 1), dt: dt, ok: false},
		{period: "", dt: dt, ok: false},
		{period: "x", dt: dt, ok: false},
		{period: "D", dt: dt, ok: false},
	}
	var ctx lib.Ctx
	ctx.Init()
	ctx.TestMode = true
	for index, test := range testCases {
		ctx.TmOffset = test.tmOffset
		got, ok := lib.PeriodStartAt(&ctx, test.period, test.rangeStart, test.dt)
		if ok != test.ok {
			t.Errorf(
				"test number %d, expected ok '%v' for period '%s', range start '%v', dt '%v', offset %d, got '%v'",
				index+1, test.ok, test.period, test.rangeStart, test.dt, test.tmOffset, ok,
			)
			continue
		}
		if !ok {
			if !got.IsZero() {
				t.Errorf("test number %d, expected zero time for period '%s', got '%v'", index+1, test.period, got)
			}
			continue
		}
		if !got.Equal(test.expected) {
			t.Errorf(
				"test number %d, expected '%v' for period '%s', range start '%v', dt '%v', offset %d, got '%v'",
				index+1, test.expected, test.period, test.rangeStart, test.dt, test.tmOffset, got,
			)
		}
	}
}

func TestPreviousPeriodStart(t *testing.T) {
	ft := testlib.YMDHMS
	// Friday
	dt := ft(2026, 9, 25, 10, 58, 33)
	var testCases = []struct {
		period     string
		rangeStart time.Time
		tmOffset   int
		dt         time.Time
		expected   time.Time
		ok         bool
	}{
		{period: "h", dt: dt, expected: ft(2026, 9, 25, 9), ok: true},
		{period: "d", dt: dt, expected: ft(2026, 9, 24), ok: true},
		{period: "d7", dt: dt, expected: ft(2026, 9, 24), ok: true},
		{period: "w", dt: dt, expected: ft(2026, 9, 14), ok: true},
		{period: "w2", dt: dt, expected: ft(2026, 9, 14), ok: true},
		{period: "m", dt: dt, expected: ft(2026, 8, 1), ok: true},
		{period: "q", dt: dt, expected: ft(2026, 4, 1), ok: true},
		{period: "y", dt: dt, expected: ft(2025, 1, 1), ok: true},
		// exactly at the boundary the previous period is the one that just ended
		{period: "h", dt: ft(2026, 9, 25, 10), expected: ft(2026, 9, 25, 9), ok: true},
		{period: "d", dt: ft(2026, 9, 21), expected: ft(2026, 9, 20), ok: true},
		{period: "w", dt: ft(2026, 9, 21), expected: ft(2026, 9, 14), ok: true},
		{period: "w", dt: ft(2026, 9, 20, 23, 59, 59), expected: ft(2026, 9, 7), ok: true},
		{period: "m", dt: ft(2026, 3, 1), expected: ft(2026, 2, 1), ok: true},
		{period: "q", dt: ft(2026, 1, 1), expected: ft(2025, 10, 1), ok: true},
		{period: "y", dt: ft(2026, 1, 1), expected: ft(2025, 1, 1), ok: true},
		// the crash scenario: sync ending Monday 03:00 after the Monday 00:00 boundary sync was lost
		{period: "w", dt: ft(2026, 9, 21, 3), expected: ft(2026, 9, 14), ok: true},
		{period: "m", dt: ft(2026, 10, 1, 3), expected: ft(2026, 9, 1), ok: true},
		// GHA2DB_TMOFFSET: boundaries on the shifted clock
		{period: "d", tmOffset: 2, dt: ft(2026, 9, 25, 21, 59), expected: ft(2026, 9, 23, 22), ok: true},
		{period: "d", tmOffset: 2, dt: ft(2026, 9, 25, 22), expected: ft(2026, 9, 24, 22), ok: true},
		{period: "w", tmOffset: -11, dt: ft(2026, 9, 21, 3), expected: ft(2026, 9, 7, 11), ok: true},
		{period: "m", tmOffset: -11, dt: ft(2026, 10, 1, 3), expected: ft(2026, 8, 1, 11), ok: true},
		// histogram quick ranges follow their class
		{period: "a_0_1", dt: dt, expected: ft(2026, 9, 24), ok: true},
		{period: "a_2_n", rangeStart: ft(2026, 8, 1), dt: dt, expected: ft(2026, 8, 1), ok: true},
		{period: "c_i_n", rangeStart: ft(2026, 1, 1), dt: dt, expected: ft(2026, 4, 1), ok: true},
		{period: "c_n", rangeStart: ft(2012, 7, 1), dt: dt, expected: ft(2025, 1, 1), ok: true},
		// no calendar period
		{period: "range:2020-01-01,2020-02-01", dt: dt, ok: false},
		{period: "", dt: dt, ok: false},
		{period: "x", dt: dt, ok: false},
	}
	var ctx lib.Ctx
	ctx.Init()
	ctx.TestMode = true
	for index, test := range testCases {
		ctx.TmOffset = test.tmOffset
		got, ok := lib.PreviousPeriodStart(&ctx, test.period, test.rangeStart, test.dt)
		if ok != test.ok {
			t.Errorf(
				"test number %d, expected ok '%v' for period '%s', range start '%v', dt '%v', offset %d, got '%v'",
				index+1, test.ok, test.period, test.rangeStart, test.dt, test.tmOffset, ok,
			)
			continue
		}
		if !ok {
			if !got.IsZero() {
				t.Errorf("test number %d, expected zero time for period '%s', got '%v'", index+1, test.period, got)
			}
			continue
		}
		if !got.Equal(test.expected) {
			t.Errorf(
				"test number %d, expected '%v' for period '%s', range start '%v', dt '%v', offset %d, got '%v'",
				index+1, test.expected, test.period, test.rangeStart, test.dt, test.tmOffset, got,
			)
		}
		// the previous period ends where the current one starts
		start, _ := lib.PeriodStartAt(&ctx, test.period, test.rangeStart, test.dt)
		next, _ := lib.PeriodStartAt(&ctx, lib.PeriodClass(test.period, test.rangeStart, test.dt), time.Time{}, got.Add(time.Second))
		if !next.Equal(got) || !got.Before(start) {
			t.Errorf(
				"test number %d, previous period start '%v' for period '%s' is not a period start before '%v' (got '%v')",
				index+1, got, test.period, start, next,
			)
		}
	}
}

func TestDescriblePeriodInHours(t *testing.T) {
	// Test cases
	var testCases = []struct {
		hours    float64
		expected string
	}{
		{hours: -337, expected: "- 2 weeks 1 hour"},
		{hours: 0, expected: "zero"},
		{hours: 336, expected: "2 weeks"},
		{hours: 360, expected: "2 weeks 1 day"},
		{hours: 337, expected: "2 weeks 1 hour"},
		{hours: 338, expected: "2 weeks 2 hours"},
		{hours: 335, expected: "1 week 6 days 23 hours"},
		{hours: 168, expected: "1 week"},
		{hours: 216, expected: "1 week 2 days"},
		{hours: 169, expected: "1 week 1 hour"},
		{hours: 170, expected: "1 week 2 hours"},
		{hours: 167, expected: "6 days 23 hours"},
		{hours: 167.9, expected: "6 days 23 hours 54 minutes"},
		{hours: 168.2, expected: "1 week 12 minutes"},
		{hours: 335.99, expected: "1 week 6 days 23 hours 59 minutes 24 seconds"},
		{hours: 100, expected: "4 days 4 hours"},
		{hours: 1000, expected: "5 weeks 6 days 16 hours"},
		{hours: 0.3, expected: "18 minutes"},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.DescriblePeriodInHours(test.hours)
		if got != expected {
			t.Errorf(
				"test number %d, expected '%v' from %v hours, got '%v'",
				index+1, expected, test.hours, got,
			)
		}
	}
}

func TestPeriodParse(t *testing.T) {
	//func PeriodParse(perStr string) (dur time.Duration) {
	// Test cases
	expectedDuration, _ := time.ParseDuration("2m31s")
	var blankDuration time.Duration
	var testCases = []struct {
		periodStr        string
		expectedBool     bool
		expectedDuration time.Duration
	}{
		{periodStr: "blah blah blah [rate reset in 2m31s] no more calls", expectedBool: true, expectedDuration: expectedDuration},
		{periodStr: "blah blah blah [rate reset in 2m31s]", expectedBool: true, expectedDuration: expectedDuration},
		{periodStr: "[rate reset in 2m31s] no more calls", expectedBool: true, expectedDuration: expectedDuration},
		{periodStr: "[rate reset in 2m31s]", expectedBool: true, expectedDuration: expectedDuration},
		{periodStr: "[rate reset in xxx]", expectedBool: false, expectedDuration: blankDuration},
		{periodStr: "[rate reset in ]", expectedBool: false, expectedDuration: blankDuration},
		{periodStr: "[rate reset in]", expectedBool: false, expectedDuration: blankDuration},
		{periodStr: "blah blah blah", expectedBool: false, expectedDuration: blankDuration},
	}
	// Execute test cases
	for index, test := range testCases {
		expectedBool := test.expectedBool
		expectedDuration := test.expectedDuration
		gotDuration, gotBool := lib.PeriodParse(test.periodStr)
		if gotBool != expectedBool {
			t.Errorf(
				"test number %d, expected boolean %v, got %v",
				index+1, expectedBool, gotBool,
			)
		}
		if gotDuration != expectedDuration {
			t.Errorf(
				"test number %d, expected duration %v, got %v",
				index+1, expectedDuration, gotDuration,
			)
		}
	}
}

func TestHourStart(t *testing.T) {
	// Test cases
	ft := testlib.YMDHMS
	var testCases = []struct {
		time     time.Time
		expected time.Time
	}{
		{time: ft(2017, 8, 29, 12, 29, 3), expected: ft(2017, 8, 29, 12)},
		{time: ft(2017, 8, 29, 13), expected: ft(2017, 8, 29, 13)},
		{time: ft(2018), expected: ft(2018)},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.HourStart(test.time)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestNextHourStart(t *testing.T) {
	// Test cases
	ft := testlib.YMDHMS
	var testCases = []struct {
		time     time.Time
		expected time.Time
	}{
		{time: ft(2017, 8, 29, 12, 29, 3), expected: ft(2017, 8, 29, 13)},
		{time: ft(2017, 8, 29, 13), expected: ft(2017, 8, 29, 14)},
		{time: ft(2018), expected: ft(2018, 1, 1, 1)},
		{time: ft(2017, 12, 31, 23, 59, 59), expected: ft(2018)},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.NextHourStart(test.time)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestPrevHourStart(t *testing.T) {
	// Test cases
	ft := testlib.YMDHMS
	var testCases = []struct {
		time     time.Time
		expected time.Time
	}{
		{time: ft(2017, 8, 29, 12, 29, 3), expected: ft(2017, 8, 29, 11)},
		{time: ft(2017, 8, 29, 13), expected: ft(2017, 8, 29, 12)},
		{time: ft(2018), expected: ft(2017, 12, 31, 23)},
		{time: ft(2017, 12, 31, 23, 59, 59), expected: ft(2017, 12, 31, 22)},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.PrevHourStart(test.time)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestDayStart(t *testing.T) {
	// Test cases
	ft := testlib.YMDHMS
	var testCases = []struct {
		time     time.Time
		expected time.Time
	}{
		{time: ft(2017, 8, 29, 12, 29, 3), expected: ft(2017, 8, 29, 0)},
		{time: ft(2017, 8, 29, 13), expected: ft(2017, 8, 29)},
		{time: ft(2018), expected: ft(2018)},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.DayStart(test.time)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestNextDayStart(t *testing.T) {
	// Test cases
	ft := testlib.YMDHMS
	var testCases = []struct {
		time     time.Time
		expected time.Time
	}{
		{time: ft(2017, 8, 29, 12, 29, 3), expected: ft(2017, 8, 30)},
		{time: ft(2017, 8, 31, 13), expected: ft(2017, 9, 1)},
		{time: ft(2018), expected: ft(2018, 1, 2)},
		{time: ft(2017, 12, 31, 23, 59, 59), expected: ft(2018)},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.NextDayStart(test.time)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestPrevDayStart(t *testing.T) {
	// Test cases
	ft := testlib.YMDHMS
	var testCases = []struct {
		time     time.Time
		expected time.Time
	}{
		{time: ft(2017, 8, 29, 12, 29, 3), expected: ft(2017, 8, 28)},
		{time: ft(2017, 8, 31, 13), expected: ft(2017, 8, 30)},
		{time: ft(2018), expected: ft(2017, 12, 31)},
		{time: ft(2017, 12, 31, 23, 59, 59), expected: ft(2017, 12, 30)},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.PrevDayStart(test.time)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestWeekStart(t *testing.T) {
	// Test cases
	ft := testlib.YMDHMS
	var testCases = []struct {
		time     time.Time
		expected time.Time
	}{
		{time: ft(2017, 8, 26, 12, 29, 3), expected: ft(2017, 8, 21)},
		{time: ft(2017, 8, 23, 13), expected: ft(2017, 8, 21)},
		{time: ft(2017, 8, 13), expected: ft(2017, 8, 7)},
		{time: ft(2017, 8, 14), expected: ft(2017, 8, 14)},
		{time: ft(2017, 8, 15), expected: ft(2017, 8, 14)},
		{time: ft(2017), expected: ft(2016, 12, 26)},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.WeekStart(test.time)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestNextWeekStart(t *testing.T) {
	// Test cases
	ft := testlib.YMDHMS
	var testCases = []struct {
		time     time.Time
		expected time.Time
	}{
		{time: ft(2017, 8, 26, 12, 29, 3), expected: ft(2017, 8, 28)},
		{time: ft(2017, 8, 23, 13), expected: ft(2017, 8, 28)},
		{time: ft(2017, 8, 13), expected: ft(2017, 8, 14)},
		{time: ft(2017, 8, 14), expected: ft(2017, 8, 21)},
		{time: ft(2017, 8, 15), expected: ft(2017, 8, 21)},
		{time: ft(2017, 12, 31), expected: ft(2018)},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.NextWeekStart(test.time)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestPrevWeekStart(t *testing.T) {
	// Test cases
	ft := testlib.YMDHMS
	var testCases = []struct {
		time     time.Time
		expected time.Time
	}{
		{time: ft(2017, 8, 26, 12, 29, 3), expected: ft(2017, 8, 14)},
		{time: ft(2017, 8, 23, 13), expected: ft(2017, 8, 14)},
		{time: ft(2017, 8, 13), expected: ft(2017, 7, 31)},
		{time: ft(2017, 8, 14), expected: ft(2017, 8, 7)},
		{time: ft(2017, 8, 15), expected: ft(2017, 8, 7)},
		{time: ft(2017, 12, 31), expected: ft(2017, 12, 18)},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.PrevWeekStart(test.time)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestMonthStart(t *testing.T) {
	// Test cases
	ft := testlib.YMDHMS
	var testCases = []struct {
		time     time.Time
		expected time.Time
	}{
		{time: ft(2017, 8, 26, 12, 29, 3), expected: ft(2017, 8, 1)},
		{time: ft(2017), expected: ft(2017)},
		{time: ft(2017, 12, 10), expected: ft(2017, 12)},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.MonthStart(test.time)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestNextMonthStart(t *testing.T) {
	// Test cases
	ft := testlib.YMDHMS
	var testCases = []struct {
		time     time.Time
		expected time.Time
	}{
		{time: ft(2017, 8, 26, 12, 29, 3), expected: ft(2017, 9, 1)},
		{time: ft(2017), expected: ft(2017, 2)},
		{time: ft(2017, 12, 10), expected: ft(2018)},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.NextMonthStart(test.time)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestPrevMonthStart(t *testing.T) {
	// Test cases
	ft := testlib.YMDHMS
	var testCases = []struct {
		time     time.Time
		expected time.Time
	}{
		{time: ft(2017, 8, 26, 12, 29, 3), expected: ft(2017, 7, 1)},
		{time: ft(2017), expected: ft(2016, 12)},
		{time: ft(2017, 12, 10), expected: ft(2017, 11)},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.PrevMonthStart(test.time)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestQuarterStart(t *testing.T) {
	// Test cases
	ft := testlib.YMDHMS
	var testCases = []struct {
		time     time.Time
		expected time.Time
	}{
		{time: ft(2017, 8, 26, 12, 29, 3), expected: ft(2017, 7, 1)},
		{time: ft(2017), expected: ft(2017)},
		{time: ft(2017, 12, 10), expected: ft(2017, 10)},
		{time: ft(2017, 10, 12), expected: ft(2017, 10)},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.QuarterStart(test.time)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestNextQuarterStart(t *testing.T) {
	// Test cases
	ft := testlib.YMDHMS
	var testCases = []struct {
		time     time.Time
		expected time.Time
	}{
		{time: ft(2017, 8, 26, 12, 29, 3), expected: ft(2017, 10)},
		{time: ft(2017), expected: ft(2017, 4)},
		{time: ft(2017, 12, 10), expected: ft(2018)},
		{time: ft(2017, 10, 12), expected: ft(2018)},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.NextQuarterStart(test.time)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestPrevQuarterStart(t *testing.T) {
	// Test cases
	ft := testlib.YMDHMS
	var testCases = []struct {
		time     time.Time
		expected time.Time
	}{
		{time: ft(2017, 8, 26, 12, 29, 3), expected: ft(2017, 4)},
		{time: ft(2017), expected: ft(2016, 10)},
		{time: ft(2017, 12, 10), expected: ft(2017, 7)},
		{time: ft(2017, 10, 12), expected: ft(2017, 7)},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.PrevQuarterStart(test.time)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestYearStart(t *testing.T) {
	// Test cases
	ft := testlib.YMDHMS
	var testCases = []struct {
		time     time.Time
		expected time.Time
	}{
		{time: ft(2017, 8, 26, 12, 29, 3), expected: ft(2017)},
		{time: ft(2017), expected: ft(2017)},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.YearStart(test.time)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestNextYearStart(t *testing.T) {
	// Test cases
	ft := testlib.YMDHMS
	var testCases = []struct {
		time     time.Time
		expected time.Time
	}{
		{time: ft(2017, 8, 26, 12, 29, 3), expected: ft(2018)},
		{time: ft(2017), expected: ft(2018)},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.NextYearStart(test.time)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestPrevYearStart(t *testing.T) {
	// Test cases
	ft := testlib.YMDHMS
	var testCases = []struct {
		time     time.Time
		expected time.Time
	}{
		{time: ft(2017, 8, 26, 12, 29, 3), expected: ft(2016)},
		{time: ft(2017), expected: ft(2016)},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.PrevYearStart(test.time)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestAddNIntervals(t *testing.T) {
	// Test cases
	ft := testlib.YMDHMS
	var testCases = []struct {
		time     time.Time
		n        int
		prev     func(time.Time) time.Time
		next     func(time.Time) time.Time
		expected time.Time
	}{
		{
			time:     ft(2017, 1, 1, 13, 15),
			n:        3,
			next:     lib.NextHourStart,
			prev:     lib.PrevHourStart,
			expected: ft(2017, 1, 1, 16),
		},
		{
			time:     ft(2017, 1, 1, 13, 15),
			n:        -3,
			next:     lib.NextHourStart,
			prev:     lib.PrevHourStart,
			expected: ft(2017, 1, 1, 10),
		},
		{
			time:     ft(2017, 1, 1, 13, 15),
			n:        0,
			next:     lib.NextDayStart,
			prev:     lib.PrevQuarterStart,
			expected: ft(2017, 1, 1, 13, 15),
		},
		{
			time:     ft(2017, 9, 27),
			n:        -7,
			next:     lib.NextDayStart,
			prev:     lib.PrevDayStart,
			expected: ft(2017, 9, 20),
		},
	}
	// Execute test cases
	for index, test := range testCases {
		expected := test.expected
		got := lib.AddNIntervals(test.time, test.n, test.next, test.prev)
		if got != expected {
			t.Errorf(
				"test number %d, expected %v, got %v",
				index+1, expected, got,
			)
		}
	}
}

func TestGetIntervalFunctions(t *testing.T) {
	// Test cases
	var testCases = []struct {
		periodAbbr        string
		allowUnknown      bool
		expectedPeriod    string
		expectedN         int
		expectedStart     func(time.Time) time.Time
		expectedNextStart func(time.Time) time.Time
		expectedPrevStart func(time.Time) time.Time
	}{
		{
			allowUnknown:      false,
			periodAbbr:        "h",
			expectedPeriod:    "hour",
			expectedN:         1,
			expectedStart:     lib.HourStart,
			expectedNextStart: lib.NextHourStart,
			expectedPrevStart: lib.PrevHourStart,
		},
		{
			allowUnknown:      false,
			periodAbbr:        "d",
			expectedPeriod:    "day",
			expectedN:         1,
			expectedStart:     lib.DayStart,
			expectedNextStart: lib.NextDayStart,
			expectedPrevStart: lib.PrevDayStart,
		},
		{
			allowUnknown:      false,
			periodAbbr:        "w",
			expectedPeriod:    "week",
			expectedN:         1,
			expectedStart:     lib.WeekStart,
			expectedNextStart: lib.NextWeekStart,
			expectedPrevStart: lib.PrevWeekStart,
		},
		{
			allowUnknown:      false,
			periodAbbr:        "m",
			expectedPeriod:    "month",
			expectedN:         1,
			expectedStart:     lib.MonthStart,
			expectedNextStart: lib.NextMonthStart,
			expectedPrevStart: lib.PrevMonthStart,
		},
		{
			allowUnknown:      false,
			periodAbbr:        "q",
			expectedPeriod:    "quarter",
			expectedN:         1,
			expectedStart:     lib.QuarterStart,
			expectedNextStart: lib.NextQuarterStart,
			expectedPrevStart: lib.PrevQuarterStart,
		},
		{
			allowUnknown:      false,
			periodAbbr:        "y",
			expectedPeriod:    "year",
			expectedN:         1,
			expectedStart:     lib.YearStart,
			expectedNextStart: lib.NextYearStart,
			expectedPrevStart: lib.PrevYearStart,
		},
		{
			allowUnknown:      false,
			periodAbbr:        "y2",
			expectedPeriod:    "year",
			expectedN:         2,
			expectedStart:     lib.YearStart,
			expectedNextStart: lib.NextYearStart,
			expectedPrevStart: lib.PrevYearStart,
		},
		{
			allowUnknown:      false,
			periodAbbr:        "d7",
			expectedPeriod:    "day",
			expectedN:         7,
			expectedStart:     lib.DayStart,
			expectedNextStart: lib.NextDayStart,
			expectedPrevStart: lib.PrevDayStart,
		},
		{
			allowUnknown:      false,
			periodAbbr:        "q0",
			expectedPeriod:    "quarter",
			expectedN:         1,
			expectedStart:     lib.QuarterStart,
			expectedNextStart: lib.NextQuarterStart,
			expectedPrevStart: lib.PrevQuarterStart,
		},
		{
			allowUnknown:      false,
			periodAbbr:        "m-2",
			expectedPeriod:    "month",
			expectedN:         1,
			expectedStart:     lib.MonthStart,
			expectedNextStart: lib.NextMonthStart,
			expectedPrevStart: lib.PrevMonthStart,
		},
		{
			allowUnknown:      true,
			periodAbbr:        "a_0_1",
			expectedPeriod:    "",
			expectedN:         1,
			expectedStart:     nil,
			expectedNextStart: nil,
			expectedPrevStart: nil,
		},
		{
			allowUnknown:      true,
			periodAbbr:        "c_n",
			expectedPeriod:    "",
			expectedN:         1,
			expectedStart:     nil,
			expectedNextStart: nil,
			expectedPrevStart: nil,
		},
	}
	// Execute test cases
	for index, test := range testCases {
		gotPeriod, gotN, gotStart, gotNextStart, gotPrevStart := lib.GetIntervalFunctions(test.periodAbbr, test.allowUnknown)
		if gotPeriod != test.expectedPeriod {
			t.Errorf(
				"test number %d, expected period %v, got %v",
				index+1, test.expectedPeriod, gotPeriod,
			)
		}
		if gotN != test.expectedN {
			t.Errorf(
				"test number %d, expected n %v, got %v",
				index+1, test.expectedN, gotN,
			)
		}
		got := reflect.ValueOf(gotStart).Pointer()
		expected := reflect.ValueOf(test.expectedStart).Pointer()
		if got != expected {
			t.Errorf(
				"test number %d, expected start function %v, got %v",
				index+1, expected, got,
			)
		}
		got = reflect.ValueOf(gotNextStart).Pointer()
		expected = reflect.ValueOf(test.expectedNextStart).Pointer()
		if got != expected {
			t.Errorf(
				"test number %d, expected next function %+v, got %+v",
				index+1, expected, got,
			)
		}
		got = reflect.ValueOf(gotPrevStart).Pointer()
		expected = reflect.ValueOf(test.expectedPrevStart).Pointer()
		if got != expected {
			t.Errorf(
				"test number %d, expected prev function %+v, got %+v",
				index+1, expected, got,
			)
		}
	}
}
