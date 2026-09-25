package devstatscode

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

var ()

// IntervalHours - return number of hour from for a given interval
func IntervalHours(period string) string {
	ary := strings.Split(period, " ")
	tokens := []string{}
	for _, token := range ary {
		if token != "" {
			tokens = append(tokens, token)
		}
	}
	if len(tokens) < 1 {
		return "0"
	}
	n := 1.0
	interval := tokens[0]
	if len(tokens) > 1 {
		var err error
		n, err = strconv.ParseFloat(tokens[0], 64)
		FatalOnError(err)
		if n < 0.0 {
			n = 0.0
		}
		interval = tokens[1]
	}
	mul := 1.0
	switch strings.ToLower(interval) {
	case "s", "sec", "second", "secs", "seconds":
		mul = 1.0 / 3600.0
	case "min", "minute", "mins", "minutes":
		mul = 1.0 / 60.0
	case "h", "hr", Hour, "hrs", "hours":
	case "d", Day, "days":
		mul = 24.0
	case "w", Week, "weeks":
		mul = 168.0
	case Month, "months":
		mul = 730.5
	case "q", Quarter, "quarters":
		mul = 2191.5
	case "y", Year, "years":
		mul = 8766.0
	default:
		Fatalf("unknown interval '%s'\n", interval)
	}
	return fmt.Sprintf("%f", n*mul)
}

// RangeHours - return number of hour from 'from' to 'to' as float64 converted to string
func RangeHours(from, to time.Time) string {
	if !to.After(from) {
		return "0"
	}
	return fmt.Sprintf("%f", to.Sub(from).Hours())
}

// GetDateAgo returns date: 'from' - 'n hours/days' etc.
func GetDateAgo(con *sql.DB, ctx *Ctx, from time.Time, ago string) (tm time.Time) {
	rows := QuerySQLWithErr(
		con,
		ctx,
		fmt.Sprintf(
			"select %s::timestamp - %s::interval",
			NValue(1),
			NValue(2),
		),
		ToYMDHMSDate(from),
		ago,
	)
	defer func() { FatalOnError(rows.Close()) }()
	for rows.Next() {
		FatalOnError(rows.Scan(&tm))
	}
	FatalOnError(rows.Err())
	return
}

// ProgressInfo display info about progress: i/n if current time >= last + period
// If displayed info, update last
func ProgressInfo(i, n int, start time.Time, last *time.Time, period time.Duration, msg string) {
	now := time.Now()
	if last.Add(period).Before(now) {
		perc := 0.0
		if n > 0 {
			perc = (float64(i) * 100.0) / float64(n)
		}
		eta := start
		if i > 0 && n > 0 {
			etaNs := float64(now.Sub(start).Nanoseconds()) * (float64(n) / float64(i))
			etaDuration := time.Duration(etaNs) * time.Nanosecond
			eta = start.Add(etaDuration)
			if msg != "" {
				Printf("%d/%d (%.3f%%), ETA: %v: %s\n", i, n, perc, eta, msg)
			} else {
				Printf("%d/%d (%.3f%%), ETA: %v\n", i, n, perc, eta)
			}
		} else {
			Printf("%s\n", msg)
		}
		*last = now
	}
}

// boundaryCrossed - true when 'from' and 'to' (both shifted by ctx.TmOffset hours) belong to
// different intervals, as defined by the interval start function (DayStart, WeekStart, ...)
func boundaryCrossed(ctx *Ctx, from, to time.Time, intervalStart func(time.Time) time.Time) bool {
	off := time.Hour * time.Duration(ctx.TmOffset)
	return intervalStart(from.Add(off)).Before(intervalStart(to.Add(off)))
}

// Lengths of a month, quarter and year in hours (as in IntervalHours)
const (
	monthHours   = 730.5
	quarterHours = 2191.5
	yearHours    = 8766.0
)

// PeriodClass - calendar period ("h", "d", "w", "m", "q", "y") deciding when 'period' is due (ComputePeriodAtThisDate)
// and since when a 'gha_computed' marker proves it was computed (PeriodStartAt)
// h*, d*, w*, m*, q*, y*: their first letter (multiples like 'd7' or 'y10' follow their base period)
// histogram quick ranges ending now (a_i_n, c_n, c_i_n, c_g_n): by their length from 'rangeStart' to 'to' (hour precision):
// d up to a month (730.5h), m up to a quarter (2191.5h), q up to a year (8766h), y when longer (d when 'rangeStart' is unknown)
// other histogram quick ranges (a_i_j, c_b, c_j_i, c_i_g, c_j_g - fully in the past): d, 'calc_metric' skips them once
// computed ('skip_past'), so they are only attempted once a day
// "" for anything else ('range:*', unknown)
func PeriodClass(period string, rangeStart, to time.Time) string {
	if period == "" {
		return ""
	}
	switch period[0:1] {
	case "h", "d", "w", "m", "q", "y":
		return period[0:1]
	case "a", "c":
		if !strings.HasSuffix(period, "_n") || rangeStart.IsZero() {
			return "d"
		}
		hours := HourStart(to).Sub(rangeStart).Hours()
		if hours <= monthHours {
			return "d"
		}
		if hours <= quarterHours {
			return "m"
		}
		if hours <= yearHours {
			return "q"
		}
		return "y"
	}
	return ""
}

// QuickRangeStarts - start dates of the histogram quick ranges by suffix, from the 'quick_ranges_data' tag values
// written by 'annotations' ('suffix;period;from;to', 'from' and 'to' are set only for annotation/CNCF date ranges)
func QuickRangeStarts(quickRangesData []string) map[string]time.Time {
	starts := make(map[string]time.Time)
	for _, data := range quickRangesData {
		ary := strings.Split(data, ";")
		if len(ary) == 4 && ary[1] == "" && ary[2] != "" {
			starts[ary[0]] = TimeParseAny(ary[2])
		}
	}
	return starts
}

// PeriodStartAt - when the period (see PeriodClass) containing 'dt' started: the calendar boundary found on the clock
// shifted by ctx.TmOffset hours, returned as an instant, 'rangeStart' is the start of a histogram quick range (zero otherwise)
// false for periods without a calendar period ('range:*', unknown)
// 'gha_computed' markers written by 'calc_metric' at or after this instant prove the period was computed since it started, see computed.go
func PeriodStartAt(ctx *Ctx, period string, rangeStart, dt time.Time) (time.Time, bool) {
	var periodStart func(time.Time) time.Time
	switch PeriodClass(period, rangeStart, dt) {
	case "h":
		periodStart = HourStart
	case "d":
		periodStart = DayStart
	case "w":
		periodStart = WeekStart
	case "m":
		periodStart = MonthStart
	case "q":
		periodStart = QuarterStart
	case "y":
		periodStart = YearStart
	default:
		return time.Time{}, false
	}
	off := time.Hour * time.Duration(ctx.TmOffset)
	return periodStart(dt.Add(off)).Add(-off), true
}

// PreviousPeriodStart - when the period (see PeriodClass) preceding the one containing 'dt' started
// the first sync after a period boundary computes the final point of the previous period together with the current one,
// when it did not succeed ('gha_computed' marker missing, see IsPeriodComputed) the recalculation must start there again
// false for periods without a calendar period ('range:*', unknown)
func PreviousPeriodStart(ctx *Ctx, period string, rangeStart, dt time.Time) (time.Time, bool) {
	start, ok := PeriodStartAt(ctx, period, rangeStart, dt)
	if !ok {
		return time.Time{}, false
	}
	return PeriodStartAt(ctx, PeriodClass(period, rangeStart, dt), time.Time{}, start.Add(-time.Second))
}

// DayBoundaryCrossed - true when the sync ending at 'to' is the first one after a day boundary,
// 'from' is where the previous sync ended (newest TSDB hour already computed)
// used to run tags/columns/annotations once per day regardless of the sync frequency
func DayBoundaryCrossed(ctx *Ctx, from, to time.Time) bool {
	return boundaryCrossed(ctx, from, to, DayStart)
}

// ComputePeriodAtThisDate - decides if a given period must be (re)calculated by the sync ending at 'to'
// when the previous sync ended at 'from' (newest TSDB hour already computed, ctx.DefaultStartDate when resetting)
// Rules are independent of the sync frequency: no time-of-day checks, no randomness (see PeriodClass)
// h: always
// d: first sync after a day boundary
// w: first sync after a week boundary (weeks start on Monday)
// m: first sync after a month boundary
// q: first sync after a quarter boundary
// y: first sync after a year boundary
// histogram quick ranges (a_*, c_*, 'rangeStart' is their start date) follow the class given by PeriodClass
// see: time_test.go
func ComputePeriodAtThisDate(ctx *Ctx, period string, rangeStart, from, to time.Time, hist bool) bool {
	if ctx.ComputeAll {
		return true
	}
	if ctx.ComputePeriods != nil {
		data, ok := ctx.ComputePeriods[period]
		if !ok {
			return false
		}
		_, ok = data[hist]
		return ok
	}
	class := PeriodClass(period, rangeStart, to)
	// Quick ranges are only defined for histograms
	if !hist && (strings.HasPrefix(period, "a") || strings.HasPrefix(period, "c")) {
		class = ""
	}
	switch class {
	case "h":
		return true
	case "d":
		return boundaryCrossed(ctx, from, to, DayStart)
	case "w":
		return boundaryCrossed(ctx, from, to, WeekStart)
	case "m":
		return boundaryCrossed(ctx, from, to, MonthStart)
	case "q":
		return boundaryCrossed(ctx, from, to, QuarterStart)
	case "y":
		return boundaryCrossed(ctx, from, to, YearStart)
	}
	Fatalf("ComputePeriodAtThisDate: unknown period: '%s', hist: %v", period, hist)
	return false
}

// HourStart - return time rounded to current hour start
func HourStart(dt time.Time) time.Time {
	return time.Date(
		dt.Year(),
		dt.Month(),
		dt.Day(),
		dt.Hour(),
		0,
		0,
		0,
		time.UTC,
	)
}

// NextHourStart - return time rounded to next hour start
func NextHourStart(dt time.Time) time.Time {
	return HourStart(dt).Add(time.Hour)
}

// PrevHourStart - return time rounded to prev hour start
func PrevHourStart(dt time.Time) time.Time {
	return HourStart(dt).Add(-time.Hour)
}

// DayStart - return time rounded to current day start
func DayStart(dt time.Time) time.Time {
	return time.Date(
		dt.Year(),
		dt.Month(),
		dt.Day(),
		0,
		0,
		0,
		0,
		time.UTC,
	)
}

// NextDayStart - return time rounded to next day start
func NextDayStart(dt time.Time) time.Time {
	return DayStart(dt).AddDate(0, 0, 1)
}

// PrevDayStart - return time rounded to prev day start
func PrevDayStart(dt time.Time) time.Time {
	return DayStart(dt).AddDate(0, 0, -1)
}

// WeekStart - return time rounded to current week start
// Assumes first week day is Sunday
func WeekStart(dt time.Time) time.Time {
	wDay := int(dt.Weekday())
	// Go returns negative numbers for `modulo` operation when argument is negative
	// So instead of wDay-1 I'm using wDay+6
	subDays := (wDay + 6) % 7
	return DayStart(dt).AddDate(0, 0, -subDays)
}

// NextWeekStart - return time rounded to next week start
func NextWeekStart(dt time.Time) time.Time {
	return WeekStart(dt).AddDate(0, 0, 7)
}

// PrevWeekStart - return time rounded to prev week start
func PrevWeekStart(dt time.Time) time.Time {
	return WeekStart(dt).AddDate(0, 0, -7)
}

// MonthStart - return time rounded to current month start
func MonthStart(dt time.Time) time.Time {
	return time.Date(
		dt.Year(),
		dt.Month(),
		1,
		0,
		0,
		0,
		0,
		time.UTC,
	)
}

// NextMonthStart - return time rounded to next month start
func NextMonthStart(dt time.Time) time.Time {
	return MonthStart(dt).AddDate(0, 1, 0)
}

// PrevMonthStart - return time rounded to prev month start
func PrevMonthStart(dt time.Time) time.Time {
	return MonthStart(dt).AddDate(0, -1, 0)
}

// QuarterStart - return time rounded to current month start
func QuarterStart(dt time.Time) time.Time {
	month := ((dt.Month()-1)/3)*3 + 1
	return time.Date(
		dt.Year(),
		month,
		1,
		0,
		0,
		0,
		0,
		time.UTC,
	)
}

// NextQuarterStart - return time rounded to next quarter start
func NextQuarterStart(dt time.Time) time.Time {
	return QuarterStart(dt).AddDate(0, 3, 0)
}

// PrevQuarterStart - return time rounded to prev quarter start
func PrevQuarterStart(dt time.Time) time.Time {
	return QuarterStart(dt).AddDate(0, -3, 0)
}

// YearStart - return time rounded to current month start
func YearStart(dt time.Time) time.Time {
	return time.Date(
		dt.Year(),
		1,
		1,
		0,
		0,
		0,
		0,
		time.UTC,
	)
}

// NextYearStart - return time rounded to next year start
func NextYearStart(dt time.Time) time.Time {
	return YearStart(dt).AddDate(1, 0, 0)
}

// PrevYearStart - return time rounded to prev year start
func PrevYearStart(dt time.Time) time.Time {
	return YearStart(dt).AddDate(-1, 0, 0)
}

// PeriodParse - tries to parse period
func PeriodParse(perStr string) (dur time.Duration, ok bool) {
	idx := strings.Index(perStr, "[rate reset in ")
	if idx == -1 {
		return
	}
	rateStr := ""
	_, err := fmt.Sscanf(perStr[idx:], "[rate reset in %s", &rateStr)
	if err != nil || len(rateStr) < 2 {
		return
	}
	rateStr = rateStr[0 : len(rateStr)-1]
	if rateStr == "" {
		return
	}
	d, err := time.ParseDuration(rateStr)
	if err != nil {
		return
	}
	dur = d
	ok = true
	return
}

// TimeParseAny - attempts to parse time from string YYYY-MM-DD HH:MI:SS
// Skipping parts from right until only YYYY id left
func TimeParseAny(dtStr string) time.Time {
	formats := []string{
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02 15",
		"2006-01-02",
		"2006-01",
		"2006",
	}
	for _, format := range formats {
		t, e := time.Parse(format, dtStr)
		if e == nil {
			return t
		}
	}
	// Printf initializes the logger (which calls ctx.Init() -> TimeParseAny for GHA2DB_STARTDT):
	// calling it while that initialization is in progress would deadlock on sync.Once.
	if IsLogInitialized() {
		Printf("Error:\nCannot parse date: '%v'\n", dtStr)
	}
	fmt.Fprintf(os.Stdout, "Error:\nCannot parse date: '%v'\n", dtStr)
	os.Exit(1)
	return time.Now()
}

// ToGHADate - return time formatted as YYYY-MM-DD-H
func ToGHADate(dt time.Time) string {
	return fmt.Sprintf("%04d-%02d-%02d-%d", dt.Year(), dt.Month(), dt.Day(), dt.Hour())
}

// ToYMDDate - return time formatted as YYYY-MM-DD
func ToYMDDate(dt time.Time) string {
	return fmt.Sprintf("%04d-%02d-%02d", dt.Year(), dt.Month(), dt.Day())
}

// ToYMDHMSDate - return time formatted as YYYY-MM-DD HH:MI:SS
func ToYMDHMSDate(dt time.Time) string {
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", dt.Year(), dt.Month(), dt.Day(), dt.Hour(), dt.Minute(), dt.Second())
}

// ToYMDHDate - return time formatted as YYYY-MM-DD HH
func ToYMDHDate(dt time.Time) string {
	return fmt.Sprintf("%04d-%02d-%02d %d", dt.Year(), dt.Month(), dt.Day(), dt.Hour())
}

// DescriblePeriodInHours - return string description of a time period given in hours
func DescriblePeriodInHours(hrs float64) (desc string) {
	secs := int((hrs * 3600.0) + 0.5)
	if secs < 0 {
		return "- " + DescriblePeriodInHours(-hrs)
	}
	if secs == 0 {
		return "zero"
	}
	weeks := secs / 604800
	if weeks > 0 {
		if weeks > 1 {
			desc += strconv.Itoa(weeks) + " weeks "
		} else {
			desc += "1 week "
		}
		secs -= weeks * 604800
	}
	days := secs / 86400
	if days > 0 {
		if days > 1 {
			desc += strconv.Itoa(days) + " days "
		} else {
			desc += "1 day "
		}
		secs -= days * 86400
	}
	hours := secs / 3600
	if hours > 0 {
		if hours > 1 {
			desc += strconv.Itoa(hours) + " hours "
		} else {
			desc += "1 hour "
		}
		secs -= hours * 3600
	}
	minutes := secs / 60
	if minutes > 0 {
		if minutes > 1 {
			desc += strconv.Itoa(minutes) + " minutes "
		} else {
			desc += "1 minute "
		}
		secs -= minutes * 60
	}
	if secs > 0 {
		if secs > 1 {
			desc += strconv.Itoa(secs) + " seconds "
		} else {
			desc += "1 second "
		}
	}

	return strings.TrimSpace(desc)
}

// AddNIntervals adds (using nextIntervalStart) or subtracts (using prevIntervalStart) N itervals to the given date
// Functions Next/Prev can use Hour, Day, Week, Month, Quarter, Year functions (defined in this module) or other custom defined functions
// With `func(time.Time) time.Time` signature
func AddNIntervals(dt time.Time, n int, nextIntervalStart, prevIntervalStart func(time.Time) time.Time) time.Time {
	if n == 0 {
		return dt
	}
	times := n
	fun := nextIntervalStart
	if n < 0 {
		times = -n
		fun = prevIntervalStart
	}
	for i := 0; i < times; i++ {
		dt = fun(dt)
	}
	return dt
}

// GetIntervalFunctions - return interval name, interval number, interval start, next, prev function from interval abbr: h|d2|w3|m4|q|y
// w3 = 3 weeks, q2 = 2 quarters, y = year (1), d7 = 7 days (not the same as w), m3 = 3 months (not the same as q)
func GetIntervalFunctions(intervalAbbr string, allowUnknown bool) (interval string, n int, intervalStart, nextIntervalStart, prevIntervalStart func(time.Time) time.Time) {
	n = 1
	switch strings.ToLower(intervalAbbr[0:1]) {
	case "h":
		interval = Hour
		intervalStart = HourStart
		nextIntervalStart = NextHourStart
		prevIntervalStart = PrevHourStart
	case "d":
		interval = Day
		intervalStart = DayStart
		nextIntervalStart = NextDayStart
		prevIntervalStart = PrevDayStart
	case "w":
		interval = Week
		intervalStart = WeekStart
		nextIntervalStart = NextWeekStart
		prevIntervalStart = PrevWeekStart
	case "m":
		interval = Month
		intervalStart = MonthStart
		nextIntervalStart = NextMonthStart
		prevIntervalStart = PrevMonthStart
	case "q":
		interval = Quarter
		intervalStart = QuarterStart
		nextIntervalStart = NextQuarterStart
		prevIntervalStart = PrevQuarterStart
	case "y":
		interval = Year
		intervalStart = YearStart
		nextIntervalStart = NextYearStart
		prevIntervalStart = PrevYearStart
	default:
		if !allowUnknown {
			Printf("Error:\nUnknown interval '%v'\n", intervalAbbr)
			fmt.Fprintf(os.Stdout, "Error:\nUnknown interval '%v'\n", intervalAbbr)
			os.Exit(1)
		} else {
			return
		}
	}
	lenIntervalAbbr := len(intervalAbbr)
	if lenIntervalAbbr > 1 {
		nStr := intervalAbbr[1:lenIntervalAbbr]
		nI, err := strconv.Atoi(nStr)
		FatalOnError(err)
		if nI > 1 {
			n = nI
		}
	}
	return
}
