package devstatscode

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	gSeeded bool
)

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

// Probab - return true with percent % probablity
func Probab(percent int) bool {
	if !gSeeded {
		rand.Seed(time.Now().UnixNano())
		gSeeded = true
	}
	return rand.Intn(100) < percent
}

// ComputePeriodAtThisDate - for some longer periods, only recalculate them on specific dates/times
// see: time_test.go
func ComputePeriodAtThisDate(ctx *Ctx, period string, idt time.Time, hist bool) bool {
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
	dt := HourStart(idt)
	// dtc: date with current hour start
	// dtn: tomorrow with current hour start
	// dth: current data with tz offset
	dtc := dt
	dtn := dt.AddDate(0, 0, 1)
	dth := dt.Add(time.Hour * time.Duration(ctx.TmOffset))
	// h: current hour with tz offset
	// ch: current hour without tz offset
	h := dth.Hour()
	ch := dtc.Hour()
	periodStart := period[0:1]
	if periodStart == "h" {
		// hour(s)
		return true
	} else if periodStart == "d" {
		// day(s)
		if len(period) == 1 {
			return true
		}
		if ctx.RandComputeAtThisDate {
			return Probab(25)
		}
		return h == 1 || h == 6 || h == 9 || h == 13 || h == 18 || h == 21
	} else if hist && periodStart == "a" {
		// histograms between annotations or the final one "a_num_n"
		periodLen := len(period)
		periodEnd := period[periodLen-2:]
		if periodEnd == "_n" {
			if ctx.RandComputeAtThisDate {
				return Probab(25)
			}
			return h == 1 || h == 8 || h == 15 || h == 13 || h == 20
		}
		if ctx.RandComputeAtThisDate {
			return Probab(15)
		}
		return h == 2 || h == 3
	} else if hist && periodStart == "c" {
		// histograms between maturity level or the final sandbox/incubation/graduation - now "c_n", "c_g_n", "c_i_n"
		if ctx.RandComputeAtThisDate {
			periodLen := len(period)
			periodEnd := period[periodLen-2:]
			if periodEnd == "_n" {
				return Probab(25)
			}
			return Probab(15)
		}
		return h == 3 || h == 4
	}
	// others
	if hist {
		// other histograms
		if periodStart == "w" {
			// weekly histograms
			if ctx.RandComputeAtThisDate {
				return Probab(30)
			}
			return h%7 == 0
		} else if periodStart == "m" || periodStart == "q" || periodStart == "y" {
			// monthly histograms
			if ctx.RandComputeAtThisDate {
				return Probab(15)
			}
			return h == 23 || h == 18
		}
	} else {
		// other charts
		if periodStart == "w" {
			// weekly charts
			if ctx.RandComputeAtThisDate {
				wday := int(dtc.Weekday())
				return Probab(60) && h >= 12 && wday >= 0 && wday <= 2
			}
			return ch == 23 && int(dtc.Weekday()) == 0
		} else if periodStart == "m" {
			// monthly charts
			if ctx.RandComputeAtThisDate {
				dom := dtn.Day()
				return Probab(80) && h < 12 && dom >= 1 && dom <= 4
			}
			return ch == 23 && dtn.Day() == 1
		} else if periodStart == "q" {
			// quarterly charts
			if ctx.RandComputeAtThisDate {
				dom := dtn.Day()
				return h > 12 && dom >= 1 && dom <= 4 && dtn.Month()%3 == 1
			}
			return ch == 23 && dtn.Day() == 1 && dtn.Month()%3 == 1
		} else if periodStart == "y" {
			// yearly charts
			if ctx.RandComputeAtThisDate {
				dom := dtn.Day()
				return h < 12 && dom >= 1 && dom <= 4 && dtn.Month() == 1
			}
			return ch == 23 && dtn.Day() == 1 && dtn.Month() == 1
		}
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
	Printf("Error:\nCannot parse date: '%v'\n", dtStr)
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
