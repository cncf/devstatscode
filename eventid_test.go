package devstatscode

import (
	"testing"
	"time"

	lib "github.com/cncf/devstatscode"
)

// epoch of the newest (first) banding rule
func newestEpoch() time.Time {
	return lib.NativeIDBandRules[0].Since
}

func TestNativeIDBandRulesInvariants(t *testing.T) {
	rules := lib.NativeIDBandRules
	if len(rules) == 0 {
		t.Fatal("no NativeIDBandRules")
	}
	if lib.NativeIDBandBase != 1000000000000 {
		t.Errorf("NativeIDBandBase = %d, want 10^12", lib.NativeIDBandBase)
	}
	maxBand := lib.ArtificialIDBase / lib.NativeIDBandBase
	seen := map[int64]int{}
	for i, rule := range rules {
		if rule.Since.IsZero() {
			t.Errorf("rule %d: zero Since", i)
		}
		if rule.Since.Location() != time.UTC {
			t.Errorf("rule %d: Since is not UTC: %v", i, rule.Since)
		}
		// newest first, strictly
		if i > 0 && !rule.Since.Before(rules[i-1].Since) {
			t.Errorf("rule %d: Since %v is not before rule %d Since %v", i, rule.Since, i-1, rules[i-1].Since)
		}
		bands := []int64{rule.DefaultBand}
		for _, b := range rule.Bands {
			bands = append(bands, b)
		}
		for _, b := range bands {
			if b <= 0 {
				t.Errorf("rule %d: band %d is not positive (0 is the raw id)", i, b)
			}
			if b >= maxBand {
				t.Errorf("rule %d: band %d does not fit below ArtificialIDBase (max %d)", i, b, maxBand-1)
			}
		}
		// every band belongs to exactly one rule
		ruleBands := map[int64]bool{}
		for _, b := range bands {
			ruleBands[b] = true
		}
		for b := range ruleBands {
			if prev, ok := seen[b]; ok {
				t.Errorf("band %d used by rules %d and %d: bands must never be reused", b, prev, i)
			}
			seen[b] = i
		}
	}
	// the git reference sequence types are banded apart from the default band
	newest := rules[0]
	for _, typ := range []string{"PushEvent", "CreateEvent", "DeleteEvent"} {
		b, ok := newest.Bands[typ]
		if !ok {
			t.Errorf("newest rule: %s has no explicit band", typ)
		} else if b == newest.DefaultBand {
			t.Errorf("newest rule: %s shares the default band %d", typ, b)
		}
	}
}

func TestNativeIDBand(t *testing.T) {
	epoch := newestEpoch()
	sec := time.Second
	cases := []struct {
		name string
		typ  string
		at   time.Time
		band int64
	}{
		{"issue before epoch", "IssuesEvent", epoch.Add(-sec), 0},
		{"push before epoch", "PushEvent", epoch.Add(-sec), 0},
		{"issue 2015", "IssuesEvent", time.Date(2015, 1, 1, 15, 0, 0, 0, time.UTC), 0},
		{"push 2025", "PushEvent", time.Date(2025, 11, 20, 12, 0, 0, 0, time.UTC), 0},
		{"issue at epoch", "IssuesEvent", epoch, 1},
		{"issue after epoch", "IssuesEvent", epoch.Add(sec), 1},
		{"issue comment", "IssueCommentEvent", epoch.Add(sec), 1},
		{"pull request", "PullRequestEvent", epoch.Add(sec), 1},
		{"pr review", "PullRequestReviewEvent", epoch.Add(sec), 1},
		{"pr review comment", "PullRequestReviewCommentEvent", epoch.Add(sec), 1},
		{"commit comment", "CommitCommentEvent", epoch.Add(sec), 1},
		{"watch", "WatchEvent", epoch.Add(sec), 1},
		{"fork", "ForkEvent", epoch.Add(sec), 1},
		{"release", "ReleaseEvent", epoch.Add(sec), 1},
		{"member", "MemberEvent", epoch.Add(sec), 1},
		{"public", "PublicEvent", epoch.Add(sec), 1},
		{"gollum", "GollumEvent", epoch.Add(sec), 1},
		{"team add", "TeamAddEvent", epoch.Add(sec), 1},
		{"pr review thread", "PullRequestReviewThreadEvent", epoch.Add(sec), 1},
		{"sponsorship", "SponsorshipEvent", epoch.Add(sec), 1},
		{"discussion comment", "DiscussionCommentEvent", epoch.Add(sec), 1},
		{"unknown type", "SomethingNewEvent", epoch.Add(sec), 1},
		{"empty type", "", epoch.Add(sec), 1},
		{"push at epoch", "PushEvent", epoch, 2},
		{"create", "CreateEvent", epoch.Add(sec), 2},
		{"delete", "DeleteEvent", epoch.Add(sec), 2},
		{"push far future", "PushEvent", time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC), 2},
		{"issue far future", "IssuesEvent", time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC), 1},
		// instants are compared: the same wall clock in another zone
		{"epoch in +02:00", "IssuesEvent", epoch.In(time.FixedZone("CEST", 2*3600)), 1},
		{"second before epoch in -08:00", "PushEvent", epoch.Add(-sec).In(time.FixedZone("PST", -8*3600)), 0},
		{"zero time", "IssuesEvent", time.Time{}, 0},
	}
	for _, c := range cases {
		if got := lib.NativeIDBand(c.typ, c.at); got != c.band {
			t.Errorf("%s: NativeIDBand(%q, %v) = %d, want %d", c.name, c.typ, c.at, got, c.band)
		}
	}
}

func TestNativeEventID(t *testing.T) {
	epoch := newestEpoch()
	before := epoch.Add(-time.Second)
	after := epoch.Add(time.Hour)
	base := lib.NativeIDBandBase
	cases := []struct {
		name string
		raw  int64
		typ  string
		at   time.Time
		id   int64
	}{
		{"before epoch raw", 15167597171, "IssuesEvent", before, 15167597171},
		{"before epoch push raw", 21500000000, "PushEvent", before, 21500000000},
		{"after epoch issue", 15167597171, "IssuesEvent", after, base + 15167597171},
		{"after epoch push", 21500000000, "PushEvent", after, 2*base + 21500000000},
		{"after epoch create", 21500000001, "CreateEvent", after, 2*base + 21500000001},
		{"after epoch delete", 21500000002, "DeleteEvent", after, 2*base + 21500000002},
		{"after epoch watch", 1, "WatchEvent", after, base + 1},
		{"after epoch unknown", 7, "FooEvent", after, base + 7},
		{"largest bandable raw", base - 1, "IssuesEvent", after, 2*base - 1},
		// not GitHub sequence ids: unchanged
		{"zero", 0, "IssuesEvent", after, 0},
		{"negative (old format hash)", -1234567890123, "IssuesEvent", after, -1234567890123},
		{"already banded / too big", base + 5, "IssuesEvent", after, base + 5},
		{"artificial", lib.ArtificialIDBase + 5, "IssuesEvent", after, lib.ArtificialIDBase + 5},
	}
	for _, c := range cases {
		if got := lib.NativeEventID(c.raw, c.typ, c.at); got != c.id {
			t.Errorf("%s: NativeEventID(%d, %q, %v) = %d, want %d", c.name, c.raw, c.typ, c.at, got, c.id)
		}
	}
}

func TestNativeEventIDString(t *testing.T) {
	epoch := newestEpoch()
	before := epoch.Add(-time.Second)
	after := epoch.Add(time.Hour)
	cases := []struct {
		name string
		raw  string
		typ  string
		at   time.Time
		id   string
	}{
		{"before epoch", "15167597171", "IssuesEvent", before, "15167597171"},
		{"after epoch issue", "15167597171", "IssuesEvent", after, "1015167597171"},
		{"after epoch push", "21500000000", "PushEvent", after, "2021500000000"},
		{"after epoch create", "1", "CreateEvent", after, "2000000000001"},
		{"leading zeros normalised", "007", "WatchEvent", after, "1000000000007"},
		{"plus sign accepted by ParseInt", "+7", "WatchEvent", after, "1000000000007"},
		{"empty", "", "WatchEvent", after, ""},
		{"non numeric", "abc", "WatchEvent", after, "abc"},
		{"float", "1.5", "WatchEvent", after, "1.5"},
		{"negative", "-5", "WatchEvent", after, "-5"},
		{"zero", "0", "WatchEvent", after, "0"},
		{"overflow", "99999999999999999999", "WatchEvent", after, "99999999999999999999"},
	}
	for _, c := range cases {
		if got := lib.NativeEventIDString(c.raw, c.typ, c.at); got != c.id {
			t.Errorf("%s: NativeEventIDString(%q, %q, %v) = %q, want %q", c.name, c.raw, c.typ, c.at, got, c.id)
		}
	}
}

func TestSplitNativeEventID(t *testing.T) {
	base := lib.NativeIDBandBase
	cases := []struct {
		id   int64
		band int64
		raw  int64
	}{
		{15167597171, 0, 15167597171},
		{base + 15167597171, 1, 15167597171},
		{2*base + 21500000000, 2, 21500000000},
		{base, 1, 0},
		{2*base - 1, 1, base - 1},
		{280 * base, 280, 0},
		{0, 0, 0},
		{-1234567890123, 0, -1234567890123},
		{lib.ArtificialIDBase, 0, lib.ArtificialIDBase},
		{lib.ArtificialIDBase + 4000000000000, 0, lib.ArtificialIDBase + 4000000000000},
	}
	for _, c := range cases {
		band, raw := lib.SplitNativeEventID(c.id)
		if band != c.band || raw != c.raw {
			t.Errorf("SplitNativeEventID(%d) = (%d, %d), want (%d, %d)", c.id, band, raw, c.band, c.raw)
		}
	}
	// round trip through NativeEventID for every band in use
	after := newestEpoch().Add(time.Hour)
	for _, typ := range []string{"IssuesEvent", "PushEvent", "CreateEvent", "DeleteEvent", "WatchEvent", "XEvent"} {
		raw := int64(21500000000)
		id := lib.NativeEventID(raw, typ, after)
		band, r := lib.SplitNativeEventID(id)
		if r != raw || band != lib.NativeIDBand(typ, after) || band == 0 {
			t.Errorf("%s: round trip (%d -> %d -> %d, %d) failed", typ, raw, id, band, r)
		}
		if id >= lib.ArtificialIDBase || id <= 0 {
			t.Errorf("%s: banded id %d is outside the native class", typ, id)
		}
	}
}
