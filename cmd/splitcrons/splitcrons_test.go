package main

import (
	"reflect"
	"testing"
)

func TestSyncCronRoundTrip(t *testing.T) {
	for _, c := range []struct{ ghaOffset, syncHours int }{{4, 6}, {4, 1}, {2, 3}, {10, 6}, {10, 2}} {
		almostHour := 60 - c.ghaOffset
		space := c.syncHours * almostHour
		seen := map[string]bool{}
		for pos := 0; pos < space; pos++ {
			cron := syncCronOf(pos, c.ghaOffset, c.syncHours, almostHour)
			if seen[cron] {
				t.Fatalf("%+v: duplicate cron %q", c, cron)
			}
			seen[cron] = true
			got, ok := syncCronPos(cron, c.ghaOffset, c.syncHours, almostHour)
			if !ok || got != pos {
				t.Fatalf("%+v: pos %d -> %q -> (%d, %v)", c, pos, cron, got, ok)
			}
		}
	}
	if got := syncCronOf(7, 4, 6, 56); got != "11 0,6,12,18 * * *" {
		t.Fatalf("syncCronOf(7) = %q", got)
	}
	if got := syncCronOf(56*3+5, 4, 6, 56); got != "9 3,9,15,21 * * *" {
		t.Fatalf("syncCronOf(173) = %q", got)
	}
	for _, cron := range []string{
		"", "4 3 * * *", "04 0,6,12,18 * * *", "3 0,6,12,18 * * *", "60 0,6,12,18 * * *", "4 6,12,18,0 * * *",
		"4 0,6,12 * * *", "4 0,6,12,18 * *", "4 0,6,12,18 * * * *", "4 0,6,12,18 1 * *", "x 0,6,12,18 * * *",
		"4 0 ,6,12,18 * * *", "4 0,6,12,18 * * 1", "4 3 * * 1",
	} {
		if pos, ok := syncCronPos(cron, 4, 6, 56); ok {
			t.Fatalf("%q should be invalid, got pos %d", cron, pos)
		}
	}
	// hourly syncs: every hour listed
	if pos, ok := syncCronPos("4 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23 * * *", 4, 1, 56); !ok || pos != 0 {
		t.Fatalf("hourly cron: (%d, %v)", pos, ok)
	}
}

func TestDailyCronRoundTrip(t *testing.T) {
	for _, c := range []struct{ ghaOffset, dailyStartHour int }{{4, 3}, {4, 2}, {2, 23}, {10, 0}} {
		almostHour := 60 - c.ghaOffset
		space := (24 - c.dailyStartHour) * almostHour
		for pos := 0; pos < space; pos++ {
			cron := dailyCronOf(pos, c.ghaOffset, c.dailyStartHour, almostHour)
			got, ok := dailyCronPos(cron, c.ghaOffset, c.dailyStartHour, almostHour, space)
			if !ok || got != pos {
				t.Fatalf("%+v: pos %d -> %q -> (%d, %v)", c, pos, cron, got, ok)
			}
		}
	}
	if got := dailyCronOf(56*6+48, 4, 3, 56); got != "52 9 * * *" {
		t.Fatalf("dailyCronOf = %q", got)
	}
	for _, cron := range []string{"", "4 2 * * *", "4 24 * * *", "3 3 * * *", "4 0,6,12,18 * * *", "4 3 1 * *", "4 3 * * 1", "04 3 * * *"} {
		if pos, ok := dailyCronPos(cron, 4, 3, 56, 21*56); ok {
			t.Fatalf("%q should be invalid, got pos %d", cron, pos)
		}
	}
}

func TestAffsCronRoundTrip(t *testing.T) {
	for _, monthly := range []bool{false, true} {
		periodDays := 7
		if monthly {
			periodDays = 28
		}
		for tm := 0; tm < periodDays*24*60; tm += 7 {
			cron := affsCronOf(tm, monthly)
			got, ok := affsCronTime(cron, monthly, periodDays)
			if !ok || got != tm {
				t.Fatalf("monthly=%v: t %d -> %q -> (%d, %v)", monthly, tm, cron, got, ok)
			}
		}
	}
	if got := affsCronOf(24*60+9*60+11, true); got != "11 9 2 * *" {
		t.Fatalf("affsCronOf monthly = %q", got)
	}
	if got := affsCronOf(3*24*60+18*60+15, false); got != "15 18 * * 3" {
		t.Fatalf("affsCronOf weekly = %q", got)
	}
	// wrong mode, out of period, malformed
	for _, cron := range []string{"", "15 18 * * 3", "4 11 0 * *", "4 11 29 * *", "60 11 1 * *", "4 24 1 * *", "4 11 1 * * *", "04 11 1 * *", "4 11 1 1 *", "a 11 1 * *"} {
		if tm, ok := affsCronTime(cron, true, 28); ok {
			t.Fatalf("monthly: %q should be invalid, got %d", cron, tm)
		}
	}
	for _, cron := range []string{"", "4 11 1 * *", "15 18 * * 7", "15 18 * * -1", "15 18 1 * 3", "15 18 * 1 3"} {
		if tm, ok := affsCronTime(cron, false, 7); ok {
			t.Fatalf("weekly: %q should be invalid, got %d", cron, tm)
		}
	}
	// pos space conversion: minute offset relative to GHA_OFFSET, clamped at 0 for earlier minutes
	if got := affsTimeToPos(24*60+9*60+11, 4, 56); got != 24*56+9*56+7 {
		t.Fatalf("affsTimeToPos = %d", got)
	}
	if got := affsTimeToPos(9*60+2, 4, 56); got != 9*56 {
		t.Fatalf("affsTimeToPos clamp = %d", got)
	}
}

func TestWeightedPositions(t *testing.T) {
	list := []weightedEntry{{idx: 0, weight: 1}, {idx: 1, weight: 1}, {idx: 2, weight: 2}}
	got := weightedPositions(list, 100)
	if want := map[int]int{0: 0, 1: 25, 2: 50}; !reflect.DeepEqual(got, want) {
		t.Fatalf("weightedPositions = %v", got)
	}
	// collisions bump forward, zero total gives nothing
	got = weightedPositions([]weightedEntry{{idx: 0, weight: 1}, {idx: 1, weight: 1e-9}, {idx: 2, weight: 1e-9}}, 10)
	if want := map[int]int{0: 0, 1: 9, 2: 0 + 1}; !reflect.DeepEqual(got, want) {
		// 1e-9 shares round to 9 (clamped to space-1) then bump on the collision with idx 1
		t.Fatalf("weightedPositions bump = %v, want %v", got, want)
	}
	if got := weightedPositions([]weightedEntry{{idx: 0, weight: 0}}, 10); len(got) != 0 {
		t.Fatalf("zero weights: %v", got)
	}
}

func TestPlaceIntoGaps(t *testing.T) {
	// one kept entry: the whole space is its gap, split proportionally (owner keeps the start)
	got := placeIntoGaps([]posEntry{{idx: 0, pos: 10, weight: 3}}, []weightedEntry{{idx: 5, weight: 1}}, 100)
	if want := map[int]int{5: 85}; !reflect.DeepEqual(got, want) {
		t.Fatalf("single kept = %v", got)
	}
	// the gap with the most time per weight unit wins: gap 30 owned by weight 100 (score 30/101)
	// vs gap 10 owned by weight 1 (score 10/2 = 5) -> the small owner's gap
	kept := []posEntry{{idx: 0, pos: 0, weight: 100}, {idx: 1, pos: 30, weight: 1}, {idx: 2, pos: 40, weight: 100}}
	got = placeIntoGaps(kept, []weightedEntry{{idx: 9, weight: 1}}, 100)
	if want := map[int]int{9: 35}; !reflect.DeepEqual(got, want) {
		t.Fatalf("best gap = %v", got)
	}
	// wrap-around gap (last -> first) and heaviest-first ordering (idx 8 is placed before idx 7)
	kept = []posEntry{{idx: 0, pos: 10, weight: 1}, {idx: 1, pos: 20, weight: 1}}
	got = placeIntoGaps(kept, []weightedEntry{{idx: 7, weight: 1}, {idx: 8, weight: 3}}, 100)
	// idx 8 first: gap 90 (20 -> 10 wrapped) owned by idx 1: off = 90*1/4 = 22 -> pos 42;
	// idx 7 next: gaps 10 (idx 0), 22 (idx 1, score 11), 68 (idx 8, score 68/4 = 17) -> off = 68*3/4 = 51 -> 93
	if want := map[int]int{8: 42, 7: 93}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wrap/order = %v", got)
	}
	// equal weights: lower index first; proportional offset never below 1 or above gap-1, bump on taken positions
	kept = []posEntry{{idx: 0, pos: 0, weight: 1}, {idx: 1, pos: 2, weight: 1}, {idx: 2, pos: 4, weight: 1e9}}
	got = placeIntoGaps(kept, []weightedEntry{{idx: 4, weight: 1}, {idx: 3, weight: 1}}, 6)
	// idx 3: gaps: 2 (idx 0, score 1), 2 (idx 1, score 1), 2 (idx 2 -> 0 wrapped, score ~0) -> first max: owner idx 0, off 1 -> pos 1
	// idx 4: gaps: 1 (idx 0), 1 (idx 3), 2 (idx 1, score 1), 2 (idx 2) -> owner idx 1, off 1 -> pos 3
	if want := map[int]int{3: 1, 4: 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ties/bump = %v", got)
	}
	// a huge owner pushes the new entry to the end of its gap (never onto the next kept position);
	// equal gaps: the first one (lowest position) wins
	kept = []posEntry{{idx: 0, pos: 0, weight: 1e12}, {idx: 1, pos: 50, weight: 1e12}}
	got = placeIntoGaps(kept, []weightedEntry{{idx: 2, weight: 1}}, 100)
	if want := map[int]int{2: 49}; !reflect.DeepEqual(got, want) {
		t.Fatalf("huge owner = %v", got)
	}
	// the caller's kept slice is left untouched (no aliasing through sort/append)
	if kept[0].idx != 0 || kept[1].idx != 1 || len(kept) != 2 {
		t.Fatalf("kept modified: %v", kept)
	}
	// zero weights never divide by zero
	got = placeIntoGaps([]posEntry{{idx: 0, pos: 0, weight: 0}}, []weightedEntry{{idx: 1, weight: 0}}, 10)
	if want := map[int]int{1: 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("zero weights = %v", got)
	}
	// nothing kept or no space: nothing placed
	if got := placeIntoGaps(nil, []weightedEntry{{idx: 1, weight: 1}}, 10); len(got) != 0 {
		t.Fatalf("no kept = %v", got)
	}
	if got := placeIntoGaps([]posEntry{{idx: 0, pos: 0, weight: 1}}, []weightedEntry{{idx: 1, weight: 1}}, 0); len(got) != 0 {
		t.Fatalf("no space = %v", got)
	}
}

func TestPreservedPositions(t *testing.T) {
	gPlaceProjs = map[string]bool{"forced": true}
	defer func() { gPlaceProjs = nil }()
	crons := map[int]string{0: "4 0,6,12,18 * * *", 1: "9 0,6,12,18 * * *", 2: "9 0,6,12,18 * * *", 3: "", 4: "8 * * * *", 5: "30 3,9,15,21 * * *", 6: "40 3,9,15,21 * * *"}
	list := []weightedEntry{
		{idx: 0, proj: "a", weight: 100}, {idx: 1, proj: "b", weight: 10}, {idx: 2, proj: "c", weight: 10},
		{idx: 3, proj: "d", weight: 1}, {idx: 4, proj: "e", weight: 1}, {idx: 5, proj: "forced", weight: 1},
		{idx: 6, proj: "g", weight: 50},
	}
	cronOf := func(e weightedEntry) string { return crons[e.idx] }
	parse := func(cron string) (int, bool) { return syncCronPos(cron, 4, 6, 56) }
	pos, reasons := preservedPositions(list, 336, cronOf, parse)
	wantReasons := map[int]string{
		2: "'9 0,6,12,18 * * *' collides with b",
		3: "new",
		4: "'8 * * * *' invalid for this mode",
		5: "PLACE",
	}
	if !reflect.DeepEqual(reasons, wantReasons) {
		t.Fatalf("reasons = %v", reasons)
	}
	// kept ones stay exactly where their crons are
	if pos[0] != 0 || pos[1] != 5 || pos[6] != 3*56+36 {
		t.Fatalf("kept positions = %v", pos)
	}
	// every entry got a distinct position inside the space
	seen := map[int]bool{}
	for _, e := range list {
		p, ok := pos[e.idx]
		if !ok || p < 0 || p >= 336 || seen[p] {
			t.Fatalf("bad/duplicate position for %s: %d (%v)", e.proj, p, ok)
		}
		seen[p] = true
	}
	// nothing kept (all crons empty): full weighted split, every entry reported as placed
	empty := func(weightedEntry) string { return "" }
	pos, reasons = preservedPositions(list[:3], 336, empty, parse)
	if want := weightedPositions(list[:3], 336); !reflect.DeepEqual(pos, want) {
		t.Fatalf("fallback positions = %v, want %v", pos, want)
	}
	if len(reasons) != 3 || reasons[0] != "new" {
		t.Fatalf("fallback reasons = %v", reasons)
	}
	// no entries at all
	pos, reasons = preservedPositions(nil, 336, empty, parse)
	if len(pos) != 0 || len(reasons) != 0 {
		t.Fatalf("empty list: %v %v", pos, reasons)
	}
}
