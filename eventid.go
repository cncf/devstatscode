package devstatscode

import (
	"strconv"
	"time"
)

// NativeIDBandRule - one generation of GitHub event id sequences: native events created at/after Since get the band
// of their type (Bands) or DefaultBand, see NativeIDBandRules
type NativeIDBandRule struct {
	Since       time.Time
	DefaultBand int64
	Bands       map[string]int64
}

// NativeIDBandRules - native event id banding rules, newest first: the first rule whose Since <= created_at decides
// the band (see NativeIDBandBase), events older than every rule keep their raw GitHub id (band 0).
// To handle another GitHub sequence reset (or an event type moving to another sequence) PREPEND a rule with a Since a
// few days in the future (every running image must carry the rule before it activates, otherwise old and new binaries
// store the same event under two ids) and with band numbers never used by any other rule (bands must stay < 281), then
// mirror it in rust/devstatscode/src/eventid.rs (the compat tests compare both).
// 2026-09-20 rule: band 1 = the issues/PRs/comments/reviews/stars/forks/releases/... sequence, band 2 = the git
// reference sequence (PushEvent, CreateEvent, DeleteEvent).
var NativeIDBandRules = []NativeIDBandRule{
	{
		Since:       time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC),
		DefaultBand: 1,
		Bands:       map[string]int64{"PushEvent": 2, "CreateEvent": 2, "DeleteEvent": 2},
	},
}

// NativeIDBand - band of a native event given its type and creation time, 0 = raw id
func NativeIDBand(eType string, createdAt time.Time) int64 {
	for _, rule := range NativeIDBandRules {
		if createdAt.Before(rule.Since) {
			continue
		}
		if band, ok := rule.Bands[eType]; ok {
			return band
		}
		return rule.DefaultBand
	}
	return 0
}

// NativeEventID - id stored in gha_events.id (and in every *.event_id) for a native GitHub event:
// raw GitHub id + band*NativeIDBandBase; ids outside (0, NativeIDBandBase) are not GitHub sequence ids and are
// returned unchanged
func NativeEventID(rawID int64, eType string, createdAt time.Time) int64 {
	if rawID <= 0 || rawID >= NativeIDBandBase {
		return rawID
	}
	return rawID + NativeIDBand(eType, createdAt)*NativeIDBandBase
}

// NativeEventIDString - NativeEventID for the JSON string id, a non-numeric id is returned unchanged
func NativeEventIDString(rawID, eType string, createdAt time.Time) string {
	raw, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		return rawID
	}
	return strconv.FormatInt(NativeEventID(raw, eType, createdAt), 10)
}

// SplitNativeEventID - (band, raw GitHub id) of a stored native event id, (0, id) for non-native ids
func SplitNativeEventID(id int64) (band, rawID int64) {
	if id <= 0 || id >= ArtificialIDBase {
		return 0, id
	}
	return id / NativeIDBandBase, id % NativeIDBandBase
}
