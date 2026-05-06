package devstatscode

import (
	"fmt"
	"sort"
	"time"
)

// TSPoint keeps single time series point
type TSPoint struct {
	t      time.Time
	added  time.Time
	period string
	name   string
	tags   map[string]string
	fields map[string]interface{}
}

// TSPoints keeps batch of TSPoint values to write
type TSPoints []TSPoint

// Str - string pretty print
func (p *TSPoint) Str() string {
	return fmt.Sprintf(
		"%s %s %s period: %s tags: %+v fields: %+v",
		ToYMDHDate(p.t),
		ToYMDHDate(p.added),
		p.name,
		p.period,
		p.tags,
		p.fields,
	)
}

// Str - string pretty print
func (ps *TSPoints) Str() string {
	s := ""
	for i, p := range *ps {
		s += fmt.Sprintf("#%d %s\n", i+1, p.Str())
	}
	return s
}

// NewTSPoint returns new point as specified by args
func NewTSPoint(ctx *Ctx, name, period string, tags map[string]string, fields map[string]interface{}, t time.Time, exact bool) TSPoint {
	var (
		otags   map[string]string
		ofields map[string]interface{}
	)
	if tags != nil {
		otags = make(map[string]string)
		for k, v := range tags {
			otags[k] = v
		}
	}
	if fields != nil {
		ofields = make(map[string]interface{})
		for k, v := range fields {
			ofields[k] = v
		}
	}
	var pt time.Time
	if exact {
		pt = t
	} else {
		pt = HourStart(t)
	}
	p := TSPoint{
		t:      pt,
		added:  time.Now(),
		name:   name,
		period: period,
		tags:   otags,
		fields: ofields,
	}
	if ctx.Debug > 0 {
		Printf("NewTSPoint: %s\n", p.Str())
	}
	return p
}

// AddTSPoint add single point to the batch
func AddTSPoint(ctx *Ctx, pts *TSPoints, pt TSPoint) {
	if ctx.Debug > 0 {
		Printf("AddTSPoint: %s\n", pt.Str())
	}
	*pts = append(*pts, pt)
	if ctx.Debug > 0 {
		Printf("AddTSPoint: point added, now %d points\n", len(*pts))
	}
}

// MakeTSPointsUniqueTimes add microseconds to duplicate TS point times.
func MakeTSPointsUniqueTimes(ctx *Ctx, pts *TSPoints) {
	type key struct {
		t      time.Time
		name   string
		period string
	}
	keys := make(map[key][]int)
	for idx, pt := range *pts {
		k := key{t: pt.t, name: pt.name, period: pt.period}
		keys[k] = append(keys[k], idx)
	}
	for k, idxs := range keys {
		n := len(idxs)
		if n < 2 {
			continue
		}
		if n > 1000000 {
			Fatalf("MakeTSPointsUniqueTimes: too many points for %v %s %s: %d", k.t, k.name, k.period, n)
		}
		sort.SliceStable(idxs, func(i, j int) bool {
			return tsPointKey((*pts)[idxs[i]]) < tsPointKey((*pts)[idxs[j]])
		})
		for idx, ptIdx := range idxs {
			(*pts)[ptIdx].t = (*pts)[ptIdx].t.Add(time.Duration(idx) * time.Microsecond)
		}
		if ctx.Debug > 0 {
			Printf("MakeTSPointsUniqueTimes: adjusted %d points for %v %s %s\n", n, k.t, k.name, k.period)
		}
	}
}

func tsPointKey(pt TSPoint) (s string) {
	s = pt.name + "\000" + pt.period
	if pt.tags != nil {
		keys := make([]string, 0, len(pt.tags))
		for key := range pt.tags {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			s += "\000" + key + "=" + pt.tags[key]
		}
	}
	if pt.fields != nil {
		keys := make([]string, 0, len(pt.fields))
		for key := range pt.fields {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			s += "\000" + key + "=" + fmt.Sprintf("%v", pt.fields[key])
		}
	}
	return
}
