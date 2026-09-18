// eventidprobe — Go reference for the Rust `devstatscode::eventid` parity test.
//
// Reads one vector per stdin line: `<RFC 3339 created_at>\t<event type>\t<raw id>`
// (the raw id is passed through NativeEventIDString, so it may be any text) and
// prints, per vector, one line:
//
//	<band> | <NativeEventIDString> | <band, raw of SplitNativeEventID(id)>   (split only for a numeric id)
//
// followed by one `RULES` line describing NativeIDBandRules (`since=<RFC 3339>
// default=<band> bands=<type:band,...>` per rule, types sorted, rules separated
// by ` ; `) so the Rust list can be compared field by field.
package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	lib "github.com/cncf/devstatscode"
)

func main() {
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 3)
		if len(parts) != 3 {
			fmt.Fprintf(w, "bad vector: %q\n", line)
			continue
		}
		at, err := time.Parse(time.RFC3339, parts[0])
		if err != nil {
			fmt.Fprintf(w, "bad time: %v\n", err)
			continue
		}
		band := lib.NativeIDBand(parts[1], at)
		id := lib.NativeEventIDString(parts[2], parts[1], at)
		split := "-"
		if n, err := strconv.ParseInt(id, 10, 64); err == nil {
			b, r := lib.SplitNativeEventID(n)
			split = fmt.Sprintf("%d, %d", b, r)
		}
		fmt.Fprintf(w, "%d | %s | %s\n", band, id, split)
	}
	rules := []string{}
	for _, rule := range lib.NativeIDBandRules {
		types := []string{}
		for t := range rule.Bands {
			types = append(types, t)
		}
		sort.Strings(types)
		bands := []string{}
		for _, t := range types {
			bands = append(bands, fmt.Sprintf("%s:%d", t, rule.Bands[t]))
		}
		rules = append(rules, fmt.Sprintf("since=%s default=%d bands=%s", rule.Since.UTC().Format(time.RFC3339), rule.DefaultBand, strings.Join(bands, ",")))
	}
	fmt.Fprintf(w, "RULES %s\n", strings.Join(rules, " ; "))
}
