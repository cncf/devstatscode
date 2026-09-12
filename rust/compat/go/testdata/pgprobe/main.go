// pgprobe — Go reference for the Rust `devstatscode::pg` tests.
//
// Connects like every DevStats tool does (`ctx.Init()` + `lib.PgConn`) and
// executes the statements read from stdin, one per line:
//
//	<sql>[\t<arg>...]      query: print columns, driver values and the result
//	                       of scanning every column into each Go destination type
//	!<sql>[\t<arg>...]     exec: print rows affected
//
// Arguments are `<kind>:<value>` with kind i (int64), f (float64), b (bool),
// s (string), x (hex bytes), t (RFC 3339 time, nanoseconds allowed), n (nil).
//
// Output (one record per line, fields separated by " | "):
//
//	Q <sql>
//	C <column names>
//	R T=<%T> V=<%v> S=<*string, quoted> B=<*[]byte> I=<*int64> F=<*float64> L=<*bool> D=<*time.Time>   (one per column)
//	N <row count>
//	X <rows affected>
//	E <%T of the error> <error text> code=<sqlstate> name=<condition name>
//
// The scan results are `ok:<value>` or `err:<message>`.
package main

import (
	"bufio"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	lib "github.com/cncf/devstatscode"
	"github.com/lib/pq"
)

func parseArg(a string) (interface{}, error) {
	if len(a) < 2 || a[1] != ':' {
		return nil, fmt.Errorf("bad argument %q", a)
	}
	v := a[2:]
	switch a[0] {
	case 'i':
		return strconv.ParseInt(v, 10, 64)
	case 'f':
		return strconv.ParseFloat(v, 64)
	case 'b':
		return strconv.ParseBool(v)
	case 's':
		return v, nil
	case 'x':
		return hex.DecodeString(v)
	case 't':
		return time.Parse(time.RFC3339Nano, v)
	case 'n':
		return nil, nil
	}
	return nil, fmt.Errorf("bad argument kind %q", a)
}

func errLine(err error) string {
	code, name := "", ""
	if pe, ok := err.(*pq.Error); ok {
		code = string(pe.Code)
		name = pe.Code.Name()
	}
	return fmt.Sprintf("E %T | %s | code=%s | name=%s", err, err.Error(), code, name)
}

// scanColumn re-runs the query scanning column `idx` into `dest` (the other
// columns into interface{}) and reports the outcome of the first row.
func scanColumn(db *sql.DB, query string, args []interface{}, ncols, idx int, dest interface{}, show func() string) string {
	rows, err := db.Query(query, args...)
	if err != nil {
		return "err:" + err.Error()
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "err:" + err.Error()
		}
		return "err:no rows"
	}
	targets := make([]interface{}, ncols)
	for i := 0; i < ncols; i++ {
		if i == idx {
			targets[i] = dest
		} else {
			var v interface{}
			targets[i] = &v
		}
	}
	if err := rows.Scan(targets...); err != nil {
		return "err:" + err.Error()
	}
	return "ok:" + show()
}

func query(db *sql.DB, q string, args []interface{}, out *bufio.Writer) {
	rows, err := db.Query(q, args...)
	if err != nil {
		fmt.Fprintln(out, errLine(err))
		return
	}
	cols, err := rows.Columns()
	if err != nil {
		fmt.Fprintln(out, errLine(err))
		_ = rows.Close()
		return
	}
	fmt.Fprintf(out, "C %s\n", strings.Join(cols, " | "))
	n := 0
	var first []interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			fmt.Fprintln(out, errLine(err))
			_ = rows.Close()
			return
		}
		if n == 0 {
			first = vals
		}
		n++
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintln(out, errLine(err))
		_ = rows.Close()
		return
	}
	_ = rows.Close()
	for i, v := range first {
		var s string
		var b []byte
		var i64 int64
		var f float64
		var l bool
		var d time.Time
		fmt.Fprintf(
			out,
			"R T=%T | V=%v | S=%s | B=%s | I=%s | F=%s | L=%s | D=%s\n",
			v,
			v,
			scanColumn(db, q, args, len(cols), i, &s, func() string { return strconv.Quote(s) }),
			scanColumn(db, q, args, len(cols), i, &b, func() string { return fmt.Sprintf("%v", b) }),
			scanColumn(db, q, args, len(cols), i, &i64, func() string { return strconv.FormatInt(i64, 10) }),
			scanColumn(db, q, args, len(cols), i, &f, func() string { return strconv.FormatFloat(f, 'g', -1, 64) }),
			scanColumn(db, q, args, len(cols), i, &l, func() string { return strconv.FormatBool(l) }),
			scanColumn(db, q, args, len(cols), i, &d, func() string { return d.String() }),
		)
	}
	fmt.Fprintf(out, "N %d\n", n)
}

func exec(db *sql.DB, q string, args []interface{}, out *bufio.Writer) {
	res, err := db.Exec(q, args...)
	if err != nil {
		fmt.Fprintln(out, errLine(err))
		return
	}
	n, err := res.RowsAffected()
	if err != nil {
		fmt.Fprintln(out, errLine(err))
		return
	}
	fmt.Fprintf(out, "X %d\n", n)
}

func main() {
	var ctx lib.Ctx
	ctx.Init()
	db := lib.PgConn(&ctx)
	defer func() { _ = db.Close() }()
	out := bufio.NewWriter(os.Stdout)
	defer func() { _ = out.Flush() }()
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		q := parts[0]
		var args []interface{}
		for _, a := range parts[1:] {
			v, err := parseArg(a)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(2)
			}
			args = append(args, v)
		}
		fmt.Fprintf(out, "Q %s\n", q)
		if strings.HasPrefix(q, "!") {
			exec(db, q[1:], args, out)
			continue
		}
		query(db, q, args, out)
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(2)
	}
}
