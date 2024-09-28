package devstatscode

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq" // As suggested by lib/pq driver
)

// WriteTSPoints write batch of points to postgresql
// use mergeSeries = "name" to put all series in "name" table, and create "series" column that conatins all point names.
//
//	without merge, alee names will create separate tables.
//
// use non-null mut when you are using this function from multiple threads that write to the same series name at the same time
//
//	use non-null mut only then.
//
// No more giant lock approach here, but it is up to user to spcify call context, especially 2 last parameters!
func WriteTSPoints(ctx *Ctx, con *sql.DB, pts *TSPoints, mergeSeries string, hllEmpty []uint8, mut *sync.Mutex) {
	npts := len(*pts)
	Printf("WriteTSPoints: writing %d points\n", len(*pts))
	if ctx.Debug > 0 {
		Printf("Points:\n%+v\n", pts.Str())
	}
	if npts == 0 {
		return
	}
	merge := false
	mergeS := ""
	if mergeSeries != "" {
		if !checkPsqlName("s" + mergeSeries) {
			return
		}
		mergeS = "s" + mergeSeries
		merge = true
	}
	tags := make(map[string]map[string]struct{})
	fields := make(map[string]map[string]int)
	for _, p := range *pts {
		if p.tags != nil {
			name := p.name
			if !merge {
				if !checkPsqlName("t" + p.name) {
					continue
				}
				name = "t" + p.name
			}
			_, ok := tags[name]
			if !ok {
				tags[name] = make(map[string]struct{})
			}
			for tagName := range p.tags {
				if !checkPsqlName(tagName) {
					continue
				}
				tags[name][tagName] = struct{}{}
			}
		}
		if p.fields != nil {
			name := p.name
			if !merge {
				if !checkPsqlName("s" + p.name) {
					continue
				}
				name = "s" + p.name
			}
			_, ok := fields[name]
			if !ok {
				fields[name] = make(map[string]int)
			}
			for fieldName, fieldValue := range p.fields {
				if !checkPsqlName(fieldName) {
					continue
				}
				fName := fieldName
				t, ok := fields[name][fName]
				if !ok {
					t = -1
				}
				ty := -1
				switch fieldValue.(type) {
				case float64:
					ty = 0
				case time.Time:
					ty = 1
				case string:
					ty = 2
				case []uint8: // HLL
					ty = 3
				default:
					Fatalf("usupported metric value type: %+v,%T (field %s)", fieldValue, fieldValue, fieldName)
				}
				if t >= 0 && t != ty {
					Fatalf(
						"Field %s has a value %+v,%T, previous values were different type %d != %d",
						fieldName, fieldValue, fieldValue, ty, t,
					)
				}
				fields[name][fName] = ty
			}
		}
	}
	if ctx.Debug > 0 {
		Printf("Merge: %v,%s\n", merge, mergeSeries)
		Printf("%d tags:\n%+v\n", len(tags), tags)
		Printf("%d fields:\n%+v\n", len(fields), fields)
	}
	sqls := []string{}
	// Only used when multiple threads are writing the same series
	if mut != nil {
		mut.Lock()
	}
	var (
		exists    bool
		colExists bool
	)
	for name, data := range tags {
		if len(data) == 0 {
			continue
		}
		exists = TableExists(con, ctx, name)
		if !exists {
			sq := "create table if not exists \"" + name + "\"("
			sq += "time timestamp primary key, "
			indices := []string{}
			for col := range data {
				sq += "\"" + escapeName(col) + "\" text, "
				iname := makePsqlName("i"+name[1:]+col, false)
				indices = append(indices, "create index if not exists \""+iname+"\" on \""+name+"\"(\""+escapeName(col)+"\")")
			}
			l := len(sq)
			sq = sq[:l-2] + ")"
			sqls = append(sqls, sq)
			sqls = append(sqls, indices...)
			sqls = append(sqls, "grant select on \""+name+"\" to ro_user")
			sqls = append(sqls, "grant select on \""+name+"\" to devstats_team")
		} else {
			for col := range data {
				eCol := escapeName(col)
				colExists = TableColumnExists(con, ctx, name, eCol)
				if !colExists {
					sq := "alter table \"" + name + "\" add column if not exists \"" + eCol + "\" text"
					sqls = append(sqls, sq)
					iname := makePsqlName("i"+name[1:]+col, false)
					sqls = append(sqls, "create index if not exists \""+iname+"\" on \""+name+"\"(\""+eCol+"\")")
				}
			}
		}
	}
	if merge {
		bTable := false
		colMap := make(map[string]struct{})
		for _, data := range fields {
			if len(data) == 0 {
				continue
			}
			if !bTable {
				exists = TableExists(con, ctx, mergeS)
				if !exists {
					sq := "create table if not exists \"" + mergeS + "\"("
					sq += "time timestamp not null, series text not null, period text not null default '', "
					indices := []string{
						"create index if not exists \"" + makePsqlName("i"+mergeS[1:]+"t", false) + "\" on \"" + mergeS + "\"(time)",
						"create index if not exists \"" + makePsqlName("i"+mergeS[1:]+"s", false) + "\" on \"" + mergeS + "\"(series)",
						"create index if not exists \"" + makePsqlName("i"+mergeS[1:]+"p", false) + "\" on \"" + mergeS + "\"(period)",
					}
					for col, ty := range data {
						col = escapeName(col)
						if ty == 0 {
							sq += "\"" + col + "\" double precision not null default 0.0, "
							// if uncommented then avoid double escape of " in col
							//indices = append(indices, "create index if not exists \""+makePsqlName("i"+mergeS[1:]+col, false)+"\" on \""+mergeS+"\"(\""+col+"\")")
						} else if ty == 1 {
							sq += "\"" + col + "\" timestamp not null default '1900-01-01 00:00:00', "
						} else if ty == 3 {
							sq += "\"" + col + "\" hll not null default hll_empty(), "
						} else {
							sq += "\"" + col + "\" text not null default '', "
						}
						colMap[col] = struct{}{}
					}
					sq += "primary key(time, series, period))"
					sqls = append(sqls, sq)
					sqls = append(sqls, indices...)
					sqls = append(sqls, "grant select on \""+mergeS+"\" to ro_user")
					sqls = append(sqls, "grant select on \""+mergeS+"\" to devstats_team")
				}
				bTable = true
			}
			for col, ty := range data {
				col = escapeName(col)
				_, ok := colMap[col]
				if !ok {
					colExists = TableColumnExists(con, ctx, mergeS, col)
					colMap[col] = struct{}{}
					if !colExists {
						if ty == 0 {
							sqls = append(sqls, "alter table \""+mergeS+"\" add column if not exists \""+col+"\" double precision not null default 0.0")
							// if uncommented then avoid double escape of " in col
							//sqls = append(sqls, "create index if not exists \""+makePsqlName("i"+mergeS[1:]+col, false)+"\" on \""+mergeS+"\"(\""+col+"\")")
						} else if ty == 1 {
							sqls = append(sqls, "alter table \""+mergeS+"\" add column if not exists \""+col+"\" timestamp not null default '1900-01-01 00:00:00'")
						} else if ty == 3 {
							sqls = append(sqls, "alter table \""+mergeS+"\" add column if not exists \""+col+"\" hll not null default hll_empty()")
						} else {
							sqls = append(sqls, "alter table \""+mergeS+"\" add column if not exists \""+col+"\" text not null default ''")
						}
					}
				}
			}
		}
	} else {
		for name, data := range fields {
			if len(data) == 0 {
				continue
			}
			exists = TableExists(con, ctx, name)
			if !exists {
				sq := "create table if not exists \"" + name + "\"("
				sq += "time timestamp not null, period text not null default '', "
				indices := []string{
					"create index if not exists \"" + makePsqlName("i"+name[1:]+"t", false) + "\" on \"" + name + "\"(time)",
					"create index if not exists \"" + makePsqlName("i"+name[1:]+"p", false) + "\" on \"" + name + "\"(period)",
				}
				for col, ty := range data {
					col = escapeName(col)
					if ty == 0 {
						sq += "\"" + col + "\" double precision not null default 0.0, "
						// if uncommented then avoid double escape of " in col
						//indices = append(indices, "create index if not exists \""+makePsqlName("i"+name[1:]+col, false)+"\" on \""+name+"\"(\""+col+"\")")
					} else if ty == 1 {
						sq += "\"" + col + "\" timestamp not null default '1900-01-01 00:00:00', "
					} else if ty == 3 {
						sq += "\"" + col + "\" hll not null default hll_empty(), "
					} else {
						sq += "\"" + col + "\" text not null default '', "
					}
				}
				sq += "primary key(time, period))"
				sqls = append(sqls, sq)
				sqls = append(sqls, indices...)
				sqls = append(sqls, "grant select on \""+name+"\" to ro_user")
				sqls = append(sqls, "grant select on \""+name+"\" to devstats_team")
			} else {
				for col, ty := range data {
					col = escapeName(col)
					colExists = TableColumnExists(con, ctx, name, col)
					if !colExists {
						if ty == 0 {
							sqls = append(sqls, "alter table \""+name+"\" add column if not exists \""+col+"\" double precision not null default 0.0")
							// if uncommented then avoid double escape of " in col
							//sqls = append(sqls, "create index if not exists \""+makePsqlName("i"+name[1:]+col, false)+"\" on \""+name+"\"(\""+col+"\")")
						} else if ty == 1 {
							sqls = append(sqls, "alter table \""+name+"\" add column if not exists \""+col+"\" timestamp not null default '1900-01-01 00:00:00'")
						} else if ty == 3 {
							sqls = append(sqls, "alter table \""+name+"\" add column if not exists \""+col+"\" hll not null default hll_empty()")
						} else {
							sqls = append(sqls, "alter table \""+name+"\" add column if not exists \""+col+"\" text not null default ''")
						}
					}
				}
			}
		}
	}
	if ctx.Debug > 0 && len(sqls) > 0 {
		Printf("structural sqls:\n%s\n", strings.Join(sqls, "\n"))
	}
	for _, q := range sqls {
		// Notice: This **may** fail, when using multiple processes (not threads) to create structures (tables, columns and indices)
		// But each operation can only fail when some other process already executed it succesfully
		// So **ALL** those failures are *OK*.
		// We can avoid thenm by using transaction, but it is much slower then, effect is the same and all we want **IS THE SPEED**
		// So this is done for purpose!
		_, err := ExecSQL(con, ctx, q)
		if err != nil {
			Printf("Ignored %s: %+v\n", q, err)
		}
	}
	// Only used when multiple threads are writing the same series
	if mut != nil {
		mut.Unlock()
	}
	ns := 0
	for _, p := range *pts {
		if p.tags != nil {
			if !checkPsqlName("t" + p.name) {
				continue
			}
			name := "t" + p.name
			namesI := []string{"time"}
			argsI := []string{"$1"}
			vals := []interface{}{p.t}
			i := 2
			for tagName, tagValue := range p.tags {
				if !checkPsqlName(tagName) {
					continue
				}
				tagName = escapeName(tagName)
				namesI = append(namesI, "\""+tagName+"\"")
				argsI = append(argsI, "$"+strconv.Itoa(i))
				vals = append(vals, tagValue)
				i++
			}
			if i == 2 {
				if ctx.Debug >= 0 {
					Printf("tag %s has no values, skipping\n", name)
				}
				continue
			}
			namesIA := strings.Join(namesI, ", ")
			argsIA := strings.Join(argsI, ", ")
			namesU := []string{}
			argsU := []string{}
			for tagName, tagValue := range p.tags {
				if !checkPsqlName(tagName) {
					continue
				}
				tagName = escapeName(tagName)
				namesU = append(namesU, "\""+tagName+"\"")
				argsU = append(argsU, "$"+strconv.Itoa(i))
				vals = append(vals, tagValue)
				i++
			}
			namesUA := strings.Join(namesU, ", ")
			argsUA := strings.Join(argsU, ", ")
			if len(namesU) > 1 {
				namesUA = "(" + namesUA + ")"
				argsUA = "(" + argsUA + ")"
			}
			var q string
			if len(namesU) > 0 {
				argT := "$" + strconv.Itoa(i)
				vals = append(vals, p.t)
				q = fmt.Sprintf(
					"insert into \"%[1]s\"("+namesIA+") values("+argsIA+") "+
						"on conflict(time) do update set "+namesUA+" = "+argsUA+" "+
						"where \"%[1]s\".time = "+argT,
					name,
				)
			} else {
				q = fmt.Sprintf(
					"insert into \"%[1]s\"("+namesIA+") values("+argsIA+") "+
						"on conflict(time) do nothing",
					name,
				)
			}
			ExecSQLWithErr(con, ctx, q, vals...)
			ns++
		}
		if p.fields != nil && !merge {
			if !checkPsqlName("s" + p.name) {
				continue
			}
			name := "s" + p.name
			namesI := []string{"time", "period"}
			argsI := []string{"$1", "$2"}
			vals := []interface{}{p.t, p.period}
			i := 3
			for fieldName, fieldValue := range p.fields {
				if !checkPsqlName(fieldName) {
					continue
				}
				fieldName = escapeName(fieldName)
				namesI = append(namesI, "\""+fieldName+"\"")
				argsI = append(argsI, "$"+strconv.Itoa(i))
				switch val := fieldValue.(type) {
				case []uint8:
					if len(val) == 0 {
						vals = append(vals, hllEmpty)
					} else {
						vals = append(vals, val)
					}
				default:
					vals = append(vals, fieldValue)
				}
				i++
			}
			if i == 3 {
				if ctx.Debug >= 0 {
					Printf("field %s has no values other than time and period, skipping\n", name)
				}
				continue
			}
			namesIA := strings.Join(namesI, ", ")
			argsIA := strings.Join(argsI, ", ")
			namesU := []string{}
			argsU := []string{}
			for fieldName, fieldValue := range p.fields {
				if !checkPsqlName(fieldName) {
					continue
				}
				fieldName = escapeName(fieldName)
				namesU = append(namesU, "\""+fieldName+"\"")
				argsU = append(argsU, "$"+strconv.Itoa(i))
				switch val := fieldValue.(type) {
				case []uint8:
					if len(val) == 0 {
						vals = append(vals, hllEmpty)
					} else {
						vals = append(vals, val)
					}
				default:
					vals = append(vals, fieldValue)
				}
				i++
			}
			namesUA := strings.Join(namesU, ", ")
			argsUA := strings.Join(argsU, ", ")
			if len(namesU) > 1 {
				namesUA = "(" + namesUA + ")"
				argsUA = "(" + argsUA + ")"
			}
			var q string
			if len(namesU) > 0 {
				argT := "$" + strconv.Itoa(i)
				argP := "$" + strconv.Itoa(i+1)
				vals = append(vals, p.t)
				vals = append(vals, p.period)
				q = fmt.Sprintf(
					"insert into \"%[1]s\"("+namesIA+") values("+argsIA+") "+
						"on conflict(time, period) do update set "+namesUA+" = "+argsUA+" "+
						"where \"%[1]s\".time = "+argT+" and \"%[1]s\".period = "+argP,
					name,
				)
			} else {
				q = fmt.Sprintf(
					"insert into \"%[1]s\"("+namesIA+") values("+argsIA+") "+
						"on conflict(time, period) do nothing",
					name,
				)
			}
			ExecSQLWithErr(con, ctx, q, vals...)
			ns++
		}
		if p.fields != nil && merge {
			namesI := []string{"time", "period", "series"}
			argsI := []string{"$1", "$2", "$3"}
			vals := []interface{}{p.t, p.period, p.name}
			i := 4
			for fieldName, fieldValue := range p.fields {
				if !checkPsqlName(fieldName) {
					continue
				}
				fieldName = escapeName(fieldName)
				namesI = append(namesI, "\""+fieldName+"\"")
				argsI = append(argsI, "$"+strconv.Itoa(i))
				switch val := fieldValue.(type) {
				case []uint8:
					if len(val) == 0 {
						vals = append(vals, hllEmpty)
					} else {
						vals = append(vals, val)
					}
				default:
					vals = append(vals, fieldValue)
				}
				i++
			}
			if i == 4 {
				if ctx.Debug >= 0 {
					Printf("field %s has no values other than time, period and series, skipping\n", mergeS)
				}
				continue
			}
			namesIA := strings.Join(namesI, ", ")
			argsIA := strings.Join(argsI, ", ")
			namesU := []string{}
			argsU := []string{}
			for fieldName, fieldValue := range p.fields {
				if !checkPsqlName(fieldName) {
					continue
				}
				fieldName = escapeName(fieldName)
				namesU = append(namesU, "\""+fieldName+"\"")
				argsU = append(argsU, "$"+strconv.Itoa(i))
				switch val := fieldValue.(type) {
				case []uint8:
					if len(val) == 0 {
						vals = append(vals, hllEmpty)
					} else {
						vals = append(vals, val)
					}
				default:
					vals = append(vals, fieldValue)
				}
				i++
			}
			namesUA := strings.Join(namesU, ", ")
			argsUA := strings.Join(argsU, ", ")
			if len(namesU) > 1 {
				namesUA = "(" + namesUA + ")"
				argsUA = "(" + argsUA + ")"
			}
			var q string
			if len(namesU) > 0 {
				argT := "$" + strconv.Itoa(i)
				argP := "$" + strconv.Itoa(i+1)
				argS := "$" + strconv.Itoa(i+2)
				vals = append(vals, p.t)
				vals = append(vals, p.period)
				vals = append(vals, p.name)
				q = fmt.Sprintf(
					"insert into \"%[1]s\"("+namesIA+") values("+argsIA+") "+
						"on conflict(time, series, period) do update set "+namesUA+" = "+argsUA+" "+
						"where \"%[1]s\".time = "+argT+" and \"%[1]s\".period = "+argP+" and \"%[1]s\".series = "+argS,
					mergeS,
				)
			} else {
				q = fmt.Sprintf(
					"insert into \"%[1]s\"("+namesIA+") values("+argsIA+") "+
						"on conflict(time, series, period) do nothing",
					mergeS,
				)
			}
			ExecSQLWithErr(con, ctx, q, vals...)
			ns++
		}
	}
	if ctx.Debug > 0 {
		Printf("upserts: %d\n", ns)
	}
}

// makePsqlName makes sure the identifier is shorter than 64
// fatal: when used to create table or column
// non-fatal: only when used for create index if not exists
// to use `create index if not exists` we must give it a name
// (so postgres can detect if index exists), name is created from table and column names
// so if this is too long, just make it shorter - hence non-fatal
func makePsqlName(name string, fatal bool) string {
	name = strings.Replace(name, `"`, `""`, -1)
	l := len(name)
	if l > 63 {
		if fatal {
			Fatalf("postgresql identifier name too long (%d, %s)", l, name)
			return name
		}
		Printf("Notice: makePsqlName: postgresql identifier name too long (%d, %s)\n", l, name)
		newName := StripUnicode(name[:32] + name[l-31:])
		return newName
	}
	return name
}

// escapeName - escapes " into "" in psql table/column names
func escapeName(name string) string {
	return strings.Replace(name, `"`, `""`, -1)
}

// checkPsqlName - prints warning when psql name exceeds 63 bytes
// return: true - name is OK, false: name is too long (warning is issued)
func checkPsqlName(name string) bool {
	name = strings.Replace(name, `"`, `""`, -1)
	l := len(name)
	if l > 63 {
		Printf("Notice: checkPsqlName: postgresql identifier name too long (%d, %s)\n", l, name)
		return false
	}
	return true
}

// GetTagValues returns tag values for a given key
func GetTagValues(con *sql.DB, ctx *Ctx, name, key string) (ret []string) {
	rows := QuerySQLWithErr(
		con,
		ctx,
		fmt.Sprintf(
			"select %s from t%s order by time asc",
			key,
			name,
		),
	)
	defer func() { FatalOnError(rows.Close()) }()
	s := ""
	for rows.Next() {
		FatalOnError(rows.Scan(&s))
		ret = append(ret, s)
	}
	FatalOnError(rows.Err())
	return
}

// TableExists - checks if a given table exists
func TableExists(con *sql.DB, ctx *Ctx, tableName string) bool {
	var s *string
	FatalOnError(QueryRowSQL(con, ctx, fmt.Sprintf("select to_regclass(%s)", NValue(1)), tableName).Scan(&s))
	return s != nil
}

// TableColumnExists - checks if a given table's has a given column
func TableColumnExists(con *sql.DB, ctx *Ctx, tableName, columnName string) bool {
	var s *string
	FatalOnError(
		QueryRowSQL(
			con,
			ctx,
			fmt.Sprintf(
				"select column_name from information_schema.columns "+
					"where table_name=%s and column_name=%s "+
					"union select null limit 1",
				NValue(1),
				NValue(2),
			),
			tableName,
			columnName,
		).Scan(&s),
	)
	return s != nil
}

// PgConnErr Connects to Postgres database
func PgConnErr(ctx *Ctx) (*sql.DB, error) {
	connectionString := "client_encoding=UTF8 sslmode='" + ctx.PgSSL + "' host='" + ctx.PgHost + "' port=" + ctx.PgPort + " dbname='" + ctx.PgDB + "' user='" + ctx.PgUser + "' password='" + ctx.PgPass + "'"
	if ctx.QOut {
		fmt.Printf("PgConnectString: %s\n", connectionString)
	}
	return sql.Open("postgres", connectionString)
}

// PgConn Connects to Postgres database
func PgConn(ctx *Ctx) *sql.DB {
	connectionString := "client_encoding=UTF8 sslmode='" + ctx.PgSSL + "' host='" + ctx.PgHost + "' port=" + ctx.PgPort + " dbname='" + ctx.PgDB + "' user='" + ctx.PgUser + "' password='" + ctx.PgPass + "'"
	if ctx.QOut {
		// Use fmt.Printf (not lib.Printf that logs to DB) here
		// Avoid trying to log something to DB while connecting
		fmt.Printf("PgConnectString: %s\n", connectionString)
	}
	con, err := sql.Open("postgres", connectionString)
	FatalOnError(err)
	return con
}

// PgConnDB Connects to Postgres database (with specific DB name)
// uses database 'dbname' instead of 'PgDB'
func PgConnDB(ctx *Ctx, dbName string) *sql.DB {
	connectionString := "client_encoding=UTF8 sslmode='" + ctx.PgSSL + "' host='" + ctx.PgHost + "' port=" + ctx.PgPort + " dbname='" + dbName + "' user='" + ctx.PgUser + "' password='" + ctx.PgPass + "'"
	if ctx.QOut {
		// Use fmt.Printf (not lib.Printf that logs to DB) here
		// Avoid trying to log something to DB while connecting
		fmt.Printf("ConnectString: %s\n", connectionString)
	}
	ctx.CanReconnect = false
	con, err := sql.Open("postgres", connectionString)
	FatalOnError(err)
	return con
}

// CreateTable is used to replace DB specific parts of Create Table SQL statement
func CreateTable(tdef string) string {
	tdef = strings.Replace(tdef, "{{ts}}", "timestamp", -1)
	tdef = strings.Replace(tdef, "{{tsnow}}", "timestamp default now()", -1)
	tdef = strings.Replace(tdef, "{{pkauto}}", "bigserial", -1)
	return "create table " + tdef
}

// Outputs query info
func queryOut(query string, args ...interface{}) {
	// Use fmt.Printf not lib.Printf here
	// If we use lib.Printf (that logs to DB) while ouputting some query's parameters
	// We would have infinite recurence
	fmt.Printf("%s\n", query)
	if len(args) > 0 {
		s := ""
		for vi, vv := range args {
			switch v := vv.(type) {
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, complex64, complex128, string, bool, time.Time:
				s += fmt.Sprintf("%d:%+v ", vi+1, v)
			case []uint8:
				s += fmt.Sprintf("%d:%v ", vi+1, FormatRawBytes(v))
			case nil:
				s += fmt.Sprintf("%d:(null) ", vi+1)
			default:
				s += fmt.Sprintf("%d:%T:%+v ", vi+1, vv, reflect.ValueOf(vv))
			}
		}
		fmt.Printf("[%s]\n", s)
		//fmt.Printf("%+v\n", args)
	}
}

// QueryRowSQL executes given SQL on Postgres DB (and returns single row)
func QueryRowSQL(con *sql.DB, ctx *Ctx, query string, args ...interface{}) *sql.Row {
	if ctx.QOut {
		queryOut(query, args...)
	}
	return con.QueryRow(query, args...)
}

// QueryRowSQLTx executes given SQL on Postgres DB (and returns single row)
func QueryRowSQLTx(tx *sql.Tx, ctx *Ctx, query string, args ...interface{}) *sql.Row {
	if ctx.QOut {
		queryOut(query, args...)
	}
	return tx.QueryRow(query, args...)
}

// QuerySQL executes given SQL on Postgres DB (and returns rowset that needs to be closed)
func QuerySQL(con *sql.DB, ctx *Ctx, query string, args ...interface{}) (*sql.Rows, error) {
	if ctx.QOut {
		queryOut(query, args...)
	}
	return con.Query(query, args...)
}

// QuerySQLLogErr executes given SQL on Postgres DB (and returns rowset that needs to be closed)
func QuerySQLLogErr(con *sql.DB, ctx *Ctx, query string, args ...interface{}) (*sql.Rows, error) {
	if ctx.QOut {
		queryOut(query, args...)
	}
	rows, err := con.Query(query, args...)
	if err != nil {
		queryOut(query, args...)
	}
	return rows, err
}

// QuerySQLWithErr wrapper to QuerySQL that exists on error
func QuerySQLWithErr(con *sql.DB, ctx *Ctx, query string, args ...interface{}) *sql.Rows {
	// Try to handle "too many connections" error
	var (
		status string
		res    *sql.Rows
		err    error
	)
	for _, try := range ctx.Trials {
		res, err = QuerySQL(con, ctx, query, args...)
		if err != nil {
			queryOut(query, args...)
		}
		status = FatalOnError(err)
		if status == "ok" {
			break
		}
		fmt.Fprintf(os.Stderr, "Will retry after %d seconds...\n", try)
		time.Sleep(time.Duration(try) * time.Second)
		fmt.Fprintf(os.Stderr, "%d seconds passed, retrying...\n", try)
		if status == Reconnect {
			if ctx.CanReconnect {
				fmt.Fprintf(os.Stderr, "Reconnect request after %d seconds\n", try)
				_ = con.Close()
				con = PgConn(ctx)
				fmt.Fprintf(os.Stderr, "Reconnected after %d seconds\n", try)
			} else {
				Fatalf("returned reconnect request, but custom DB connect strings are in use")
			}
		}
	}
	if status != OK {
		Fatalf("too many attempts, tried %d times", len(ctx.Trials))
	}
	return res
}

// QuerySQLTx executes given SQL on Postgres DB (and returns rowset that needs to be closed)
// It is for running inside transaction
func QuerySQLTx(con *sql.Tx, ctx *Ctx, query string, args ...interface{}) (*sql.Rows, error) {
	if ctx.QOut {
		queryOut(query, args...)
	}
	return con.Query(query, args...)
}

// QuerySQLTxWithErr wrapper to QuerySQLTx that exists on error
// It is for running inside transaction
func QuerySQLTxWithErr(con *sql.Tx, ctx *Ctx, query string, args ...interface{}) *sql.Rows {
	// Try to handle "too many connections" error
	var (
		status string
		res    *sql.Rows
		err    error
		db     *sql.DB
	)
	for _, try := range ctx.Trials {
		if db == nil {
			res, err = QuerySQLTx(con, ctx, query, args...)
		} else {
			res, err = QuerySQL(db, ctx, query, args...)
		}
		if err != nil {
			queryOut(query, args...)
		}
		status = FatalOnError(err)
		if status == "ok" {
			break
		}
		fmt.Fprintf(os.Stderr, "Will retry after %d seconds...\n", try)
		time.Sleep(time.Duration(try) * time.Second)
		fmt.Fprintf(os.Stderr, "%d seconds passed, retrying...\n", try)
		if status == Reconnect {
			fmt.Fprintf(os.Stderr, "Reconnect request after %d seconds, breaking transaction\n", try)
			if ctx.CanReconnect {
				//db = PgConn(ctx)
				//fmt.Fprintf(os.Stderr, "Reconnected after %d seconds, breaking transaction\n", try)
				Fatalf("reconnect request from within the transaction is not supported")
			} else {
				Fatalf("returned reconnect request, but custom DB connect strings are in use")
			}
		}
	}
	if status != OK {
		Fatalf("too many attempts, tried %d times", len(ctx.Trials))
	}
	return res
}

// ExecSQLLogErr executes given SQL on Postgres DB (and return single state result, that doesn't need to be closed)
func ExecSQLLogErr(con *sql.DB, ctx *Ctx, query string, args ...interface{}) (sql.Result, error) {
	if ctx.QOut {
		queryOut(query, args...)
	}
	res, err := con.Exec(query, args...)
	if err != nil {
		queryOut(query, args...)
	}
	return res, err
}

// ExecSQL executes given SQL on Postgres DB (and return single state result, that doesn't need to be closed)
func ExecSQL(con *sql.DB, ctx *Ctx, query string, args ...interface{}) (sql.Result, error) {
	if ctx.QOut {
		queryOut(query, args...)
	}
	return con.Exec(query, args...)
}

// ExecSQLWithErr wrapper to ExecSQL that exists on error
func ExecSQLWithErr(con *sql.DB, ctx *Ctx, query string, args ...interface{}) sql.Result {
	// Try to handle "too many connections" error
	var (
		status string
		res    sql.Result
		err    error
	)
	for _, try := range ctx.Trials {
		res, err = ExecSQL(con, ctx, query, args...)
		if err != nil {
			fmt.Printf("Failed sql: ")
			queryOut(query, args...)
		}
		status = FatalOnError(err)
		if status == "ok" {
			break
		}
		fmt.Fprintf(os.Stderr, "Will retry after %d seconds...\n", try)
		time.Sleep(time.Duration(try) * time.Second)
		fmt.Fprintf(os.Stderr, "%d seconds passed, retrying...\n", try)
		if status == Reconnect {
			fmt.Fprintf(os.Stderr, "Reconnect request after %d seconds\n", try)
			if ctx.CanReconnect {
				_ = con.Close()
				con = PgConn(ctx)
				fmt.Fprintf(os.Stderr, "Reconnected after %d seconds\n", try)
			} else {
				Fatalf("returned reconnect request, but custom DB connect strings are in use")
			}
		}
	}
	if status != OK {
		Fatalf("too many attempts, tried %d times", len(ctx.Trials))
	}
	return res
}

// ExecSQLTx executes given SQL on Postgres DB (and return single state result, that doesn't need to be closed)
// It is for running inside transaction
func ExecSQLTx(con *sql.Tx, ctx *Ctx, query string, args ...interface{}) (sql.Result, error) {
	if ctx.QOut {
		queryOut(query, args...)
	}
	return con.Exec(query, args...)
}

// ExecSQLTxWithErr wrapper to ExecSQLTx that exists on error
// It is for running inside transaction
func ExecSQLTxWithErr(con *sql.Tx, ctx *Ctx, query string, args ...interface{}) sql.Result {
	// Try to handle "too many connections" error
	var (
		status string
		res    sql.Result
		err    error
		db     *sql.DB
	)
	for _, try := range ctx.Trials {
		if db == nil {
			res, err = ExecSQLTx(con, ctx, query, args...)
		} else {
			res, err = ExecSQL(db, ctx, query, args...)
		}
		if err != nil {
			queryOut(query, args...)
		}
		status = FatalOnError(err)
		if status == "ok" {
			break
		}
		fmt.Fprintf(os.Stderr, "Will retry after %d seconds...\n", try)
		time.Sleep(time.Duration(try) * time.Second)
		fmt.Fprintf(os.Stderr, "%d seconds passed, retrying...\n", try)
		if status == Reconnect {
			fmt.Fprintf(os.Stderr, "Reconnect request after %d seconds, breaking transaction\n", try)
			if ctx.CanReconnect {
				//db = PgConn(ctx)
				//fmt.Fprintf(os.Stderr, "Reconnected after %d seconds, breaking transaction\n", try)
				Fatalf("reconnect request from within the transaction is not supported")
			} else {
				Fatalf("returned reconnect request, but custom DB connect strings are in use")
			}
		}
	}
	if status != OK {
		Fatalf("too many attempts, tried %d times", len(ctx.Trials))
	}
	return res
}

// NValues will return values($1, $2, .., $n)
func NValues(n int) string {
	s := "values("
	i := 1
	for i <= n {
		s += "$" + strconv.Itoa(i) + ", "
		i++
	}
	return s[:len(s)-2] + ")"
}

// NArray will return values($1, $2, .., $n)
func NArray(n, offset int) string {
	s := "("
	i := 1 + offset
	n += offset
	for i <= n {
		s += "$" + strconv.Itoa(i) + ", "
		i++
	}
	return s[:len(s)-2] + ")"
}

// NValue will return $n
func NValue(index int) string {
	return fmt.Sprintf("$%d", index)
}

// InsertIgnore - will return insert statement with ignore option specific for DB
func InsertIgnore(query string) string {
	return fmt.Sprintf("insert %s on conflict do nothing", query)
}

// BoolOrNil - return either nil or value of boolPtr
func BoolOrNil(boolPtr *bool) interface{} {
	if boolPtr == nil {
		return nil
	}
	return *boolPtr
}

// NegatedBoolOrNil - return either nil or negated value of boolPtr
func NegatedBoolOrNil(boolPtr *bool) interface{} {
	if boolPtr == nil {
		return nil
	}
	return !*boolPtr
}

// TimeOrNil - return either nil or value of timePtr
func TimeOrNil(timePtr *time.Time) interface{} {
	if timePtr == nil {
		return nil
	}
	return *timePtr
}

// IntOrNil - return either nil or value of intPtr
func IntOrNil(intPtr *int) interface{} {
	if intPtr == nil {
		return nil
	}
	return *intPtr
}

// FirstIntOrNil - return either nil or value of intPtr
func FirstIntOrNil(intPtrs []*int) interface{} {
	for _, intPtr := range intPtrs {
		if intPtr != nil {
			return *intPtr
		}
	}
	return nil
}

// CleanUTF8 - clean UTF8 string to containg only Pq allowed runes
func CleanUTF8(str string) string {
	if strings.Contains(str, "\x00") {
		return strings.Replace(str, "\x00", "", -1)
	}
	return str
}

// StringOrNil - return either nil or value of strPtr
func StringOrNil(strPtr *string) interface{} {
	if strPtr == nil {
		return nil
	}
	return CleanUTF8(*strPtr)
}

// TruncToBytes - truncates text to <= size bytes (note that this can be a lot less UTF-8 runes)
func TruncToBytes(str string, size int) string {
	str = CleanUTF8(str)
	length := len(str)
	if length < size {
		return str
	}
	res := ""
	i := 0
	for _, r := range str {
		if len(res+string(r)) > size {
			break
		}
		res += string(r)
		i++
	}
	return res
}

// TruncStringOrNil - return either nil or value of strPtr truncated to maxLen chars
func TruncStringOrNil(strPtr *string, maxLen int) interface{} {
	if strPtr == nil {
		return nil
	}
	return TruncToBytes(*strPtr, maxLen)
}

// DatabaseExists - checks if database stored in context exists
// If closeConn is true - then it closes connection after checking if database exists
// If closeConn is false, then it returns open connection to default database "postgres"
func DatabaseExists(ctx *Ctx, closeConn bool) (exists bool, c *sql.DB) {
	// We cannot connect to database stored in context, because it is possible it's not there
	db := ctx.PgDB
	ctx.PgDB = "postgres"

	// Connect to Postgres DB using its default database "postgres"
	c = PgConn(ctx)
	if closeConn {
		defer func() {
			FatalOnError(c.Close())
			c = nil
		}()
	}

	// Try to get database name from `pg_database` - it will return row if database exists
	rows := QuerySQLWithErr(c, ctx, "select 1 from pg_database where datname = $1", db)
	defer func() { FatalOnError(rows.Close()) }()
	for rows.Next() {
		exists = true
	}
	FatalOnError(rows.Err())

	// Restore original database name in the context
	ctx.PgDB = db

	return
}

// DropDatabaseIfExists - drops requested database if exists
// Returns true if database existed and was dropped
func DropDatabaseIfExists(ctx *Ctx) bool {
	// Check if database exists
	exists, c := DatabaseExists(ctx, false)
	defer func() { FatalOnError(c.Close()) }()

	// Drop database if exists
	if exists {
		ExecSQLWithErr(c, ctx, "drop database "+ctx.PgDB)
	}

	// Return whatever we created DB or not
	return exists
}

// CreateDatabaseIfNeeded - creates requested database if not exists
// Returns true if database was not existing existed and created dropped
func CreateDatabaseIfNeeded(ctx *Ctx) bool {
	// Check if database exists
	exists, c := DatabaseExists(ctx, false)
	defer func() { FatalOnError(c.Close()) }()

	// Create database if not exists
	if !exists {
		ExecSQLWithErr(c, ctx, "create database "+ctx.PgDB)
	}

	// Return whatever we created DB or not
	return !exists
}

// CreateDatabaseIfNeededExtended - creates requested database if not exists
// Returns true if database was not existing existed and created dropped
// Allows specifying additional database parameters
func CreateDatabaseIfNeededExtended(ctx *Ctx, extraParams string) bool {
	// Check if database exists
	exists, c := DatabaseExists(ctx, false)
	defer func() { FatalOnError(c.Close()) }()

	// Create database if not exists
	if !exists {
		ExecSQLWithErr(c, ctx, "create database "+ctx.PgDB+" "+extraParams)
	}

	// Return whatever we created DB or not
	return !exists
}

// ClearOrphanedLocks clears affs_lock/giant_lock DB mtx on 'devstats' database, if it is older than 30/60 hours
// It clears logs on `devstats` database
func ClearOrphanedLocks() {
	// Environment context parse
	var ctx Ctx
	ctx.Init()
	if ctx.SkipPDB {
		return
	}
	c0 := PgConn(&ctx)
	defer func() { _ = c0.Close() }()
	_, _ = ExecSQL(c0, &ctx, "delete from gha_computed where metric like 'affs_lock%' and dt < now() - '"+ctx.ClearAffsLockPeriod+"'::interval")
	// Point to logs database
	ctx.PgDB = Devstats
	// Connect to DB
	c := PgConn(&ctx)
	defer func() { _ = c.Close() }()
	_, _ = ExecSQL(c, &ctx, "delete from gha_computed where metric like 'affs_lock%' and dt < now() - '"+ctx.ClearAffsLockPeriod+"'::interval")
	_, _ = ExecSQL(c, &ctx, "delete from gha_computed where metric like 'giant_lock%' and dt < now() - '"+ctx.ClearGiantLockPeriod+"'::interval")
}
