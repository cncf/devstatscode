package devstatscode

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	lib "github.com/cncf/devstatscode"
	testlib "github.com/cncf/devstatscode/test"
)

func TestCleanUTF8(t *testing.T) {
	// Test cases
	var testCases = []struct {
		value    string
		expected string
	}{
		{value: "value", expected: "value"},
		{value: "val\x00ue", expected: "value"},
		{value: "val\u0000ue", expected: "value"},
		{value: "v\x00a\U00000000l\u0000ue", expected: "value"},
		{value: "平仮名, ひらがな", expected: "平仮名, ひらがな"},
		{value: "\u0000平仮名\x00ひらがな\U00000000", expected: "平仮名ひらがな"},
	}
	// Execute test cases
	for index, test := range testCases {
		got := lib.CleanUTF8(test.value)
		if got != test.expected {
			t.Errorf("test number %d, expected %v, got %v", index+1, test.expected, got)
		}
	}
}

func TestTruncToBytes(t *testing.T) {
	// Test cases
	var testCases = []struct {
		value       string
		n           int
		expectedStr string
		expectedLen int
	}{
		{
			value:       "value",
			n:           3,
			expectedStr: "val",
			expectedLen: 3,
		},
		{
			value:       "平仮名, ひらがな",
			n:           6,
			expectedStr: "平仮",
			expectedLen: 6,
		},
		{
			value:       "平仮名, ひらがな",
			n:           8,
			expectedStr: "平仮",
			expectedLen: 6,
		},
		{
			value:       "平仮名, ひらがな",
			n:           9,
			expectedStr: "平仮名",
			expectedLen: 9,
		},
		{
			value:       "\u0000平仮名, \x00ひら\U00000000がな",
			n:           9,
			expectedStr: "平仮名",
			expectedLen: 9,
		},
	}
	// Execute test cases
	for index, test := range testCases {
		gotStr := lib.TruncToBytes(test.value, test.n)
		if gotStr != test.expectedStr {
			t.Errorf("test number %d, expected string %v, got %v", index+1, test.expectedStr, gotStr)
		}
		gotLen := len(gotStr)
		if gotLen != test.expectedLen {
			t.Errorf("test number %d, expected length %v, got %v", index+1, test.expectedLen, gotLen)
		}
	}
}

func TestTruncStringOrNil(t *testing.T) {
	// Test cases
	sValues := []string{"value", "平仮名, ひらがな", "\u0000平仮名, \x00ひら\U00000000がな"}
	var testCases = []struct {
		value    *string
		n        int
		expected interface{}
	}{
		{
			value:    nil,
			n:        10,
			expected: nil,
		},
		{
			value:    &sValues[0],
			n:        3,
			expected: "val",
		},
		{
			value:    &sValues[1],
			n:        6,
			expected: "平仮",
		},
		{
			value:    &sValues[2],
			n:        9,
			expected: "平仮名",
		},
	}
	// Execute test cases
	for index, test := range testCases {
		got := lib.TruncStringOrNil(test.value, test.n)
		if got != test.expected {
			t.Errorf("test number %d, expected %v, got %v", index+1, test.expected, got)
		}
	}
}

func TestBoolOrNil(t *testing.T) {
	result := lib.BoolOrNil(nil)
	if result != nil {
		t.Errorf("test nil case: expected <nil>, got %v", result)
	}
	val := true
	result = lib.BoolOrNil(&val)
	if result != val {
		t.Errorf("expected true, got %v", result)
	}
}

func TestNegatedBoolOrNil(t *testing.T) {
	result := lib.NegatedBoolOrNil(nil)
	if result != nil {
		t.Errorf("test nil case: expected <nil>, got %v", result)
	}
	val := true
	result = lib.NegatedBoolOrNil(&val)
	expected := !val
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestTimeOrNil(t *testing.T) {
	result := lib.TimeOrNil(nil)
	if result != nil {
		t.Errorf("test nil case: expected <nil>, got %v", result)
	}
	val := time.Now()
	result = lib.TimeOrNil(&val)
	if result != val {
		t.Errorf("expected %v, got %v", val, result)
	}
}

func TestIntOrNil(t *testing.T) {
	result := lib.IntOrNil(nil)
	if result != nil {
		t.Errorf("test nil case: expected <nil>, got %v", result)
	}
	val := 2
	result = lib.IntOrNil(&val)
	if result != val {
		t.Errorf("expected %v, got %v", val, result)
	}
}

func TestFirstIntOrNil(t *testing.T) {
	nn1 := 1
	nn2 := 2
	var testCases = []struct {
		array    []*int
		expected interface{}
	}{
		{array: []*int{}, expected: nil},
		{array: []*int{nil}, expected: nil},
		{array: []*int{&nn1}, expected: nn1},
		{array: []*int{nil, nil}, expected: nil},
		{array: []*int{nil, &nn1}, expected: nn1},
		{array: []*int{&nn1, nil}, expected: nn1},
		{array: []*int{&nn1, &nn2}, expected: nn1},
		{array: []*int{&nn2, &nn1}, expected: nn2},
		{array: []*int{nil, &nn2, &nn1}, expected: nn2},
	}
	// Execute test cases
	for index, test := range testCases {
		got := lib.FirstIntOrNil(test.array)
		if got != test.expected {
			t.Errorf("test number %d, expected %v, got %v", index+1, test.expected, got)
		}
	}
}

func TestStringOrNil(t *testing.T) {
	result := lib.StringOrNil(nil)
	if result != nil {
		t.Errorf("test nil case: expected <nil>, got %v", result)
	}
	val := "hello\x00 world"
	expected := "hello world"
	result = lib.StringOrNil(&val)
	if result != expected {
		t.Errorf("expected %v, got %v", val, result)
	}
}

func TestPostgres(t *testing.T) {
	// Environment context parse
	var ctx lib.Ctx
	ctx.Init()
	ctx.TestMode = true

	// Do not allow to run tests in "gha" database
	if ctx.PgDB != "dbtest" {
		t.Errorf("tests can only be run on \"dbtest\" database")
		return
	}

	// Drop database if exists
	lib.DropDatabaseIfExists(&ctx)

	// Create database if needed
	createdDatabase := lib.CreateDatabaseIfNeeded(&ctx)
	if !createdDatabase {
		t.Errorf("failed to create database \"%s\"", ctx.PgDB)
	}

	// Drop database after tests
	defer func() {
		// Drop database after tests
		lib.DropDatabaseIfExists(&ctx)
	}()

	// Connect to Postgres DB
	c := lib.PgConn(&ctx)
	defer func() { lib.FatalOnError(c.Close()) }()

	// Create example table
	lib.ExecSQLWithErr(
		c,
		&ctx,
		lib.CreateTable(
			"test(an_int int, a_string text, a_dt {{ts}}, primary key(an_int))",
		),
	)

	// Insert single row
	lib.ExecSQLWithErr(
		c,
		&ctx,
		"insert into test(an_int, a_string, a_dt) "+lib.NValues(3),
		lib.AnyArray{1, "string", time.Now()}...,
	)

	// Get inserted int
	i := 0
	lib.FatalOnError(lib.QueryRowSQL(c, &ctx, "select an_int from test").Scan(&i))
	if i != 1 {
		t.Errorf("expected to insert 1, got %v", i)
	}

	// Insert another row
	lib.ExecSQLWithErr(
		c,
		&ctx,
		"insert into test(an_int, a_string, a_dt) "+lib.NValues(3),
		lib.AnyArray{11, "another string", time.Now()}...,
	)

	// Get all ints from database
	gotArr := getInts(c, &ctx)

	expectedArr := []int{1, 11}
	if !testlib.CompareIntSlices(gotArr, expectedArr) {
		t.Errorf("expected %v after two inserts, got %v", expectedArr, gotArr)
	}

	// Start transaction
	tx, err := c.Begin()
	if err != nil {
		t.Error(err.Error())
	}

	// Insert another row
	lib.ExecSQLTxWithErr(
		tx,
		&ctx,
		"insert into test(an_int, a_string, a_dt) "+lib.NValues(3),
		lib.AnyArray{21, "this will be rolled back", time.Now()}...,
	)

	// Rollback transaction
	lib.FatalOnError(tx.Rollback())

	// Get all ints from database
	gotArr = getInts(c, &ctx)

	if !testlib.CompareIntSlices(gotArr, expectedArr) {
		t.Errorf("expected %v after rollback, got %v", expectedArr, gotArr)
	}

	// Start transaction
	tx, err = c.Begin()
	if err != nil {
		t.Error(err.Error())
	}

	// Insert another row
	lib.ExecSQLTxWithErr(
		tx,
		&ctx,
		"insert into test(an_int, a_string, a_dt) "+lib.NValues(3),
		lib.AnyArray{31, "this will be committed", time.Now()}...,
	)

	// Commit transaction
	lib.FatalOnError(tx.Commit())

	// Get all ints from database
	gotArr = getInts(c, &ctx)

	expectedArr = []int{1, 11, 31}
	if !testlib.CompareIntSlices(gotArr, expectedArr) {
		t.Errorf("expected %v after commit, got %v", expectedArr, gotArr)
	}

	// Insert ignore row (that violetes primary key constraint)
	lib.ExecSQLWithErr(
		c,
		&ctx,
		lib.InsertIgnore("into test(an_int, a_string, a_dt) "+lib.NValues(3)),
		lib.AnyArray{1, "conflicting key", time.Now()}...,
	)

	// Get all ints from database
	gotArr = getInts(c, &ctx)

	if !testlib.CompareIntSlices(gotArr, expectedArr) {
		t.Errorf("expected %v after insert ignore, got %v", expectedArr, gotArr)
	}
}

// getInts - gets all ints from database, sorted
func getInts(c *sql.DB, ctx *lib.Ctx) []int {
	// Get inserted values
	rows := lib.QuerySQLWithErr(c, ctx, "select an_int from test order by an_int asc")
	defer func() { lib.FatalOnError(rows.Close()) }()

	var (
		i   int
		arr []int
	)
	for rows.Next() {
		lib.FatalOnError(rows.Scan(&i))
		arr = append(arr, i)
	}
	lib.FatalOnError(rows.Err())
	return arr
}

// compactInfoSQL - identity, attribute slots and the complete definition of a public table
const compactInfoSQL = `select c.oid::bigint,
  (select count(*) from pg_attribute a where a.attrelid = c.oid and a.attnum > 0),
  (select count(*) from pg_attribute a where a.attrelid = c.oid and a.attnum > 0 and a.attisdropped),
  concat_ws(E'\n',
    'owner ' || pg_get_userbyid(c.relowner),
    'acl ' || coalesce((select string_agg(x, ',' order by x) from unnest(c.relacl::text[]) x), '-'),
    (select string_agg(format('column %s %s notnull=%s default=%s', a.attname,
      format_type(a.atttypid, a.atttypmod), a.attnotnull, coalesce(pg_get_expr(d.adbin, d.adrelid), '-')),
      E'\n' order by a.attnum)
     from pg_attribute a left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
     where a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped),
    (select string_agg(format('constraint %s %s', k.conname, pg_get_constraintdef(k.oid)), E'\n' order by k.conname)
     from pg_constraint k where k.conrelid = c.oid),
    (select string_agg('index ' || i.indexdef, E'\n' order by i.indexname)
     from pg_indexes i where i.schemaname = 'public' and i.tablename = c.relname)
  )
from pg_class c join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'public' and c.relname = $1`

// compactInfo - what CompactTable must keep (definition, data) and what it changes (oid, attribute slots)
type compactInfo struct {
	oid     int64
	slots   int
	dropped int
	def     string
	data    string
}

func getCompactInfo(c *sql.DB, ctx *lib.Ctx, table string) (info compactInfo) {
	lib.FatalOnError(lib.QueryRowSQL(c, ctx, compactInfoSQL, table).Scan(&info.oid, &info.slots, &info.dropped, &info.def))
	lib.FatalOnError(
		lib.QueryRowSQL(
			c,
			ctx,
			"select count(*)::text || ' ' || coalesce(md5(string_agg(md5(t::text), '' order by md5(t::text))), '') from public.\""+
				strings.Replace(table, "\"", "\"\"", -1)+"\" t",
		).Scan(&info.data),
	)
	return
}

// compactChurn - adds and drops n columns: the table uses n more attribute slots, all of them dropped
func compactChurn(c *sql.DB, ctx *lib.Ctx, table string, n int) {
	lib.ExecSQLWithErr(
		c,
		ctx,
		fmt.Sprintf(
			"do $$ begin for i in 1..%d loop "+
				"execute format('alter table %%I add column %%I double precision not null default 0.0', '%s', 'churn ' || i); "+
				"execute format('alter table %%I drop column %%I', '%s', 'churn ' || i); "+
				"end loop; end $$",
			n, table, table,
		),
	)
}

func TestCompactTable(t *testing.T) {
	var ctx lib.Ctx
	ctx.Init()
	ctx.TestMode = true

	// Do not allow to run tests in "gha" database
	if ctx.PgDB != "dbtest" {
		t.Errorf("tests can only be run on \"dbtest\" database")
		return
	}
	if ctx.ColumnsCompactSlots != 1300 {
		t.Fatalf("expected the default 1300 compact slots, got %d", ctx.ColumnsCompactSlots)
	}
	lib.DropDatabaseIfExists(&ctx)
	if !lib.CreateDatabaseIfNeeded(&ctx) {
		t.Errorf("failed to create database \"%s\"", ctx.PgDB)
	}
	defer func() {
		lib.DropDatabaseIfExists(&ctx)
	}()
	c := lib.PgConn(&ctx)
	defer func() { lib.FatalOnError(c.Close()) }()
	exec := func(sqls ...string) {
		for _, s := range sqls {
			lib.ExecSQLWithErr(c, &ctx, s)
		}
	}
	// A series table without any data, like the TSDB writer creates it, with 12 attribute slots (9 dropped)
	plain := func(table, create string) {
		if create == "" {
			create = "create table"
		}
		exec(create + " \"" + table + "\"(time timestamp not null, series text not null, period text not null default '')")
		compactChurn(c, &ctx, table, 9)
	}
	// untouched: CompactTable refused to work and the table is exactly as it was
	untouched := func(what, table string, before compactInfo, compacted bool) {
		after := getCompactInfo(c, &ctx, table)
		if compacted || after != before {
			t.Errorf("%s: expected '%s' to stay untouched, compacted: %v\nbefore: %+v\nafter:  %+v", what, table, compacted, before, after)
		}
	}
	// rebuilt: the table was rebuilt: only its identity and the attribute slots changed
	rebuilt := func(what, table string, before compactInfo, compacted bool) {
		after := getCompactInfo(c, &ctx, table)
		if !compacted || after.oid == before.oid || after.dropped != 0 || after.slots != before.slots-before.dropped ||
			after.def != before.def || after.data != before.data {
			t.Errorf("%s: expected '%s' to be rebuilt, compacted: %v\nbefore: %+v\nafter:  %+v", what, table, compacted, before, after)
		}
		leftovers := 0
		lib.FatalOnError(lib.QueryRowSQL(c, &ctx, "select count(*) from pg_namespace where nspname like 'devstats_compact%'").Scan(&leftovers))
		if leftovers != 0 {
			t.Errorf("%s: %d scratch schemas left", what, leftovers)
		}
	}
	hll := false
	lib.FatalOnError(lib.QueryRowSQL(c, &ctx, "select count(*) > 0 from pg_available_extensions where name = 'hll'").Scan(&hll))
	if hll {
		_, err := lib.ExecSQL(c, &ctx, "create extension if not exists hll")
		hll = err == nil
	}

	// The table that the TSDB writer and the 'columns' tool build, with everything that must be preserved
	longName := strings.Repeat("long-repository-group-name-", 3)[:63]
	exec(
		"create table \"sact\"(time timestamp not null, series text not null, period text not null default '', "+
			"\"Kubernetes\" double precision not null default 0.0, \"a text\" text not null default '', "+
			"\"a ts\" timestamp not null default '1900-01-01 00:00:00', calc double precision default (1.5 * 2), "+
			"primary key(time, series, period), constraint \"positive K\" check (\"Kubernetes\" >= 0))",
		"create index \"iactt\" on \"sact\"(time)",
		"create index \"iacts\" on \"sact\"(series)",
		"create index \"iactp\" on \"sact\"(period)",
		"create unique index \"uact desc\" on \"sact\"(series, time desc, period)",
		"alter table \"sact\" add constraint \"sact uq\" unique (period, series, time)",
		"create index \"iact expr\" on \"sact\"((lower(series)))",
		"create index \"iact part\" on \"sact\"(\"Kubernetes\") where \"Kubernetes\" > 10",
		"grant select on \"sact\" to public",
		"grant select, insert on \"sact\" to pg_monitor with grant option",
		"alter table \"sact\" add column \""+longName+"\" double precision not null default 0.0",
		"alter table \"sact\" add column \"Weird \"\"quoted\"\" name, with comma and ünïcode\" double precision not null default 0.0",
		// the 'columns' tool way: nullable first, then mass update and not null with a default
		"alter table \"sact\" add column \"Envoy\" double precision",
		"insert into \"sact\"(time, series, period, \"Kubernetes\", \"a text\", \"Envoy\", \""+longName+"\") "+
			"select '2020-01-01'::timestamp + (i || ' hours')::interval, 'act' || (i % 7), "+
			"case when i % 2 = 0 then 'h' else 'd' end, i, 'text ' || i, -i, i / 3.0 from generate_series(1, 500) i",
		"alter table \"sact\" alter column \"Envoy\" set not null, alter column \"Envoy\" set default 0.0",
	)
	if hll {
		exec(
			"alter table \"sact\" add column \"an hll\" hll not null default hll_empty()",
			"update \"sact\" set \"an hll\" = hll_add(hll_empty(), hll_hash_text(series))",
		)
	}
	compactChurn(c, &ctx, "sact", 40)
	before := getCompactInfo(c, &ctx, "sact")
	if before.dropped != 40 || !strings.Contains(before.def, "column "+longName+" double precision") ||
		!strings.Contains(before.def, "pg_monitor=a*r*/") || !strings.Contains(before.def, "acl =r/") {
		t.Fatalf("unexpected test table: %+v", before)
	}

	// The default threshold (1300) is far away, compaction disabled, a threshold just above the used slots
	untouched("default threshold", "sact", before, lib.CompactTable(c, &ctx, "sact"))
	ctx.ColumnsCompactSlots = 0
	untouched("disabled", "sact", before, lib.CompactTable(c, &ctx, "sact"))
	ctx.ColumnsCompactSlots = before.slots + 1
	untouched("below the threshold", "sact", before, lib.CompactTable(c, &ctx, "sact"))
	untouched("missing table", "sact", before, lib.CompactTable(c, &ctx, "no such table"))

	// At the threshold: rebuilt
	ctx.ColumnsCompactSlots = before.slots
	rebuilt("at the threshold", "sact", before, lib.CompactTable(c, &ctx, "sact"))

	// Nothing more to release: the same threshold is reached again but no slot belongs to a dropped column
	after := getCompactInfo(c, &ctx, "sact")
	ctx.ColumnsCompactSlots = after.slots
	untouched("no dropped slots", "sact", after, lib.CompactTable(c, &ctx, "sact"))

	// The rebuilt table works as before: the writer and the 'columns' tool can add rows and columns
	exec(
		"insert into \"sact\"(time, series, period) values('2021-01-01', 'new', 'h')",
		"alter table \"sact\" add column if not exists \"New Group\" double precision not null default 0.0",
	)
	_, err := lib.ExecSQL(c, &ctx, "insert into \"sact\"(time, series, period) values('2021-01-01', 'new', 'h')")
	if err == nil || !strings.Contains(err.Error(), "duplicate key value violates unique constraint") {
		t.Errorf("expected a unique violation, got: %+v", err)
	}
	_, err = lib.ExecSQL(c, &ctx, "insert into \"sact\"(time, series, period, \"Kubernetes\") values('2021-01-02', 'new', 'h', -1)")
	if err == nil || !strings.Contains(err.Error(), "positive K") {
		t.Errorf("expected a check constraint violation, got: %+v", err)
	}

	// Another session holds a lock on the table: nothing happens, then the lock is released
	ctx.ColumnsCompactSlots = 10
	plain("slock", "")
	before = getCompactInfo(c, &ctx, "slock")
	tx, err := c.Begin()
	lib.FatalOnError(err)
	lib.ExecSQLTxWithErr(tx, &ctx, "lock table \"slock\" in access share mode")
	untouched("locked", "slock", before, lib.CompactTableWithLockTimeout(c, &ctx, "slock", "200ms"))
	lib.FatalOnError(tx.Rollback())
	rebuilt("unlocked", "slock", before, lib.CompactTableWithLockTimeout(c, &ctx, "slock", "200ms"))

	// A failure in the middle (the scratch schema cannot be created) changes nothing
	plain("sfail", "")
	before = getCompactInfo(c, &ctx, "sfail")
	scratch := ""
	lib.FatalOnError(lib.QueryRowSQL(c, &ctx, "select 'devstats_compact_' || md5('sfail')").Scan(&scratch))
	exec("create schema " + scratch)
	untouched("scratch schema exists", "sfail", before, lib.CompactTable(c, &ctx, "sfail"))
	exec("drop schema " + scratch)
	rebuilt("scratch schema dropped", "sfail", before, lib.CompactTable(c, &ctx, "sfail"))

	// A rebuilt table that would not be the same as the original (the copy has no per-column statistics target)
	// is rolled back after the swap
	plain("sstat", "")
	exec("alter table \"sstat\" alter column series set statistics 500")
	before = getCompactInfo(c, &ctx, "sstat")
	untouched("statistics target", "sstat", before, lib.CompactTable(c, &ctx, "sstat"))

	// Tables that are not plain series tables are left alone
	exec(
		"create table sref(id text primary key)",
		"create table sparent(time timestamp not null, series text not null, period text not null default '')",
		"create function strigf() returns trigger language plpgsql as $$ begin return new; end $$",
	)
	for _, test := range []struct {
		table  string
		create string
		sqls   []string
	}{
		{table: "sview", sqls: []string{"create view vsview as select series from \"sview\""}},
		{table: "strig", sqls: []string{"create trigger strigt before insert on \"strig\" for each row execute function strigf()"}},
		{table: "srule", sqls: []string{"create rule srulen as on delete to \"srule\" do instead nothing"}},
		{table: "sfkout", sqls: []string{"alter table \"sfkout\" add constraint sfkoutk foreign key (series) references sref(id)"}},
		{table: "sfkin", sqls: []string{
			"alter table \"sfkin\" add constraint sfkinu unique (series)",
			"create table sfkinref(series text references \"sfkin\"(series))",
		}},
		{table: "sexcl", sqls: []string{"alter table \"sexcl\" add constraint sexclx exclude using btree (series with =)"}},
		{table: "snotvalid", sqls: []string{"alter table \"snotvalid\" add constraint snotvalidc check (series <> '') not valid"}},
		{table: "sident", sqls: []string{"alter table \"sident\" add column id bigint generated always as identity"}},
		{table: "sgen", sqls: []string{"alter table \"sgen\" add column g text generated always as (series || period) stored"}},
		{table: "sserial", sqls: []string{"alter table \"sserial\" add column n bigserial"}},
		{table: "sextstat", sqls: []string{"create statistics sextstats on series, period from \"sextstat\""}},
		{table: "scolpriv", sqls: []string{"grant select (series) on \"scolpriv\" to public"}},
		{table: "sstorage", sqls: []string{"alter table \"sstorage\" set (fillfactor = 70)"}},
		{table: "srowsec", sqls: []string{"alter table \"srowsec\" enable row level security"}},
		{table: "sreplident", sqls: []string{"alter table \"sreplident\" replica identity full"}},
		{table: "scomment", sqls: []string{"comment on table \"scomment\" is 'a comment'"}},
		{table: "sclustered", sqls: []string{
			"create index sclusteredi on \"sclustered\"(time)",
			"cluster \"sclustered\" using sclusteredi",
		}},
		{table: "sinherit", sqls: []string{"alter table \"sinherit\" inherit sparent"}},
		{table: "sunlogged", create: "create unlogged table"},
	} {
		plain(test.table, test.create)
		exec(test.sqls...)
		before = getCompactInfo(c, &ctx, test.table)
		if before.slots != 12+strings.Count(strings.Join(test.sqls, ";"), " add column ") || before.dropped != 9 {
			t.Fatalf("unexpected '%s' table: %+v", test.table, before)
		}
		untouched("not a plain series table", test.table, before, lib.CompactTable(c, &ctx, test.table))
	}
	// A partitioned table is listed in pg_tables too
	exec("create table \"spartitioned\"(time timestamp not null, series text not null, period text not null default '') partition by range (time)")
	compactChurn(c, &ctx, "spartitioned", 9)
	before = getCompactInfo(c, &ctx, "spartitioned")
	untouched("partitioned", "spartitioned", before, lib.CompactTable(c, &ctx, "spartitioned"))

	// Many workers want to compact the same table at once: it is rebuilt exactly once
	plain("smany", "")
	exec("insert into \"smany\"(time, series, period) select '2020-01-01'::timestamp + (i || ' hours')::interval, 's', 'h' from generate_series(1, 2000) i")
	before = getCompactInfo(c, &ctx, "smany")
	results := make([]bool, 8)
	wg := sync.WaitGroup{}
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = lib.CompactTable(c, &ctx, "smany")
		}(i)
	}
	wg.Wait()
	compacted := 0
	for _, result := range results {
		if result {
			compacted++
		}
	}
	rebuilt("concurrent workers", "smany", before, compacted == 1)

	// A table that ran out of attribute slots refuses new columns until it is compacted (the default threshold)
	ctx.ColumnsCompactSlots = 1300
	exec(
		"create table \"sfull\"(time timestamp not null, series text not null, period text not null default '', "+
			"\"Kubernetes\" double precision not null default 0.0, primary key(time, series, period))",
		"insert into \"sfull\"(time, series, period, \"Kubernetes\") values('2020-01-01', 's', 'h', 1.5)",
	)
	compactChurn(c, &ctx, "sfull", 1596)
	before = getCompactInfo(c, &ctx, "sfull")
	_, err = lib.ExecSQL(c, &ctx, "alter table \"sfull\" add column \"Envoy\" double precision not null default 0.0")
	if before.slots != 1600 || err == nil || !strings.Contains(err.Error(), "tables can have at most 1600 columns") {
		t.Fatalf("expected a table without free attribute slots: %+v, error: %+v", before, err)
	}
	rebuilt("out of attribute slots", "sfull", before, lib.CompactTable(c, &ctx, "sfull"))
	exec("alter table \"sfull\" add column \"Envoy\" double precision not null default 0.0")
}

// TestRepoNameAt - the name a repository had at a given time comes from the native GH Archive events:
// the name of the first native event of the repository id at or after that time, else the current name
func TestRepoNameAt(t *testing.T) {
	var ctx lib.Ctx
	ctx.Init()
	ctx.TestMode = true

	// Do not allow to run tests in "gha" database
	if ctx.PgDB != "dbtest" {
		t.Errorf("tests can only be run on \"dbtest\" database")
		return
	}
	lib.DropDatabaseIfExists(&ctx)
	if !lib.CreateDatabaseIfNeeded(&ctx) {
		t.Errorf("failed to create database \"%s\"", ctx.PgDB)
	}
	defer func() {
		lib.DropDatabaseIfExists(&ctx)
	}()
	c := lib.PgConn(&ctx)
	defer func() { lib.FatalOnError(c.Close()) }()
	at := func(s string) time.Time {
		dt, err := time.Parse("2006-01-02 15:04:05", s)
		lib.FatalOnError(err)
		return dt
	}
	check := func(what string, repoID int64, current string, orgID interface{}, dt, expName string, expOrgID interface{}, expChanged bool) {
		t.Helper()
		name, nameOrgID, changed := lib.RepoNameAt(c, &ctx, repoID, current, orgID, at(dt))
		if name != expName || nameOrgID != expOrgID || changed != expChanged {
			t.Errorf(
				"%s: RepoNameAt(%d, %s, %v, %s) = (%s, %v, %v), expected (%s, %v, %v)",
				what, repoID, current, orgID, dt, name, nameOrgID, changed, expName, expOrgID, expChanged,
			)
		}
	}

	// Lookup failure (no tables yet): the current name, remembered for that repository id
	check("no tables", 99, "org/failed", int64(7), "2020-05-01 00:00:00", "org/failed", int64(7), false)
	check("no tables, again", 99, "org/failed", nil, "2020-05-01 00:00:00", "org/failed", nil, false)

	lib.ExecSQLWithErr(c, &ctx, "create table gha_repos(id bigint not null, name varchar(160) not null, org_id bigint, org_login varchar(100), primary key(id, name))")
	lib.ExecSQLWithErr(
		c, &ctx,
		"create table gha_events(id bigint not null primary key, type varchar(40) not null, actor_id bigint not null, repo_id bigint not null, "+
			"created_at timestamp not null, dup_actor_login varchar(120) not null, dup_repo_name varchar(160) not null, org_id bigint)",
	)
	lib.ExecSQLWithErr(c, &ctx, "create index events_repo_name_created_at_idx on gha_events(repo_id, dup_repo_name, created_at)")
	repo := func(id int64, name string, orgID interface{}) {
		lib.ExecSQLWithErr(c, &ctx, "insert into gha_repos(id, name, org_id) values($1, $2, $3)", id, name, orgID)
	}
	nextID := int64(1000)
	event := func(id, repoID int64, name string, orgID interface{}, dt string) {
		if id == 0 {
			nextID++
			id = nextID
		}
		lib.ExecSQLWithErr(
			c, &ctx,
			"insert into gha_events(id, type, actor_id, repo_id, created_at, dup_actor_login, dup_repo_name, org_id) values($1, 'PushEvent', 1, $2, $3, 'a', $4, $5)",
			id, repoID, at(dt), name, orgID,
		)
	}
	artificial := lib.ArtificialIDBase

	// 1: one name, native events: no lookup of the events at all
	repo(1, "org/one", int64(10))
	event(0, 1, "org/one", int64(10), "2020-01-01 00:00:00")
	event(0, 1, "org/one", int64(10), "2020-06-01 00:00:00")
	check("one name, before", 1, "org/one", int64(10), "2019-01-01 00:00:00", "org/one", int64(10), false)
	check("one name, inside", 1, "org/one", int64(10), "2020-03-01 00:00:00", "org/one", int64(10), false)
	check("one name, after", 1, "org/one", nil, "2021-01-01 00:00:00", "org/one", nil, false)
	// invalid input: the given name
	check("zero time", 1, "x/y", int64(3), "0001-01-01 00:00:00", "x/y", int64(3), false)
	check("no repository id", 0, "x/y", int64(3), "2020-03-01 00:00:00", "x/y", int64(3), false)
	check("negative repository id", -1, "x/y", nil, "2020-03-01 00:00:00", "x/y", nil, false)

	// 2: the database knows one name, GitHub answers with another one (not in gha_repos yet)
	repo(2, "old/two", int64(20))
	event(0, 2, "old/two", int64(20), "2020-01-01 00:00:00")
	event(0, 2, "old/two", int64(20), "2020-06-01 10:00:00")
	check("unknown current name, before the last event", 2, "new/two", int64(21), "2020-03-01 00:00:00", "old/two", int64(20), true)
	check("unknown current name, at the last event", 2, "new/two", int64(21), "2020-06-01 10:00:00", "old/two", int64(20), true)
	check("unknown current name, after the last event", 2, "new/two", int64(21), "2020-06-01 10:00:01", "new/two", int64(21), false)

	// 3: renamed once (transfer to another organization); artificial events never decide.
	// The organization id of a name is the one of its newest native event, else the gha_repos one
	repo(3, "old/three", int64(31))
	repo(3, "new/three", nil)
	event(0, 3, "old/three", nil, "2020-01-01 00:00:00")
	event(0, 3, "old/three", int64(30), "2020-06-01 10:00:00")
	event(0, 3, "new/three", int64(32), "2020-06-01 12:00:00")
	event(0, 3, "new/three", int64(32), "2020-12-01 00:00:00")
	event(artificial+1, 3, "old/three", int64(30), "2020-08-01 00:00:00")
	event(artificial+2, 3, "old/three", int64(30), "2021-06-01 00:00:00")
	event(-5, 3, "old/three", int64(30), "2020-09-01 00:00:00")
	event(artificial+3, 3, "new/three", int64(32), "2019-06-01 00:00:00")
	check("renamed, older than everything", 3, "new/three", int64(32), "2019-01-01 00:00:00", "old/three", int64(30), true)
	check("renamed, before", 3, "new/three", int64(32), "2020-03-01 00:00:00", "old/three", int64(30), true)
	check("renamed, the last event under the old name", 3, "new/three", int64(32), "2020-06-01 10:00:00", "old/three", int64(30), true)
	check("renamed, between", 3, "new/three", int64(32), "2020-06-01 11:00:00", "new/three", int64(32), false)
	check("renamed, the first event under the new name", 3, "new/three", int64(32), "2020-06-01 12:00:00", "new/three", int64(32), false)
	check("renamed, after, old artificial events around", 3, "new/three", int64(32), "2020-08-15 00:00:00", "new/three", int64(32), false)
	check("renamed, newer than everything", 3, "new/three", int64(32), "2021-01-01 00:00:00", "new/three", int64(32), false)
	// the caller's name is the old one (no heartbeat): the new name's time gets the new name and its organization
	check("renamed, caller has the old name, before", 3, "old/three", int64(30), "2020-03-01 00:00:00", "old/three", int64(30), false)
	check("renamed, caller has the old name, after", 3, "old/three", int64(30), "2020-07-01 00:00:00", "new/three", int64(32), true)
	check("renamed, caller has the old name, newer than everything", 3, "old/three", int64(30), "2021-01-01 00:00:00", "old/three", int64(30), false)

	// 4: renamed and renamed back; a user-owned name has no organization
	repo(4, "org/four", int64(40))
	repo(4, "user/four", nil)
	event(0, 4, "org/four", int64(40), "2020-01-01 00:00:00")
	event(0, 4, "org/four", int64(40), "2020-03-01 00:00:00")
	event(0, 4, "user/four", nil, "2020-04-01 00:00:00")
	event(0, 4, "user/four", nil, "2020-06-01 00:00:00")
	event(0, 4, "org/four", int64(40), "2020-07-01 00:00:00")
	event(0, 4, "org/four", int64(40), "2020-09-01 00:00:00")
	check("renamed back, first period", 4, "org/four", int64(40), "2020-02-01 00:00:00", "org/four", int64(40), false)
	check("renamed back, gap before the other name", 4, "org/four", int64(40), "2020-03-15 00:00:00", "user/four", nil, true)
	check("renamed back, other name", 4, "org/four", int64(40), "2020-05-01 00:00:00", "user/four", nil, true)
	check("renamed back, gap before the current name", 4, "org/four", int64(40), "2020-06-15 00:00:00", "org/four", int64(40), false)
	check("renamed back, last period", 4, "org/four", int64(40), "2020-08-01 00:00:00", "org/four", int64(40), false)
	check("renamed back, newer than everything", 4, "org/four", int64(40), "2020-10-01 00:00:00", "org/four", int64(40), false)

	// 5: two names, no native events at all
	repo(5, "a/five", int64(50))
	repo(5, "b/five", int64(51))
	event(artificial+10, 5, "a/five", int64(50), "2020-01-01 00:00:00")
	event(artificial+11, 5, "b/five", int64(51), "2020-06-01 00:00:00")
	check("no native events", 5, "b/five", int64(51), "2020-03-01 00:00:00", "b/five", int64(51), false)
	check("no native events, other name", 5, "c/five", nil, "2019-01-01 00:00:00", "c/five", nil, false)

	// 6: not in gha_repos at all
	check("unknown repository", 6, "any/six", int64(60), "2020-03-01 00:00:00", "any/six", int64(60), false)

	// 7: two names with a native event in the same second: the alphabetically first name
	repo(7, "tie/b", int64(70))
	repo(7, "tie/a", int64(71))
	event(0, 7, "tie/b", int64(70), "2020-01-01 00:00:00")
	event(0, 7, "tie/b", int64(70), "2020-03-01 00:00:00")
	event(0, 7, "tie/b", int64(70), "2020-06-01 00:00:00")
	event(0, 7, "tie/a", int64(71), "2020-06-01 00:00:00")
	event(0, 7, "tie/a", int64(71), "2020-09-01 00:00:00")
	check("tie", 7, "tie/b", int64(70), "2020-06-01 00:00:00", "tie/a", int64(71), true)
	check("tie, a second earlier", 7, "tie/b", int64(70), "2020-05-31 23:59:59", "tie/a", int64(71), true)
	check("tie, long before", 7, "tie/b", int64(70), "2020-02-01 00:00:00", "tie/b", int64(70), false)
	check("tie, after the last earlier event", 7, "tie/b", int64(70), "2020-03-01 00:00:01", "tie/a", int64(71), true)
	check("tie, after", 7, "tie/b", int64(70), "2020-06-01 00:00:01", "tie/a", int64(71), true)

	// 8: three names, the answer moves through all of them
	repo(8, "n1/eight", int64(81))
	repo(8, "n2/eight", int64(82))
	repo(8, "n3/eight", int64(83))
	event(0, 8, "n1/eight", int64(81), "2020-01-01 00:00:00")
	event(0, 8, "n1/eight", int64(81), "2020-02-01 00:00:00")
	event(0, 8, "n2/eight", int64(82), "2020-03-01 00:00:00")
	event(0, 8, "n2/eight", int64(82), "2020-04-01 00:00:00")
	event(0, 8, "n3/eight", int64(83), "2020-05-01 00:00:00")
	check("three names, first", 8, "n3/eight", int64(83), "2020-01-15 00:00:00", "n1/eight", int64(81), true)
	check("three names, second", 8, "n3/eight", int64(83), "2020-02-15 00:00:00", "n2/eight", int64(82), true)
	check("three names, second again", 8, "n3/eight", int64(83), "2020-03-15 00:00:00", "n2/eight", int64(82), true)
	check("three names, third", 8, "n3/eight", int64(83), "2020-04-15 00:00:00", "n3/eight", int64(83), false)
	check("three names, after", 8, "n3/eight", int64(83), "2020-06-01 00:00:00", "n3/eight", int64(83), false)

	// The answers are cached per repository id for the process: events written later at the newest end
	// (what the events feed does) do not change them
	event(0, 2, "new/two", int64(21), "2020-07-01 00:00:00")
	check("cached, after the last old event", 2, "new/two", int64(21), "2020-06-15 00:00:00", "new/two", int64(21), false)
	check("cached, before", 2, "new/two", int64(21), "2020-03-01 00:00:00", "old/two", int64(20), true)
}
