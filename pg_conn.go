package devstatscode

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq" // As suggested by lib/pq driver
)

var (
	sharedAffiliationsMtx sync.RWMutex
	sharedAffiliationsDB  *sql.DB
	sharedAffiliationsCtx *Ctx
)

// HandleRowIsTooBig - handle possible error like: Error: 'pq: row is too big: size 8744, maximum size 8160'
// rtry := lib.HandleRowIsTooBig(con, &ctx, table, info, addedCols, &mtx, err)
// con - connection
// ctx - context
// table - table on which "row too big" error may happen
// info - for debugging purposes, can be any string
// addedCols - columns that were already added (used if called from 'columns' app that might already add new columns and then those new will be least used), in other context this should be an empty map
// this is map of [table_name][column_name] -> empty struct (just to signal presence)
// mtx - mutext protecting that map, we don't need it in non-column mode
// err  -error from some DB operation (like add a column, modify constraint, insert/update) - all processing only happens when that err is present and is pq.Error with name "program_limit_exceeded"
// return value: false if nothing was dropped, true if columns were dropped so retry of last operation is needed (hence rtry name)
func HandleRowIsTooBig(con *sql.DB, ctx *Ctx, table, info string, addedCols map[string]map[string]struct{}, mtx *sync.Mutex, err error) bool {
	if err == nil {
		return false
	}
	switch e := err.(type) {
	case *pq.Error:
		errName := e.Code.Name()
		if errName == "program_limit_exceeded" {
			if strings.Contains(err.Error(), "pq: row is too big") {
				colsMap := map[string]struct{}{
					"time":   {},
					"series": {},
					"period": {},
				}
				if mtx != nil {
					mtx.Lock()
				}
				cols, ok := addedCols[table]
				if ok {
					for col := range cols {
						colsMap[col] = struct{}{}
					}
				}
				if mtx != nil {
					mtx.Unlock()
				}
				return DropLeastUsedCol(con, ctx, table, info, colsMap)
			}
		}
	}
	if !strings.Contains(err.Error(), "already exists") {
		Printf("Error handle row is too big %s: %+v\n", info, err)
	}
	return false
}

// GetCurrentTableColumns - return arry of string - current columns for given table
func GetCurrentTableColumns(con *sql.DB, ctx *Ctx, table string) ([]string, map[string]struct{}, error) {
	rows, err := QuerySQL(
		con,
		ctx,
		"select column_name from information_schema.columns where table_schema = 'public' and table_name = "+NValue(1),
		table,
	)
	if err != nil {
		Printf("Error select column_name %s: %+v\n", table, err)
		return []string{}, map[string]struct{}{}, err
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			Printf("Error rows close %s: %+v\n", table, err)
		}
	}()
	colNames := []string{}
	colNamesMap := make(map[string]struct{})
	for rows.Next() {
		var colName string
		er := rows.Scan(&colName)
		if er != nil {
			Printf("Error scan column name %s: %+v\n", table, er)
			return []string{}, map[string]struct{}{}, err
		}
		colNames = append(colNames, colName)
		colNamesMap[colName] = struct{}{}
	}
	err = rows.Err()
	if err != nil {
		Printf("Error rows error %s: %+v\n", table, err)
		return []string{}, map[string]struct{}{}, err
	}
	return colNames, colNamesMap, nil
}

// IdentifyColumnsToDelete - delete cols that are in 'currCols' and not in 'neededCols' excluding a few special ones
func IdentifyColumnsToDelete(currCols, neededCols []string) []string {
	toDrop := []string{}
	needed := make(map[string]struct{})
	for _, col := range neededCols {
		needed[col] = struct{}{}
	}
	for _, col := range currCols {
		lCol := strings.ToLower(col)
		if col == "time" || col == "series" || col == "period" || lCol == "all" || lCol == "none" {
			continue
		}
		_, need := needed[col]
		if !need {
			toDrop = append(toDrop, col)
		}
	}
	return toDrop
}

// CompactLockTimeout - how long CompactTable waits for the exclusive lock on the table (PostgreSQL interval)
const CompactLockTimeout = "15s"

// compactMtx - one table rebuild at a time in a process
var compactMtx sync.Mutex

// compactSlotsSQL - attribute slots used by a public table: all of them and those of the dropped columns
const compactSlotsSQL = `select count(*), count(*) filter (where a.attisdropped)
from pg_attribute a
where a.attnum > 0 and a.attrelid = (
  select c.oid from pg_class c join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'public' and c.relname = $1
)`

// compactPreflightSQL - one row for each reason why a public table cannot be rebuilt by CompactTable
const compactPreflightSQL = `with t as (
  select c.* from pg_class c join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'public' and c.relname = $1
)
select r.reason from t cross join lateral (values
  (1, t.relkind <> 'r', 'it is not an ordinary table'),
  (2, t.relpersistence <> 'p', 'it is not a permanent table'),
  (3, pg_get_userbyid(t.relowner) <> current_user, 'it is not owned by the connected role'),
  (4, t.relispartition or exists (
    select 1 from pg_inherits i where i.inhrelid = t.oid or i.inhparent = t.oid
  ), 'it is a partition or takes part in inheritance'),
  (5, t.reltablespace <> 0, 'it uses a non-default tablespace'),
  (6, t.reloptions is not null or exists (
    select 1 from pg_class x where x.oid = t.reltoastrelid and x.reloptions is not null
  ), 'it has storage options'),
  (7, t.reloftype <> 0, 'it is a typed table'),
  (8, t.relrowsecurity or t.relforcerowsecurity or exists (
    select 1 from pg_policy p where p.polrelid = t.oid
  ), 'it uses row security'),
  (9, t.relreplident <> 'd', 'it has a non-default replica identity'),
  (10, exists (select 1 from pg_publication_rel p where p.prrelid = t.oid), 'it is published'),
  (11, exists (select 1 from pg_trigger g where g.tgrelid = t.oid and not g.tgisinternal), 'it has triggers'),
  (12, exists (select 1 from pg_rewrite w where w.ev_class = t.oid), 'it has rules'),
  (13, exists (
    select 1 from pg_depend d join pg_rewrite w on w.oid = d.objid
    where d.classid = 'pg_rewrite'::regclass and d.refclassid = 'pg_class'::regclass
      and d.refobjid = t.oid and w.ev_class <> t.oid
  ), 'views depend on it'),
  (14, exists (
    select 1 from pg_constraint k where k.contype = 'f' and (k.conrelid = t.oid or k.confrelid = t.oid)
  ), 'it has foreign keys'),
  (15, exists (
    select 1 from pg_constraint k
    where k.conrelid = t.oid and (k.contype not in ('p', 'u', 'c', 'n', 'f') or not k.convalidated)
  ), 'it has unsupported constraints'),
  (16, exists (
    select 1 from pg_attribute a
    where a.attrelid = t.oid and a.attnum > 0 and not a.attisdropped
      and (a.attidentity <> '' or a.attgenerated <> '')
  ), 'it has identity or generated columns'),
  (17, exists (
    select 1 from pg_depend d join pg_class s on s.oid = d.objid
    where d.classid = 'pg_class'::regclass and d.refclassid = 'pg_class'::regclass
      and d.refobjid = t.oid and s.relkind = 'S'
  ) or exists (
    select 1 from pg_attrdef ad
    join pg_depend d on d.classid = 'pg_attrdef'::regclass and d.objid = ad.oid
    join pg_class s on s.oid = d.refobjid
    where ad.adrelid = t.oid and d.refclassid = 'pg_class'::regclass and s.relkind = 'S'
  ), 'it uses sequences'),
  (18, exists (select 1 from pg_statistic_ext e where e.stxrelid = t.oid), 'it has extended statistics'),
  (19, exists (
    select 1 from pg_attribute a
    where a.attrelid = t.oid and a.attnum > 0 and not a.attisdropped and a.attacl is not null
  ), 'it has column privileges'),
  (20, exists (
    select 1 from aclexplode(t.relacl) a where a.grantor <> t.relowner
  ), 'it has privileges granted by another role'),
  (21, exists (
    select 1 from pg_index i join pg_class x on x.oid = i.indexrelid
    where i.indrelid = t.oid and (
      not i.indisvalid or not i.indisready or not i.indislive or i.indisclustered or i.indisreplident
      or x.reltablespace <> 0
    )
  ), 'it has special indexes'),
  (22, exists (
    select 1 from pg_description x
    where (x.classoid = 'pg_class'::regclass and x.objsubid = 0 and (
      x.objoid = t.oid or x.objoid in (select i.indexrelid from pg_index i where i.indrelid = t.oid)
    )) or (x.classoid = 'pg_constraint'::regclass and x.objoid in (
      select k.oid from pg_constraint k where k.conrelid = t.oid
    ))
  ), 'it has comments'),
  (23, exists (
    select 1 from pg_seclabel s where s.classoid = 'pg_class'::regclass and s.objoid = t.oid
  ), 'it has security labels'),
  (24, exists (
    select 1 from pg_depend d
    where d.classid = 'pg_class'::regclass and d.objid = t.oid and d.deptype = 'e'
  ), 'it belongs to an extension')
) as r(ord, failed, reason)
where r.failed
order by r.ord`

// compactSignatureSQL - the definition of a public table that a rebuild must preserve:
// live columns in order, constraints, indexes and the table's own properties (owner, privileges, ...)
const compactSignatureSQL = `with t as (
  select c.* from pg_class c join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'public' and c.relname = $1
)
select
  coalesce((
    select string_agg(format(
      '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s', a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull,
      pg_get_expr(d.adbin, d.adrelid), a.attcollation, a.attstorage, a.attcompression, a.attstattarget,
      a.attoptions, a.attidentity, a.attgenerated, a.attacl
    ), E'\n' order by a.attnum)
    from pg_attribute a left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
    where a.attrelid = t.oid and a.attnum > 0 and not a.attisdropped
  ), ''),
  coalesce((
    select string_agg(format(
      '%s|%s|%s|%s|%s|%s|%s', k.conname, k.contype, pg_get_constraintdef(k.oid), k.convalidated,
      k.condeferrable, k.condeferred, k.connoinherit
    ), E'\n' order by k.conname)
    from pg_constraint k where k.conrelid = t.oid
  ), ''),
  coalesce((
    select string_agg(format(
      '%s|%s|%s|%s|%s|%s|%s|%s', x.relname, pg_get_indexdef(i.indexrelid), i.indisprimary, i.indisunique,
      i.indisvalid, i.indisclustered, i.indisreplident, x.reltablespace
    ), E'\n' order by x.relname)
    from pg_index i join pg_class x on x.oid = i.indexrelid where i.indrelid = t.oid
  ), ''),
  format(
    '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s', t.relkind, t.relpersistence, pg_get_userbyid(t.relowner),
    t.reltablespace, t.reloptions, t.relrowsecurity, t.relforcerowsecurity, t.relreplident, t.reloftype,
    t.relispartition, (
      select array_agg(x order by x)
      from unnest(coalesce(t.relacl, acldefault('r', t.relowner))::text[]) x
    )
  )
from t`

// compactDefsSQL - statements that re-create what "create table (like ... excluding indexes)" does not copy:
// primary key and unique constraints, the other indexes and the privileges
const compactDefsSQL = `with t as (
  select c.* from pg_class c join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'public' and c.relname = $1
)
select d.sql from (
  select 1 as ord, k.conname::text as name, format(
    'alter table public.%I add constraint %I %s', t.relname, k.conname, pg_get_constraintdef(k.oid)
  ) as sql
  from t join pg_constraint k on k.conrelid = t.oid where k.contype in ('p', 'u')
  union all
  select 2, x.relname::text, pg_get_indexdef(i.indexrelid)
  from t join pg_index i on i.indrelid = t.oid join pg_class x on x.oid = i.indexrelid
  where not exists (select 1 from pg_constraint k where k.conindid = i.indexrelid)
  union all
  select 3, pg_get_userbyid(a.grantee)::text || a.is_grantable::text, format(
    'grant %s on public.%I to %s%s', string_agg(a.privilege_type, ', ' order by a.privilege_type), t.relname,
    case when a.grantee = 0 then 'public' else quote_ident(pg_get_userbyid(a.grantee)) end,
    case when a.is_grantable then ' with grant option' else '' end
  )
  from t cross join lateral aclexplode(t.relacl) a where a.grantee <> t.relowner
  group by t.relname, a.grantee, a.is_grantable
) d
order by d.ord, d.name`

// pgQuoteIdent - quotes SQL identifier
func pgQuoteIdent(name string) string {
	return "\"" + strings.Replace(name, "\"", "\"\"", -1) + "\""
}

// compactTableTx - rebuilds the table in a single transaction: either everything is done or nothing changes
// returns: copied rows, slots before, dropped slots before, slots after, reason why nothing was done
// reason is "" on success and "-" when (under the lock) there is nothing to do
func compactTableTx(con *sql.DB, ctx *Ctx, table, lockTimeout string) (nRows int64, slots, dropped, newSlots int, reason string) {
	tx, err := con.Begin()
	if err != nil {
		reason = err.Error()
		return
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	fail := func(e error) (int64, int, int, int, string) {
		return nRows, slots, dropped, newSlots, e.Error()
	}
	qTable := "public." + pgQuoteIdent(table)
	_, err = ExecSQLTx(tx, ctx, "set local lock_timeout = '"+strings.Replace(lockTimeout, "'", "''", -1)+"'")
	if err != nil {
		return fail(err)
	}
	_, err = ExecSQLTx(tx, ctx, "lock table "+qTable+" in access exclusive mode")
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "55P03" {
			reason = "could not lock it within " + lockTimeout
			return
		}
		return fail(err)
	}
	// Other 'columns' worker could have rebuilt it while we were waiting for the lock
	err = QueryRowSQLTx(tx, ctx, compactSlotsSQL, table).Scan(&slots, &dropped)
	if err != nil {
		return fail(err)
	}
	if slots < ctx.ColumnsCompactSlots || dropped == 0 {
		reason = "-"
		return
	}
	queryStrings := func(query string) ([]string, error) {
		rows, er := QuerySQLTx(tx, ctx, query, table)
		if er != nil {
			return nil, er
		}
		defer func() { _ = rows.Close() }()
		strs := []string{}
		for rows.Next() {
			str := ""
			er = rows.Scan(&str)
			if er != nil {
				return nil, er
			}
			strs = append(strs, str)
		}
		return strs, rows.Err()
	}
	reasons, err := queryStrings(compactPreflightSQL)
	if err != nil {
		return fail(err)
	}
	if len(reasons) > 0 {
		reason = strings.Join(reasons, ", ")
		return
	}
	Printf(
		"Compacting '%s' table: %d attribute slots (%d dropped, %d live), threshold %d\n",
		table, slots, dropped, slots-dropped, ctx.ColumnsCompactSlots,
	)
	var before, after [4]string
	err = QueryRowSQLTx(tx, ctx, compactSignatureSQL, table).Scan(&before[0], &before[1], &before[2], &before[3])
	if err != nil {
		return fail(err)
	}
	defs, err := queryStrings(compactDefsSQL)
	if err != nil {
		return fail(err)
	}
	scratch := ""
	err = QueryRowSQLTx(tx, ctx, "select 'devstats_compact_' || md5($1::text)", table).Scan(&scratch)
	if err != nil {
		return fail(err)
	}
	qScratch := pgQuoteIdent(scratch)
	qCopy := qScratch + "." + pgQuoteIdent(table)
	_, err = ExecSQLTx(tx, ctx, "create schema "+qScratch)
	if err != nil {
		return fail(err)
	}
	// The copy is created under its final name, so all generated constraint names are the same as in the original
	_, err = ExecSQLTx(tx, ctx, "create table "+qCopy+" (like "+qTable+" including all excluding indexes)")
	if err != nil {
		return fail(err)
	}
	res, err := ExecSQLTx(tx, ctx, "insert into "+qCopy+" select * from "+qTable)
	if err != nil {
		return fail(err)
	}
	nRows, err = res.RowsAffected()
	if err != nil {
		return fail(err)
	}
	var origRows int64
	err = QueryRowSQLTx(tx, ctx, "select count(*) from "+qTable).Scan(&origRows)
	if err != nil {
		return fail(err)
	}
	if origRows != nRows {
		reason = fmt.Sprintf("copied %d rows but the table has %d rows", nRows, origRows)
		return
	}
	stmts := []string{
		"drop table " + qTable,
		"alter table " + qCopy + " set schema public",
		"drop schema " + qScratch,
	}
	stmts = append(stmts, defs...)
	for _, stmt := range stmts {
		_, err = ExecSQLTx(tx, ctx, stmt)
		if err != nil {
			return fail(err)
		}
	}
	err = QueryRowSQLTx(tx, ctx, compactSignatureSQL, table).Scan(&after[0], &after[1], &after[2], &after[3])
	if err != nil {
		return fail(err)
	}
	diffs := []string{}
	for i, part := range []string{"columns", "constraints", "indexes", "table properties"} {
		if before[i] != after[i] {
			diffs = append(diffs, part)
		}
	}
	if len(diffs) > 0 {
		reason = "the rebuilt table would not match the original (" + strings.Join(diffs, ", ") + ")"
		return
	}
	newDropped := 0
	err = QueryRowSQLTx(tx, ctx, compactSlotsSQL, table).Scan(&newSlots, &newDropped)
	if err != nil {
		return fail(err)
	}
	if newSlots != slots-dropped || newDropped != 0 {
		reason = fmt.Sprintf("the rebuilt table would use %d attribute slots (%d dropped), expected %d", newSlots, newDropped, slots-dropped)
		return
	}
	err = tx.Commit()
	if err != nil {
		return fail(err)
	}
	committed = true
	return
}

// CompactTable - rebuilds a public series table with only its live columns
// PostgreSQL never reuses the attribute slot of a dropped column and a table can use at most 1600 slots,
// so a table whose columns are added and dropped again and again finally refuses new columns.
// The table is rebuilt when it uses at least ctx.ColumnsCompactSlots slots and some of them are dropped columns.
// Everything happens in one transaction under an exclusive table lock: the table is either fully rebuilt
// (the same rows, columns, defaults, constraints, indexes and privileges) or left untouched with a warning.
// It never fails the caller. Returns true when the table was rebuilt.
func CompactTable(con *sql.DB, ctx *Ctx, table string) bool {
	return CompactTableWithLockTimeout(con, ctx, table, CompactLockTimeout)
}

// CompactTableWithLockTimeout - CompactTable waiting for the table lock for at most lockTimeout (PostgreSQL interval)
func CompactTableWithLockTimeout(con *sql.DB, ctx *Ctx, table, lockTimeout string) bool {
	if ctx.ColumnsCompactSlots <= 0 {
		return false
	}
	var slots, dropped int
	err := QueryRowSQL(con, ctx, compactSlotsSQL, table).Scan(&slots, &dropped)
	if err != nil {
		Printf("Warning: cannot compact '%s' table: %+v\n", table, err)
		return false
	}
	if slots < ctx.ColumnsCompactSlots || dropped == 0 {
		return false
	}
	compactMtx.Lock()
	defer compactMtx.Unlock()
	nRows, lockedSlots, lockedDropped, newSlots, reason := compactTableTx(con, ctx, table, lockTimeout)
	if reason == "-" {
		return false
	}
	if reason != "" {
		if lockedSlots > 0 {
			slots, dropped = lockedSlots, lockedDropped
		}
		Printf("Warning: cannot compact '%s' table (%d attribute slots, %d dropped): %s\n", table, slots, dropped, reason)
		return false
	}
	_, err = ExecSQL(con, ctx, "analyze public."+pgQuoteIdent(table))
	if err != nil {
		Printf("Warning: cannot analyze compacted '%s' table: %+v\n", table, err)
	}
	Printf("Compacted '%s' table: %d rows, %d -> %d attribute slots\n", table, nRows, lockedSlots, newSlots)
	return true
}

// DropLeastUsedCol - drops two least used columns from table
// con - connection
// ctx - context
// table - table on which "row too big" error may happen
// info - for debugging purposes, can be any string
// protectedCols - map with column names that cannot be dropped - they are time, series, period 9always)  + eventually columns that were added before via 'columns' program
func DropLeastUsedCol(con *sql.DB, ctx *Ctx, table, info string, protectedCols map[string]struct{}) bool {
	rows, err := QuerySQL(
		con,
		ctx,
		"select column_name from information_schema.columns where table_schema = 'public' and table_name = "+NValue(1),
		table,
	)
	if err != nil {
		Printf("Error select column_name (ignored) %s: %+v\n", info, err)
		return false
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			Printf("Error rows close (ignored) %s: %+v\n", info, err)
		}
	}()
	colNames := []string{}
	for rows.Next() {
		var colName string
		er := rows.Scan(&colName)
		if er != nil {
			Printf("Error scan column name (ignored) %s: %+v\n", info, er)
			return false
		}
		_, protected := protectedCols[colName]
		if !protected {
			colNames = append(colNames, colName)
		}
	}
	err = rows.Err()
	if err != nil {
		Printf("Error rows error (ignored) %s: %+v\n", info, err)
		return false
	}
	sort.Strings(colNames)
	if ctx.Debug > 0 {
		Printf("Table '%s' has %d columns: %+v\n", table, len(colNames), colNames)
	} else {
		Printf("Table '%s' has %d column\n", table, len(colNames))
	}
	if len(colNames) < 80 {
		// No cleanup needed if less than 80 columns
		return false
	}
	query := "select "
	for _, col := range colNames {
		query += `coalesce(avg("` + col + `"), 0.0), `
	}
	query = query[:len(query)-2] + ` from "` + table + `"`
	rows, err = QuerySQL(con, ctx, query)
	if err != nil {
		Printf("Error avg (ignored) %s: %+v\n", info, err)
		return false
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			Printf("Error avg rows close (ignored) %s: %+v\n", info, err)
		}
	}()
	colAvgs := make([]float64, len(colNames))
	scanArgs := make([]interface{}, len(colNames))
	for i := range colAvgs {
		scanArgs[i] = &colAvgs[i]
	}
	for rows.Next() {
		er := rows.Scan(scanArgs...)
		if er != nil {
			Printf("Error avg scan (ignored) %s: %+v\n", info, er)
			return false
		}
	}
	err = rows.Err()
	if err != nil {
		Printf("Error avg rows (ignored) %s: %+v\n", info, err)
		return false
	}
	if ctx.Debug > 0 {
		Printf("Table '%s' columns averages: %+v\n", table, colAvgs)
	}
	if len(colAvgs) < 80 {
		// No cleanup needed if less than 80 columns
		return false
	}
	var min1, min2 int
	if colAvgs[1] < colAvgs[0] {
		min1, min2 = 1, 0
	} else {
		min1, min2 = 0, 1
	}

	for i := 2; i < len(colAvgs); i++ {
		if colAvgs[i] < colAvgs[min1] {
			min2 = min1
			min1 = i
		} else if colAvgs[i] < colAvgs[min2] {
			min2 = i
		}
	}
	Printf(
		"Two least used columns are: '%s' and '%s' with averages: %f, %f, indices: %d, %d\n",
		colNames[min1], colNames[min2], colAvgs[min1], colAvgs[min2], min1, min2,
	)
	_, err = ExecSQL(
		con,
		ctx,
		"alter table \""+table+"\" drop column if exists \""+colNames[min1]+"\"",
	)
	if err != nil {
		Printf("Error drop columns 1 (ignored) %s: %+v\n", info, err)
		return false
	}
	_, err = ExecSQL(
		con,
		ctx,
		"alter table \""+table+"\" drop column if exists \""+colNames[min2]+"\"",
	)
	if err != nil {
		Printf("Error drop column 2 (ignored) %s: %+v\n", info, err)
		return false
	}
	Printf("Dropped '%s' and '%s' from '%s' table\n", colNames[min1], colNames[min2], table)
	return true
}

// WriteTSPoints write batch of points to postgresql, splitting large batches into chunks of up to 1000 elements
func WriteTSPoints(ctx *Ctx, con *sql.DB, pts *TSPoints, mergeSeries string, hllEmpty []uint8, mut *sync.Mutex) {
	npts := len(*pts)
	if npts == 0 {
		return
	}
	Printf("WriteTSPoints: writing %d points in batches of up to 1000\n", npts)

	const batchSize = 1000
	for i := 0; i < npts; i += batchSize {
		end := i + batchSize
		if end > npts {
			end = npts
		}
		batch := TSPoints((*pts)[i:end])
		WriteTSPointsBatch(ctx, con, &batch, mergeSeries, hllEmpty, mut)
	}
}

// WriteTSPointsBatch write batch of points to postgresql (internal implementation)
func WriteTSPointsBatch(ctx *Ctx, con *sql.DB, pts *TSPoints, mergeSeries string, hllEmpty []uint8, mut *sync.Mutex) {
	npts := len(*pts)
	if npts == 0 {
		return
	}
	Printf("WriteTSPointsBatch: writing %d points\n", npts)
	if ctx.Debug > 0 {
		Printf("Points:\n%+v\n", pts.Str())
	}
	defer func() { Printf("WriteTSPointsBatch: writing %d points - finished\n", npts) }()
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
			// FIXME: can fail due to 'Ignored alter table "sact" add column if not exists "col-name" double precision not null default 0.0: pq: tables can have at most 1600 columns'
			// I *think* the correct handling is to ensure that columns program is always called, even in "daily sync"
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

// SetSharedAffiliationsDB sets a process-wide direct connection used for shared actor inserts.
func SetSharedAffiliationsDB(con *sql.DB, ctx *Ctx) {
	sharedAffiliationsMtx.Lock()
	sharedAffiliationsDB = con
	sharedAffiliationsCtx = ctx
	sharedAffiliationsMtx.Unlock()
}

func getSharedAffiliationsDB(ctx *Ctx) (*sql.DB, *Ctx) {
	sharedAffiliationsMtx.RLock()
	con, actx := sharedAffiliationsDB, sharedAffiliationsCtx
	sharedAffiliationsMtx.RUnlock()
	if con != nil {
		return con, actx
	}

	sharedAffiliationsMtx.Lock()
	defer sharedAffiliationsMtx.Unlock()
	if sharedAffiliationsDB == nil {
		sharedAffiliationsCtx = ctx.CopyContext()
		sharedAffiliationsDB = PgConnDB(sharedAffiliationsCtx, ctx.AffiliationsDB)
	}
	return sharedAffiliationsDB, sharedAffiliationsCtx
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

func execSQLAutocommitTransactionRetry(con *sql.DB, ctx *Ctx, query string, args ...interface{}) sql.Result {
	var lastErr error
	for attempt := 0; ; attempt++ {
		res, err := ExecSQL(con, ctx, query, args...)
		if err == nil {
			return res
		}
		lastErr = err

		var pqErr *pq.Error
		retryTransaction := errors.As(err, &pqErr) && (pqErr.Code == "40001" || pqErr.Code == "40P01")
		if retryTransaction {
			fmt.Fprintf(
				os.Stderr,
				"PqError: code=%s, name=%s, detail=%s; retrying complete autocommit transaction\n",
				pqErr.Code,
				pqErr.Code.Name(),
				pqErr.Detail,
			)
		} else {
			queryOut(query, args...)
			status := FatalOnError(err)
			if status != Retry && status != Reconnect {
				Fatalf("unexpected shared actor insert status %q: %v", status, err)
			}
		}

		if attempt >= len(ctx.Trials) {
			break
		}
		delay := ctx.Trials[attempt]
		fmt.Fprintf(os.Stderr, "Will retry shared actor insert after %d seconds...\n", delay)
		time.Sleep(time.Duration(delay) * time.Second)
	}

	fmt.Printf("Failed sql: ")
	queryOut(query, args...)
	Fatalf("shared actor insert failed after %d attempts: %v", len(ctx.Trials)+1, lastErr)
	return nil
}

// InsertActorTx inserts an actor in the current transaction in legacy mode and directly
// into the shared affiliations database as a retryable autocommit statement in shared mode.
func InsertActorTx(con *sql.Tx, ctx *Ctx, id interface{}, login, name string) sql.Result {
	query := InsertIgnore("into gha_actors(id, login, name) " + NValues(3))
	args := AnyArray{id, login, name}
	if ctx.AffiliationsDB == "" {
		return ExecSQLTxWithErr(con, ctx, query, args...)
	}
	sharedDB, sharedCtx := getSharedAffiliationsDB(ctx)
	return execSQLAutocommitTransactionRetry(sharedDB, sharedCtx, query, args...)
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
