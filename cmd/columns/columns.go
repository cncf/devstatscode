package main

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	lib "github.com/cncf/devstatscode"
	yaml "gopkg.in/yaml.v2"

	"github.com/lib/pq"
)

// columns contains list of columns that must be present on a certain series
type columns struct {
	Columns []column `yaml:"columns"`
}

// column contain configuration of columns needed on a specific series
type column struct {
	TableRegexp string `yaml:"table_regexp"`
	Tag         string `yaml:"tag"`
	Column      string `yaml:"column"`
	HLL         bool   `yaml:"hll"`
}

func dropLeastUsedCol(con *sql.DB, ctx *lib.Ctx, table, info string, protectedCols map[string]struct{}) bool {
	rows, err := lib.QuerySQL(
		con,
		ctx,
		"select column_name from information_schema.columns where table_schema = 'public' and table_name = "+lib.NValue(1),
		table,
	)
	if err != nil {
		lib.Printf("Error (ignored) %s: %+v", info, err)
		return false
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			lib.Printf("Error (ignored) %s: %+v", info, err)
		}
	}()
	colNames := []string{}
	for rows.Next() {
		var colName string
		er := rows.Scan(&colName)
		if er != nil {
			lib.Printf("Error (ignored) %s: %+v", info, er)
			return false
		}
		_, protected := protectedCols[colName]
		if !protected {
			colNames = append(colNames, colName)
		}
	}
	err = rows.Err()
	if err != nil {
		lib.Printf("Error (ignored) %s: %+v", info, err)
		return false
	}
	if ctx.Debug > 0 {
		lib.Printf("Table '%s' columns: %+v\n", table, colNames)
	} else {
		lib.Printf("Table '%s' has %d column\n", table, len(colNames))
	}
	if len(colNames) < 80 {
		// No cleanup needed if less than 80 columns
		return false
	}
	query := "select "
	for _, col := range colNames {
		query += `avg("` + col + `"), `
	}
	query = query[:len(query)-2] + ` from "` + table + `"`
	rows, err = lib.QuerySQL(con, ctx, query)
	if err != nil {
		lib.Printf("Error (ignored) %s: %+v", info, err)
		return false
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			lib.Printf("Error (ignored) %s: %+v", info, err)
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
			lib.Printf("Error (ignored) %s: %+v", info, er)
			return false
		}
	}
	err = rows.Err()
	if err != nil {
		lib.Printf("Error (ignored) %s: %+v", info, err)
		return false
	}
	if ctx.Debug > 0 {
		lib.Printf("Table '%s' columns averages: %+v\n", table, colAvgs)
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
	lib.Printf(
		"Two least used columns are: '%s' and '%s' with averages: %f, %f, indices: %d, %d\n",
		colNames[min1], colNames[min2], colAvgs[min1], colAvgs[min2], min1, min2,
	)
	_, err = lib.ExecSQL(
		con,
		ctx,
		"alter table \""+table+"\" drop column \""+colNames[min1]+"\"",
	)
	if err != nil {
		lib.Printf("Error (ignored) %s: %+v", info, err)
		return false
	}
	_, err = lib.ExecSQL(
		con,
		ctx,
		"alter table \""+table+"\" drop column \""+colNames[min2]+"\"",
	)
	if err != nil {
		lib.Printf("Error (ignored) %s: %+v", info, err)
		return false
	}
	lib.Printf("Dropped '%s' and '%s' from '%s' table\n", colNames[min1], colNames[min2], table)
	return true
}

func handleRowIsTooBig(con *sql.DB, ctx *lib.Ctx, table, info string, addedCols map[string]map[string]struct{}, mtx *sync.Mutex, err error) bool {
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
				mtx.Lock()
				cols, ok := addedCols[table]
				if ok {
					for col := range cols {
						colsMap[col] = struct{}{}
					}
				}
				mtx.Unlock()
				return dropLeastUsedCol(con, ctx, table, info, colsMap)
			}
		}
	}
	if !strings.Contains(err.Error(), "already exists") {
		lib.Printf("Error %s: %+v", info, err)
	}
	return false
}

// Ensure that specific TSDB series have all needed columns
func ensureColumns() {
	// Environment context parse
	var ctx lib.Ctx
	ctx.Init()
	lib.SetupTimeoutSignal(&ctx)

	// If skip TSDB or only ES output - nothing to do
	if ctx.SkipTSDB {
		return
	}

	// Connect to Postgres DB
	con := lib.PgConn(&ctx)
	defer func() { lib.FatalOnError(con.Close()) }()

	// Local or cron mode?
	dataPrefix := ctx.DataDir
	if ctx.Local {
		dataPrefix = "./"
	}

	// Read columns config
	data, err := lib.ReadFile(&ctx, dataPrefix+ctx.ColumnsYaml)
	if err != nil {
		lib.FatalOnError(err)
		return
	}
	var allColumns columns
	lib.FatalOnError(yaml.Unmarshal(data, &allColumns))
	if ctx.Debug > 0 {
		lib.Printf("Read %d columns configs from '%s'\n", len(allColumns.Columns), dataPrefix+ctx.ColumnsYaml)
	}

	thrN := lib.GetThreadsNum(&ctx)
	ch := make(chan [3][]string)
	nThreads := 0
	allTables := []string{}
	allCols := []string{}
	allHLLs := []string{}
	maxTrials := 3
	addedCols := make(map[string]map[string]struct{})
	mtx := sync.Mutex{}
	// Use integer index to pass to go rountine
	for i := range allColumns.Columns {
		go func(ch chan [3][]string, idx int) {
			tables := []string{}
			cols := []string{}
			hlls := []string{}
			// Refer to current column config using index passed to anonymous function
			col := &allColumns.Columns[idx]
			if ctx.Debug > 0 {
				lib.Printf("Ensure column config: %+v\n", col)
			}
			crows := lib.QuerySQLWithErr(
				con,
				&ctx,
				fmt.Sprintf(
					"select \"%s\" from \"%s\" order by time asc",
					col.Column,
					col.Tag,
				),
			)
			var colType string
			if col.HLL {
				colType = "hll"
			} else {
				colType = "double precision"
			}
			defer func() { lib.FatalOnError(crows.Close()) }()
			colName := ""
			colNames := []string{}
			for crows.Next() {
				lib.FatalOnError(crows.Scan(&colName))
				colNames = append(colNames, colName)
			}
			lib.FatalOnError(crows.Err())
			if len(colNames) == 0 {
				lib.Printf("Warning: no tag values for (%s, %s)\n", col.Column, col.Tag)
				if ch != nil {
					ch <- [3][]string{tables, cols, hlls}
				}
				return
			}
			if ctx.Debug > 0 {
				lib.Printf("Ensure columns: %+v --> %+v\n", col, colNames)
			}
			rows := lib.QuerySQLWithErr(
				con,
				&ctx,
				fmt.Sprintf(
					"select tablename from pg_catalog.pg_tables where "+
						// "schemaname = 'public' and substring(tablename from %s) is not null",
						"schemaname = 'public' and tablename ~ %s",
					lib.NValue(1),
				),
				col.TableRegexp,
			)
			defer func() { lib.FatalOnError(rows.Close()) }()
			table := ""
			numTables := 0
			for rows.Next() {
				lib.FatalOnError(rows.Scan(&table))
				for _, colName := range colNames {
					trials := 0
				retryCol:
					_, err := lib.ExecSQL(
						con,
						&ctx,
						"alter table \""+table+"\" add column \""+colName+"\" "+colType,
					)
					if err == nil {
						mtx.Lock()
						_, ok := addedCols[table]
						if !ok {
							addedCols[table] = make(map[string]struct{})
						}
						addedCols[table][colName] = struct{}{}
						mtx.Unlock()
						lib.Printf("Added column \"%s\" to \"%s\" table\n", colName, table)
						tables = append(tables, table)
						cols = append(cols, colName)
						if col.HLL {
							hlls = append(hlls, "y")
						} else {
							hlls = append(hlls, "n")
						}
						//} else {
						//	lib.Printf("%+v\n", err)
					} else {
						info := "add column " + colName + "/" + colType
						rtry := handleRowIsTooBig(con, &ctx, table, info, addedCols, &mtx, err)
						if rtry {
							trials++
							if trials < maxTrials {
								goto retryCol
							} else {
								lib.Printf("Give up '%s' after %d trials\n", info, maxTrials)
							}
						}
					}
				}
				numTables++
			}
			lib.FatalOnError(rows.Err())
			if numTables == 0 {
				lib.Printf("Warning: '%+v': no table hits\n", col)
			}
			// Synchronize go routine
			if ch != nil {
				ch <- [3][]string{tables, cols, hlls}
			}
		}(ch, i)
		// go routine called with 'ch' channel to sync and column config index
		nThreads++
		if nThreads >= thrN {
			data := <-ch
			tables := data[0]
			cols := data[1]
			hlls := data[2]
			for i, table := range tables {
				col := cols[i]
				hll := hlls[i]
				allTables = append(allTables, table)
				allCols = append(allCols, col)
				allHLLs = append(allHLLs, hll)
			}
			nThreads--
		}
	}
	// Usually all work happens on '<-ch'
	for nThreads > 0 {
		data := <-ch
		tables := data[0]
		cols := data[1]
		hlls := data[2]
		for i, table := range tables {
			col := cols[i]
			hll := hlls[i]
			allTables = append(allTables, table)
			allCols = append(allCols, col)
			allHLLs = append(allHLLs, hll)
		}
		nThreads--
	}
	if ctx.Debug > 1 {
		lib.Printf("Tables: %+v\n", allTables)
		lib.Printf("Columns: %+v\n", allCols)
		lib.Printf("HLLs: %+v\n", allHLLs)
	}
	cfg := make(map[string]map[string]string)
	for i, table := range allTables {
		col := allCols[i]
		hll := allHLLs[i]
		_, ok := cfg[table]
		if !ok {
			cfg[table] = make(map[string]string)
		}
		cfg[table][col] = hll
	}
	if ctx.Debug > 0 {
		lib.Printf("Cfg: %+v\n", cfg)
	}

	// process separate tables in parallel
	sch := make(chan [2]string)
	def := map[string]string{"n": "0.0", "y": "hll_empty()"}
	nThreads = 0
	for table, columns := range cfg {
		go func(sch chan [2]string, tab string, cols map[string]string) {
			trials := 0
		retry:
			s := "update \"" + tab + "\" set "
			for col, hll := range cols {
				dVal := def[hll]
				s += "\"" + col + "\" = " + dVal + ", "
			}
			s = s[:len(s)-2]
			dtStart := time.Now()
			_, err := lib.ExecSQL(con, &ctx, s)
			dtEnd := time.Now()
			nCols := len(cols)
			if err == nil {
				lib.Printf("Mass updated \"%s\", columns: %d, took: %v\n", tab, nCols, dtEnd.Sub(dtStart))
			} else {
				rtry := handleRowIsTooBig(con, &ctx, tab, "mass add columns", addedCols, &mtx, err)
				if rtry {
					trials++
					if trials < maxTrials {
						goto retry
					} else {
						lib.Printf("Give up 'mass add columns' after %d trials\n", maxTrials)
					}
				}
			}
			s = "alter table \"" + tab + "\" "
			for col, hll := range cols {
				dVal := def[hll]
				s += "alter column \"" + col + "\" set not null, alter column \"" + col + "\" set default " + dVal + ", "
			}
			s = s[:len(s)-2]
			dtStart = time.Now()
			_, err = lib.ExecSQL(con, &ctx, s)
			dtEnd = time.Now()
			if err == nil {
				lib.Printf("Altered \"%s\" defaults and restrictions, columns: %d, took: %v\n", tab, nCols, dtEnd.Sub(dtStart))
			} else {
				rtry := handleRowIsTooBig(con, &ctx, tab, "mass alter defaults", addedCols, &mtx, err)
				if rtry {
					trials++
					if trials < maxTrials {
						goto retry
					} else {
						lib.Printf("Give up 'mass alter defaults' after %d trials\n", maxTrials)
					}
				}
			}
			if sch != nil {
				sch <- [2]string{tab, "ok"}
			}
		}(sch, table, columns)
		nThreads++
		if nThreads >= thrN {
			<-sch
			nThreads--
		}
	}
	for nThreads > 0 {
		<-sch
		nThreads--
	}
}

func main() {
	dtStart := time.Now()
	ensureColumns()
	dtEnd := time.Now()
	lib.Printf("Time: %v\n", dtEnd.Sub(dtStart))
}
