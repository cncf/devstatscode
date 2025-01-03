package main

import (
	"fmt"
	"time"

	lib "github.com/cncf/devstatscode"
	yaml "gopkg.in/yaml.v2"
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
					"select \"%s\" from \"%s\"",
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
					_, err := lib.ExecSQL(
						con,
						&ctx,
						"alter table \""+table+"\" add column \""+colName+"\" "+colType,
					)
					if err == nil {
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
			s := "update \"" + tab + "\" set "
			for col, hll := range cols {
				dVal := def[hll]
				s += "\"" + col + "\" = " + dVal + ", "
			}
			s = s[:len(s)-2]
			dtStart := time.Now()
			lib.ExecSQLWithErr(con, &ctx, s)
			dtEnd := time.Now()
			nCols := len(cols)
			lib.Printf("Mass updated \"%s\", columns: %d, took: %v\n", tab, nCols, dtEnd.Sub(dtStart))
			s = "alter table \"" + tab + "\" "
			for col, hll := range cols {
				dVal := def[hll]
				s += "alter column \"" + col + "\" set not null, alter column \"" + col + "\" set default " + dVal + ", "
			}
			s = s[:len(s)-2]
			dtStart = time.Now()
			lib.ExecSQLWithErr(con, &ctx, s)
			dtEnd = time.Now()
			lib.Printf("Altered \"%s\" defaults and restrictions, columns: %d, took: %v\n", tab, nCols, dtEnd.Sub(dtStart))
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
