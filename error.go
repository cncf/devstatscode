package devstatscode

import (
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/lib/pq"
)

// FatalOnError displays error message (if error present) and exits program
func FatalOnError(err error) string {
	if err != nil {
		tm := time.Now()
		switch e := err.(type) {
		case *pq.Error:
			errName := e.Code.Name()
			if errName == "too_many_connections" {
				fmt.Fprintf(os.Stderr, "PqError: code=%s, name=%s, detail=%s\n", e.Code, errName, e.Detail)
				fmt.Fprintf(os.Stderr, "Warning: too many postgres connections: %+v: '%s'\n", tm, err.Error())
				return Retry
			} else if errName == "cannot_connect_now" {
				fmt.Fprintf(os.Stderr, "PqError: code=%s, name=%s, detail=%s\n", e.Code, errName, e.Detail)
				fmt.Fprintf(os.Stderr, "Warning: DB shutting down: %+v: '%s', sleeping 15 minutes to settle\n", tm, err.Error())
				time.Sleep(time.Duration(900) * time.Second)
				tm = time.Now()
				fmt.Fprintf(os.Stderr, "Warning: DB shutting down: %+v: '%s', waited 15 minutes, retrying\n", tm, err.Error())
				return Reconnect
			}
			Printf("PqError: code=%s, name=%s, detail=%s\n", e.Code, errName, e.Detail)
			fmt.Fprintf(os.Stderr, "PqError: code=%s, name=%s, detail=%s\n", e.Code, errName, e.Detail)
			if os.Getenv("DURABLE_PQ") != "" && os.Getenv("DURABLE_PQ") != "0" && os.Getenv("DURABLE_PQ") != "false" {
				if PqRetryable(e) {
					fmt.Fprintf(os.Stderr, "retrying with DURABLE_PQ\n")
					return Reconnect
				}
				if errName == "" {
					errName = string(e.Code)
				}
				Printf("%s error is not retryable, even with DURABLE_PQ\n", errName)
			}
		default:
			fmt.Fprintf(os.Stderr, "ErrorType: %T, error: %+v\n", e, e)
			fmt.Fprintf(os.Stderr, "ErrorType: %T, error: %+v\n", e, e)
		}
		if strings.Contains(err.Error(), "driver: bad connection") {
			fmt.Fprintf(os.Stderr, "Warning: bad driver, retrying\n")
			return Reconnect
		}
		if strings.Contains(err.Error(), "cannot assign requested address") {
			fmt.Fprintf(os.Stderr, "Warning: cannot assign requested address, retrying in 15 minutes\n")
			time.Sleep(time.Duration(900) * time.Second)
			fmt.Fprintf(os.Stderr, "Warning: cannot assign requested address - waited 15 minutes, retrying\n")
			return Reconnect
		}
		/*
			if strings.Contains(err.Error(), "database is closed") {
				Printf("Warning: database is closed, retrying\n")
				return Reconnect
			}
		*/
		fmt.Fprintf(os.Stderr, "Error(time=%+v):\nError: '%s'\nStacktrace:\n%s\n", tm, err.Error(), string(debug.Stack()))
		if os.Getenv("NO_FATAL_DELAY") == "" {
			time.Sleep(time.Duration(60) * time.Second)
		}
		panic(fmt.Sprintf("stacktrace: %+v", err))
	}
	return OK
}

// Fatalf - it will call FatalOnError using fmt.Errorf with args provided
func Fatalf(f string, a ...interface{}) {
	FatalOnError(fmt.Errorf(f, a...))
}

// PqRetryable tells whether a PostgreSQL error is a condition that can go away on its own,
// so that DURABLE_PQ should retry the statement (after a growing delay, possibly reconnecting):
// connection problems, server shutdown/restart, exhausted resources, transaction rollbacks
// (deadlocks, serialization failures), and the catalog races of concurrent
// "create table/index if not exists" / "add column if not exists" that DevStats runs from
// many processes at once.
// Everything else - syntax errors, constraint violations on data (duplicate keys, nulls,
// foreign keys), data errors (bad casts, overflows), unknown columns/functions/types,
// permission errors, program limits - is deterministic: retrying the very same statement
// can never succeed, so it must fail right away.
func PqRetryable(e *pq.Error) bool {
	code := string(e.Code)
	if len(code) < 5 {
		// No SQLSTATE (should never happen with a server error) - assume a connection level problem
		return true
	}
	switch code[:2] {
	case "08", // connection_exception: connection_failure, connection_does_not_exist, protocol_violation, ...
		"40", // transaction_rollback: serialization_failure, deadlock_detected, statement_completion_unknown
		"53", // insufficient_resources: too_many_connections, out_of_memory, disk_full, ...
		"57", // operator_intervention: admin_shutdown, crash_shutdown, cannot_connect_now, query_canceled, ...
		"58", // system_error: io_error, undefined_file, ...
		"72": // snapshot_too_old
		return true
	}
	switch code {
	case "XX000", // internal_error: "tuple concurrently updated", "could not open relation with OID", ...
		"55000", "55006", "55P03", // object_not_in_prerequisite_state, object_in_use, lock_not_available
		"25006", "25P03", // read_only_sql_transaction (failover to a replica), idle_in_transaction_session_timeout
		"42P01", "42P07", "42701", "42710": // undefined_table, duplicate_table, duplicate_column, duplicate_object: concurrent drop/create races
		return true
	case "23505":
		// unique_violation: only the system catalog races of concurrent "create ... if not exists"
		// (pg_type_typname_nsp_index, pg_class_relname_nsp_index, ...) - a duplicate key in DevStats
		// data (gha_* tables, s* series tables) is a bug, retrying it changes nothing.
		return strings.HasPrefix(e.Constraint, "pg_")
	}
	return false
}

// FatalNoLog displays error message (if error present) and exits program, should be used for very early init state
func FatalNoLog(err error) string {
	if err != nil {
		tm := time.Now()
		fmt.Fprintf(os.Stderr, "Error(time=%+v):\nError: '%s'\nStacktrace:\n", tm, err.Error())
		if os.Getenv("NO_FATAL_DELAY") == "" {
			time.Sleep(time.Duration(60) * time.Second)
		}
		panic(fmt.Sprintf("stacktrace: %+v", err))
	}
	return OK
}
