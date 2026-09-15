package devstatscode

import (
	"testing"

	lib "github.com/cncf/devstatscode"
	"github.com/lib/pq"
)

func TestPqRetryable(t *testing.T) {
	var testCases = []struct {
		code       string
		constraint string
		expected   bool
	}{
		// Connection problems, shutdowns, exhausted resources, rollbacks: the DB may come back
		{"08000", "", true}, // connection_exception
		{"08006", "", true}, // connection_failure
		{"08003", "", true}, // connection_does_not_exist
		{"08P01", "", true}, // protocol_violation
		{"40001", "", true}, // serialization_failure
		{"40P01", "", true}, // deadlock_detected
		{"40003", "", true}, // statement_completion_unknown
		{"53300", "", true}, // too_many_connections
		{"53200", "", true}, // out_of_memory
		{"53100", "", true}, // disk_full
		{"57P01", "", true}, // admin_shutdown
		{"57P03", "", true}, // cannot_connect_now
		{"57014", "", true}, // query_canceled
		{"58030", "", true}, // io_error
		{"72000", "", true}, // snapshot_too_old
		{"XX000", "", true}, // internal_error ("tuple concurrently updated", ...)
		{"55P03", "", true}, // lock_not_available
		{"55006", "", true}, // object_in_use
		{"55000", "", true}, // object_not_in_prerequisite_state
		{"25006", "", true}, // read_only_sql_transaction
		{"25P03", "", true}, // idle_in_transaction_session_timeout
		// Concurrent "create ... if not exists" / "drop ..." races
		{"42P01", "", true},                           // undefined_table
		{"42P07", "", true},                           // duplicate_table
		{"42701", "", true},                           // duplicate_column
		{"42710", "", true},                           // duplicate_object
		{"23505", "pg_type_typname_nsp_index", true},  // unique_violation on a system catalog
		{"23505", "pg_class_relname_nsp_index", true}, // unique_violation on a system catalog
		{"", "", true},                                // no SQLSTATE at all
		// Deterministic: the same statement can never succeed
		{"42601", "", false},                               // syntax_error (zero-length delimited identifier, ...)
		{"42703", "", false},                               // undefined_column
		{"42883", "", false},                               // undefined_function
		{"42704", "", false},                               // undefined_object
		{"42804", "", false},                               // datatype_mismatch
		{"42501", "", false},                               // insufficient_privilege
		{"42702", "", false},                               // ambiguous_column
		{"42P10", "", false},                               // invalid_column_reference
		{"23505", "gha_issues_pkey", false},                // unique_violation on DevStats data
		{"23505", "suser_activity_time_period_key", false}, // unique_violation on a series table
		{"23505", "", false},                               // unique_violation, unknown constraint
		{"23502", "", false},                               // not_null_violation
		{"23503", "", false},                               // foreign_key_violation
		{"23514", "", false},                               // check_violation
		{"22001", "", false},                               // string_data_right_truncation
		{"22003", "", false},                               // numeric_value_out_of_range
		{"22012", "", false},                               // division_by_zero
		{"22P02", "", false},                               // invalid_text_representation
		{"22021", "", false},                               // character_not_in_repertoire
		{"22007", "", false},                               // invalid_datetime_format
		{"3D000", "", false},                               // invalid_catalog_name
		{"3F000", "", false},                               // invalid_schema_name
		{"54000", "", false},                               // program_limit_exceeded
		{"54011", "", false},                               // too_many_columns
		{"0A000", "", false},                               // feature_not_supported
		{"28P01", "", false},                               // invalid_password
		{"25P02", "", false},                               // in_failed_sql_transaction
		{"P0001", "", false},                               // raise_exception
		{"XX001", "", false},                               // data_corrupted
		{"XX002", "", false},                               // index_corrupted
		{"21000", "", false},                               // cardinality_violation
	}
	for _, test := range testCases {
		e := &pq.Error{Code: pq.ErrorCode(test.code), Constraint: test.constraint}
		got := lib.PqRetryable(e)
		if got != test.expected {
			t.Errorf("code '%s' constraint '%s': expected retryable=%v, got %v", test.code, test.constraint, test.expected, got)
		}
	}
}
