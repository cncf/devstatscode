package devstatscode

import (
	"testing"

	lib "github.com/cncf/devstatscode"
)

func TestPeriodComputedKey(t *testing.T) {
	var testCases = []struct {
		seriesNameOrFunc string
		sqlFile          string
		period           string
		expected         string
	}{
		{seriesNameOrFunc: "reviewers", sqlFile: "/etc/gha2db/metrics/kubernetes/reviewers.sql", period: "d", expected: "kubernetes/reviewers.sql reviewers d"},
		{seriesNameOrFunc: "multi_row_multi_column", sqlFile: "./metrics/all/companies.sql", period: "w", expected: "all/companies.sql multi_row_multi_column w"},
		{seriesNameOrFunc: "multi_row_multi_column", sqlFile: "metrics/all/companies.sql", period: "w", expected: "all/companies.sql multi_row_multi_column w"},
		{seriesNameOrFunc: "f", sqlFile: "all/companies.sql", period: "d7", expected: "all/companies.sql f d7"},
		{seriesNameOrFunc: "f", sqlFile: "companies.sql", period: "y10", expected: "companies.sql f y10"},
		{seriesNameOrFunc: "f", sqlFile: "", period: "h", expected: " f h"},
		{seriesNameOrFunc: "hist_reviewers_d", sqlFile: "/a/b/c/kubernetes/hist_reviewers.sql", period: "a_0_n", expected: "kubernetes/hist_reviewers.sql hist_reviewers_d a_0_n"},
	}
	for index, test := range testCases {
		got := lib.PeriodComputedKey(test.seriesNameOrFunc, test.sqlFile, test.period)
		if got != test.expected {
			t.Errorf("test number %d, expected '%s', got '%s'", index+1, test.expected, got)
		}
	}
}
