package devstatscode

import (
	"bytes"
	"testing"

	lib "github.com/cncf/devstatscode"
)

func TestIsPlainLine(t *testing.T) {
	var testCases = []struct {
		line     []byte
		expected bool
	}{
		{line: []byte(`{"id":"1"}`), expected: true},
		{line: []byte(""), expected: true},
		{line: []byte(`{"a":"zażółć"}`), expected: true},
		{line: []byte("{\"id\":\"1\"}\x00"), expected: false},
		{line: []byte("\x00"), expected: false},
		{line: []byte("{\"a\":\"\xff\"}"), expected: false},
	}
	for index, test := range testCases {
		got := lib.IsPlainLine(test.line)
		if got != test.expected {
			t.Errorf("test number %d, expected %v, got %v", index+1, test.expected, got)
		}
	}
}

func TestTrimJSONWS(t *testing.T) {
	var testCases = []struct {
		in, expected string
	}{
		{in: " \t\r\n{}\n\r\t ", expected: "{}"},
		{in: "   ", expected: ""},
		{in: "", expected: ""},
		{in: "{}", expected: "{}"},
		// Vertical tab, form feed and NBSP are not JSON whitespace.
		{in: "\x0b{}\x0c", expected: "\x0b{}\x0c"},
		{in: "\u00a0{}", expected: "\u00a0{}"},
	}
	for index, test := range testCases {
		got := string(lib.TrimJSONWS([]byte(test.in)))
		if got != test.expected {
			t.Errorf("test number %d, expected %q, got %q", index+1, test.expected, got)
		}
	}
}

func TestSplitNULSegments(t *testing.T) {
	var testCases = []struct {
		line     string
		expected []string
	}{
		{line: "a\x00\x00\x00b", expected: []string{"a", "b"}},
		{line: "\x00\x00", expected: []string{}},
		{line: " \x00 x \x00\n", expected: []string{"x"}},
		{line: "abc", expected: []string{"abc"}},
	}
	for index, test := range testCases {
		got := lib.SplitNULSegments([]byte(test.line))
		if len(got) != len(test.expected) {
			t.Errorf("test number %d, expected %d segments %q, got %d %q", index+1, len(test.expected), test.expected, len(got), got)
			continue
		}
		for i := range got {
			if string(got[i]) != test.expected[i] {
				t.Errorf("test number %d, segment %d: expected %q, got %q", index+1, i, test.expected[i], string(got[i]))
			}
		}
	}
}

func jsonChunk(s string) lib.JSONChunk {
	return lib.JSONChunk{Bytes: []byte(s)}
}

func brokenChunk(s string) lib.JSONChunk {
	return lib.JSONChunk{Bytes: []byte(s), Broken: true}
}

func TestRecoverJSONChunks(t *testing.T) {
	ev := `{"id":"29055979701","type":"IssueCommentEvent"}`
	ev2 := `{"id":"29056015615","type":"PushEvent","repo":{"name":"yowmamasita/anmeldung"}}`
	nuls := string(bytes.Repeat([]byte{0}, 1409))
	oldFmt := `{"repository":{"name":"rust","owner":"rust-lang"},"actor":"bors","type":"PushEvent","created_at":"2012/03/11 12:00:00 -0700"}`
	var testCases = []struct {
		name     string
		line     string
		expected []lib.JSONChunk
	}{
		{name: "clean line", line: `{"id":"1","type":"PushEvent"}`, expected: []lib.JSONChunk{jsonChunk(`{"id":"1","type":"PushEvent"}`)}},
		{name: "clean line with whitespace", line: "  {\"id\":\"1\"}\r\n", expected: []lib.JSONChunk{jsonChunk(`{"id":"1"}`)}},
		{name: "empty", line: "", expected: nil},
		{name: "NULs only", line: "\x00\x00\x00", expected: nil},
		{name: "whitespace and NULs", line: " \t\x00 \n", expected: nil},
		{name: "trailing NULs after event", line: ev + nuls, expected: []lib.JSONChunk{jsonChunk(ev)}},
		// The real 2023-05-14-19 damage: event + 1409 NULs + glued event.
		{name: "event NULs event", line: ev + nuls + ev2, expected: []lib.JSONChunk{jsonChunk(ev), jsonChunk(ev2)}},
		// The middle event is cut by NULs: both halves are broken, never merged.
		{
			name: "three chunks, NULs inside event",
			line: "{\"id\":\"1\"}\x00\x00{\"id\":\x00\"2\"}\x00{\"id\":\"3\"}",
			expected: []lib.JSONChunk{
				jsonChunk(`{"id":"1"}`), brokenChunk(`{"id":`), brokenChunk(`"2"}`), jsonChunk(`{"id":"3"}`),
			},
		},
		{
			name:     "glued without NULs",
			line:     `{"id":"1"}{"id":"2"} {"id":"3"}`,
			expected: []lib.JSONChunk{jsonChunk(`{"id":"1"}`), jsonChunk(`{"id":"2"}`), jsonChunk(`{"id":"3"}`)},
		},
		{name: "garbage tail", line: `{"id":"1"}garbage`, expected: []lib.JSONChunk{jsonChunk(`{"id":"1"}`), brokenChunk("garbage")}},
		{name: "garbage head", line: `garbage{"id":"1"}`, expected: []lib.JSONChunk{brokenChunk(`garbage{"id":"1"}`)}},
		{name: "extra closing brace", line: `{"id":"1"}}`, expected: []lib.JSONChunk{jsonChunk(`{"id":"1"}`), brokenChunk("}")}},
		{name: "truncated event", line: `{"id":"1","type":"Pu`, expected: []lib.JSONChunk{brokenChunk(`{"id":"1","type":"Pu`)}},
		{name: "truncated second event", line: `{"id":"1"}{"id":"2" `, expected: []lib.JSONChunk{jsonChunk(`{"id":"1"}`), brokenChunk(`{"id":"2"`)}},
		{name: "lone brace", line: `{"id":"1"} {`, expected: []lib.JSONChunk{jsonChunk(`{"id":"1"}`), brokenChunk("{")}},
		{
			name: "invalid UTF-8 segment is broken whole",
			line: "{\"id\":\"1\"}\x00{\"a\":\"\xff\xfe\"}{\"id\":\"3\"}\x00{\"id\":\"4\"}",
			expected: []lib.JSONChunk{
				jsonChunk(`{"id":"1"}`), brokenChunk("{\"a\":\"\xff\xfe\"}{\"id\":\"3\"}"), jsonChunk(`{"id":"4"}`),
			},
		},
		{
			name: "non-object values",
			line: `[1,2] "s" 7 null {"id":"1"}`,
			expected: []lib.JSONChunk{
				jsonChunk("[1,2]"), jsonChunk(`"s"`), jsonChunk("7"), jsonChunk("null"), jsonChunk(`{"id":"1"}`),
			},
		},
		{name: "number glued to letters", line: "123abc", expected: []lib.JSONChunk{brokenChunk("123abc")}},
		// json.Decoder: a string/number/literal must be followed by whitespace or EOF, an object/array is self-delimiting.
		{name: "string glued to object", line: `"s"{"id":"1"}`, expected: []lib.JSONChunk{brokenChunk(`"s"{"id":"1"}`)}},
		{name: "number glued to object", line: `7{"id":"1"}`, expected: []lib.JSONChunk{brokenChunk(`7{"id":"1"}`)}},
		{name: "object string object", line: `{"id":"1"}"s"{"id":"2"}`, expected: []lib.JSONChunk{jsonChunk(`{"id":"1"}`), brokenChunk(`"s"{"id":"2"}`)}},
		{name: "array then number", line: "[1]2", expected: []lib.JSONChunk{jsonChunk("[1]"), jsonChunk("2")}},
		{name: "object then string", line: `{"id":"1"}"s"`, expected: []lib.JSONChunk{jsonChunk(`{"id":"1"}`), jsonChunk(`"s"`)}},
		{
			name:     "whitespace around NULs",
			line:     " \n{\"id\":\"1\"}\r\n\x00\x00 \t{\"id\":\"2\"}\n \x00",
			expected: []lib.JSONChunk{jsonChunk(`{"id":"1"}`), jsonChunk(`{"id":"2"}`)},
		},
		{name: "old format event", line: oldFmt, expected: []lib.JSONChunk{jsonChunk(oldFmt)}},
	}
	for index, test := range testCases {
		got := lib.RecoverJSONChunks([]byte(test.line))
		if len(got) != len(test.expected) {
			t.Errorf("test number %d (%s), expected %d chunks %+v, got %d %+v", index+1, test.name, len(test.expected), test.expected, len(got), got)
			continue
		}
		for i := range got {
			if got[i].Broken != test.expected[i].Broken || !bytes.Equal(got[i].Bytes, test.expected[i].Bytes) {
				t.Errorf("test number %d (%s), chunk %d: expected %+v, got %+v", index+1, test.name, i, test.expected[i], got[i])
			}
		}
	}
}
