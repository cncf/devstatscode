package devstatscode

import (
	"encoding/json"
	"io/ioutil"
	"strconv"

	jsoniter "github.com/json-iterator/go"
)

// prettyJSONDecoder - jsoniter config used by PrettyPrintJSON to decode:
// numbers are decoded as json.Number so integers are not rounded through float64.
var prettyJSONDecoder = jsoniter.Config{
	EscapeHTML:             true,
	UseNumber:              true,
	ValidateJsonRawMessage: true,
}.Froze()

// normalizeJSONNumbers - replaces json.Number values with int64/uint64 when
// they are integral (kept exact) and float64 otherwise (formatted the usual
// jsoniter way: shortest representation, exponent below 1e-6 / from 1e21).
func normalizeJSONNumbers(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		for k, e := range t {
			t[k] = normalizeJSONNumbers(e)
		}
		return t
	case []interface{}:
		for i, e := range t {
			t[i] = normalizeJSONNumbers(e)
		}
		return t
	case json.Number:
		s := string(t)
		if i, err := strconv.ParseInt(s, 10, 64); err == nil && !(i == 0 && s[0] == '-') {
			return i
		}
		if u, err := strconv.ParseUint(s, 10, 64); err == nil {
			return u
		}
		f, err := strconv.ParseFloat(s, 64)
		FatalOnError(err)
		return f
	}
	return v
}

// PrettyPrintJSON - pretty formats raw JSON bytes (2 space indent, sorted object keys)
// The default jsoniter config iterates Go maps, so every call produced a different
// key order (sqlitedb could never detect an unchanged dashboard); jsoniter's
// SortMapKeys breaks MarshalIndent's nesting, hence encoding/json for the output.
func PrettyPrintJSON(jsonBytes []byte) []byte {
	var jsonObj interface{}
	FatalOnError(prettyJSONDecoder.Unmarshal(jsonBytes, &jsonObj))
	pretty, err := json.MarshalIndent(normalizeJSONNumbers(jsonObj), "", "  ")
	FatalOnError(err)
	return pretty
}

// ObjectToJSON - serialize given object as JSON
func ObjectToJSON(obj interface{}, fn string) {
	jsonBytes, err := jsoniter.Marshal(obj)
	FatalOnError(err)
	pretty := PrettyPrintJSON(jsonBytes)
	FatalOnError(ioutil.WriteFile(fn, pretty, 0644))
}
