package devstatscode

import (
	"io/ioutil"

	jsoniter "github.com/json-iterator/go"
)

// PrettyPrintJSON - pretty formats raw JSON bytes
func PrettyPrintJSON(jsonBytes []byte) []byte {
	var jsonObj interface{}
	FatalOnError(jsoniter.Unmarshal(jsonBytes, &jsonObj))
	pretty, err := jsoniter.MarshalIndent(jsonObj, "", "  ")
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
