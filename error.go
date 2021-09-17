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
				Printf("PqError: code=%s, name=%s, detail=%s\n", e.Code, errName, e.Detail)
				Printf("Warning: too many postgres connections: %+v: '%s'\n", tm, err.Error())
				return Retry
			} else if errName == "cannot_connect_now" {
				Printf("PqError: code=%s, name=%s, detail=%s\n", e.Code, errName, e.Detail)
				Printf("Warning: DB shutting down: %+v: '%s'\n", tm, err.Error())
				// FIXME
				return Reconnect
			}
			Printf("PqError: code=%s, name=%s, detail=%s\n", e.Code, errName, e.Detail)
			fmt.Fprintf(os.Stderr, "PqError: code=%s, name=%s, detail=%s\n", e.Code, errName, e.Detail)
			if os.Getenv("DURABLE_PQ") != "" {
				return Reconnect
			}
		default:
			Printf("ErrorType: %T, error: %+v\n", e, e)
			fmt.Fprintf(os.Stderr, "ErrorType: %T, error: %+v\n", e, e)
		}
		if strings.Contains(err.Error(), "driver: bad connection") {
			// FIXME
			Printf("Warning: bad driver, retrying\n")
			return Reconnect
		}
		Printf("Error(time=%+v):\nError: '%s'\nStacktrace:\n%s\n", tm, err.Error(), string(debug.Stack()))
		fmt.Fprintf(os.Stderr, "Error(time=%+v):\nError: '%s'\nStacktrace:\n", tm, err.Error())
		panic("stacktrace")
	}
	return OK
}

// Fatalf - it will call FatalOnError using fmt.Errorf with args provided
func Fatalf(f string, a ...interface{}) {
	FatalOnError(fmt.Errorf(f, a...))
}

// FatalNoLog displays error message (if error present) and exits program, should be used for very early init state
func FatalNoLog(err error) string {
	if err != nil {
		tm := time.Now()
		fmt.Fprintf(os.Stderr, "Error(time=%+v):\nError: '%s'\nStacktrace:\n", tm, err.Error())
		panic("stacktrace")
	}
	return OK
}
