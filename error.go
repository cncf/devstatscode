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
				fmt.Fprintf(os.Stderr, "Warning: DB shutting down: %+v: '%s', sleeping 5 minutes to settle\n", tm, err.Error())
				time.Sleep(time.Duration(300) * time.Second)
				fmt.Fprintf(os.Stderr, "Warning: DB shutting down: %+v: '%s', waited 5 minutes, retrying\n", tm, err.Error())
				return Reconnect
			}
			Printf("PqError: code=%s, name=%s, detail=%s\n", e.Code, errName, e.Detail)
			fmt.Fprintf(os.Stderr, "PqError: code=%s, name=%s, detail=%s\n", e.Code, errName, e.Detail)
			if os.Getenv("DURABLE_PQ") != "" && os.Getenv("DURABLE_PQ") != "0" && os.Getenv("DURABLE_PQ") != "false" {
				switch errName {
				case "program_limit_exceeded", "undefined_column", "invalid_catalog_name", "character_not_in_repertoire":
					Printf("%s error is not retryable, even with DURABLE_PQ\n", errName)
				default:
					fmt.Fprintf(os.Stderr, "retrying with DURABLE_PQ\n")
					return Reconnect
				}
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
			fmt.Fprintf(os.Stderr, "Warning: cannot assign requested address, retrying in 5 minutes\n")
			time.Sleep(time.Duration(300) * time.Second)
			fmt.Fprintf(os.Stderr, "Warning: cannot assign requested address - waited 5 minutes, retrying\n")
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

// FatalNoLog displays error message (if error present) and exits program, should be used for very early init state
func FatalNoLog(err error) string {
	if err != nil {
		tm := time.Now()
		fmt.Fprintf(os.Stderr, "Error(time=%+v):\nError: '%s'\nStacktrace:\n", tm, err.Error())
		time.Sleep(time.Duration(60) * time.Second)
		panic(fmt.Sprintf("stacktrace: %+v", err))
	}
	return OK
}
