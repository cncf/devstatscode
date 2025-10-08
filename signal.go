package devstatscode

import (
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// FinishAfterTimeout - finish program 'prog' after given timeout 'seconds', exit with 'status' code
func FinishAfterTimeout(prog string, seconds, status int) {
	time.Sleep(time.Duration(seconds) * time.Second)
	Printf("Program '%s' reached timeout after %d seconds, sending signal to exit %d\n", prog, seconds, status)
	err := syscall.Kill(syscall.Getpid(), syscall.SIGALRM)
	if err != nil {
		Printf("Error: %+v sending '%s' timeout signal after %d seconds, exiting %d status\n", err, prog, seconds, status)
		os.Exit(status)
		return
	}
	Printf("Program '%s': sent timeout signal after %d seconds, requesting %d exit status\n", prog, seconds, status)
}

// SetupTimeoutSignal - if GHA2DB_MAX_RUN_DURATION contains configuration for 'prog'
// Then it is given as "...,prog:duration:exit_status:,..." - it means that the 'prog'
// can only run 'duration' seconds, and after that time it receives timeout, logs it
// and exists with 'exit_status'
func SetupTimeoutSignal(ctx *Ctx) {
	prog := filepath.Base(os.Args[0])
	ary := strings.Split(prog, ".")
	lAry := len(ary)
	if lAry > 1 {
		prog = strings.Join(ary[:lAry-1], ".")
	}
	if ctx.MaxRunDuration == nil {
		return
	}
	data, ok := ctx.MaxRunDuration[prog]
	if !ok {
		return
	}
	seconds, status := data[0], data[1]
	if data[0] <= 0 {
		return
	}
	go FinishAfterTimeout(prog, seconds, status)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGALRM)
	go func() {
		for {
			sig := <-sigs
			if prog == "calc_metric" && ctx.AllowMetricFail {
				Printf("Program '%s': timeout %v after %d seconds, will exit with %d code, but will not fail due to this\n", prog, sig, seconds, status)
			} else {
				Printf("Program '%s': timeout %v after %d seconds, will exit with %d code\n", prog, sig, seconds, status)
				os.Exit(status)
			}
		}
	}()
	Printf("Program '%s': timeout handler installed: exit %d after %d seconds\n", prog, status, seconds)
}
