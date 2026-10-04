package main

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	lib "github.com/cncf/devstatscode"
)

// captureStdout runs f with stdout redirected to a pipe (stderr silenced)
// and returns everything it printed
func captureStdout(t *testing.T, f func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	oldOut, oldErr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = w, devNull
	done := make(chan string)
	go func() {
		var b bytes.Buffer
		_, _ = io.Copy(&b, r)
		done <- b.String()
	}()
	f()
	_ = w.Close()
	os.Stdout, os.Stderr = oldOut, oldErr
	_ = devNull.Close()
	return <-done
}

func TestLogBrokenJSON(t *testing.T) {
	lib.FatalOnError(os.Setenv("GHA2DB_SKIPLOG", "1"))
	defer func() { _ = os.Unsetenv("GHA2DB_SKIPLOG") }()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(wd) }()

	dt := time.Date(2015, 1, 1, 15, 0, 0, 0, time.UTC)
	chunk := []byte("{\"id\":\"1\",\"repo\":{\"name\":\"a/b\xff\"}}")
	errBroken := errors.New("broken JSON chunk")
	var ctx lib.Ctx

	// Without GHA2DB_JSON nothing is saved (no jsons/ directory is needed)
	out := captureStdout(t, func() { logBrokenJSON(&ctx, 48, 144, chunk, dt, errBroken, 1) })
	if _, err := os.Stat("jsons"); !os.IsNotExist(err) {
		t.Errorf("jsons/ should not exist: %v", err)
	}
	for _, want := range []string{
		"Error(2015-01-01-15): broken JSON chunk\n",
		// The echoed chunk is valid UTF-8: the invalid byte is dropped
		"2015-01-01 15:00:00 +0000 UTC: Cannot unmarshal:\n{\"id\":\"1\",\"repo\":{\"name\":\"a/b\"}}\nbroken JSON chunk\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in %q", want, out)
		}
	}
	if strings.Contains(out, "cannot save") {
		t.Errorf("unexpected save error in %q", out)
	}

	// With GHA2DB_JSON the chunk is saved as is, further chunks of the
	// same line get a -n suffix
	ctx.JSONOut = true
	if err := os.Mkdir("jsons", 0755); err != nil {
		t.Fatal(err)
	}
	out = captureStdout(t, func() {
		logBrokenJSON(&ctx, 48, 144, chunk, dt, errBroken, 1)
		logBrokenJSON(&ctx, 48, 144, []byte("garbage"), dt, errBroken, 2)
	})
	if strings.Contains(out, "cannot save") {
		t.Errorf("unexpected save error in %q", out)
	}
	for name, want := range map[string][]byte{
		"error_2015-01-01-15-49-144.json":   chunk,
		"error_2015-01-01-15-49-144-2.json": []byte("garbage"),
	} {
		got, err := ioutil.ReadFile(filepath.Join("jsons", name))
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s = %q, expected %q", name, got, want)
		}
	}
	files, err := ioutil.ReadDir("jsons")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Errorf("expected 2 files in jsons/, got %d", len(files))
	}

	// Saving is best effort: a missing jsons/ directory is reported, never fatal
	if err := os.RemoveAll("jsons"); err != nil {
		t.Fatal(err)
	}
	out = captureStdout(t, func() { logBrokenJSON(&ctx, 0, 4, []byte("x"), dt, errBroken, 1) })
	want := "2015-01-01-15: cannot save broken JSON: open jsons/error_2015-01-01-15-1-4.json: no such file or directory\n"
	if !strings.Contains(out, want) {
		t.Errorf("expected %q in %q", want, out)
	}
	if !strings.Contains(out, "Error(2015-01-01-15): broken JSON chunk\n") {
		t.Errorf("expected the Error line in %q", out)
	}
}
