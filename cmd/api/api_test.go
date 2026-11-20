package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lib "github.com/cncf/devstatscode"
	jsoniter "github.com/json-iterator/go"
)

func TestMain(m *testing.M) {
	lib.FatalOnError(os.Setenv("GHA2DB_SKIPLOG", "1"))
	os.Exit(m.Run())
}

// setupSiteStats - empty site stats cache, projects test/Test Project/testdb -> testdb and other -> otherdb,
// given TTL (seconds) and calculation
func setupSiteStats(t *testing.T, ttl int64, calc func(project, db string) (siteStatsPayload, error)) {
	gNameToDB = map[string]string{"test": "testdb", "Test Project": "testdb", "testdb": "testdb", "other": "otherdb"}
	gMtx = &sync.RWMutex{}
	siteStatsCacheMtx.Lock()
	siteStatsCache = map[string]siteStatsCacheEntry{}
	siteStatsCalls = map[string]*siteStatsCall{}
	siteStatsCacheMtx.Unlock()
	oldTTL, oldCalc := siteStatsCacheTTL, siteStatsCalc
	siteStatsCacheTTL, siteStatsCalc = ttl, calc
	t.Cleanup(func() { siteStatsCacheTTL, siteStatsCalc = oldTTL, oldCalc })
}

func siteStatsRequest(project string) (int, string) {
	w := httptest.NewRecorder()
	apiSiteStats("test", w, map[string]interface{}{"project": project})
	return w.Code, w.Body.String()
}

// siteStatsBody - expected response body
func siteStatsBody(project, db string, contributors int64) string {
	w := httptest.NewRecorder()
	_ = jsoniter.NewEncoder(w).Encode(siteStatsPayload{Project: project, DB: db, Contributors: contributors})
	return w.Body.String()
}

// waitNoSiteStatsCalls - wait until no calculation is running
func waitNoSiteStatsCalls(t *testing.T) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		siteStatsCacheMtx.Lock()
		n := len(siteStatsCalls)
		siteStatsCacheMtx.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d site stats calculation(s) still running", n)
		}
		time.Sleep(time.Millisecond)
	}
}

func setSiteStatsEntry(db string, age time.Duration, contributors int64) {
	siteStatsCacheMtx.Lock()
	siteStatsCache[db] = siteStatsCacheEntry{
		dt:        time.Now().Add(-age),
		siteStats: siteStatsPayload{Project: "first", DB: db, Contributors: contributors},
	}
	siteStatsCacheMtx.Unlock()
}

func TestSiteStatsCachedPerDatabase(t *testing.T) {
	var calls int64
	setupSiteStats(t, 3600, func(project, db string) (siteStatsPayload, error) {
		n := atomic.AddInt64(&calls, 1)
		return siteStatsPayload{Project: project, DB: db, Contributors: 100 * n}, nil
	})
	// The first request calculates, the next ones (any name of the same project) use its value
	for i, project := range []string{"test", "test", "Test Project", "testdb"} {
		code, body := siteStatsRequest(project)
		if expected := siteStatsBody(project, "testdb", 100); code != http.StatusOK || body != expected {
			t.Errorf("request %d (%s): got %d %q, expected 200 %q", i+1, project, code, body, expected)
		}
	}
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Errorf("expected 1 calculation for 4 requests, got %d", got)
	}
	// Another database is calculated separately
	code, body := siteStatsRequest("other")
	if expected := siteStatsBody("other", "otherdb", 200); code != http.StatusOK || body != expected {
		t.Errorf("other database: got %d %q, expected 200 %q", code, body, expected)
	}
	// Unknown project: an error, nothing is calculated
	code, body = siteStatsRequest("nope")
	if expected := "{\"error\":\"API 'SiteStats': database not found for project 'nope'\"}\n"; code != http.StatusBadRequest || body != expected {
		t.Errorf("unknown project: got %d %q, expected 400 %q", code, body, expected)
	}
	if got := atomic.LoadInt64(&calls); got != 2 {
		t.Errorf("expected 2 calculations, got %d", got)
	}
}

func TestSiteStatsFreshUntilTTL(t *testing.T) {
	var calls int64
	setupSiteStats(t, 3600, func(project, db string) (siteStatsPayload, error) {
		atomic.AddInt64(&calls, 1)
		return siteStatsPayload{Project: project, DB: db, Contributors: 2}, nil
	})
	// Younger than TTL: cached value, no calculation
	setSiteStatsEntry("testdb", 3590*time.Second, 1)
	code, body := siteStatsRequest("test")
	if expected := siteStatsBody("test", "testdb", 1); code != http.StatusOK || body != expected {
		t.Errorf("fresh: got %d %q, expected 200 %q", code, body, expected)
	}
	if got := atomic.LoadInt64(&calls); got != 0 {
		t.Errorf("fresh value must not be recalculated, got %d calculation(s)", got)
	}
	// TTL reached: the expired value is still returned, one calculation replaces it
	setSiteStatsEntry("testdb", 3600*time.Second, 1)
	code, body = siteStatsRequest("test")
	if expected := siteStatsBody("test", "testdb", 1); code != http.StatusOK || body != expected {
		t.Errorf("expired: got %d %q, expected 200 %q", code, body, expected)
	}
	waitNoSiteStatsCalls(t)
	code, body = siteStatsRequest("test")
	if expected := siteStatsBody("test", "testdb", 2); code != http.StatusOK || body != expected {
		t.Errorf("recalculated: got %d %q, expected 200 %q", code, body, expected)
	}
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Errorf("expected 1 calculation, got %d", got)
	}
}

func TestSiteStatsSingleCalculationForConcurrentRequests(t *testing.T) {
	var calls int64
	started := make(chan struct{}, 100)
	release := make(chan struct{})
	setupSiteStats(t, 3600, func(project, db string) (siteStatsPayload, error) {
		atomic.AddInt64(&calls, 1)
		started <- struct{}{}
		<-release
		return siteStatsPayload{Project: project, DB: db, Contributors: 7}, nil
	})
	// No value yet: all requests wait for the same calculation
	projects := []string{"test", "Test Project", "testdb"}
	const n = 30
	codes := make([]int, n)
	bodies := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i], bodies[i] = siteStatsRequest(projects[i%len(projects)])
		}(i)
	}
	<-started
	// Requests arriving while it runs do not start another one
	time.Sleep(100 * time.Millisecond)
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Errorf("expected 1 running calculation, got %d", got)
	}
	close(release)
	wg.Wait()
	for i := 0; i < n; i++ {
		if expected := siteStatsBody(projects[i%len(projects)], "testdb", 7); codes[i] != http.StatusOK || bodies[i] != expected {
			t.Errorf("request %d: got %d %q, expected 200 %q", i+1, codes[i], bodies[i], expected)
		}
	}
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Errorf("expected 1 calculation for %d concurrent requests, got %d", n, got)
	}
}

func TestSiteStatsStaleValueWhileRecalculating(t *testing.T) {
	var calls int64
	started := make(chan struct{}, 100)
	release := make(chan struct{})
	setupSiteStats(t, 3600, func(project, db string) (siteStatsPayload, error) {
		atomic.AddInt64(&calls, 1)
		started <- struct{}{}
		<-release
		return siteStatsPayload{Project: project, DB: db, Contributors: 2}, nil
	})
	setSiteStatsEntry("testdb", 2*time.Hour, 1)
	// Expired value: returned at once (the calculation is still blocked), also to the requests that follow
	for i, project := range []string{"test", "Test Project", "test"} {
		code, body := siteStatsRequest(project)
		if expected := siteStatsBody(project, "testdb", 1); code != http.StatusOK || body != expected {
			t.Errorf("request %d (%s): got %d %q, expected 200 %q", i+1, project, code, body, expected)
		}
	}
	<-started
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Errorf("expected 1 running calculation, got %d", got)
	}
	close(release)
	waitNoSiteStatsCalls(t)
	// New value replaces the expired one
	code, body := siteStatsRequest("testdb")
	if expected := siteStatsBody("testdb", "testdb", 2); code != http.StatusOK || body != expected {
		t.Errorf("after recalculation: got %d %q, expected 200 %q", code, body, expected)
	}
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Errorf("expected 1 calculation, got %d", got)
	}
}

func TestSiteStatsCalculationError(t *testing.T) {
	var calls int64
	fail := int64(1)
	started := make(chan struct{}, 100)
	release := make(chan struct{})
	setupSiteStats(t, 3600, func(project, db string) (siteStatsPayload, error) {
		n := atomic.AddInt64(&calls, 1)
		if n == 1 {
			started <- struct{}{}
			<-release
		}
		if atomic.LoadInt64(&fail) == 1 {
			return siteStatsPayload{}, errors.New("boom")
		}
		return siteStatsPayload{Project: project, DB: db, Contributors: 3}, nil
	})
	// No value and the calculation fails: all waiting requests get the error
	const n = 5
	codes := make([]int, n)
	bodies := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i], bodies[i] = siteStatsRequest("test")
		}(i)
	}
	<-started
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()
	// Requests that came after the failure start (and wait for) their own calculation, which fails too
	for i := 0; i < n; i++ {
		if expected := "{\"error\":\"API 'SiteStats': boom\"}\n"; codes[i] != http.StatusBadRequest || bodies[i] != expected {
			t.Errorf("request %d: got %d %q, expected 400 %q", i+1, codes[i], bodies[i], expected)
		}
	}
	waitNoSiteStatsCalls(t)
	siteStatsCacheMtx.Lock()
	cached := len(siteStatsCache)
	siteStatsCacheMtx.Unlock()
	if cached != 0 {
		t.Errorf("failed calculation must not be cached, got %d cache entries", cached)
	}
	// The next request calculates again
	before := atomic.LoadInt64(&calls)
	atomic.StoreInt64(&fail, 0)
	code, body := siteStatsRequest("test")
	if expected := siteStatsBody("test", "testdb", 3); code != http.StatusOK || body != expected {
		t.Errorf("after failure: got %d %q, expected 200 %q", code, body, expected)
	}
	if got := atomic.LoadInt64(&calls); got != before+1 {
		t.Errorf("expected a new calculation after the failure, got %d -> %d", before, got)
	}
}

func TestSiteStatsStaleValueKeptWhenCalculationFails(t *testing.T) {
	var calls int64
	setupSiteStats(t, 3600, func(project, db string) (siteStatsPayload, error) {
		if atomic.AddInt64(&calls, 1) < 3 {
			return siteStatsPayload{}, errors.New("boom")
		}
		return siteStatsPayload{Project: project, DB: db, Contributors: 2}, nil
	})
	setSiteStatsEntry("testdb", 2*time.Hour, 1)
	// Failing calculations: the expired value stays, every request tries (one at a time) to replace it
	for i := 0; i < 3; i++ {
		code, body := siteStatsRequest("test")
		if expected := siteStatsBody("test", "testdb", 1); code != http.StatusOK || body != expected {
			t.Errorf("request %d: got %d %q, expected 200 %q", i+1, code, body, expected)
		}
		waitNoSiteStatsCalls(t)
		if got := atomic.LoadInt64(&calls); got != int64(i+1) {
			t.Errorf("request %d: expected %d calculation(s), got %d", i+1, i+1, got)
		}
	}
	// The third one succeeded
	code, body := siteStatsRequest("test")
	if expected := siteStatsBody("test", "testdb", 2); code != http.StatusOK || body != expected {
		t.Errorf("after successful calculation: got %d %q, expected 200 %q", code, body, expected)
	}
	if got := atomic.LoadInt64(&calls); got != 3 {
		t.Errorf("expected 3 calculations, got %d", got)
	}
}

func TestSiteStatsCalculationPanic(t *testing.T) {
	var calls int64
	setupSiteStats(t, 3600, func(project, db string) (siteStatsPayload, error) {
		if atomic.AddInt64(&calls, 1) == 1 {
			panic("oops")
		}
		return siteStatsPayload{Project: project, DB: db, Contributors: 4}, nil
	})
	// The waiting request gets an error, the calculation is not left as running
	code, body := siteStatsRequest("test")
	if expected := "{\"error\":\"API 'SiteStats': site stats calculation panicked: oops\"}\n"; code != http.StatusBadRequest || body != expected {
		t.Errorf("panic: got %d %q, expected 400 %q", code, body, expected)
	}
	waitNoSiteStatsCalls(t)
	code, body = siteStatsRequest("test")
	if expected := siteStatsBody("test", "testdb", 4); code != http.StatusOK || body != expected {
		t.Errorf("after panic: got %d %q, expected 200 %q", code, body, expected)
	}
}

func TestSetSiteStatsCacheTTL(t *testing.T) {
	const name = "GHA2DB_SITE_STATS_CACHE_TTL"
	old := siteStatsCacheTTL
	defer func() {
		siteStatsCacheTTL = old
		_ = os.Unsetenv(name)
	}()
	cases := []struct {
		value    string
		expected int64
	}{
		{"", 777},
		{"1", 1},
		{"+5", 5},
		{"172800", 172800},
		{"0", 777},
		{"-3", 777},
		{"abc", 777},
		{"1.5", 777},
		{" 7", 777},
		{"7 ", 777},
		{"1_000", 777},
		{"0x10", 777},
		{"9223372036854775808", 777},
	}
	for _, test := range cases {
		siteStatsCacheTTL = 777
		lib.FatalOnError(os.Setenv(name, test.value))
		setSiteStatsCacheTTL()
		if siteStatsCacheTTL != test.expected {
			t.Errorf("%s=%q: got TTL %d, expected %d", name, test.value, siteStatsCacheTTL, test.expected)
		}
	}
}
