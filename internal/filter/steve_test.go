package filter

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// fetchOK returns a fetch func for cache.do that answers a fixed allowedSet,
// with no upstream call.
func fetchOK(name string) func() (allowedSet, *http.Response, error) {
	return func() (allowedSet, *http.Response, error) {
		return allowedSet{names: []string{name}}, nil, nil
	}
}

func TestCacheEvictsEarliestAtBound(t *testing.T) {
	t.Parallel()
	clock := newClock()
	c := newCache(time.Minute, 2, clock.Now)
	ctx := context.Background()

	if _, _, err := c.do(ctx, "a", fetchOK("a")); err != nil {
		t.Fatalf("do a: %v", err)
	}
	clock.advance(time.Second)
	if _, _, err := c.do(ctx, "b", fetchOK("b")); err != nil {
		t.Fatalf("do b: %v", err)
	}
	clock.advance(time.Second)
	if _, _, err := c.do(ctx, "c", fetchOK("c")); err != nil {
		t.Fatalf("do c: %v", err)
	}

	if _, ok := c.entries["a"]; ok {
		t.Error("the bound keeps entry a, want it evicted as the earliest")
	}
	if _, ok := c.entries["b"]; !ok {
		t.Error("the bound evicts entry b, want it kept")
	}
	if _, ok := c.entries["c"]; !ok {
		t.Error("the bound evicts entry c, want it kept")
	}
	if len(c.entries) != 2 {
		t.Errorf("entries = %d, want 2", len(c.entries))
	}
}

func TestCacheServesALiveEntryFromTheBoundSet(t *testing.T) {
	t.Parallel()
	clock := newClock()
	c := newCache(time.Minute, 2, clock.Now)
	ctx := context.Background()

	calls := 0
	fetchA := func() (allowedSet, *http.Response, error) {
		calls++
		return allowedSet{names: []string{"a"}}, nil, nil
	}
	if _, _, err := c.do(ctx, "a", fetchA); err != nil {
		t.Fatalf("do a: %v", err)
	}
	clock.advance(time.Second)
	if _, _, err := c.do(ctx, "b", fetchOK("b")); err != nil {
		t.Fatalf("do b: %v", err)
	}

	set, resp, err := c.do(ctx, "a", fetchA)
	if err != nil || resp != nil {
		t.Fatalf("do a again: resp=%v err=%v", resp, err)
	}
	if calls != 1 {
		t.Errorf("fetch calls for a = %d, want 1, the cache must serve the live entry", calls)
	}
	if len(set.names) != 1 || set.names[0] != "a" {
		t.Errorf("names = %v, want [a]", set.names)
	}
}

func TestLimiterWithinRateNotDelayed(t *testing.T) {
	t.Parallel()
	clock := newClock()
	l := newLimiter(defaultFetchRate, 2*defaultFetchRate, clock.Now)
	ctx := context.Background()

	for i := range 10 {
		if err := l.wait(ctx, fetchWaitCap); err != nil {
			t.Fatalf("wait %d: %v, want no delay inside the burst", i, err)
		}
	}
}

func TestFetchThrottledAnswers429(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	h := newHarnessOpt(t, listUpstream(steveHandler("a"), namespaceListHandler), func(cfg *Config) {
		cfg.FetchRate = 0.001
		cfg.MeterProvider = provider
	})

	resp1, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, http.Header{"Authorization": []string{"Bearer caller-1"}}))
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, want 200", resp1.StatusCode)
	}

	resp2, body2 := h.do(t, h.request(t, http.MethodGet, listPath, nil, http.Header{"Authorization": []string{"Bearer caller-2"}}))
	if resp2.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second status = %d, want 429", resp2.StatusCode)
	}
	var status statusBody
	if err := json.Unmarshal(body2, &status); err != nil {
		t.Fatalf("unmarshal status body: %v", err)
	}
	if status.Reason != reasonThrottled {
		t.Errorf("reason = %q, want %q", status.Reason, reasonThrottled)
	}
	if got := h.upstream.countPath(stevePath); got != 1 {
		t.Errorf("Steve requests = %d, want 1, the throttled fetch must skip the upstream call", got)
	}

	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	sum := findSum(t, data, "drover.filter.fetch.throttled")
	if len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 1 {
		t.Fatalf("fetch throttled data points = %+v, want one point with value 1", sum.DataPoints)
	}
}

func TestConfigDefaults(t *testing.T) {
	t.Parallel()
	tokenFile := filepath.Join(t.TempDir(), "token")
	writeToken(t, tokenFile, "service")
	up := newUpstream(t, namespaceListHandler)
	target, err := url.Parse(up.server.URL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}
	svc, err := New(Config{Upstream: target, TokenFile: tokenFile})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	if svc.cache.maxEntries != defaultMaxCacheEntries {
		t.Errorf("max cache entries = %d, want %d", svc.cache.maxEntries, defaultMaxCacheEntries)
	}
	if svc.limiter.rate != defaultFetchRate {
		t.Errorf("fetch rate = %v, want %v", svc.limiter.rate, defaultFetchRate)
	}
	if want := 2 * float64(defaultFetchRate); svc.limiter.burst != want {
		t.Errorf("fetch burst = %v, want %v", svc.limiter.burst, want)
	}
}

func TestFetchAllowedResolvesCallerName(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstreamFull(steveHandler("a"), projectsHandler(),
		selfSubjectReviewHandler("u-alice"), namespaceListHandler))

	set, denied, err := h.svc.fetchAllowed(context.Background(), "c-1", callerToken, "")
	if denied != nil || err != nil {
		t.Fatalf("fetchAllowed: denied=%v err=%v", denied, err)
	}
	if set.user != "u-alice" {
		t.Errorf("user = %q, want u-alice", set.user)
	}
}

func TestFetchAllowedKeepsUserEmptyOnIdentityForbidden(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstreamFull(steveHandler("a"), projectsHandler(),
		func(w http.ResponseWriter, r *http.Request) { http.Error(w, "forbidden", http.StatusForbidden) },
		namespaceListHandler))

	set, denied, err := h.svc.fetchAllowed(context.Background(), "c-1", callerToken, "")
	if denied != nil || err != nil {
		t.Fatalf("fetchAllowed: denied=%v err=%v", denied, err)
	}
	if set.user != "" {
		t.Errorf("user = %q, want empty on a 403 identity answer", set.user)
	}
	if len(set.names) != 1 || set.names[0] != "a" {
		t.Errorf("names = %v, want [a], the allowed set must stay correct", set.names)
	}
}

func TestFetchAllowedKeepsUserEmptyOnIdentityNotFound(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstreamFull(steveHandler("a"), projectsHandler(),
		func(w http.ResponseWriter, r *http.Request) { http.Error(w, "not found", http.StatusNotFound) },
		namespaceListHandler))

	set, denied, err := h.svc.fetchAllowed(context.Background(), "c-1", callerToken, "")
	if denied != nil || err != nil {
		t.Fatalf("fetchAllowed: denied=%v err=%v", denied, err)
	}
	if set.user != "" {
		t.Errorf("user = %q, want empty on a 404 identity answer, as a cluster below 1.28 gives", set.user)
	}
}

func TestFetchAllowedKeepsUserEmptyOnMalformedIdentityBody(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstreamFull(steveHandler("a"), projectsHandler(),
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"status": tomato}`)
		},
		namespaceListHandler))

	set, denied, err := h.svc.fetchAllowed(context.Background(), "c-1", callerToken, "")
	if denied != nil || err != nil {
		t.Fatalf("fetchAllowed: denied=%v err=%v", denied, err)
	}
	if set.user != "" {
		t.Errorf("user = %q, want empty on a body that does not parse", set.user)
	}
}
