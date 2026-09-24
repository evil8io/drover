package filter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
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
	if limit, _ := sum.DataPoints[0].Attributes.Value(attribute.Key("limit")); limit.AsString() != limitShared {
		t.Errorf("limit attribute = %q, want %q", limit.AsString(), limitShared)
	}
}

// TestFetchThrottledPerCaller checks that the misses of one credential past
// the burst of the limit per caller answer 429, while a fetch of another
// credential still gets a token. A denied Steve answer is not cached, so
// each request of that credential is a miss.
func TestFetchThrottledPerCaller(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	const attacker = "Bearer attacker"
	steve := func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == attacker {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		steveHandler("a")(w, r)
	}
	h := newHarnessOpt(t, listUpstream(steve, namespaceListHandler), func(cfg *Config) {
		cfg.FetchRatePerCaller = 1
		cfg.MeterProvider = provider
	})
	attackerHeader := http.Header{"Authorization": []string{attacker}}

	for i := range 2 {
		resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, attackerHeader))
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("request %d status = %d, want the 403 of Steve inside the burst", i, resp.StatusCode)
		}
	}

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath, nil, attackerHeader))
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status past the burst = %d, want 429", resp.StatusCode)
	}
	var status statusBody
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatalf("unmarshal status body: %v", err)
	}
	if want := serviceName + ": the fetch rate limit per caller has no free token"; status.Message != want {
		t.Errorf("message = %q, want %q", status.Message, want)
	}
	if got := h.upstream.countPath(stevePath); got != 2 {
		t.Errorf("Steve requests = %d, want 2, the throttled fetch must skip the upstream call", got)
	}

	resp, _ = h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status of another credential = %d, want 200", resp.StatusCode)
	}

	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	sum := findSum(t, data, "drover.filter.fetch.throttled")
	if len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 1 {
		t.Fatalf("fetch throttled data points = %+v, want one point with value 1", sum.DataPoints)
	}
	if limit, _ := sum.DataPoints[0].Attributes.Value(attribute.Key("limit")); limit.AsString() != limitCaller {
		t.Errorf("limit attribute = %q, want %q", limit.AsString(), limitCaller)
	}
}

// TestCallerLimitersStayBounded checks that more caller credentials than the
// cache bound leave that count of limiters per caller, and that the limiter
// with the oldest refill goes first.
func TestCallerLimitersStayBounded(t *testing.T) {
	t.Parallel()
	h := newHarnessOpt(t, listUpstream(steveHandler("a"), namespaceListHandler), func(cfg *Config) {
		cfg.MaxCacheEntries = 2
	})

	headers := make([]http.Header, 0, 5)
	for i := range 5 {
		header := http.Header{"Authorization": []string{fmt.Sprintf("Bearer caller-%d", i)}}
		headers = append(headers, header)
		resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, header))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d status = %d, want 200", i, resp.StatusCode)
		}
		h.clock.advance(time.Second)
	}

	h.svc.callers.mu.Lock()
	defer h.svc.callers.mu.Unlock()
	if got := len(h.svc.callers.limiters); got != 2 {
		t.Errorf("limiters = %d, want 2", got)
	}
	for _, i := range []int{3, 4} {
		if _, ok := h.svc.callers.limiters[callerHash(headers[i])]; !ok {
			t.Errorf("the limiter of caller-%d is evicted, want the two newest kept", i)
		}
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
	if svc.callers.rate != defaultFetchRatePerCaller {
		t.Errorf("fetch rate per caller = %v, want %v", svc.callers.rate, defaultFetchRatePerCaller)
	}
	if want := 2 * float64(defaultFetchRatePerCaller); svc.callers.burst != want {
		t.Errorf("fetch burst per caller = %v, want %v", svc.callers.burst, want)
	}
	if svc.callers.maxEntries != defaultMaxCacheEntries {
		t.Errorf("max caller limiters = %d, want %d", svc.callers.maxEntries, defaultMaxCacheEntries)
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

func TestCredentialKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		header http.Header
		want   string
	}{
		{
			name:   "authorization",
			header: http.Header{"Authorization": []string{"Bearer x"}},
			want:   "authorization\nBearer x",
		},
		{
			name:   "authorization ignores a cookie",
			header: http.Header{"Authorization": []string{"Bearer x"}, "Cookie": []string{"R_SESS=a"}},
			want:   "authorization\nBearer x",
		},
		{
			name:   "session cookie",
			header: http.Header{"Cookie": []string{"R_SESS=a"}},
			want:   "cookie\na",
		},
		{
			name:   "session cookie ignores another cookie on the same line",
			header: http.Header{"Cookie": []string{"CSRF=x; R_SESS=a"}},
			want:   "cookie\na",
		},
		{
			name:   "session cookie on a second Cookie line",
			header: http.Header{"Cookie": []string{"CSRF=x", "R_SESS=a"}},
			want:   "cookie\na",
		},
		{
			name:   "authorization ignores a second value",
			header: http.Header{"Authorization": []string{"Bearer x", "Bearer y"}},
			want:   "authorization\nBearer x",
		},
		{
			name:   "session cookie ignores a second session cookie",
			header: http.Header{"Cookie": []string{"R_SESS=a; R_SESS=b"}},
			want:   "cookie\na",
		},
		{
			name:   "session cookie ignores a session cookie on a second Cookie line",
			header: http.Header{"Cookie": []string{"R_SESS=a", "R_SESS=b"}},
			want:   "cookie\na",
		},
		{
			name:   "empty authorization reads the session cookie",
			header: http.Header{"Authorization": []string{""}, "Cookie": []string{"R_SESS=a"}},
			want:   "cookie\na",
		},
		{
			name:   "empty authorization and no cookie",
			header: http.Header{"Authorization": []string{""}},
			want:   noCredential,
		},
		{
			name:   "no session cookie",
			header: http.Header{"Cookie": []string{"CSRF=x"}},
			want:   noCredential,
		},
		{
			name:   "no credential",
			header: http.Header{},
			want:   noCredential,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := credentialKey(test.header); got != test.want {
				t.Errorf("credentialKey(%v) = %q, want %q", test.header, got, test.want)
			}
		})
	}
}

// TestAllowedSetCacheKeyIgnoresCookieWithAuthorization checks that two
// requests with the same Authorization, and a different Cookie, share one
// allowed set fetch.
func TestAllowedSetCacheKeyIgnoresCookieWithAuthorization(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler("a"), namespaceListHandler))

	header1 := callerHeader()
	header1.Set("Cookie", "CSRF=1")
	resp1, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, header1))
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, want 200", resp1.StatusCode)
	}

	header2 := callerHeader()
	header2.Set("Cookie", "CSRF=2")
	resp2, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, header2))
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("second status = %d, want 200", resp2.StatusCode)
	}

	if got := h.upstream.countPath(stevePath); got != 1 {
		t.Errorf("allowed set requests = %d, want 1", got)
	}
}

// TestAllowedSetCacheKeySeparatesSessionCookies checks that two callers with
// no Authorization, and a different R_SESS cookie, get two allowed set
// fetches.
func TestAllowedSetCacheKeySeparatesSessionCookies(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler("a"), namespaceListHandler))

	resp1, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, http.Header{"Cookie": []string{"R_SESS=alice"}}))
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, want 200", resp1.StatusCode)
	}
	resp2, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, http.Header{"Cookie": []string{"R_SESS=bob"}}))
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("second status = %d, want 200", resp2.StatusCode)
	}

	if got := h.upstream.countPath(stevePath); got != 2 {
		t.Errorf("allowed set requests = %d, want 2", got)
	}
}

// TestAllowedSetCacheKeyIgnoresNonSessionCookie checks that two callers with
// the same R_SESS, and a different other cookie, share one allowed set
// fetch.
func TestAllowedSetCacheKeyIgnoresNonSessionCookie(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler("a"), namespaceListHandler))

	resp1, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, http.Header{"Cookie": []string{"R_SESS=x; CSRF=x"}}))
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, want 200", resp1.StatusCode)
	}
	resp2, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, http.Header{"Cookie": []string{"R_SESS=x; CSRF=y"}}))
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("second status = %d, want 200", resp2.StatusCode)
	}

	if got := h.upstream.countPath(stevePath); got != 1 {
		t.Errorf("allowed set requests = %d, want 1", got)
	}
}

// TestAllowedSetCacheKeyReadsSecondCookieLine checks that an R_SESS cookie on
// the second Cookie header line is part of the cache key, and that the Steve
// request carries every Cookie line of the caller, joined with "; ".
func TestAllowedSetCacheKeyReadsSecondCookieLine(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler("a"), namespaceListHandler))

	header1 := http.Header{}
	header1.Add("Cookie", "CSRF=abc")
	header1.Add("Cookie", "R_SESS=alice")
	resp1, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, header1))
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, want 200", resp1.StatusCode)
	}

	var steve recorded
	for _, request := range h.upstream.all() {
		if request.path == stevePath {
			steve = request
			break
		}
	}
	if want := "CSRF=abc; R_SESS=alice"; steve.header.Get("Cookie") != want {
		t.Errorf("Steve Cookie = %q, want %q", steve.header.Get("Cookie"), want)
	}

	header2 := http.Header{}
	header2.Add("Cookie", "CSRF=abc")
	header2.Add("Cookie", "R_SESS=bob")
	resp2, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, header2))
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("second status = %d, want 200", resp2.StatusCode)
	}

	if got := h.upstream.countPath(stevePath); got != 2 {
		t.Errorf("allowed set requests = %d, want 2, the R_SESS of the second line must be part of the key", got)
	}
}

// TestAllowedSetCacheKeyIgnoresSecondSessionCookie checks that list requests
// with one first R_SESS cookie, and a second R_SESS cookie that changes per
// request, share one allowed set fetch.
func TestAllowedSetCacheKeyIgnoresSecondSessionCookie(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler("a"), namespaceListHandler))

	for i := range 5 {
		header := http.Header{"Cookie": []string{fmt.Sprintf("R_SESS=alice; R_SESS=random-%d", i)}}
		resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, header))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d status = %d, want 200", i, resp.StatusCode)
		}
	}

	if got := h.upstream.countPath(stevePath); got != 1 {
		t.Errorf("allowed set requests = %d, want 1", got)
	}
}

// TestAllowedSetCacheKeyIgnoresSecondAuthorization checks that list requests
// with one first Authorization value, and a second value that changes per
// request, share one allowed set fetch, and that the fetch sends the first
// value only.
func TestAllowedSetCacheKeyIgnoresSecondAuthorization(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler("a"), namespaceListHandler))

	for i := range 5 {
		header := http.Header{"Authorization": []string{callerToken, fmt.Sprintf("Bearer random-%d", i)}}
		resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, header))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d status = %d, want 200", i, resp.StatusCode)
		}
	}

	if got := h.upstream.countPath(stevePath); got != 1 {
		t.Errorf("allowed set requests = %d, want 1", got)
	}
	for _, request := range h.upstream.all() {
		if request.path == stevePath && !slices.Equal(request.header.Values("Authorization"), []string{callerToken}) {
			t.Errorf("Steve Authorization = %q, want only %q", request.header.Values("Authorization"), callerToken)
		}
	}
}

// credentialSteveHandler answers Steve with the one namespace ns-<credential>,
// for the credential that Rancher reads: the first Authorization value
// without the Bearer prefix, else the first R_SESS cookie.
func credentialSteveHandler(w http.ResponseWriter, r *http.Request) {
	credential := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if credential == "" {
		if session, err := r.Cookie(sessionCookie); err == nil {
			credential = session.Value
		}
	}
	steveHandler("ns-"+credential)(w, r)
}

// TestAllowedSetCacheKeySeparatesFirstCredentials checks that a caller whose
// second Authorization value or second R_SESS cookie is the credential of
// another caller gets its own fetch and its own allowed set, never the
// cached set of the other caller.
func TestAllowedSetCacheKeySeparatesFirstCredentials(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		victim   http.Header
		attacker http.Header
	}{
		{
			name:     "authorization",
			victim:   http.Header{"Authorization": []string{"Bearer bob"}},
			attacker: http.Header{"Authorization": []string{"Bearer alice", "Bearer bob"}, "Cookie": []string{"R_SESS=bob"}},
		},
		{
			name:     "session cookie",
			victim:   http.Header{"Cookie": []string{"R_SESS=bob"}},
			attacker: http.Header{"Cookie": []string{"R_SESS=alice; R_SESS=bob"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, listUpstream(credentialSteveHandler, namespaceListHandler))

			steps := []struct {
				header http.Header
				want   string
			}{
				{test.victim, "ns-bob"},
				{test.attacker, "ns-alice"},
				{test.victim, "ns-bob"},
				{test.attacker, "ns-alice"},
			}
			for i, step := range steps {
				resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, step.header))
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("request %d status = %d, want 200", i, resp.StatusCode)
				}
				want := nameLabel + " in (" + step.want + ")"
				if got := h.upstream.privileged(t).query.Get("labelSelector"); got != want {
					t.Errorf("request %d labelSelector = %q, want %q", i, got, want)
				}
			}

			if got := h.upstream.countPath(stevePath); got != 2 {
				t.Errorf("allowed set requests = %d, want 2", got)
			}
		})
	}
}

// TestAllowedSetCacheKeyReadsCookieWithEmptyAuthorization checks that a
// request with an empty Authorization value keys on its R_SESS cookie, so two
// such requests with a different cookie get two allowed set fetches.
func TestAllowedSetCacheKeyReadsCookieWithEmptyAuthorization(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler("a"), namespaceListHandler))

	for _, session := range []string{"alice", "bob"} {
		header := http.Header{"Authorization": []string{""}, "Cookie": []string{"R_SESS=" + session}}
		resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, header))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status with R_SESS=%s = %d, want 200", session, resp.StatusCode)
		}
	}

	if native := h.upstream.all()[0].header.Values("Authorization"); !slices.Equal(native, []string{""}) {
		t.Fatalf("native Authorization = %q, want one empty value", native)
	}
	if got := h.upstream.countPath(stevePath); got != 2 {
		t.Errorf("allowed set requests = %d, want 2", got)
	}
}

// projectsHandlerRaw answers /v3/projects with the given ids unchanged,
// unlike projectsHandler, which prefixes every id with the test cluster. A
// test that needs an id of another cluster uses this instead.
func projectsHandlerRaw(ids ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		items := make([]string, 0, len(ids))
		for _, id := range ids {
			items = append(items, fmt.Sprintf(`{"id":%q}`, id))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, fmt.Sprintf(`{"type":"collection","data":[%s]}`, strings.Join(items, ",")))
	}
}

// TestFetchProjectIDsIsClusterScoped checks that the project list request
// carries clusterId for the cluster of the caller, and that a project id of
// another cluster is dropped from the allowed set.
func TestFetchProjectIDsIsClusterScoped(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstreamWithProjects(
		steveLabeledHandler(steveNamespace{name: "a", project: "p-1"}),
		projectsHandlerRaw("c-1:p-1", "c-2:p-other"),
		namespaceListHandler,
	))

	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var projects recorded
	for _, request := range h.upstream.all() {
		if request.path == projectsPath {
			projects = request
			break
		}
	}
	if got := projects.query.Get("clusterId"); got != "c-1" {
		t.Errorf("clusterId = %q, want c-1", got)
	}

	privileged := h.upstream.privileged(t)
	want := "field.cattle.io/projectId in (p-1)"
	if got := privileged.query.Get("labelSelector"); got != want {
		t.Errorf("labelSelector = %q, want %q, the id of the other cluster must not reach the selector", got, want)
	}
}

// TestConfigDefaultsWithNegativeValues checks that New substitutes the
// defaults for a negative CacheTTL, MaxCacheEntries, FetchRate,
// FetchRatePerCaller and MaxWatches, the same as it does for zero.
func TestConfigDefaultsWithNegativeValues(t *testing.T) {
	t.Parallel()
	tokenFile := filepath.Join(t.TempDir(), "token")
	writeToken(t, tokenFile, "service")
	up := newUpstream(t, namespaceListHandler)
	target, err := url.Parse(up.server.URL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}
	svc, err := New(Config{
		Upstream:           target,
		TokenFile:          tokenFile,
		CacheTTL:           -time.Second,
		MaxCacheEntries:    -1,
		FetchRate:          -1,
		FetchRatePerCaller: -1,
		MaxWatches:         -1,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	if svc.cache.ttl != defaultCacheTTL {
		t.Errorf("cache ttl = %v, want %v", svc.cache.ttl, defaultCacheTTL)
	}
	if svc.cache.maxEntries != defaultMaxCacheEntries {
		t.Errorf("max cache entries = %d, want %d", svc.cache.maxEntries, defaultMaxCacheEntries)
	}
	if svc.limiter.rate != defaultFetchRate {
		t.Errorf("fetch rate = %v, want %v", svc.limiter.rate, defaultFetchRate)
	}
	if svc.callers.rate != defaultFetchRatePerCaller {
		t.Errorf("fetch rate per caller = %v, want %v", svc.callers.rate, defaultFetchRatePerCaller)
	}
	if svc.maxWatches != defaultMaxWatches {
		t.Errorf("max watches = %d, want %d", svc.maxWatches, defaultMaxWatches)
	}
}

// TestCacheDoesNotSpinWithNonPositiveMaxEntries checks that cache.do returns
// promptly when maxEntries is 0 or negative, instead of spinning in the
// eviction loop.
func TestCacheDoesNotSpinWithNonPositiveMaxEntries(t *testing.T) {
	t.Parallel()
	for _, maxEntries := range []int{0, -1} {
		t.Run(fmt.Sprintf("maxEntries=%d", maxEntries), func(t *testing.T) {
			t.Parallel()
			clock := newClock()
			c := newCache(time.Minute, maxEntries, clock.Now)

			done := make(chan error, 1)
			go func() {
				_, _, err := c.do(context.Background(), "a", fetchOK("a"))
				done <- err
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("do: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("cache.do did not return with maxEntries %d", maxEntries)
			}
		})
	}
}
