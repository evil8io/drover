package filter

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/evil8io/drover/internal/telemetry"
)

// TestPropagatesAnIncomingTraceparent checks that a caller's traceparent
// header reaches the upstream, with the same trace id, on every request that
// the filter sends. Setup installs the global propagator that carries the
// header, also with telemetry off.
func TestPropagatesAnIncomingTraceparent(t *testing.T) {
	t.Parallel()
	if _, err := telemetry.Setup(context.Background(), telemetry.Config{}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	h := newHarness(t, listUpstream(steveHandler("a"), namespaceListHandler))

	const (
		traceID     = "4bf92f3577b34da6a3ce929d0e0e4736"
		traceparent = "00-" + traceID + "-00f067aa0ba902b7-01"
		headerName  = "Traceparent"
	)
	header := callerHeader()
	header.Set(headerName, traceparent)

	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, header))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	requests := h.upstream.all()
	if len(requests) == 0 {
		t.Fatal("the upstream got no request")
	}
	for _, request := range requests {
		got := request.header.Get(headerName)
		if !strings.Contains(got, traceID) {
			t.Errorf("request %s %q Traceparent = %q, want the trace id %q", request.method, request.path, got, traceID)
		}
	}
}

// TestFilterMetricsCountRequests checks that a namespace list request
// increments drover.filter.requests once, with its outcome attribute. It
// reads the SDK's own manual reader, not a live exporter.
func TestFilterMetricsCountRequests(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	tokenPath := filepath.Join(t.TempDir(), "token")
	writeToken(t, tokenPath, "service")
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"kind":"NamespaceList","items":[]}`)
	})
	target, err := url.Parse(up.server.URL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}
	svc, err := New(Config{
		Upstream:      target,
		TokenFile:     tokenPath,
		MeterProvider: provider,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	proxy := httptest.NewServer(svc)
	t.Cleanup(proxy.Close)

	req, err := http.NewRequest(http.MethodGet, proxy.URL+listPath, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", callerToken)
	resp, err := proxy.Client().Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}

	sum := findSum(t, data, "drover.filter.requests")
	if len(sum.DataPoints) != 1 {
		t.Fatalf("data points = %d, want 1", len(sum.DataPoints))
	}
	point := sum.DataPoints[0]
	if point.Value != 1 {
		t.Errorf("value = %d, want 1", point.Value)
	}
	outcome, ok := point.Attributes.Value(attribute.Key("outcome"))
	if !ok || outcome.AsString() != outcomeNative {
		t.Errorf("outcome attribute = %v, ok=%v, want %q", outcome, ok, outcomeNative)
	}
}

// findSum returns the metricdata.Sum[int64] of the metric named name, from
// the first scope that has it.
func findSum(t *testing.T, data metricdata.ResourceMetrics, name string) metricdata.Sum[int64] {
	t.Helper()
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %s has type %T, want metricdata.Sum[int64]", name, m.Data)
			}
			return sum
		}
	}
	t.Fatalf("no metric named %s", name)
	return metricdata.Sum[int64]{}
}
