package telemetry

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// newTestTracerProvider returns a sdktrace.TracerProvider that exports each
// span, through the redacting wrapper, to a new tracetest.InMemoryExporter.
// It shuts the provider down at the end of t.
func newTestTracerProvider(t *testing.T) (*sdktrace.TracerProvider, *tracetest.InMemoryExporter) {
	t.Helper()

	memExporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(newURLQueryRedactingExporter(memExporter)))
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})
	return tp, memExporter
}

func TestURLQueryRedactingExporterDropsTheQueryOfAClientSpan(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	tp, memExporter := newTestTracerProvider(t)
	client := &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport, otelhttp.WithTracerProvider(tp))}

	req, err := http.NewRequest(http.MethodGet,
		server.URL+"/api/v1/namespaces?labelSelector=kubernetes.io%2Fmetadata.name+in+%28a%2Cb%29", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if _, err := io.Copy(io.Discard, res.Body); err != nil {
		t.Fatalf("read the response body: %v", err)
	}
	if err := res.Body.Close(); err != nil {
		t.Fatalf("close the response body: %v", err)
	}

	spans := memExporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}

	got, ok := attributeString(spans[0].Attributes, semconv.URLFullKey)
	if !ok {
		t.Fatal("the span has no url.full attribute")
	}
	if want := server.URL + "/api/v1/namespaces"; got != want {
		t.Errorf("url.full = %q, want %q", got, want)
	}
}

func TestURLQueryRedactingExporterLeavesASpanWithNoURL(t *testing.T) {
	t.Parallel()

	tp, memExporter := newTestTracerProvider(t)

	_, span := tp.Tracer("test").Start(context.Background(), "no_url")
	span.End()

	if _, ok := attributeString(memExporter.GetSpans()[0].Attributes, semconv.URLFullKey); ok {
		t.Fatal("the span has a url.full attribute, want none")
	}
}

func TestURLQueryRedactingExporterLeavesAValueThatDoesNotParse(t *testing.T) {
	t.Parallel()

	const raw = "://not-a-url"

	tp, memExporter := newTestTracerProvider(t)

	_, span := tp.Tracer("test").Start(context.Background(), "bad_url",
		trace.WithAttributes(semconv.URLFull(raw)))
	span.End()

	got, ok := attributeString(memExporter.GetSpans()[0].Attributes, semconv.URLFullKey)
	if !ok {
		t.Fatal("the span has no url.full attribute")
	}
	if got != raw {
		t.Errorf("url.full = %q, want %q", got, raw)
	}
}

// attributeString returns the string value of the attribute of attrs with
// key, and whether attrs has that key.
func attributeString(attrs []attribute.KeyValue, key attribute.Key) (string, bool) {
	for _, attr := range attrs {
		if attr.Key == key {
			return attr.Value.AsString(), true
		}
	}
	return "", false
}
