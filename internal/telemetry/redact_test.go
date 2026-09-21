package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

func TestURLQueryRedactorDropsTheQuery(t *testing.T) {
	t.Parallel()

	const (
		full = "https://rancher.example/k8s/clusters/c-1/api/v1/namespaces?labelSelector=kubernetes.io%2Fmetadata.name+in+%28a%2Cb%29"
		want = "https://rancher.example/k8s/clusters/c-1/api/v1/namespaces"
	)

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(urlQueryRedactor{}),
		sdktrace.WithSyncer(exporter),
	)
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})

	_, span := tp.Tracer("test").Start(context.Background(), "list_namespaces",
		trace.WithAttributes(semconv.URLFull(full)))
	span.End()

	got, ok := attributeString(exporter.GetSpans()[0].Attributes, semconv.URLFullKey)
	if !ok {
		t.Fatal("the span has no url.full attribute")
	}
	if got != want {
		t.Errorf("url.full = %q, want %q", got, want)
	}
}

func TestURLQueryRedactorLeavesASpanWithNoURL(t *testing.T) {
	t.Parallel()

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(urlQueryRedactor{}),
		sdktrace.WithSyncer(exporter),
	)
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})

	_, span := tp.Tracer("test").Start(context.Background(), "no_url")
	span.End()

	if _, ok := attributeString(exporter.GetSpans()[0].Attributes, semconv.URLFullKey); ok {
		t.Fatal("the span has a url.full attribute, want none")
	}
}

func TestURLQueryRedactorLeavesAValueThatDoesNotParse(t *testing.T) {
	t.Parallel()

	const raw = "://not-a-url"

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(urlQueryRedactor{}),
		sdktrace.WithSyncer(exporter),
	)
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})

	_, span := tp.Tracer("test").Start(context.Background(), "bad_url",
		trace.WithAttributes(semconv.URLFull(raw)))
	span.End()

	got, ok := attributeString(exporter.GetSpans()[0].Attributes, semconv.URLFullKey)
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
