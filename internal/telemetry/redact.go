package telemetry

import (
	"context"
	"net/url"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// redactedURLKeys lists the span attributes that carry a URL with a query
// that must not reach the trace backend. The query of the privileged
// namespace list names every allowed namespace of the caller.
var redactedURLKeys = map[attribute.Key]bool{
	semconv.URLFullKey: true,
}

// newURLQueryRedactingExporter wraps next so an exported span has the query
// and the fragment removed from each attribute in redactedURLKeys. otelhttp
// sets url.full on a client span inside RoundTrip, after the span starts, so
// a SpanProcessor never sees the final value. A SpanExporter sees the span
// after RoundTrip ends.
func newURLQueryRedactingExporter(next sdktrace.SpanExporter) sdktrace.SpanExporter {
	return urlQueryRedactingExporter{next: next}
}

type urlQueryRedactingExporter struct {
	next sdktrace.SpanExporter
}

func (e urlQueryRedactingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	redacted := make([]sdktrace.ReadOnlySpan, len(spans))
	for i, span := range spans {
		redacted[i] = redactedSpan{ReadOnlySpan: span}
	}
	return e.next.ExportSpans(ctx, redacted)
}

func (e urlQueryRedactingExporter) Shutdown(ctx context.Context) error {
	return e.next.Shutdown(ctx)
}

// redactedSpan wraps a sdktrace.ReadOnlySpan and overrides Attributes to
// redact the keys in redactedURLKeys.
type redactedSpan struct {
	sdktrace.ReadOnlySpan
}

func (s redactedSpan) Attributes() []attribute.KeyValue {
	attrs := s.ReadOnlySpan.Attributes()
	out := make([]attribute.KeyValue, len(attrs))
	for i, attr := range attrs {
		if redactedURLKeys[attr.Key] {
			if redacted, ok := redactQuery(attr.Value.AsString()); ok {
				attr = attribute.String(string(attr.Key), redacted)
			}
		}
		out[i] = attr
	}
	return out
}

// redactQuery returns raw without its query and its fragment, and reports
// whether raw parses as a URL.
func redactQuery(raw string) (string, bool) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", false
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), true
}
