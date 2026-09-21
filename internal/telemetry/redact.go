package telemetry

import (
	"context"
	"net/url"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// urlQueryRedactor is a sdktrace.SpanProcessor that removes the query and the
// fragment of the url.full attribute of a span. otelhttp puts the full
// request URL of a client span on this attribute. The query of the
// privileged namespace list names every allowed namespace of the caller, and
// that set must not reach the trace backend.
type urlQueryRedactor struct{}

// OnStart rewrites the url.full attribute of s, when present and when it
// parses as a URL, to the URL without its query and its fragment.
func (urlQueryRedactor) OnStart(_ context.Context, s sdktrace.ReadWriteSpan) {
	for _, attr := range s.Attributes() {
		if attr.Key != semconv.URLFullKey {
			continue
		}
		parsed, err := url.Parse(attr.Value.AsString())
		if err != nil {
			return
		}
		parsed.RawQuery = ""
		parsed.Fragment = ""
		s.SetAttributes(attribute.String(string(semconv.URLFullKey), parsed.String()))
		return
	}
}

// OnEnd does nothing. urlQueryRedactor redacts an attribute at span start.
func (urlQueryRedactor) OnEnd(sdktrace.ReadOnlySpan) {}

// Shutdown does nothing. urlQueryRedactor holds no resource.
func (urlQueryRedactor) Shutdown(context.Context) error { return nil }

// ForceFlush does nothing. urlQueryRedactor holds no buffer.
func (urlQueryRedactor) ForceFlush(context.Context) error { return nil }
