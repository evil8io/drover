package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/url"

	"github.com/evil8io/drover/internal/telemetry"
)

// parseRancherURL returns the URL of a Rancher server. The name is the flag name,
// for the error message.
func parseRancherURL(name, raw string) (*url.URL, error) {
	if raw == "" {
		return nil, fmt.Errorf("%s is required", name)
	}
	target, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	if target.Scheme != "http" && target.Scheme != "https" {
		return nil, fmt.Errorf("%s scheme %q is not http or https", name, target.Scheme)
	}
	if target.Host == "" {
		return nil, fmt.Errorf("%s has no host", name)
	}
	if target.Path != "" && target.Path != "/" {
		return nil, fmt.Errorf("%s path %q is not empty", name, target.Path)
	}
	target.Path = ""
	target.RawPath = ""
	return target, nil
}

func parseLevel(name string) (slog.Level, error) {
	switch name {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("-log-level %q is not debug, info, warn or error", name)
}

// telemetryFlags holds the four OTLP flags that every subcommand shares.
type telemetryFlags struct {
	endpoint    string
	traces      bool
	metrics     bool
	serviceName string
}

// registerTelemetryFlags adds otlp-endpoint, otlp-traces, otlp-metrics and
// service-name to flags. getenv resolves the environment defaults of
// otlp-endpoint (OTEL_EXPORTER_OTLP_ENDPOINT) and service-name
// (OTEL_SERVICE_NAME, or drover when that is also empty).
func registerTelemetryFlags(flags *flag.FlagSet, getenv func(string) string) *telemetryFlags {
	tf := &telemetryFlags{}
	flags.StringVar(&tf.endpoint, "otlp-endpoint", getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		"OTLP gRPC endpoint, host:port or a URL; empty turns telemetry off")
	flags.BoolVar(&tf.traces, "otlp-traces", true, "send traces to the OTLP endpoint")
	flags.BoolVar(&tf.metrics, "otlp-metrics", true, "send metrics to the OTLP endpoint")
	serviceName := getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName = "drover"
	}
	flags.StringVar(&tf.serviceName, "service-name", serviceName, "service.name resource attribute")
	return tf
}

// config returns the telemetry configuration that the flags describe.
func (tf *telemetryFlags) config() telemetry.Config {
	return telemetry.Config{
		Endpoint:    tf.endpoint,
		Traces:      tf.traces,
		Metrics:     tf.metrics,
		ServiceName: tf.serviceName,
		Version:     version,
	}
}
