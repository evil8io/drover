package main

import (
	"io"
	"testing"
)

func TestParseProjectSyncConfigTelemetryDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := parseProjectSyncConfig([]string{
		"--rancher-url", "https://rancher.example.com",
		"--token-file", "/dev/null",
		"--labels", "cost-center",
	}, io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseProjectSyncConfig: %v", err)
	}
	if cfg.telemetry.Endpoint != "" {
		t.Errorf("otlp endpoint = %q, want empty", cfg.telemetry.Endpoint)
	}
	if !cfg.telemetry.Traces {
		t.Error("otlp traces = false, want true")
	}
	if !cfg.telemetry.Metrics {
		t.Error("otlp metrics = false, want true")
	}
	if cfg.telemetry.ServiceName != "drover" {
		t.Errorf("service name = %q, want drover", cfg.telemetry.ServiceName)
	}
	if cfg.telemetry.Version != version {
		t.Errorf("telemetry version = %q, want %q", cfg.telemetry.Version, version)
	}
}

func TestParseProjectSyncConfigInsecureSkipVerify(t *testing.T) {
	t.Parallel()

	cfg, err := parseProjectSyncConfig([]string{
		"--rancher-url", "https://rancher.example.com",
		"--token-file", "/dev/null",
		"--labels", "cost-center",
	}, io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseProjectSyncConfig: %v", err)
	}
	if cfg.insecureSkipVerify {
		t.Error("insecure skip verify = true, want false")
	}

	cfg, err = parseProjectSyncConfig([]string{
		"--rancher-url", "https://rancher.example.com",
		"--token-file", "/dev/null",
		"--labels", "cost-center",
		"--rancher-insecure-skip-verify",
	}, io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseProjectSyncConfig: %v", err)
	}
	if !cfg.insecureSkipVerify {
		t.Error("insecure skip verify = false, want true")
	}
}

func TestParseProjectSyncConfigTelemetryFlags(t *testing.T) {
	t.Parallel()

	cfg, err := parseProjectSyncConfig([]string{
		"--rancher-url", "https://rancher.example.com",
		"--token-file", "/dev/null",
		"--labels", "cost-center",
		"--otlp-endpoint", "collector:4317",
		"--otlp-traces=false",
		"--otlp-metrics=false",
		"--service-name", "custom",
	}, io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseProjectSyncConfig: %v", err)
	}
	if cfg.telemetry.Endpoint != "collector:4317" {
		t.Errorf("otlp endpoint = %q, want collector:4317", cfg.telemetry.Endpoint)
	}
	if cfg.telemetry.Traces {
		t.Error("otlp traces = true, want false")
	}
	if cfg.telemetry.Metrics {
		t.Error("otlp metrics = true, want false")
	}
	if cfg.telemetry.ServiceName != "custom" {
		t.Errorf("service name = %q, want custom", cfg.telemetry.ServiceName)
	}
}

func TestParseProjectSyncConfigTelemetryFromEnvironment(t *testing.T) {
	t.Parallel()
	environment := map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": "collector:4317",
		"OTEL_SERVICE_NAME":           "custom",
	}
	getenv := func(name string) string { return environment[name] }

	cfg, err := parseProjectSyncConfig([]string{
		"--rancher-url", "https://rancher.example.com",
		"--token-file", "/dev/null",
		"--labels", "cost-center",
	}, io.Discard, getenv)
	if err != nil {
		t.Fatalf("parseProjectSyncConfig: %v", err)
	}
	if cfg.telemetry.Endpoint != "collector:4317" {
		t.Errorf("otlp endpoint = %q, want collector:4317", cfg.telemetry.Endpoint)
	}
	if cfg.telemetry.ServiceName != "custom" {
		t.Errorf("service name = %q, want custom", cfg.telemetry.ServiceName)
	}
}
