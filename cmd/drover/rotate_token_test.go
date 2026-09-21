package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func credentialsDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatalf("write the file %s: %v", name, err)
		}
	}
	return dir
}

func noEnvironment(string) string { return "" }

func TestParseRotateConfigErrors(t *testing.T) {
	t.Parallel()
	credentials := credentialsDir(t, map[string]string{"username": "drover\n", "password": "secret\n"})
	partial := credentialsDir(t, map[string]string{"password": "secret\n"})
	empty := credentialsDir(t, map[string]string{"username": "drover\n", "password": "  \n"})

	base := []string{
		"--rancher-url", "https://rancher.example.com",
		"--credentials-dir", credentials,
		"--token-secret", "cattle-system/drover-token",
		"--kube-url", "https://kubernetes.default.svc",
	}
	with := func(args ...string) []string { return append(append([]string{}, base...), args...) }

	tests := []struct {
		name string
		args []string
	}{
		{"no rancher url", []string{"--credentials-dir", credentials, "--token-secret", "a/b", "--kube-url", "https://k"}},
		{"rancher url with a path", []string{
			"--rancher-url", "https://rancher.example.com/rancher",
			"--credentials-dir", credentials, "--token-secret", "a/b", "--kube-url", "https://k",
		}},
		{"rancher url with another scheme", []string{
			"--rancher-url", "ftp://rancher.example.com",
			"--credentials-dir", credentials, "--token-secret", "a/b", "--kube-url", "https://k",
		}},
		{"rancher url without a host", []string{
			"--rancher-url", "https://",
			"--credentials-dir", credentials, "--token-secret", "a/b", "--kube-url", "https://k",
		}},
		{"no credentials dir", []string{
			"--rancher-url", "https://rancher.example.com", "--token-secret", "a/b", "--kube-url", "https://k",
		}},
		{"no token secret", []string{
			"--rancher-url", "https://rancher.example.com", "--credentials-dir", credentials, "--kube-url", "https://k",
		}},
		{"token secret without a namespace", with("--token-secret", "drover-token")},
		{"token secret with an empty namespace", with("--token-secret", "/drover-token")},
		{"token secret with two slashes", with("--token-secret", "a/b/c")},
		{"password secret without a namespace", with("--password-secret", "u-drover")},
		{"password secret with two slashes", with("--password-secret", "a/b/c")},
		{"credentials dir without a username file", with("--credentials-dir", partial)},
		{"credentials dir with an empty password file", with("--credentials-dir", empty)},
		{"credentials dir that does not exist", with("--credentials-dir", credentials+".missing")},
		{"empty token key", with("--token-key", "")},
		{"ttl zero", with("--ttl", "0s")},
		{"renew window as long as the ttl", with("--ttl", "24h", "--renew-before", "24h")},
		{"renew window zero", with("--renew-before", "0s")},
		{"keep zero", with("--keep", "0")},
		{"empty description", with("--description", "")},
		{"empty service account dir", with("--kube-service-account-dir", "")},
		{"kube url with a path", with("--kube-url", "https://kubernetes.default.svc/api")},
		{"unknown log level", with("--log-level", "trace")},
		{"no kube url and no cluster environment", []string{
			"--rancher-url", "https://rancher.example.com",
			"--credentials-dir", credentials, "--token-secret", "cattle-system/drover-token",
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := parseRotateConfig(test.args, io.Discard, noEnvironment); err == nil {
				t.Errorf("parseRotateConfig(%v) returned no error", test.args)
			}
		})
	}
}

func TestParseRotateConfigValid(t *testing.T) {
	t.Parallel()
	credentials := credentialsDir(t, map[string]string{"username": " drover \n", "password": "secret\n"})
	serviceAccount := credentialsDir(t, map[string]string{"token": "kube\n", "ca.crt": "pem\n"})

	cfg, err := parseRotateConfig([]string{
		"--rancher-url", "http://rancher.cattle-system.svc/",
		"--credentials-dir", credentials,
		"--token-secret", "cattle-system/drover-token",
		"--token-key", "api-token",
		"--ttl", "72h",
		"--renew-before", "36h",
		"--keep", "4",
		"--description", "token rotation",
		"--kube-url", "https://kubernetes.default.svc:443",
		"--kube-service-account-dir", serviceAccount,
		"--log-level", "debug",
	}, io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseRotateConfig: %v", err)
	}

	if got := cfg.rancher.String(); got != "http://rancher.cattle-system.svc" {
		t.Errorf("rancher URL = %q", got)
	}
	if got := cfg.kube.String(); got != "https://kubernetes.default.svc:443" {
		t.Errorf("kube URL = %q", got)
	}
	if cfg.namespace != "cattle-system" || cfg.secret != "drover-token" {
		t.Errorf("secret = %s/%s, want cattle-system/drover-token", cfg.namespace, cfg.secret)
	}
	if cfg.key != "api-token" {
		t.Errorf("key = %q, want api-token", cfg.key)
	}
	if cfg.username != "drover" || cfg.password != "secret" {
		t.Errorf("credentials = %q, and the test expects the trimmed user name", cfg.username)
	}
	if cfg.ttl != 72*time.Hour || cfg.renewBefore != 36*time.Hour {
		t.Errorf("ttl = %s, renew before = %s", cfg.ttl, cfg.renewBefore)
	}
	if cfg.keep != 4 {
		t.Errorf("keep = %d, want 4", cfg.keep)
	}
	if cfg.description != "token rotation" {
		t.Errorf("description = %q, want token rotation", cfg.description)
	}
	if want := filepath.Join(serviceAccount, "token"); cfg.kubeTokenFile != want {
		t.Errorf("token file = %q, want %q", cfg.kubeTokenFile, want)
	}
	if want := filepath.Join(serviceAccount, "ca.crt"); cfg.kubeCAFile != want {
		t.Errorf("CA file = %q, want %q", cfg.kubeCAFile, want)
	}
}

func TestParseRotateConfigInCluster(t *testing.T) {
	t.Parallel()
	credentials := credentialsDir(t, map[string]string{"username": "drover\n", "password": "secret\n"})
	environment := map[string]string{
		"KUBERNETES_SERVICE_HOST": "10.100.0.1",
		"KUBERNETES_SERVICE_PORT": "443",
	}

	cfg, err := parseRotateConfig([]string{
		"--rancher-url", "http://rancher.cattle-system.svc",
		"--credentials-dir", credentials,
		"--token-secret", "cattle-system/drover-token",
	}, io.Discard, func(name string) string { return environment[name] })
	if err != nil {
		t.Fatalf("parseRotateConfig: %v", err)
	}

	if got := cfg.kube.String(); got != "https://10.100.0.1:443" {
		t.Errorf("kube URL = %q, want https://10.100.0.1:443", got)
	}
	if want := filepath.Join(defaultServiceAccountDir, "token"); cfg.kubeTokenFile != want {
		t.Errorf("token file = %q, want %q", cfg.kubeTokenFile, want)
	}
	if cfg.key != "token" || cfg.ttl != 48*time.Hour || cfg.renewBefore != 24*time.Hour || cfg.keep != 2 {
		t.Errorf("the defaults are key %q, ttl %s, renew before %s, keep %d",
			cfg.key, cfg.ttl, cfg.renewBefore, cfg.keep)
	}
	if cfg.description != "drover rotate-token" {
		t.Errorf("description = %q, want drover rotate-token", cfg.description)
	}
}

func TestParseRotateConfigInsecureSkipVerify(t *testing.T) {
	t.Parallel()
	credentials := credentialsDir(t, map[string]string{"username": "drover\n", "password": "secret\n"})
	base := []string{
		"--rancher-url", "https://rancher.example.com",
		"--credentials-dir", credentials,
		"--token-secret", "cattle-system/drover-token",
		"--kube-url", "https://kubernetes.default.svc",
	}

	cfg, err := parseRotateConfig(base, io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseRotateConfig: %v", err)
	}
	if cfg.rancherInsecureSkipVerify {
		t.Error("insecure skip verify = true, want false")
	}

	cfg, err = parseRotateConfig(append(append([]string{}, base...), "--rancher-insecure-skip-verify"), io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseRotateConfig: %v", err)
	}
	if !cfg.rancherInsecureSkipVerify {
		t.Error("insecure skip verify = false, want true")
	}
}

func TestParseRotateConfigTelemetryDefaults(t *testing.T) {
	t.Parallel()
	credentials := credentialsDir(t, map[string]string{"username": "drover\n", "password": "secret\n"})

	cfg, err := parseRotateConfig([]string{
		"--rancher-url", "https://rancher.example.com",
		"--credentials-dir", credentials,
		"--token-secret", "cattle-system/drover-token",
		"--kube-url", "https://kubernetes.default.svc",
	}, io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseRotateConfig: %v", err)
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

func TestParseRotateConfigTelemetryFlags(t *testing.T) {
	t.Parallel()
	credentials := credentialsDir(t, map[string]string{"username": "drover\n", "password": "secret\n"})

	cfg, err := parseRotateConfig([]string{
		"--rancher-url", "https://rancher.example.com",
		"--credentials-dir", credentials,
		"--token-secret", "cattle-system/drover-token",
		"--kube-url", "https://kubernetes.default.svc",
		"--otlp-endpoint", "collector:4317",
		"--otlp-traces=false",
		"--otlp-metrics=false",
		"--service-name", "custom",
	}, io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseRotateConfig: %v", err)
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

func TestParseRotateConfigTelemetryFromEnvironment(t *testing.T) {
	t.Parallel()
	credentials := credentialsDir(t, map[string]string{"username": "drover\n", "password": "secret\n"})
	environment := map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": "collector:4317",
		"OTEL_SERVICE_NAME":           "custom",
	}
	getenv := func(name string) string { return environment[name] }

	cfg, err := parseRotateConfig([]string{
		"--rancher-url", "https://rancher.example.com",
		"--credentials-dir", credentials,
		"--token-secret", "cattle-system/drover-token",
		"--kube-url", "https://kubernetes.default.svc",
	}, io.Discard, getenv)
	if err != nil {
		t.Fatalf("parseRotateConfig: %v", err)
	}
	if cfg.telemetry.Endpoint != "collector:4317" {
		t.Errorf("otlp endpoint = %q, want collector:4317", cfg.telemetry.Endpoint)
	}
	if cfg.telemetry.ServiceName != "custom" {
		t.Errorf("service name = %q, want custom", cfg.telemetry.ServiceName)
	}
}

func TestParseRotateConfigPasswordSecret(t *testing.T) {
	t.Parallel()
	credentials := credentialsDir(t, map[string]string{"username": "drover\n", "password": "secret\n"})
	base := []string{
		"--rancher-url", "https://rancher.example.com",
		"--credentials-dir", credentials,
		"--token-secret", "cattle-system/drover-token",
		"--kube-url", "https://kubernetes.default.svc",
	}
	with := func(args ...string) []string { return append(append([]string{}, base...), args...) }

	cfg, err := parseRotateConfig(base, io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseRotateConfig: %v", err)
	}
	if cfg.passwordNamespace != "" || cfg.passwordSecret != "" {
		t.Errorf("password secret = %s/%s, and the test expects no value", cfg.passwordNamespace, cfg.passwordSecret)
	}

	cfg, err = parseRotateConfig(with("--password-secret=cattle-local-user-passwords/u-drover"), io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseRotateConfig: %v", err)
	}
	if cfg.passwordNamespace != "cattle-local-user-passwords" || cfg.passwordSecret != "u-drover" {
		t.Errorf("password secret = %s/%s, want cattle-local-user-passwords/u-drover",
			cfg.passwordNamespace, cfg.passwordSecret)
	}

	_, err = parseRotateConfig(with("--password-secret=u-drover"), io.Discard, noEnvironment)
	if err == nil {
		t.Fatal("parseRotateConfig returned no error for a value without a slash")
	}
	if !strings.Contains(err.Error(), "-password-secret") {
		t.Errorf("the error is %q, and the test expects the flag name", err)
	}
}

func TestRunDispatchesRotateToken(t *testing.T) {
	t.Parallel()
	if _, ok := subcommands["rotate-token"]; !ok {
		t.Fatal("the dispatcher has no rotate-token subcommand")
	}
	if code := run([]string{"rotate-token"}); code != 2 {
		t.Errorf("run without flags returned %d, want 2", code)
	}
}
