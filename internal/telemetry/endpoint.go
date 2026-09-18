package telemetry

import (
	"fmt"
	"net/url"
	"strings"
)

// parseEndpoint returns the exporter endpoint of raw, and reports whether the
// endpoint needs TLS. raw is host:port, or a URL. The https scheme needs TLS.
// Every other scheme, and a bare host:port, is insecure.
func parseEndpoint(raw string) (endpoint string, secure bool, err error) {
	if !strings.Contains(raw, "://") {
		return raw, false, nil
	}
	target, err := url.Parse(raw)
	if err != nil {
		return "", false, fmt.Errorf("parse %q: %w", raw, err)
	}
	if target.Host == "" {
		return "", false, fmt.Errorf("%q has no host", raw)
	}
	return target.Host, target.Scheme == "https", nil
}
