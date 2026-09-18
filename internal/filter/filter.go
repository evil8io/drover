// Package filter proxies a Rancher server. It answers the namespace list of a
// caller that has the get permission on a namespace, but no list permission.
package filter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	defaultCacheTTL = 15 * time.Second
	serviceName     = "rancher-namespace-filter"
)

// Config configures the handler that New returns.
type Config struct {
	// Upstream is the Rancher URL. The scheme is http or https, and the path is empty.
	Upstream *url.URL
	// CAFile is a PEM bundle that verifies an https upstream. An empty value selects the system pool.
	CAFile string
	// TokenFile contains the API token of the Rancher service user.
	TokenFile string
	// CacheTTL is the lifetime of one cached allowed set. Zero selects 15 s.
	CacheTTL time.Duration
	// Logger gets one line for each intercepted request. Nil selects slog.Default.
	Logger *slog.Logger
	// Now gives the time to the cache. Nil selects time.Now.
	Now func() time.Time
}

type service struct {
	upstream  *url.URL
	tokenFile string
	logger    *slog.Logger
	now       func() time.Time
	base      http.RoundTripper
	cache     *cache
}

// New returns a handler that proxies every request to the upstream. It answers
// GET /healthz with 200 and the body ok.
func New(cfg Config) (http.Handler, error) {
	if cfg.Upstream == nil {
		return nil, errors.New("upstream is required")
	}
	if cfg.Upstream.Scheme != "http" && cfg.Upstream.Scheme != "https" {
		return nil, fmt.Errorf("upstream scheme %q is not http or https", cfg.Upstream.Scheme)
	}
	if cfg.Upstream.Host == "" {
		return nil, errors.New("upstream has no host")
	}
	if p := cfg.Upstream.Path; p != "" && p != "/" {
		return nil, fmt.Errorf("upstream path %q is not empty", p)
	}
	if cfg.TokenFile == "" {
		return nil, errors.New("token file is required")
	}

	upstream := *cfg.Upstream
	upstream.Path = ""
	upstream.RawPath = ""

	base, err := newTransport(cfg.CAFile)
	if err != nil {
		return nil, err
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	ttl := cfg.CacheTTL
	if ttl == 0 {
		ttl = defaultCacheTTL
	}

	svc := &service{
		upstream:  &upstream,
		tokenFile: cfg.TokenFile,
		logger:    logger,
		now:       now,
		base:      base,
		cache:     newCache(ttl, now),
	}

	proxy := &httputil.ReverseProxy{
		Rewrite:       svc.rewrite,
		Transport:     svc,
		FlushInterval: -1,
		ErrorHandler:  svc.handleError,
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/healthz" {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = w.Write([]byte("ok"))
			return
		}
		proxy.ServeHTTP(w, r)
	}), nil
}

func newTransport(caFile string) (*http.Transport, error) {
	tr := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		DisableCompression:  true,
		ForceAttemptHTTP2:   false,
		TLSNextProto:        map[string]func(string, *tls.Conn) http.RoundTripper{},
		TLSHandshakeTimeout: 10 * time.Second,
		IdleConnTimeout:     90 * time.Second,
		MaxIdleConnsPerHost: 100,
	}
	if caFile == "" {
		return tr, nil
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read upstream CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("upstream CA file %s has no certificate", caFile)
	}
	tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	return tr, nil
}

func (s *service) rewrite(pr *httputil.ProxyRequest) {
	pr.SetURL(s.upstream)
	pr.Out.Host = pr.In.Host

	for _, name := range []string{"X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"} {
		pr.Out.Header.Del(name)
		for _, value := range pr.In.Header.Values(name) {
			pr.Out.Header.Add(name, value)
		}
	}
	ip, _, err := net.SplitHostPort(pr.In.RemoteAddr)
	if err != nil {
		return
	}
	if prior := pr.Out.Header.Get("X-Forwarded-For"); prior != "" {
		ip = prior + ", " + ip
	}
	pr.Out.Header.Set("X-Forwarded-For", ip)
}

func (s *service) handleError(w http.ResponseWriter, r *http.Request, err error) {
	level := slog.LevelError
	if errors.Is(err, context.Canceled) {
		level = slog.LevelDebug
	}
	s.logger.Log(r.Context(), level, "proxy error", "method", r.Method, "path", r.URL.Path, "error", err.Error())
	writeStatus(w, http.StatusBadGateway, reasonInternalError, serviceName+": "+err.Error())
}

func readToken(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read token file: %w", err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("token file %s is empty", path)
	}
	return token, nil
}
