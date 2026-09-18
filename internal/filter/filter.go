// Package filter proxies a Rancher server. It answers the namespace list of a
// caller that has the get permission on a namespace, but no list permission.
package filter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/evil8io/drover/internal/rancherclient"
)

const (
	defaultCacheTTL = 15 * time.Second
	serviceName     = "drover"
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

// Service proxies a Rancher server. It implements http.Handler.
type Service struct {
	upstream  *url.URL
	tokenFile string
	logger    *slog.Logger
	now       func() time.Time
	base      http.RoundTripper
	cache     *cache
	proxy     *httputil.ReverseProxy

	draining atomic.Bool
	watches  *watchRegistry
}

// New returns a Service that proxies every request to the upstream. It answers
// GET /healthz with 200 and the body ok, always. It answers GET /readyz with
// 200 and the body ok, until StartDrain runs, and with 503 and the body
// draining after that.
func New(cfg Config) (*Service, error) {
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

	base, err := rancherclient.Transport(cfg.CAFile)
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

	svc := &Service{
		upstream:  &upstream,
		tokenFile: cfg.TokenFile,
		logger:    logger,
		now:       now,
		base:      base,
		cache:     newCache(ttl, now),
		watches:   newWatchRegistry(),
	}
	svc.proxy = &httputil.ReverseProxy{
		Rewrite:       svc.rewrite,
		Transport:     svc,
		FlushInterval: -1,
		ErrorHandler:  svc.handleError,
	}

	return svc, nil
}

// ServeHTTP answers GET /healthz and GET /readyz itself, and proxies every
// other request to the upstream.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		switch r.URL.Path {
		case "/healthz":
			writePlain(w, http.StatusOK, "ok")
			return
		case "/readyz":
			if s.draining.Load() {
				writePlain(w, http.StatusServiceUnavailable, "draining")
				return
			}
			writePlain(w, http.StatusOK, "ok")
			return
		}
	}
	s.proxy.ServeHTTP(w, r)
}

func writePlain(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(body))
}

// StartDrain marks the service as draining, so /readyz answers 503 from this
// point on, and it ends every open watch stream with a plain EOF. It returns
// the count of streams it ends. A caller runs this once, before
// http.Server.Shutdown.
func (s *Service) StartDrain() int {
	s.draining.Store(true)
	return s.watches.closeAll()
}

func (s *Service) rewrite(pr *httputil.ProxyRequest) {
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

func (s *Service) handleError(w http.ResponseWriter, r *http.Request, err error) {
	level := slog.LevelError
	if errors.Is(err, context.Canceled) {
		level = slog.LevelDebug
	}
	s.logger.Log(r.Context(), level, "proxy error", "method", r.Method, "path", r.URL.Path, "error", err.Error())
	writeStatus(w, http.StatusBadGateway, reasonInternalError, serviceName+": "+err.Error())
}
