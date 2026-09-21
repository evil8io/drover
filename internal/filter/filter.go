// Package filter proxies a Rancher server. It answers the namespace list of a
// caller that has the get permission on a namespace, but no list permission,
// and it answers a cluster-wide list of a namespaced kind with one request
// per allowed namespace.
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

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/evil8io/drover/internal/rancherclient"
)

const (
	defaultCacheTTL   = 15 * time.Second
	defaultMaxWatches = 1000
	bodyReadTimeout   = 10 * time.Second
	serviceName       = "drover"
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
	// MaxCacheEntries is the hard bound on the cached allowed sets. Zero selects 1000.
	MaxCacheEntries int
	// FetchRate is the fetches per second that the shared rate limit allows,
	// for a fetch of an allowed set. Zero selects 50. The burst is twice the
	// rate, and at least 1.
	FetchRate float64
	// Fanout answers a cluster-wide list of a namespaced kind with one
	// request per allowed namespace. It is off by default, because it makes
	// the filter the data path of most reads of a tenant.
	Fanout bool
	// FanoutMaxNamespaces is the count of allowed namespaces above which a
	// fan-out answers 403. Zero selects 200.
	FanoutMaxNamespaces int
	// FanoutConcurrency is the count of namespaced requests of one fan-out
	// that run at a time. Zero selects 16. It also bounds the answers that
	// the merge holds.
	FanoutConcurrency int
	// FanoutMaxWatchNamespaces is the count of allowed namespaces above which
	// a cluster-wide watch answers 403. Zero selects 50. A merged watch holds
	// one upstream connection per namespace for its whole life, so its bound
	// is below the bound of a list.
	FanoutMaxWatchNamespaces int
	// MaxWatches is the count of open watch streams of the service above
	// which a new watch answers 503. Zero selects 1000.
	MaxWatches int
	// Logger gets one line for each intercepted request. Nil selects slog.Default.
	Logger *slog.Logger
	// MeterProvider creates the meter of the filter metrics. Nil selects
	// otel.GetMeterProvider().
	MeterProvider metric.MeterProvider
	// TracerProvider creates the tracer of the request span. Nil selects
	// otel.GetTracerProvider().
	TracerProvider trace.TracerProvider
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
	limiter   *limiter
	proxy     *httputil.ReverseProxy
	handler   http.Handler
	metrics   *metrics

	fanoutEnabled            bool
	fanoutMaxNamespaces      int
	fanoutConcurrency        int
	fanoutMaxWatchNamespaces int
	maxWatches               int

	draining atomic.Bool
	watches  *watchRegistry
	clusters *clusterSet
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
	if ttl <= 0 {
		ttl = defaultCacheTTL
	}
	maxCacheEntries := cfg.MaxCacheEntries
	if maxCacheEntries <= 0 {
		maxCacheEntries = defaultMaxCacheEntries
	}
	fetchRate := cfg.FetchRate
	if fetchRate <= 0 {
		fetchRate = defaultFetchRate
	}
	maxWatches := cfg.MaxWatches
	if maxWatches <= 0 {
		maxWatches = defaultMaxWatches
	}
	fetchBurst := max(2*fetchRate, 1)
	fanoutMaxNamespaces := cfg.FanoutMaxNamespaces
	if fanoutMaxNamespaces <= 0 {
		fanoutMaxNamespaces = defaultFanoutMaxNamespaces
	}
	fanoutConcurrency := cfg.FanoutConcurrency
	if fanoutConcurrency <= 0 {
		fanoutConcurrency = defaultFanoutConcurrency
	}
	fanoutMaxWatchNamespaces := cfg.FanoutMaxWatchNamespaces
	if fanoutMaxWatchNamespaces <= 0 {
		fanoutMaxWatchNamespaces = defaultFanoutMaxWatchNamespaces
	}
	meterProvider := cfg.MeterProvider
	if meterProvider == nil {
		meterProvider = otel.GetMeterProvider()
	}
	tracerProvider := cfg.TracerProvider
	if tracerProvider == nil {
		tracerProvider = otel.GetTracerProvider()
	}
	m, err := newMetrics(meterProvider)
	if err != nil {
		return nil, fmt.Errorf("build the filter metrics: %w", err)
	}

	svc := &Service{
		upstream:  &upstream,
		tokenFile: cfg.TokenFile,
		logger:    logger,
		now:       now,
		base: otelhttp.NewTransport(base,
			otelhttp.WithMeterProvider(meterProvider),
			otelhttp.WithTracerProvider(tracerProvider),
			otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return spanName(r) }),
		),
		cache:    newCache(ttl, maxCacheEntries, now),
		limiter:  newLimiter(fetchRate, fetchBurst, now),
		metrics:  m,
		watches:  newWatchRegistry(),
		clusters: newClusterSet(),

		fanoutEnabled:            cfg.Fanout,
		fanoutMaxNamespaces:      fanoutMaxNamespaces,
		fanoutConcurrency:        fanoutConcurrency,
		fanoutMaxWatchNamespaces: fanoutMaxWatchNamespaces,
		maxWatches:               maxWatches,
	}
	svc.proxy = &httputil.ReverseProxy{
		Rewrite:       svc.rewrite,
		Transport:     svc,
		FlushInterval: -1,
		ErrorHandler:  svc.handleError,
	}
	svc.handler = otelhttp.NewHandler(http.HandlerFunc(svc.serve), "api-filter",
		otelhttp.WithMeterProvider(meterProvider),
		otelhttp.WithTracerProvider(tracerProvider),
		// otelhttp names a server span after the http method, so a span of
		// the filter would read GET. The path template says what the request
		// asks for, and it keeps the value set bounded.
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return spanName(r) }),
		otelhttp.WithFilter(func(r *http.Request) bool {
			return r.URL.Path != "/healthz" && r.URL.Path != "/readyz"
		}),
	)

	return svc, nil
}

// ServeHTTP answers GET /healthz and GET /readyz itself, and proxies every
// other request to the upstream.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// serve is the traced entry point behind ServeHTTP.
func (s *Service) serve(w http.ResponseWriter, r *http.Request) {
	setRouteAttribute(r)
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
	if r.Method == http.MethodPost {
		// A review body is small, and a caller that sends it slowly holds a
		// goroutine. The server resets the deadline before the next request.
		controller := http.NewResponseController(w)
		_ = controller.SetReadDeadline(time.Now().Add(bodyReadTimeout))
		defer func() { _ = controller.SetReadDeadline(time.Time{}) }()
	}
	s.proxy.ServeHTTP(w, r)
}

// setRouteAttribute puts the path template of the request on its span, as
// http.route. A collector that rebuilds a span name from the semantic
// conventions reads that attribute, and it gives the span the method alone
// when the attribute is absent. The name that the span formatter writes at
// the source and the name that such a collector writes are then the same.
func setRouteAttribute(r *http.Request) {
	span := trace.SpanFromContext(r.Context())
	if !span.IsRecording() {
		return
	}
	span.SetAttributes(semconv.HTTPRoute(pathTemplate(r.URL.Path)))
}

func writePlain(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(body))
}

// StartDrain marks the service as draining, so /readyz answers 503 from this
// point on, and it ends every open watch stream with a plain EOF. It returns
// the count of streams it ends, once each of them has its close frame. A
// caller runs this once, before http.Server.Shutdown, because Shutdown does
// not wait for an upgraded connection.
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
	writeStatus(w, http.StatusBadGateway, reasonInternalError, serviceName+": "+publicMessage(err))
}
