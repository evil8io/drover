// Package rotate renews the Rancher API token in a Kubernetes Secret. One run
// checks the token in the Secret and creates a new token when the old token
// expires inside the renew window.
package rotate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/evil8io/drover/internal/rancherclient"
)

const (
	maxBody = 1 << 20

	stepPasswordSync = "password_sync"
	stepSecretGet    = "secret_get"
	stepTokenCheck   = "token_check"
	stepLogin        = "login"
	stepTokenCreate  = "token_create"
	stepSecretPatch  = "secret_patch"
	stepTokenPrune   = "token_prune"
	stepLogout       = "logout"

	outcomeOK      = "ok"
	outcomeValid   = "valid"
	outcomeRotate  = "rotate"
	outcomeClamped = "clamped"
	outcomeFailed  = "failed"
)

// Config configures one run.
type Config struct {
	// Rancher is the Rancher URL. The scheme is http or https, and the path is empty.
	Rancher *url.URL
	// RancherClient sends every Rancher request. Nil selects a client with a 30 s timeout.
	RancherClient *http.Client
	// Kube is the Kubernetes API URL. The path is empty.
	Kube *url.URL
	// KubeClient sends every Kubernetes request. Nil selects a client with a 30 s timeout.
	KubeClient *http.Client
	// KubeTokenFile contains the ServiceAccount token. Run reads the file for
	// every request, because the kubelet replaces the token.
	KubeTokenFile string
	// Namespace and Secret name the Secret with the Rancher API token.
	Namespace string
	Secret    string
	// Key is the key of the token inside the Secret.
	Key string
	// Username and Password are the credentials of the Rancher service user.
	Username string
	Password string
	// PasswordNamespace and PasswordSecret name the Secret that Rancher reads
	// for the password of the service user. Both empty turn the password step
	// off. With both set, a run writes the hash of Password into that Secret,
	// before it reads the token Secret.
	PasswordNamespace string
	PasswordSecret    string
	// TTL is the lifetime of a new token. Rancher reduces a TTL above its own maximum.
	TTL time.Duration
	// RenewBefore is the remaining lifetime that starts a rotation.
	RenewBefore time.Duration
	// Keep is the number of tokens with this description to keep. The new token
	// and the token that the run read from the Secret count, and Run never
	// deletes them.
	Keep int
	// Description goes on every token that Run creates. It also selects the
	// tokens that Run deletes.
	Description string
	// UserAgent goes into every Rancher request. Rancher applies a CSRF check to
	// a browser, so the value must not name one.
	UserAgent string
	// Logger gets one line per step. Nil selects slog.Default.
	Logger *slog.Logger
	// Now gives the time to the expiry check. Nil selects time.Now.
	Now func() time.Time
	// MeterProvider creates the meter of the rotate metrics, and the meter that
	// instruments RancherClient. Nil selects otel.GetMeterProvider().
	MeterProvider metric.MeterProvider
	// TracerProvider creates the tracer of the rotate spans. Nil selects
	// otel.GetTracerProvider().
	TracerProvider trace.TracerProvider
}

type rotator struct {
	cfg     Config
	logger  *slog.Logger
	now     func() time.Time
	tracer  trace.Tracer
	metrics *metrics
}

// Run renews the token in the Secret. It returns nil when the token in the
// Secret is still valid.
func Run(ctx context.Context, cfg Config) error {
	r, err := newRotator(cfg)
	if err != nil {
		return err
	}
	return r.run(ctx)
}

// run wraps one call of check in the rotate span, and records the duration of
// the run.
func (r *rotator) run(ctx context.Context) error {
	ctx, span := r.tracer.Start(ctx, "rotate")
	defer span.End()
	start := time.Now()

	err := r.check(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	r.metrics.recordDuration(ctx, time.Since(start))
	return err
}

// check syncs the password, when configured, and then checks the token. A
// failed password sync does not stop the token check.
func (r *rotator) check(ctx context.Context) error {
	var passwordErr error
	if r.cfg.PasswordSecret != "" {
		if passwordErr = r.syncPassword(ctx); passwordErr != nil {
			r.logger.ErrorContext(ctx, "the password sync failed, and the run continues with the token",
				"step", stepPasswordSync, "outcome", outcomeFailed, "error", passwordErr.Error())
		}
	}
	return errors.Join(passwordErr, r.checkToken(ctx))
}

// checkToken reads the token in the Secret, and renews it when the token is
// not valid. It returns nil when the token in the Secret is still valid.
func (r *rotator) checkToken(ctx context.Context) error {
	current, err := r.secretToken(ctx)
	if err != nil {
		return err
	}
	if current != "" {
		valid, err := r.tokenIsValid(ctx, current)
		if err != nil {
			return err
		}
		if valid {
			return nil
		}
	}
	return r.rotate(ctx, tokenName(current))
}

func newRotator(cfg Config) (*rotator, error) {
	for _, field := range []struct {
		name  string
		value *url.URL
	}{{"the Rancher URL", cfg.Rancher}, {"the Kubernetes URL", cfg.Kube}} {
		if field.value == nil {
			return nil, fmt.Errorf("%s is required", field.name)
		}
		if field.value.Scheme != "http" && field.value.Scheme != "https" {
			return nil, fmt.Errorf("%s scheme %q is not http or https", field.name, field.value.Scheme)
		}
		if field.value.Host == "" {
			return nil, fmt.Errorf("%s has no host", field.name)
		}
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"the ServiceAccount token file", cfg.KubeTokenFile},
		{"the namespace", cfg.Namespace},
		{"the Secret name", cfg.Secret},
		{"the Secret key", cfg.Key},
		{"the user name", cfg.Username},
		{"the password", cfg.Password},
		{"the description", cfg.Description},
	} {
		if field.value == "" {
			return nil, fmt.Errorf("%s is required", field.name)
		}
	}
	if cfg.TTL <= 0 {
		return nil, errors.New("the TTL must be longer than zero")
	}
	if cfg.RenewBefore <= 0 {
		return nil, errors.New("the renew window must be longer than zero")
	}
	if cfg.Keep < 2 {
		return nil, errors.New("the keep count must be 2 or more, because a mounted Secret updates after a delay")
	}
	if (cfg.PasswordNamespace == "") != (cfg.PasswordSecret == "") {
		return nil, errors.New("the password Secret needs both a namespace and a name")
	}

	r := &rotator{cfg: cfg, logger: cfg.Logger, now: cfg.Now}
	r.cfg.Rancher = base(cfg.Rancher)
	r.cfg.Kube = base(cfg.Kube)
	if r.logger == nil {
		r.logger = slog.Default()
	}
	if r.now == nil {
		r.now = time.Now
	}
	if r.cfg.RancherClient == nil {
		r.cfg.RancherClient = &http.Client{Timeout: 30 * time.Second}
	}
	if r.cfg.KubeClient == nil {
		r.cfg.KubeClient = &http.Client{Timeout: 30 * time.Second}
	}

	meterProvider := cfg.MeterProvider
	if meterProvider == nil {
		meterProvider = otel.GetMeterProvider()
	}
	m, err := newMetrics(meterProvider)
	if err != nil {
		return nil, fmt.Errorf("build the rotate metrics: %w", err)
	}
	r.metrics = m
	r.cfg.RancherClient = &http.Client{
		Transport:     rancherclient.WrapTransport(r.cfg.RancherClient.Transport, meterProvider),
		CheckRedirect: r.cfg.RancherClient.CheckRedirect,
		Jar:           r.cfg.RancherClient.Jar,
		Timeout:       r.cfg.RancherClient.Timeout,
	}

	tracerProvider := cfg.TracerProvider
	if tracerProvider == nil {
		tracerProvider = otel.GetTracerProvider()
	}
	r.tracer = tracerProvider.Tracer(tracerName)

	return r, nil
}

func (r *rotator) rotate(ctx context.Context, secretName string) error {
	session, err := r.login(ctx)
	if err != nil {
		return err
	}
	defer r.logout(ctx, session)

	created, err := r.createToken(ctx, session)
	if err != nil {
		return err
	}
	if err := r.patchSecret(ctx, created.value); err != nil {
		return err
	}
	pruneErr := r.prune(ctx, created, secretName, session)
	return errors.Join(pruneErr, r.checkGrantedTTL(ctx, created.ttl))
}

// checkGrantedTTL returns an error when a token with the granted TTL expires
// inside the renew window at once. The TTL zero never expires.
func (r *rotator) checkGrantedTTL(ctx context.Context, granted time.Duration) error {
	if granted <= 0 || granted > r.cfg.RenewBefore {
		return nil
	}
	r.logger.ErrorContext(ctx, "the granted ttl is inside the renew window",
		"step", stepTokenCreate, "outcome", outcomeClamped, "reason", "window",
		"granted_ttl_ms", granted.Milliseconds(), "renew_before_ms", r.cfg.RenewBefore.Milliseconds())
	return fmt.Errorf("rancher granted a ttl of %s, which is not longer than the renew window of %s, "+
		"so every run rotates: raise auth-token-max-ttl-minutes or lower -renew-before", granted, r.cfg.RenewBefore)
}

// step starts a child span named name, and returns the traced context and a
// function that ends the span. The caller calls the returned function once,
// as a defer, with the outcome of the step and the error that the step
// returns. A non-nil error always records the outcome failed and the span
// error status, regardless of the outcome that the caller passes.
func (r *rotator) step(ctx context.Context, name string) (context.Context, func(outcome string, err error)) {
	ctx, span := r.tracer.Start(ctx, name)
	return ctx, func(outcome string, err error) {
		if err != nil {
			outcome = outcomeFailed
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		r.metrics.step(ctx, name, outcome)
		span.End()
	}
}

type call struct {
	client      *http.Client
	method      string
	url         string
	bearer      string
	contentType string
	body        []byte
}

// do sends one request and reads the answer. The error covers the transport
// only. A message never quotes a response body, because a body can contain a
// token.
func (r *rotator) do(ctx context.Context, c call) (int, []byte, error) {
	var body io.Reader
	if c.body != nil {
		body = bytes.NewReader(c.body)
	}
	req, err := http.NewRequestWithContext(ctx, c.method, c.url, body)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Accept", "application/json")
	if r.cfg.UserAgent != "" {
		req.Header.Set("User-Agent", r.cfg.UserAgent)
	}
	if c.bearer != "" {
		req.Header.Set("Authorization", "Bearer "+c.bearer)
	}
	if c.body != nil {
		contentType := c.contentType
		if contentType == "" {
			contentType = "application/json"
		}
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, data, nil
}

func (r *rotator) rancherDo(ctx context.Context, method, path, bearer string, body any) (int, []byte, error) {
	data, err := marshal(body)
	if err != nil {
		return 0, nil, err
	}
	return r.do(ctx, call{
		client: r.cfg.RancherClient,
		method: method,
		url:    r.cfg.Rancher.String() + path,
		bearer: bearer,
		body:   data,
	})
}

// base returns the URL without a path, a query, or a fragment, so that a
// caller appends a path to it.
func base(u *url.URL) *url.URL {
	out := *u
	out.Path, out.RawPath, out.RawQuery, out.Fragment = "", "", "", ""
	return &out
}

func marshal(body any) ([]byte, error) {
	if body == nil {
		return nil, nil
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("encode the request body: %w", err)
	}
	return data, nil
}
