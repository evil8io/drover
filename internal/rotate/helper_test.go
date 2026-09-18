package rotate

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	testNamespace   = "cattle-system"
	testSecret      = "drover-token"
	testKey         = "token"
	testUser        = "drover"
	testPassword    = "5eKr3t-p4ssw0rd"
	testDescription = "drover rotate-token"
	testKubeToken   = "kube-service-account-token"
	testUserAgent   = "drover/test"
	testSecretPath  = "/api/v1/namespaces/" + testNamespace + "/secrets/" + testSecret
)

var testNow = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

type recordedCall struct {
	method string
	path   string
	query  url.Values
	auth   string
	agent  string
	body   []byte
}

func newRecordedCall(r *http.Request, body []byte) recordedCall {
	return recordedCall{
		method: r.Method,
		path:   r.URL.Path,
		query:  r.URL.Query(),
		auth:   r.Header.Get("Authorization"),
		agent:  r.Header.Get("User-Agent"),
		body:   body,
	}
}

// route names the call in an assertion.
func (c recordedCall) route() string {
	route := c.method + " " + c.path
	if action := c.query.Get("action"); action != "" {
		route += "?action=" + action
	}
	return route
}

func (c recordedCall) field(t *testing.T, name string) string {
	t.Helper()
	var body map[string]any
	decoder := json.NewDecoder(bytes.NewReader(c.body))
	decoder.UseNumber()
	if err := decoder.Decode(&body); err != nil {
		t.Fatalf("decode the body of %s: %v", c.route(), err)
	}
	value, ok := body[name]
	if !ok {
		return ""
	}
	return fmt.Sprint(value)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

type fakeToken struct {
	name        string
	value       string
	description string
	created     time.Time
	ttlMillis   int64
	isDerived   bool
	noExpiresAt bool
}

func (t *fakeToken) end() time.Time {
	return t.created.Add(time.Duration(t.ttlMillis) * time.Millisecond)
}

func (t *fakeToken) expiresAt() string {
	if t.noExpiresAt || t.ttlMillis == 0 {
		return ""
	}
	return t.end().UTC().Format(time.RFC3339)
}

func (t *fakeToken) item(now time.Time, current bool) map[string]any {
	return map[string]any{
		"id":          t.name,
		"name":        t.name,
		"type":        "token",
		"baseType":    "token",
		"userId":      "u-" + testUser,
		"description": t.description,
		"created":     t.created.UTC().Format(time.RFC3339),
		"expiresAt":   t.expiresAt(),
		"ttl":         t.ttlMillis,
		"isDerived":   t.isDerived,
		"current":     current,
		"expired":     t.ttlMillis != 0 && !t.end().After(now),
	}
}

// fakeRancher answers the six Rancher calls of the rotation. It keeps the tokens
// of one user, as the real API does.
type fakeRancher struct {
	server      *httptest.Server
	mu          sync.Mutex
	calls       []recordedCall
	tokens      map[string]*fakeToken
	issued      []string
	session     string
	loginStatus int
	grantedTTL  int64
	counter     int
	now         time.Time
}

func newFakeRancher(t *testing.T) *fakeRancher {
	t.Helper()
	f := &fakeRancher{tokens: map[string]*fakeToken{}, now: testNow}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.server.Close)
	return f
}

// addToken puts a token of the service user in the store. The age is the
// distance from the test time to the creation time.
func (f *fakeRancher) addToken(name, description string, age, ttl time.Duration, derived bool) *fakeToken {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.add(name, description, f.now.Add(-age), ttl.Milliseconds(), derived)
}

func (f *fakeRancher) add(name, description string, created time.Time, ttlMillis int64, derived bool) *fakeToken {
	token := &fakeToken{
		name:        name,
		value:       name + ":" + name + "key",
		description: description,
		created:     created,
		ttlMillis:   ttlMillis,
		isDerived:   derived,
	}
	f.tokens[name] = token
	f.issued = append(f.issued, token.value, name+"key")
	return token
}

func (f *fakeRancher) all() []recordedCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

func (f *fakeRancher) routes() []string {
	routes := []string{}
	for _, call := range f.all() {
		routes = append(routes, call.route())
	}
	return routes
}

func (f *fakeRancher) names() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Sorted(maps.Keys(f.tokens))
}

func (f *fakeRancher) secrets() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.issued)
}

func (f *fakeRancher) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, newRecordedCall(r, body))

	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v3-public/localProviders/local" &&
		r.URL.Query().Get("action") == "login":
		f.serveLogin(w, body)
	case r.Method == http.MethodPost && r.URL.Path == tokensPath && r.URL.Query().Get("action") == "logout":
		f.serveLogout(w, r)
	case r.Method == http.MethodPost && r.URL.Path == tokensPath:
		f.serveCreate(w, r, body)
	case r.Method == http.MethodGet && r.URL.Path == tokensPath:
		f.serveList(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, tokensPath+"/"):
		f.serveGet(w, r)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, tokensPath+"/"):
		f.serveDelete(w, r)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// caller returns the token of the Authorization header.
func (f *fakeRancher) caller(r *http.Request) *fakeToken {
	value := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	name, _, found := strings.Cut(value, ":")
	if !found {
		return nil
	}
	token, ok := f.tokens[name]
	if !ok || token.value != value {
		return nil
	}
	return token
}

func (f *fakeRancher) serveLogin(w http.ResponseWriter, body []byte) {
	if f.loginStatus != 0 && f.loginStatus != http.StatusCreated {
		http.Error(w, "the login failed", f.loginStatus)
		return
	}
	var in struct {
		Username     string `json:"username"`
		Password     string `json:"password"`
		Description  string `json:"description"`
		ResponseType string `json:"responseType"`
	}
	if err := json.Unmarshal(body, &in); err != nil || in.Username != testUser || in.Password != testPassword {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	f.counter++
	token := f.add(fmt.Sprintf("token-session%d", f.counter), in.Description, f.now, (16 * time.Hour).Milliseconds(), false)
	f.session = token.name
	writeJSON(w, http.StatusCreated, map[string]any{
		"token":     token.value,
		"expiresAt": token.expiresAt(),
		"id":        token.name,
		"baseType":  "token",
		"type":      "token",
	})
}

func (f *fakeRancher) serveCreate(w http.ResponseWriter, r *http.Request, body []byte) {
	caller := f.caller(r)
	if caller == nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var in struct {
		TTL         int64  `json:"ttl"`
		Description string `json:"description"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	granted := in.TTL
	if f.grantedTTL != 0 {
		granted = f.grantedTTL
	}
	f.counter++
	token := f.add(fmt.Sprintf("token-derived%d", f.counter), in.Description, f.now, granted, true)
	token.noExpiresAt = true

	item := token.item(f.now, false)
	item["token"] = token.value
	writeJSON(w, http.StatusCreated, item)
}

func (f *fakeRancher) serveList(w http.ResponseWriter, r *http.Request) {
	if f.caller(r) == nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	items := []map[string]any{}
	for _, name := range slices.Sorted(maps.Keys(f.tokens)) {
		token := f.tokens[name]
		items = append(items, token.item(f.now, token.name == f.session && !token.isDerived))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"type":         "collection",
		"resourceType": "token",
		"pagination":   map[string]any{"limit": 1000},
		"data":         items,
	})
}

func (f *fakeRancher) serveGet(w http.ResponseWriter, r *http.Request) {
	if f.caller(r) == nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	token, ok := f.tokens[strings.TrimPrefix(r.URL.Path, tokensPath+"/")]
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, token.item(f.now, token.name == f.session && !token.isDerived))
}

func (f *fakeRancher) serveDelete(w http.ResponseWriter, r *http.Request) {
	if f.caller(r) == nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, tokensPath+"/")
	token, ok := f.tokens[name]
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if name == f.session && !token.isDerived {
		http.Error(w, "Cannot delete token for current session. Use logout instead", http.StatusBadRequest)
		return
	}
	delete(f.tokens, name)
	w.WriteHeader(http.StatusNoContent)
}

func (f *fakeRancher) serveLogout(w http.ResponseWriter, r *http.Request) {
	caller := f.caller(r)
	if caller == nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	delete(f.tokens, caller.name)
	if caller.name == f.session {
		f.session = ""
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

// fakeKube answers the get and the patch of the token Secret. It reads the
// ServiceAccount token from a file in a temporary directory.
type fakeKube struct {
	server    *httptest.Server
	mu        sync.Mutex
	calls     []recordedCall
	data      map[string]string
	missing   bool
	tokenFile string
	afterGet  func()
}

func newFakeKube(t *testing.T, data map[string]string) *fakeKube {
	t.Helper()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(testKubeToken+"\n"), 0o600); err != nil {
		t.Fatalf("write the ServiceAccount token file: %v", err)
	}
	if data == nil {
		data = map[string]string{}
	}
	k := &fakeKube{data: data, tokenFile: tokenFile}
	k.server = httptest.NewServer(http.HandlerFunc(k.serve))
	t.Cleanup(k.server.Close)
	return k
}

func (k *fakeKube) all() []recordedCall {
	k.mu.Lock()
	defer k.mu.Unlock()
	return slices.Clone(k.calls)
}

func (k *fakeKube) routes() []string {
	routes := []string{}
	for _, call := range k.all() {
		routes = append(routes, call.route())
	}
	return routes
}

func (k *fakeKube) value(key string) string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.data[key]
}

func (k *fakeKube) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	k.mu.Lock()
	defer k.mu.Unlock()
	k.calls = append(k.calls, newRecordedCall(r, body))

	if r.URL.Path != testSecretPath || k.missing {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	switch r.Method {
	case http.MethodGet:
		k.writeSecret(w)
		if k.afterGet != nil {
			k.afterGet()
		}
	case http.MethodPatch:
		if r.Header.Get("Content-Type") != mergePatchType {
			http.Error(w, "unsupported media type", http.StatusUnsupportedMediaType)
			return
		}
		var patch struct {
			StringData map[string]string `json:"stringData"`
		}
		if err := json.Unmarshal(body, &patch); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		for key, value := range patch.StringData {
			k.data[key] = value
		}
		k.writeSecret(w)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (k *fakeKube) writeSecret(w http.ResponseWriter) {
	encoded := map[string]string{}
	for key, value := range k.data {
		encoded[key] = base64.StdEncoding.EncodeToString([]byte(value))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"kind":       "Secret",
		"apiVersion": "v1",
		"metadata":   map[string]any{"name": testSecret, "namespace": testNamespace},
		"data":       encoded,
	})
}

type harness struct {
	kube    *fakeKube
	rancher *fakeRancher
	logs    *bytes.Buffer
	cfg     Config
}

func newHarness(t *testing.T, kube *fakeKube, rancher *fakeRancher) *harness {
	t.Helper()
	logs := &bytes.Buffer{}
	return &harness{
		kube:    kube,
		rancher: rancher,
		logs:    logs,
		cfg: Config{
			Rancher:       mustParseURL(t, rancher.server.URL),
			RancherClient: rancher.server.Client(),
			Kube:          mustParseURL(t, kube.server.URL),
			KubeClient:    kube.server.Client(),
			KubeTokenFile: kube.tokenFile,
			Namespace:     testNamespace,
			Secret:        testSecret,
			Key:           testKey,
			Username:      testUser,
			Password:      testPassword,
			TTL:           48 * time.Hour,
			RenewBefore:   24 * time.Hour,
			Keep:          2,
			Description:   testDescription,
			UserAgent:     testUserAgent,
			Logger:        slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
			Now:           func() time.Time { return testNow },
		},
	}
}

func (h *harness) run() error {
	return Run(context.Background(), h.cfg)
}

// assertNoSecret checks that no log line has a token value, a token key, the
// password, or the ServiceAccount token.
func (h *harness) assertNoSecret(t *testing.T) {
	t.Helper()
	logs := h.logs.String()
	for _, secret := range append(h.rancher.secrets(), testPassword, testKubeToken) {
		if strings.Contains(logs, secret) {
			t.Errorf("the log has the secret %q", secret)
		}
	}
}

func (h *harness) assertLog(t *testing.T, want ...string) {
	t.Helper()
	logs := h.logs.String()
	for _, text := range want {
		if !strings.Contains(logs, text) {
			t.Errorf("the log has no %q, log:\n%s", text, logs)
		}
	}
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	target, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse the URL %s: %v", raw, err)
	}
	return target
}

func assertRoutes(t *testing.T, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("the calls are\n%v\nand the test expects\n%v", got, want)
	}
}
