package projectsync

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	// baoRancher is the Rancher URL that OpenBao uses in the tests.
	baoRancher = "https://rancher.example.com"
	baoRole    = "project-sync"
)

// fakeOpenBao answers the login of the Kubernetes auth mount at
// auth/kubernetes, the mounts of the Kubernetes secrets engine, their config,
// their roles, and the ACL policies. It keeps them in memory, and records
// every request.
type fakeOpenBao struct {
	server *httptest.Server

	mu       sync.Mutex
	requests []recorded
	issued   []string
	// loginStatus is the status of a login, zero for success.
	loginStatus int
	// missing is the status that a config write of a cluster answers, as for
	// a mount that does not exist: 404, or 400 as an older server answers.
	missing  map[string]int
	mounts   map[string]string
	roles    map[string]map[string]map[string]any
	policies map[string]string
	// denied are the routes that answer 403 to every client token.
	denied map[string]bool
}

func newFakeOpenBao(t *testing.T) *fakeOpenBao {
	t.Helper()
	f := &fakeOpenBao{
		missing:  make(map[string]int),
		mounts:   make(map[string]string),
		roles:    make(map[string]map[string]map[string]any),
		policies: map[string]string{"default": "# default", "root": ""},
		denied:   make(map[string]bool),
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeOpenBao) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewReader(body))

	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, recorded{
		method: r.Method, path: r.URL.Path, query: r.URL.Query(), header: r.Header.Clone(), body: string(body), at: time.Now(),
	})

	route := strings.TrimPrefix(r.URL.Path, "/v1/")
	if r.Method == http.MethodPost && route == "auth/kubernetes/login" {
		if f.loginStatus != 0 {
			writeJSON(w, f.loginStatus, map[string][]string{"errors": {"permission denied"}})
			return
		}
		token := fmt.Sprintf("bao-%d", len(f.issued)+1)
		f.issued = append(f.issued, token)
		writeJSON(w, http.StatusOK, map[string]any{"auth": map[string]any{"client_token": token, "lease_duration": 300}})
		return
	}
	if !slices.Contains(f.issued, r.Header.Get("X-Vault-Token")) || f.denied[route] {
		writeJSON(w, http.StatusForbidden, map[string][]string{"errors": {"permission denied"}})
		return
	}
	list := r.URL.Query().Get("list") == "true"

	switch {
	case strings.HasPrefix(route, "sys/mounts/"):
		f.serveMount(w, r, strings.TrimPrefix(route, "sys/mounts/"), body)
	case route == "sys/policies/acl" && list:
		f.writeKeys(w, sortedKeys(f.policies))
	case strings.HasPrefix(route, "sys/policies/acl/"):
		f.servePolicy(w, r, strings.TrimPrefix(route, "sys/policies/acl/"), body)
	case strings.HasPrefix(route, "kubernetes/"):
		f.serveEngine(w, r, strings.TrimPrefix(route, "kubernetes/"), list, body)
	default:
		writeJSON(w, http.StatusNotFound, map[string][]string{"errors": {}})
	}
}

func (f *fakeOpenBao) noHandler(w http.ResponseWriter, route string) {
	writeJSON(w, http.StatusNotFound, map[string][]string{"errors": {
		fmt.Sprintf("no handler for route %q. route entry not found.", route)}})
}

func (f *fakeOpenBao) writeKeys(w http.ResponseWriter, keys []string) {
	if len(keys) == 0 {
		writeJSON(w, http.StatusNotFound, map[string][]string{"errors": {}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"keys": keys}})
}

func (f *fakeOpenBao) serveMount(w http.ResponseWriter, r *http.Request, path string, body []byte) {
	switch r.Method {
	case http.MethodGet:
		kind, ok := f.mounts[path]
		if !ok {
			writeJSON(w, http.StatusBadRequest, map[string][]string{"errors": {"No secret engine mount at " + path + "/"}})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"type": kind, "data": map[string]any{"type": kind}})
	case http.MethodPost:
		var request struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(body, &request)
		f.mounts[path] = request.Type
		w.WriteHeader(http.StatusNoContent)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string][]string{"errors": {}})
	}
}

func (f *fakeOpenBao) servePolicy(w http.ResponseWriter, r *http.Request, name string, body []byte) {
	switch r.Method {
	case http.MethodGet:
		text, ok := f.policies[name]
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string][]string{"errors": {}})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"name": name, "policy": text}})
	case http.MethodPut, http.MethodPost:
		var request struct {
			Policy string `json:"policy"`
		}
		_ = json.Unmarshal(body, &request)
		f.policies[name] = request.Policy
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		delete(f.policies, name)
		w.WriteHeader(http.StatusNoContent)
	}
}

// serveEngine serves <cluster>/config and <cluster>/roles of the mounts under
// kubernetes/.
func (f *fakeOpenBao) serveEngine(w http.ResponseWriter, r *http.Request, rest string, list bool, body []byte) {
	cluster, sub, _ := strings.Cut(rest, "/")
	if _, ok := f.mounts["kubernetes/"+cluster]; !ok {
		f.noHandler(w, "kubernetes/"+rest)
		return
	}
	switch {
	case sub == "config" && r.Method == http.MethodPost:
		switch f.missing[cluster] {
		case http.StatusNotFound:
			f.noHandler(w, "kubernetes/"+rest)
		case http.StatusBadRequest:
			writeJSON(w, http.StatusBadRequest, map[string][]string{"errors": {
				fmt.Sprintf("no handler for route '%s'", "kubernetes/"+rest)}})
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	case sub == "roles" && list:
		f.writeKeys(w, sortedKeys(f.roles[cluster]))
	case strings.HasPrefix(sub, "roles/"):
		name := strings.TrimPrefix(sub, "roles/")
		switch r.Method {
		case http.MethodGet:
			data, ok := f.roles[cluster][name]
			if !ok {
				writeJSON(w, http.StatusNotFound, map[string][]string{"errors": {}})
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"data": data})
		case http.MethodPost, http.MethodPut:
			var data map[string]any
			_ = json.Unmarshal(body, &data)
			if f.roles[cluster] == nil {
				f.roles[cluster] = make(map[string]map[string]any)
			}
			f.roles[cluster][name] = data
			w.WriteHeader(http.StatusNoContent)
		case http.MethodDelete:
			delete(f.roles[cluster], name)
			w.WriteHeader(http.StatusNoContent)
		}
	default:
		f.noHandler(w, "kubernetes/"+rest)
	}
}

func (f *fakeOpenBao) deny(route string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.denied[route] = true
}

// revokeTokens makes every issued client token invalid, as an expiry does.
func (f *fakeOpenBao) revokeTokens() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.issued = []string{"revoked"}
}

func (f *fakeOpenBao) role(cluster, name string) (map[string]any, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.roles[cluster][name]
	return data, ok
}

func (f *fakeOpenBao) setRole(cluster, name string, data map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.roles[cluster] == nil {
		f.roles[cluster] = make(map[string]map[string]any)
	}
	f.roles[cluster][name] = data
}

func (f *fakeOpenBao) policy(name string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	text, ok := f.policies[name]
	return text, ok
}

func (f *fakeOpenBao) setPolicy(name, text string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.policies[name] = text
}

func (f *fakeOpenBao) addMount(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mounts[path] = "kubernetes"
}

// changes returns every request that writes: a POST, PUT, or DELETE, but the
// login.
func (f *fakeOpenBao) changes() []recorded {
	var out []recorded
	for _, req := range f.all() {
		if req.method != http.MethodGet && req.path != "/v1/auth/kubernetes/login" {
			out = append(out, req)
		}
	}
	return out
}

func (f *fakeOpenBao) setMissing(cluster string, status int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if status == 0 {
		delete(f.missing, cluster)
		return
	}
	f.missing[cluster] = status
}

func (f *fakeOpenBao) setLoginStatus(status int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loginStatus = status
}

func (f *fakeOpenBao) all() []recorded {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests)
}

func (f *fakeOpenBao) logins() []recorded {
	return f.ofPath("/v1/auth/kubernetes/login")
}

func (f *fakeOpenBao) writes(cluster string) []recorded {
	return f.ofPath("/v1/kubernetes/" + cluster + "/config")
}

func (f *fakeOpenBao) ofPath(path string) []recorded {
	var out []recorded
	for _, req := range f.all() {
		if req.path == path {
			out = append(out, req)
		}
	}
	return out
}

func (f *fakeOpenBao) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = nil
}

// testClock is a clock that a test moves by hand.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// openbaoSetup is a syncer with the OpenBao config on, against an accounts
// fake and an OpenBao fake, with one clock for the syncer and the token
// requests.
type openbaoSetup struct {
	fake    *accountsFake
	bao     *fakeOpenBao
	syncer  *Syncer
	logs    *syncBuffer
	clock   *testClock
	jwtFile string
}

func newOpenBaoSetup(t *testing.T, fake *accountsFake, opts ...func(*Config)) *openbaoSetup {
	t.Helper()
	bao := newFakeOpenBao(t)
	address, err := url.Parse(bao.server.URL)
	if err != nil {
		t.Fatalf("parse the OpenBao URL: %v", err)
	}
	rancher, err := url.Parse(baoRancher)
	if err != nil {
		t.Fatalf("parse the Rancher URL: %v", err)
	}
	jwtFile := tokenFile(t, "jwt-1\n")
	syncer, logs := newAccountsSyncer(t, fake, append([]func(*Config){func(cfg *Config) {
		cfg.OpenBao = &OpenBaoConfig{
			Address:     address,
			AuthPath:    "kubernetes",
			Role:        baoRole,
			JWTFile:     jwtFile,
			MountPrefix: "kubernetes",
			RancherURL:  rancher,
			TokenTTL:    24 * time.Hour,

			CredentialTTL:    15 * time.Minute,
			CredentialMaxTTL: 2 * time.Hour,
		}
	}}, opts...)...)

	clock := newTestClock()
	syncer.openbao.now = clock.Now
	fake.mu.Lock()
	fake.tokenClock = clock.Now
	fake.mu.Unlock()
	return &openbaoSetup{fake: fake, bao: bao, syncer: syncer, logs: logs, clock: clock, jwtFile: jwtFile}
}

// tokenRequestPath is the TokenRequest path of the OpenBao ServiceAccount of
// cluster.
func tokenRequestPath(cluster string) string {
	return serviceAccountsPath(cluster, openbaoNamespace) + "/" + openbaoAccount + "/token"
}

// decodeMap decodes a recorded JSON body into a map, so that a test compares
// every key.
func decodeMap(t *testing.T, body string) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("decode the body %s: %v", body, err)
	}
	return out
}
