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
// auth/kubernetes, and the config write of the secrets engine mounts under
// kubernetes/. It records every request.
type fakeOpenBao struct {
	server *httptest.Server

	mu       sync.Mutex
	requests []recorded
	issued   []string
	// loginStatus is the status of a login, zero for success.
	loginStatus int
	// missing is the status that a config write of a cluster answers when its
	// mount does not exist: 404, or 400 as an older server answers.
	missing map[string]int
}

func newFakeOpenBao(t *testing.T) *fakeOpenBao {
	t.Helper()
	f := &fakeOpenBao{missing: make(map[string]int)}
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
		method: r.Method, path: r.URL.Path, header: r.Header.Clone(), body: string(body), at: time.Now(),
	})

	route := strings.TrimPrefix(r.URL.Path, "/v1/")
	switch {
	case r.Method == http.MethodPost && route == "auth/kubernetes/login":
		if f.loginStatus != 0 {
			writeJSON(w, f.loginStatus, map[string][]string{"errors": {"permission denied"}})
			return
		}
		token := fmt.Sprintf("bao-%d", len(f.issued)+1)
		f.issued = append(f.issued, token)
		writeJSON(w, http.StatusOK, map[string]any{"auth": map[string]any{"client_token": token}})
	case r.Method == http.MethodPost && strings.HasPrefix(route, "kubernetes/") && strings.HasSuffix(route, "/config"):
		if !slices.Contains(f.issued, r.Header.Get("X-Vault-Token")) {
			writeJSON(w, http.StatusForbidden, map[string][]string{"errors": {"permission denied"}})
			return
		}
		cluster := strings.TrimSuffix(strings.TrimPrefix(route, "kubernetes/"), "/config")
		switch f.missing[cluster] {
		case http.StatusNotFound:
			writeJSON(w, http.StatusNotFound, map[string][]string{"errors": {
				fmt.Sprintf("no handler for route %q. route entry not found.", route)}})
		case http.StatusBadRequest:
			writeJSON(w, http.StatusBadRequest, map[string][]string{"errors": {
				fmt.Sprintf("no handler for route '%s'", route)}})
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	default:
		writeJSON(w, http.StatusNotFound, map[string][]string{"errors": {}})
	}
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
