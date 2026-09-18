package rotate

import (
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

var rotationRoutes = []string{
	"POST /v3-public/localProviders/local?action=login",
	"POST /v3/tokens",
	"GET /v3/tokens",
	"POST /v3/tokens?action=logout",
}

func TestRunFirstRotation(t *testing.T) {
	t.Parallel()
	kube := newFakeKube(t, nil)
	rancher := newFakeRancher(t)
	h := newHarness(t, kube, rancher)

	if err := h.run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertRoutes(t, rancher.routes(), rotationRoutes)
	assertRoutes(t, kube.routes(), []string{"GET " + testSecretPath, "PATCH " + testSecretPath})

	names := rancher.names()
	if len(names) != 1 {
		t.Fatalf("the tokens after the run are %v, and the test expects one", names)
	}
	if got, want := kube.value(testKey), names[0]+":"+names[0]+"key"; got != want {
		t.Errorf("the Secret has the token %q, and the test expects %q", got, want)
	}

	calls := rancher.all()
	login, create, list := calls[0], calls[1], calls[2]
	if login.auth != "" {
		t.Errorf("the login has an Authorization header")
	}
	if login.agent != testUserAgent {
		t.Errorf("the login User-Agent is %q, and the test expects %q", login.agent, testUserAgent)
	}
	if got := login.field(t, "responseType"); got != "json" {
		t.Errorf("the login responseType is %q, and the test expects json", got)
	}
	if got, want := login.field(t, "description"), testDescription+" login"; got != want {
		t.Errorf("the login description is %q, and the test expects %q", got, want)
	}
	if got := create.field(t, "ttl"); got != "172800000" {
		t.Errorf("the create ttl is %q ms, and the test expects 172800000 ms", got)
	}
	if !strings.HasPrefix(create.auth, "Bearer token-session") {
		t.Errorf("the create runs as %q, and the test expects the session token", create.auth)
	}
	if got, want := list.auth, "Bearer "+kube.value(testKey); got != want {
		t.Errorf("the list runs as %q, and the test expects the new token", got)
	}
	for _, call := range kube.all() {
		if got, want := call.auth, "Bearer "+testKubeToken; got != want {
			t.Errorf("%s runs as %q, and the test expects the ServiceAccount token", call.route(), got)
		}
	}
	h.assertNoSecret(t)
}

func TestRunValidToken(t *testing.T) {
	t.Parallel()
	rancher := newFakeRancher(t)
	token := rancher.addToken("token-current", testDescription, time.Hour, 48*time.Hour, true)
	kube := newFakeKube(t, map[string]string{testKey: token.value})
	h := newHarness(t, kube, rancher)

	if err := h.run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertRoutes(t, rancher.routes(), []string{"GET /v3/tokens/token-current"})
	assertRoutes(t, kube.routes(), []string{"GET " + testSecretPath})
	if got := kube.value(testKey); got != token.value {
		t.Errorf("the Secret changed to %q", got)
	}
	h.assertLog(t, "token is valid", `"expires_at":"2026-01-03T11:00:00Z"`)
	h.assertNoSecret(t)
}

func TestRunTokenInsideRenewWindow(t *testing.T) {
	t.Parallel()
	rancher := newFakeRancher(t)
	token := rancher.addToken("token-old", testDescription, 30*time.Hour, 48*time.Hour, true)
	kube := newFakeKube(t, map[string]string{testKey: token.value})
	h := newHarness(t, kube, rancher)

	if err := h.run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertRoutes(t, rancher.routes(), slices.Concat([]string{"GET /v3/tokens/token-old"}, rotationRoutes))
	if got := kube.value(testKey); got == token.value {
		t.Errorf("the Secret still has the old token")
	}
	h.assertLog(t, "the token expires inside the renew window", `"reason":"window"`)
	h.assertNoSecret(t)
}

func TestRunRejectedToken(t *testing.T) {
	t.Parallel()
	rancher := newFakeRancher(t)
	kube := newFakeKube(t, map[string]string{testKey: "token-gone:token-gonekey"})
	h := newHarness(t, kube, rancher)

	if err := h.run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertRoutes(t, rancher.routes(), slices.Concat([]string{"GET /v3/tokens/token-gone"}, rotationRoutes))
	h.assertLog(t, `"reason":"rejected"`)
	if got := kube.value(testKey); got == "token-gone:token-gonekey" {
		t.Errorf("the Secret still has the rejected token")
	}
	h.assertNoSecret(t)
}

func TestRunPrunesOldTokens(t *testing.T) {
	t.Parallel()
	rancher := newFakeRancher(t)
	for index, name := range []string{"token-old1", "token-old2", "token-old3", "token-old4"} {
		age := time.Duration(index+1) * 10 * time.Hour
		rancher.addToken(name, testDescription, age, 48*time.Hour, true)
	}
	rancher.addToken("token-foreign", "kubeconfig token", 50*time.Hour, 48*time.Hour, true)
	rancher.addToken("token-stale-login", testDescription+" login", 6*time.Hour, 16*time.Hour, false)

	kube := newFakeKube(t, nil)
	h := newHarness(t, kube, rancher)
	h.cfg.Keep = 3

	if err := h.run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	deleted := []string{}
	for _, call := range rancher.all() {
		if call.method == "DELETE" {
			deleted = append(deleted, strings.TrimPrefix(call.path, tokensPath+"/"))
		}
	}
	assertRoutes(t, deleted, []string{"token-old3", "token-old4", "token-stale-login"})

	created, _, _ := strings.Cut(kube.value(testKey), ":")
	want := []string{created, "token-foreign", "token-old1", "token-old2"}
	slices.Sort(want)
	assertRoutes(t, rancher.names(), want)
	h.assertLog(t, `"step":"token_prune"`, `"kept":3`, `"deleted":3`)
	h.assertNoSecret(t)
}

func TestRunLoginFailure(t *testing.T) {
	t.Parallel()
	rancher := newFakeRancher(t)
	rancher.loginStatus = 401
	kube := newFakeKube(t, nil)
	h := newHarness(t, kube, rancher)

	err := h.run()
	if err == nil {
		t.Fatal("Run returned no error")
	}
	if !strings.Contains(err.Error(), "the user or the password is wrong") {
		t.Errorf("the error is %q, and the test expects the wrong password", err)
	}
	assertRoutes(t, kube.routes(), []string{"GET " + testSecretPath})
	if got := kube.value(testKey); got != "" {
		t.Errorf("the Secret has the token %q after the failure", got)
	}
	h.assertNoSecret(t)
}

func TestRunMissingSecret(t *testing.T) {
	t.Parallel()
	kube := newFakeKube(t, nil)
	kube.missing = true
	rancher := newFakeRancher(t)
	h := newHarness(t, kube, rancher)

	err := h.run()
	if err == nil {
		t.Fatal("Run returned no error")
	}
	if !strings.Contains(err.Error(), "the Secret cattle-system/drover-token does not exist") {
		t.Errorf("the error is %q, and the test expects the absent Secret", err)
	}
	assertRoutes(t, rancher.routes(), []string{})
}

func TestRunClampedTTL(t *testing.T) {
	t.Parallel()
	rancher := newFakeRancher(t)
	rancher.grantedTTL = (24 * time.Hour).Milliseconds()
	kube := newFakeKube(t, nil)
	h := newHarness(t, kube, rancher)

	if err := h.run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	h.assertLog(t, "rancher clamped the ttl", `"requested_ttl_ms":172800000`, `"granted_ttl_ms":86400000`)
	h.assertNoSecret(t)
}

func TestRunExpiryWithoutExpiresAt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		age        time.Duration
		ttl        time.Duration
		wantRotate bool
	}{
		{"created and ttl end inside the window", 30 * time.Hour, 48 * time.Hour, true},
		{"created and ttl end after the window", time.Hour, 48 * time.Hour, false},
		{"the ttl zero never expires", 100 * time.Hour, 0, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			rancher := newFakeRancher(t)
			token := rancher.addToken("token-current", testDescription, test.age, test.ttl, true)
			token.noExpiresAt = true
			kube := newFakeKube(t, map[string]string{testKey: token.value})
			h := newHarness(t, kube, rancher)

			if err := h.run(); err != nil {
				t.Fatalf("Run: %v", err)
			}

			rotated := slices.Contains(rancher.routes(), "POST /v3-public/localProviders/local?action=login")
			if rotated != test.wantRotate {
				t.Errorf("the run rotated: %t, and the test expects %t", rotated, test.wantRotate)
			}
			h.assertNoSecret(t)
		})
	}
}

func TestRunReadsTheServiceAccountTokenPerRequest(t *testing.T) {
	t.Parallel()
	kube := newFakeKube(t, nil)
	rancher := newFakeRancher(t)
	const second = "second-service-account-token"
	kube.afterGet = func() {
		if err := os.WriteFile(kube.tokenFile, []byte(second+"\n"), 0o600); err != nil {
			t.Errorf("write the ServiceAccount token file: %v", err)
		}
	}
	h := newHarness(t, kube, rancher)

	if err := h.run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	calls := kube.all()
	if len(calls) != 2 {
		t.Fatalf("the Kubernetes calls are %v, and the test expects two", kube.routes())
	}
	if got, want := calls[0].auth, "Bearer "+testKubeToken; got != want {
		t.Errorf("the get runs as %q, and the test expects %q", got, want)
	}
	if got, want := calls[1].auth, "Bearer "+second; got != want {
		t.Errorf("the patch runs as %q, and the test expects %q", got, want)
	}
}
