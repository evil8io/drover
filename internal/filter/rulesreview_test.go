package filter

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

// namespaceRule grants get on the named namespaces.
func namespaceRule(names ...string) resourceRule {
	return resourceRule{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{"namespaces"}, ResourceNames: names}
}

// wantNoServiceToken checks that no upstream request used the service token.
func wantNoServiceToken(t *testing.T, up *upstream) {
	t.Helper()
	for _, request := range up.all() {
		if request.header.Get("Authorization") == serviceAuth {
			t.Errorf("request %q uses the service token", request.path)
		}
	}
}

// rulesReviewRequests returns the recorded rules review requests.
func rulesReviewRequests(up *upstream) []recorded {
	var requests []recorded
	for _, request := range up.all() {
		if request.path == rulesReviewTestPath {
			requests = append(requests, request)
		}
	}
	return requests
}

// privilegedCount returns the count of upstream requests with the service token.
func privilegedCount(up *upstream) int {
	count := 0
	for _, request := range up.all() {
		if request.header.Get("Authorization") == serviceAuth {
			count++
		}
	}
	return count
}

func TestListServiceAccountUsesRulesReview(t *testing.T) {
	t.Parallel()
	h := newHarness(t, rulesUpstream(rulesReviewHandler(namespaceRule("b", "a")), namespaceListHandler))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath, nil, serviceAccountHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %q", resp.StatusCode, body)
	}

	reviews := rulesReviewRequests(h.upstream)
	if len(reviews) != 1 {
		t.Fatalf("rules review requests = %d, want 1", len(reviews))
	}
	review := reviews[0]
	if review.method != http.MethodPost {
		t.Errorf("rules review method = %s, want POST", review.method)
	}
	if got := review.header.Get("Authorization"); got != serviceAccountToken {
		t.Errorf("rules review Authorization = %q, want the token of the caller", got)
	}
	if got := review.header.Get("Content-Type"); got != jsonContentType {
		t.Errorf("rules review Content-Type = %q, want %q", got, jsonContentType)
	}
	var sent struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Spec       struct {
			Namespace string `json:"namespace"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(review.body, &sent); err != nil {
		t.Fatalf("parse the rules review body %q: %v", review.body, err)
	}
	if sent.Kind != "SelfSubjectRulesReview" || sent.APIVersion != "authorization.k8s.io/v1" {
		t.Errorf("rules review kind = %q and apiVersion = %q, want SelfSubjectRulesReview and authorization.k8s.io/v1",
			sent.Kind, sent.APIVersion)
	}
	if sent.Spec.Namespace != "tenant-system" {
		t.Errorf("rules review namespace = %q, want tenant-system", sent.Spec.Namespace)
	}

	privileged := h.upstream.privileged(t)
	if privileged.path != listPath {
		t.Errorf("privileged path = %q, want %q", privileged.path, listPath)
	}
	if got := privileged.header.Get("Authorization"); got != serviceAuth {
		t.Errorf("privileged Authorization = %q, want %q", got, serviceAuth)
	}
	if got, want := privileged.query.Get("labelSelector"), "kubernetes.io/metadata.name in (a,b)"; got != want {
		t.Errorf("labelSelector = %q, want %q", got, want)
	}

	for _, path := range []string{stevePath, projectsPath, selfSubjectReviewPath} {
		if got := h.upstream.countPath(path); got != 0 {
			t.Errorf("requests to %q = %d, want 0", path, got)
		}
	}
	if !strings.Contains(h.logs.String(), "user="+serviceAccountSubjectValue) {
		t.Errorf("logs have no user=%s line: %s", serviceAccountSubjectValue, h.logs.String())
	}
}

func TestListServiceAccountUnboundedRuleGetsNative403(t *testing.T) {
	t.Parallel()
	h := newHarness(t, rulesUpstream(rulesReviewHandler(namespaceRule()), namespaceListHandler))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath, nil, serviceAccountHeader()))
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
	if string(body) != nativeForbidden {
		t.Errorf("body = %q, want the native answer", body)
	}
	wantNoServiceToken(t, h.upstream)
	logs := h.logs.String()
	for _, want := range []string{"outcome=denied", "status=403"} {
		if !strings.Contains(logs, want) {
			t.Errorf("logs have no %s line: %s", want, logs)
		}
	}
}

func TestListServiceAccountNoRuleGetsNative403(t *testing.T) {
	t.Parallel()
	pods := resourceRule{Verbs: []string{"get", "list"}, APIGroups: []string{""}, Resources: []string{"pods"}, ResourceNames: []string{"a"}}
	h := newHarness(t, rulesUpstream(rulesReviewHandler(pods), namespaceListHandler))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath, nil, serviceAccountHeader()))
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
	if string(body) != nativeForbidden {
		t.Errorf("body = %q, want the native answer", body)
	}
	wantNoServiceToken(t, h.upstream)
}

func TestListServiceAccountCachesDenial(t *testing.T) {
	t.Parallel()
	h := newHarness(t, rulesUpstream(rulesReviewHandler(namespaceRule()), namespaceListHandler))

	for i := range 2 {
		resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, serviceAccountHeader()))
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("request %d: status = %d, want 403", i, resp.StatusCode)
		}
	}
	if got := h.upstream.countPath(rulesReviewTestPath); got != 1 {
		t.Fatalf("rules review requests = %d, want 1", got)
	}

	h.clock.advance(defaultCacheTTL + time.Second)
	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, serviceAccountHeader()))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	if got := h.upstream.countPath(rulesReviewTestPath); got != 2 {
		t.Errorf("rules review requests = %d, want 2", got)
	}
}

func TestListServiceAccountCachesAllowedSet(t *testing.T) {
	t.Parallel()
	h := newHarness(t, rulesUpstream(rulesReviewHandler(namespaceRule("a")), namespaceListHandler))

	for i := range 2 {
		resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, serviceAccountHeader()))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i, resp.StatusCode)
		}
	}
	if got := h.upstream.countPath(rulesReviewTestPath); got != 1 {
		t.Errorf("rules review requests = %d, want 1", got)
	}
	if got := privilegedCount(h.upstream); got != 2 {
		t.Errorf("privileged requests = %d, want 2", got)
	}
}

func TestListServiceAccountRulesReviewDenied(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, rulesUpstream(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", jsonContentType)
				w.WriteHeader(status)
				_, _ = w.Write(statusJSON(status, http.StatusText(status), "the rules review denies the caller"))
			}, namespaceListHandler))

			resp, body := h.do(t, h.request(t, http.MethodGet, listPath, nil, serviceAccountHeader()))
			if resp.StatusCode != http.StatusForbidden {
				t.Errorf("status = %d, want 403", resp.StatusCode)
			}
			if string(body) != nativeForbidden {
				t.Errorf("body = %q, want the native answer", body)
			}
			wantNoServiceToken(t, h.upstream)
		})
	}
}

func TestListServiceAccountRulesReviewError(t *testing.T) {
	t.Parallel()
	h := newHarness(t, rulesUpstream(errorStatus, namespaceListHandler))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath, nil, serviceAccountHeader()))
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}

	var status statusBody
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatalf("parse status body %q: %v", body, err)
	}
	if status.Kind != "Status" {
		t.Errorf("kind = %q, want Status", status.Kind)
	}
	if status.Code != http.StatusBadGateway {
		t.Errorf("code = %d, want 502", status.Code)
	}
	if status.Reason != reasonInternalError {
		t.Errorf("reason = %q, want %q", status.Reason, reasonInternalError)
	}
	if !strings.Contains(status.Message, "rules review request returned") {
		t.Errorf("message = %q, want to contain %q", status.Message, "rules review request returned")
	}
	wantNoServiceToken(t, h.upstream)
}

func TestListServiceAccountWildcardRules(t *testing.T) {
	t.Parallel()
	rule := resourceRule{Verbs: []string{"*"}, APIGroups: []string{"*"}, Resources: []string{"*"}, ResourceNames: []string{"a"}}
	h := newHarness(t, rulesUpstream(rulesReviewHandler(rule), namespaceListHandler))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath, nil, serviceAccountHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %q", resp.StatusCode, body)
	}
	privileged := h.upstream.privileged(t)
	if got, want := privileged.query.Get("labelSelector"), "kubernetes.io/metadata.name in (a)"; got != want {
		t.Errorf("labelSelector = %q, want %q", got, want)
	}
}

func TestListNonServiceAccountJWTUsesSteve(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler("a"), namespaceListHandler))

	header := http.Header{"Authorization": []string{"Bearer " + fakeJWT(`{"sub":"user@example.com"}`)}}
	resp, body := h.do(t, h.request(t, http.MethodGet, listPath, nil, header))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %q", resp.StatusCode, body)
	}
	if got := h.upstream.countPath(stevePath); got != 1 {
		t.Errorf("allowed set requests = %d, want 1", got)
	}
	if got := h.upstream.countPath(rulesReviewTestPath); got != 0 {
		t.Errorf("rules review requests = %d, want 0", got)
	}
}

func TestWatchServiceAccountHasNoSelector(t *testing.T) {
	t.Parallel()
	const allowed = `{"type":"ADDED","object":{"metadata":{"name":"a"}}}` + "\n"
	h := newHarness(t, rulesUpstream(rulesReviewHandler(namespaceRule("a")), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", jsonContentType)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, allowed)
		_, _ = io.WriteString(w, `{"type":"ADDED","object":{"metadata":{"name":"other"}}}`+"\n")
	}))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, serviceAccountHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != allowed {
		t.Errorf("body = %q, want the allowed event %q", body, allowed)
	}

	privileged := h.upstream.privileged(t)
	if got := privileged.query.Get("watch"); got != "true" {
		t.Errorf("privileged watch = %q, want true", got)
	}
	if got := privileged.query.Get("labelSelector"); got != "" {
		t.Errorf("privileged labelSelector = %q, want no selector", got)
	}
}

func TestServiceAccountSubject(t *testing.T) {
	t.Parallel()
	payload := func(sub string) string {
		return base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"` + sub + `"}`))
	}
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256"}`))
	token := func(payload string) string { return header + "." + payload + ".c2lnbmF0dXJl" }

	const padded = "system:serviceaccount:tenant-system:app"
	paddedPayload := base64.URLEncoding.EncodeToString([]byte(`{"sub":"` + padded + `"}`))
	if !strings.HasSuffix(paddedPayload, "=") {
		t.Fatalf("payload %q has no padding", paddedPayload)
	}

	tests := []struct {
		name          string
		auth          string
		wantSubject   string
		wantNamespace string
		wantOK        bool
	}{
		{"raw payload", serviceAccountToken, serviceAccountSubjectValue, "tenant-system", true},
		{"padded payload", "Bearer " + token(paddedPayload), padded, "tenant-system", true},
		{"scheme in upper case", "BEARER " + token(payload(serviceAccountSubjectValue)), serviceAccountSubjectValue, "tenant-system", true},
		{"basic scheme", "Basic " + token(payload(serviceAccountSubjectValue)), "", "", false},
		{"rancher token", "Bearer token-abc:xyz", "", "", false},
		{"two segments", "Bearer " + header + "." + payload(serviceAccountSubjectValue), "", "", false},
		{"payload not base64", "Bearer " + token("!!!"), "", "", false},
		{"payload not json", "Bearer " + token(base64.RawURLEncoding.EncodeToString([]byte("not json"))), "", "", false},
		{"sub not a string", "Bearer " + token(base64.RawURLEncoding.EncodeToString([]byte(`{"sub":1}`))), "", "", false},
		{"user subject", "Bearer " + token(payload("user@example.com")), "", "", false},
		{"no name", "Bearer " + token(payload("system:serviceaccount:ns")), "", "", false},
		{"empty namespace", "Bearer " + token(payload("system:serviceaccount::name")), "", "", false},
		{"empty name", "Bearer " + token(payload("system:serviceaccount:ns:")), "", "", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			subject, namespace, ok := serviceAccountSubject(test.auth)
			if subject != test.wantSubject || namespace != test.wantNamespace || ok != test.wantOK {
				t.Errorf("serviceAccountSubject(%q) = (%q, %q, %v), want (%q, %q, %v)",
					test.auth, subject, namespace, ok, test.wantSubject, test.wantNamespace, test.wantOK)
			}
		})
	}
}

func TestNamespaceNames(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		rules       []resourceRule
		want        []string
		wantBounded bool
	}{
		{"one bounded rule", []resourceRule{namespaceRule("b", "a")}, []string{"a", "b"}, true},
		{
			"union sorts and deduplicates",
			[]resourceRule{namespaceRule("c", "a"), namespaceRule("b", "a")},
			[]string{"a", "b", "c"},
			true,
		},
		{
			"verb wildcard",
			[]resourceRule{{Verbs: []string{"*"}, APIGroups: []string{""}, Resources: []string{"namespaces"}, ResourceNames: []string{"a"}}},
			[]string{"a"},
			true,
		},
		{
			"group wildcard",
			[]resourceRule{{Verbs: []string{"get"}, APIGroups: []string{"*"}, Resources: []string{"namespaces"}, ResourceNames: []string{"a"}}},
			[]string{"a"},
			true,
		},
		{
			"resource wildcard",
			[]resourceRule{{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{"*"}, ResourceNames: []string{"a"}}},
			[]string{"a"},
			true,
		},
		{
			"list only",
			[]resourceRule{{Verbs: []string{"list"}, APIGroups: []string{""}, Resources: []string{"namespaces"}}},
			nil,
			true,
		},
		{
			"status subresource",
			[]resourceRule{{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{"namespaces/status"}}},
			nil,
			true,
		},
		{
			"apps group",
			[]resourceRule{{Verbs: []string{"get"}, APIGroups: []string{"apps"}, Resources: []string{"namespaces"}}},
			nil,
			true,
		},
		{"unbounded rule", []resourceRule{namespaceRule()}, nil, false},
		{"bounded and unbounded", []resourceRule{namespaceRule("a"), namespaceRule()}, nil, false},
		{"empty name", []resourceRule{namespaceRule("", "a")}, []string{"a"}, true},
		{"no rules", nil, nil, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, bounded := namespaceNames(test.rules)
			if !slices.Equal(got, test.want) || bounded != test.wantBounded {
				t.Errorf("namespaceNames() = (%v, %v), want (%v, %v)", got, bounded, test.want, test.wantBounded)
			}
		})
	}
}
