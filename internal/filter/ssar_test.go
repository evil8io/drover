package filter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

const (
	reviewTemplate = `{"kind":"SelfSubjectAccessReview","apiVersion":"authorization.k8s.io/v1",` +
		`"metadata":{"creationTimestamp":null},"spec":{"resourceAttributes":{%s}},"status":{}}`

	deniedAnswer = `{"kind":"SelfSubjectAccessReview","apiVersion":"authorization.k8s.io/v1",` +
		`"metadata":{"creationTimestamp":null},"spec":{"resourceAttributes":{"verb":"list","resource":"namespaces"}},` +
		`"status":{"allowed":false,"denied":true,"reason":"RBAC: no policy matched"}}`

	allowedAnswer = `{"kind":"SelfSubjectAccessReview","apiVersion":"authorization.k8s.io/v1",` +
		`"metadata":{"creationTimestamp":null},"spec":{"resourceAttributes":{"verb":"list","resource":"namespaces"}},` +
		`"status":{"allowed":true,"reason":"RBAC: allowed by a role binding"}}`
)

func reviewUpstream(answer string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, answer)
	}
}

// reviewUpstreamWithNames routes the review request to reviewUpstream(answer),
// and answers the caller's allowed set with the given namespace names and no projects.
func reviewUpstreamWithNames(answer string, names ...string) http.HandlerFunc {
	review := reviewUpstream(answer)
	steve := steveHandler(names...)
	projects := projectsHandler()
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case stevePath:
			steve(w, r)
		case projectsPath:
			projects(w, r)
		default:
			review(w, r)
		}
	}
}

// echoReviewHandler answers a SelfSubjectAccessReview the way the API server
// does: it reads spec into a map with exact-case keys, and it echoes the
// resourceAttributes member only, denied.
func echoReviewHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var review struct {
			Spec map[string]json.RawMessage `json:"spec"`
		}
		_ = json.Unmarshal(body, &review)
		attributes, ok := review.Spec["resourceAttributes"]
		if !ok {
			attributes = json.RawMessage("null")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"kind":"SelfSubjectAccessReview","apiVersion":"authorization.k8s.io/v1",`+
			`"metadata":{"creationTimestamp":null},"spec":{"resourceAttributes":%s},`+
			`"status":{"allowed":false,"denied":true,"reason":"RBAC: no policy matched"}}`, string(attributes))
	}
}

// reviewUpstreamEcho routes the review to echoReviewHandler, and answers the
// caller's allowed set with the given namespace names and no projects.
func reviewUpstreamEcho(names ...string) http.HandlerFunc {
	review := echoReviewHandler()
	steve := steveHandler(names...)
	projects := projectsHandler()
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case stevePath:
			steve(w, r)
		case projectsPath:
			projects(w, r)
		default:
			review(w, r)
		}
	}
}

// reviewBodyOfSize returns a namespace list review body of exactly size
// bytes, padded in the name attribute.
func reviewBodyOfSize(t *testing.T, size int) []byte {
	t.Helper()
	empty := fmt.Sprintf(reviewTemplate, `"verb":"list","resource":"namespaces","name":""`)
	if len(empty) > size {
		t.Fatalf("target size %d is smaller than the empty body %d", size, len(empty))
	}
	pad := size - len(empty)
	attributes := fmt.Sprintf(`"verb":"list","resource":"namespaces","name":"%s"`, strings.Repeat("x", pad))
	return []byte(fmt.Sprintf(reviewTemplate, attributes))
}

func (h *harness) postReview(t *testing.T, body []byte, header http.Header) (*http.Response, []byte) {
	t.Helper()
	return h.do(t, h.reviewRequest(t, body, header))
}

// postReviewDirect is postReview with no network hop, for a test that reads
// the exported spans of the request.
func (h *harness) postReviewDirect(t *testing.T, body []byte, header http.Header) (*http.Response, []byte) {
	t.Helper()
	return h.doDirect(t, h.reviewRequest(t, body, header))
}

func (h *harness) reviewRequest(t *testing.T, body []byte, header http.Header) *http.Request {
	t.Helper()
	all := http.Header{}
	for name, values := range header {
		for _, value := range values {
			all.Add(name, value)
		}
	}
	if all.Get("Content-Type") == "" {
		all.Set("Content-Type", jsonContentType)
	}
	return h.request(t, http.MethodPost, reviewPath, bytes.NewReader(body), all)
}

func TestReviewGrantsNamespaceList(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		attributes string
	}{
		{"list", `"verb":"list","resource":"namespaces"`},
		{"watch", `"verb":"watch","resource":"namespaces"`},
		{"list/namespace", `"verb":"list","resource":"namespaces","namespace":"default"`},
		{"watch/namespace", `"verb":"watch","resource":"namespaces","namespace":"default"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, reviewUpstreamWithNames(deniedAnswer, "prod"))

			request := fmt.Sprintf(reviewTemplate, test.attributes)
			resp, body := h.postReview(t, []byte(request), nil)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}

			var object map[string]any
			if err := json.Unmarshal(body, &object); err != nil {
				t.Fatalf("parse the response %q: %v", body, err)
			}
			status, ok := object["status"].(map[string]any)
			if !ok {
				t.Fatalf("the response has no status object: %q", body)
			}
			if status["allowed"] != true {
				t.Errorf("allowed = %v, want true", status["allowed"])
			}
			if _, ok := status["denied"]; ok {
				t.Error("the response has a denied field")
			}
			if status["reason"] != grantedReason {
				t.Errorf("reason = %v, want %q", status["reason"], grantedReason)
			}
			if _, ok := object["metadata"].(map[string]any); !ok {
				t.Error("the response has no metadata object")
			}
			if _, ok := object["spec"].(map[string]any); !ok {
				t.Error("the response has no spec object")
			}
			if object["kind"] != "SelfSubjectAccessReview" {
				t.Errorf("kind = %v, want SelfSubjectAccessReview", object["kind"])
			}
			if resp.ContentLength != int64(len(body)) {
				t.Errorf("ContentLength = %d, want %d", resp.ContentLength, len(body))
			}
			if got := resp.Header.Get("Content-Length"); got != strconv.Itoa(len(body)) {
				t.Errorf("Content-Length = %q, want %d", got, len(body))
			}
			if !strings.Contains(h.logs.String(), "outcome=granted") {
				t.Errorf("logs have no outcome=granted line: %s", h.logs.String())
			}

			if got := h.upstream.countPath(reviewPath); got != 1 {
				t.Fatalf("review requests = %d, want 1", got)
			}
			sent := h.upstream.all()
			if string(sent[0].body) != request {
				t.Errorf("upstream body = %q, want %q", sent[0].body, request)
			}
			if got := sent[0].header.Get("Accept-Encoding"); got != "" {
				t.Errorf("upstream Accept-Encoding = %q, want no header", got)
			}
		})
	}
}

// TestReviewGrantLogsAndSpanHaveCallerName checks that a resolved caller
// identity reaches the review log line and the drover.user span attribute.
// This is the path that grants a denied review.
func TestReviewGrantLogsAndSpanHaveCallerName(t *testing.T) {
	t.Parallel()
	upstream := func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case stevePath:
			steveHandler("prod")(w, r)
		case projectsPath:
			projectsHandler()(w, r)
		case selfSubjectReviewPath:
			selfSubjectReviewHandler("u-alice")(w, r)
		default:
			reviewUpstream(deniedAnswer)(w, r)
		}
	}
	h, exporter := newHarnessWithSpans(t, upstream)

	request := fmt.Sprintf(reviewTemplate, `"verb":"list","resource":"namespaces"`)
	resp, _ := h.postReviewDirect(t, []byte(request), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(h.logs.String(), "user=u-alice") {
		t.Errorf("logs have no user=u-alice line: %s", h.logs.String())
	}

	span := requestSpan(t, exporter)
	got, ok := spanAttributeString(span, "drover.user")
	if !ok || got != "u-alice" {
		t.Errorf("drover.user attribute = %q, ok=%v, want u-alice", got, ok)
	}
}

func TestReviewKeepsAllowed(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == stevePath || r.URL.Path == projectsPath {
			t.Error("the upstream got an allowed set request")
		}
		reviewUpstream(allowedAnswer)(w, r)
	})

	request := fmt.Sprintf(reviewTemplate, `"verb":"list","resource":"namespaces"`)
	resp, body := h.postReview(t, []byte(request), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != allowedAnswer {
		t.Errorf("body = %q, want the answer of the upstream", body)
	}
}

func TestReviewGrantsProtobufNamespaceList(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		attributes protoAttributes
	}{
		{"list", protoAttributes{verb: "list", resource: "namespaces"}},
		{"watch", protoAttributes{verb: "watch", resource: "namespaces"}},
		{"list/namespace", protoAttributes{verb: "list", resource: "namespaces", namespace: "default"}},
		{"watch/namespace", protoAttributes{verb: "watch", resource: "namespaces", namespace: "default"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, reviewUpstreamWithNames(deniedAnswer, "prod"))

			header := http.Header{
				"Content-Type": []string{protobufContentType},
				"Accept":       []string{protobufContentType + ", */*"},
			}
			request := protobufReview(test.attributes)
			resp, body := h.postReview(t, request, header)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			if got := resp.Header.Get("Content-Type"); got != jsonContentType {
				t.Errorf("Content-Type = %q, want %q", got, jsonContentType)
			}

			var object map[string]any
			if err := json.Unmarshal(body, &object); err != nil {
				t.Fatalf("parse the response %q: %v", body, err)
			}
			status, ok := object["status"].(map[string]any)
			if !ok {
				t.Fatalf("the response has no status object: %q", body)
			}
			if status["allowed"] != true {
				t.Errorf("allowed = %v, want true", status["allowed"])
			}
			if status["reason"] != grantedReason {
				t.Errorf("reason = %v, want %q", status["reason"], grantedReason)
			}

			if got := h.upstream.countPath(reviewPath); got != 1 {
				t.Fatalf("review requests = %d, want 1", got)
			}
			sent := h.upstream.all()
			wantAttributes := fmt.Sprintf(`"verb":%q,"resource":"namespaces"`, test.attributes.verb)
			if test.attributes.namespace != "" {
				wantAttributes = fmt.Sprintf(`"namespace":%q,%s`, test.attributes.namespace, wantAttributes)
			}
			wantBody := fmt.Sprintf(`{"apiVersion":"authorization.k8s.io/v1","kind":"SelfSubjectAccessReview",`+
				`"spec":{"resourceAttributes":{%s}}}`, wantAttributes)
			if string(sent[0].body) != wantBody {
				t.Errorf("upstream body = %q, want %q", sent[0].body, wantBody)
			}
			for _, name := range []string{"Content-Type", "Accept"} {
				if got := sent[0].header.Get(name); got != jsonContentType {
					t.Errorf("upstream %s = %q, want %q", name, got, jsonContentType)
				}
			}
		})
	}
}

func TestReviewPassesThrough(t *testing.T) {
	t.Parallel()
	protobufHeader := http.Header{"Content-Type": []string{protobufContentType}}
	tests := []struct {
		name   string
		body   []byte
		header http.Header
	}{
		{
			name: "other resource",
			body: []byte(fmt.Sprintf(reviewTemplate, `"verb":"list","resource":"pods"`)),
		},
		{
			name: "name set",
			body: []byte(fmt.Sprintf(reviewTemplate, `"verb":"list","resource":"namespaces","name":"a"`)),
		},
		{
			name: "get verb",
			body: []byte(fmt.Sprintf(reviewTemplate, `"verb":"get","resource":"namespaces"`)),
		},
		{
			name: "invalid json",
			body: []byte(`{"kind":"SelfSubjectAccessReview"`),
		},
		{
			name: "non resource attributes",
			body: []byte(`{"spec":{"nonResourceAttributes":{"path":"/healthz","verb":"get"}}}`),
		},
		{
			name:   "impersonation",
			body:   []byte(fmt.Sprintf(reviewTemplate, `"verb":"list","resource":"namespaces"`)),
			header: http.Header{"Impersonate-User": []string{"someone"}},
		},
		{
			name:   "protobuf other resource",
			body:   protobufReview(protoAttributes{verb: "get", resource: "pods"}),
			header: protobufHeader,
		},
		{
			name:   "protobuf truncated",
			body:   protobufReview(protoAttributes{verb: "list", resource: "namespaces"})[:12],
			header: protobufHeader,
		},
		{
			name:   "protobuf without the prefix",
			body:   protobufReview(protoAttributes{verb: "list", resource: "namespaces"})[len(protobufPrefix):],
			header: protobufHeader,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == stevePath || r.URL.Path == projectsPath {
					t.Error("the upstream got an allowed set request")
				}
				reviewUpstream(deniedAnswer)(w, r)
			})

			resp, body := h.postReview(t, test.body, test.header)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			if string(body) != deniedAnswer {
				t.Errorf("body = %q, want the answer of the upstream", body)
			}

			sent := h.upstream.all()
			if len(sent) != 1 {
				t.Fatalf("upstream requests = %d, want 1", len(sent))
			}
			if !bytes.Equal(sent[0].body, test.body) {
				t.Errorf("upstream body = %q, want %q", sent[0].body, test.body)
			}
			wantType := test.header.Get("Content-Type")
			if wantType == "" {
				wantType = jsonContentType
			}
			if got := sent[0].header.Get("Content-Type"); got != wantType {
				t.Errorf("upstream Content-Type = %q, want %q", got, wantType)
			}
		})
	}
}

func TestReviewBodyTooLarge(t *testing.T) {
	t.Parallel()
	h := newHarness(t, reviewUpstream(deniedAnswer))

	large := fmt.Sprintf(reviewTemplate, `"verb":"list","resource":"namespaces","name":"`+strings.Repeat("x", maxReviewBody)+`"`)
	resp, body := h.postReview(t, []byte(large), nil)
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}

	var status struct {
		Kind   string `json:"kind"`
		Reason string `json:"reason"`
		Code   int    `json:"code"`
	}
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatalf("parse the status body %q: %v", body, err)
	}
	if status.Kind != "Status" || status.Code != http.StatusRequestEntityTooLarge || status.Reason != reasonTooLarge {
		t.Errorf("status body = %+v", status)
	}
	if h.upstream.count() != 0 {
		t.Errorf("upstream requests = %d, want 0", h.upstream.count())
	}
}

func TestReviewStaysDeniedWithEmptyAllowedSet(t *testing.T) {
	t.Parallel()
	h := newHarness(t, reviewUpstreamWithNames(deniedAnswer))

	request := fmt.Sprintf(reviewTemplate, `"verb":"list","resource":"namespaces"`)
	resp, body := h.postReview(t, []byte(request), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != deniedAnswer {
		t.Errorf("body = %q, want the answer of the upstream", body)
	}
	if !strings.Contains(h.logs.String(), "outcome=native") {
		t.Errorf("logs have no outcome=native line: %s", h.logs.String())
	}
}

func TestReviewStaysDeniedOnAllowedSetError(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == stevePath {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		reviewUpstream(deniedAnswer)(w, r)
	})

	request := fmt.Sprintf(reviewTemplate, `"verb":"list","resource":"namespaces"`)
	resp, body := h.postReview(t, []byte(request), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != deniedAnswer {
		t.Errorf("body = %q, want the answer of the upstream", body)
	}
	if !strings.Contains(h.logs.String(), "outcome=native") {
		t.Errorf("logs have no outcome=native line: %s", h.logs.String())
	}
	if strings.Contains(h.logs.String(), "level=ERROR") {
		t.Errorf("logs contain an ERROR line, want WARN: %s", h.logs.String())
	}
	if !strings.Contains(h.logs.String(), "level=WARN") {
		t.Errorf("logs have no WARN line: %s", h.logs.String())
	}
}

func TestReviewStaysDeniedOnSteveForbidden(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == stevePath {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		reviewUpstream(deniedAnswer)(w, r)
	})

	request := fmt.Sprintf(reviewTemplate, `"verb":"list","resource":"namespaces"`)
	resp, body := h.postReview(t, []byte(request), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != deniedAnswer {
		t.Errorf("body = %q, want the answer of the upstream", body)
	}
}

// reviewUpstreamWithRules routes the rules review to rulesReviewHandler(rules),
// and every other request to reviewUpstream(answer).
func reviewUpstreamWithRules(answer string, rules ...resourceRule) http.HandlerFunc {
	review := reviewUpstream(answer)
	rulesReview := rulesReviewHandler(rules...)
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case rulesReviewTestPath:
			rulesReview(w, r)
		default:
			review(w, r)
		}
	}
}

func TestReviewGrantsServiceAccountNamespaceList(t *testing.T) {
	t.Parallel()
	h := newHarness(t, reviewUpstreamWithRules(deniedAnswer, namespaceRule("prod")))

	request := fmt.Sprintf(reviewTemplate, `"verb":"list","resource":"namespaces"`)
	resp, body := h.postReview(t, []byte(request), serviceAccountHeader())
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var object map[string]any
	if err := json.Unmarshal(body, &object); err != nil {
		t.Fatalf("parse the response %q: %v", body, err)
	}
	status, ok := object["status"].(map[string]any)
	if !ok {
		t.Fatalf("the response has no status object: %q", body)
	}
	if status["allowed"] != true {
		t.Errorf("allowed = %v, want true", status["allowed"])
	}
	if status["reason"] != grantedReason {
		t.Errorf("reason = %v, want %q", status["reason"], grantedReason)
	}
	if !strings.Contains(h.logs.String(), "outcome=granted") {
		t.Errorf("logs have no outcome=granted line: %s", h.logs.String())
	}
	if !strings.Contains(h.logs.String(), "user="+serviceAccountSubjectValue) {
		t.Errorf("logs have no user=%s line: %s", serviceAccountSubjectValue, h.logs.String())
	}
	if got := h.upstream.countPath(rulesReviewTestPath); got != 1 {
		t.Errorf("rules review requests = %d, want 1", got)
	}
	if got := h.upstream.countPath(stevePath); got != 0 {
		t.Errorf("allowed set requests = %d, want 0", got)
	}
}

func TestReviewStaysDeniedForServiceAccountWithoutNames(t *testing.T) {
	t.Parallel()
	h := newHarness(t, reviewUpstreamWithRules(deniedAnswer, namespaceRule()))

	request := fmt.Sprintf(reviewTemplate, `"verb":"list","resource":"namespaces"`)
	resp, body := h.postReview(t, []byte(request), serviceAccountHeader())
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != deniedAnswer {
		t.Errorf("body = %q, want the answer of the upstream", body)
	}
	if got := h.upstream.countPath(rulesReviewTestPath); got != 1 {
		t.Errorf("rules review requests = %d, want 1", got)
	}
}

func TestReviewBodyLimit(t *testing.T) {
	t.Parallel()

	t.Run("exact size passes to the upstream", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, reviewUpstream(allowedAnswer))

		body := reviewBodyOfSize(t, maxReviewBody)
		resp, respBody := h.postReview(t, body, nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		if string(respBody) != allowedAnswer {
			t.Errorf("body = %q, want the answer of the upstream", respBody)
		}
		if got := h.upstream.countPath(reviewPath); got != 1 {
			t.Errorf("review requests = %d, want 1", got)
		}
	})

	t.Run("size plus one answers 413", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, reviewUpstream(deniedAnswer))

		body := reviewBodyOfSize(t, maxReviewBody+1)
		resp, _ := h.postReview(t, body, nil)
		if resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want 413", resp.StatusCode)
		}
		if h.upstream.count() != 0 {
			t.Errorf("upstream requests = %d, want 0", h.upstream.count())
		}
	})
}

// TestReviewStaysDeniedOnKeyCaseDifferential sends a spec with a
// "resourceAttributes" member and a "ResourceAttributes" member. The fake
// upstream, like the API server, evaluates and echoes the first member only.
// The filter must grant on that echoed member, not on its own read of the body.
func TestReviewStaysDeniedOnKeyCaseDifferential(t *testing.T) {
	t.Parallel()
	h := newHarness(t, reviewUpstreamEcho("prod"))

	body := []byte(`{"kind":"SelfSubjectAccessReview","apiVersion":"authorization.k8s.io/v1",` +
		`"metadata":{"creationTimestamp":null},` +
		`"spec":{"resourceAttributes":{"verb":"delete","resource":"secrets","namespace":"kube-system"},` +
		`"ResourceAttributes":{"verb":"list","resource":"namespaces"}},"status":{}}`)

	resp, respBody := h.postReview(t, body, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var object map[string]any
	if err := json.Unmarshal(respBody, &object); err != nil {
		t.Fatalf("parse the response %q: %v", respBody, err)
	}
	status, ok := object["status"].(map[string]any)
	if !ok {
		t.Fatalf("the response has no status object: %q", respBody)
	}
	if status["allowed"] != false {
		t.Errorf("allowed = %v, want false", status["allowed"])
	}

	spec, ok := object["spec"].(map[string]any)
	if !ok {
		t.Fatalf("the response has no spec object: %q", respBody)
	}
	attributes, ok := spec["resourceAttributes"].(map[string]any)
	if !ok {
		t.Fatalf("the response has no resourceAttributes: %q", respBody)
	}
	if attributes["verb"] != "delete" || attributes["resource"] != "secrets" || attributes["namespace"] != "kube-system" {
		t.Errorf("resourceAttributes = %v, want the delete secrets review", attributes)
	}
}

// TestReviewGrantsOnEchoedNamespaceList checks that a plain namespace list
// review of a caller with an allowed set is still granted, with the echoed
// spec present in the answer.
func TestReviewGrantsOnEchoedNamespaceList(t *testing.T) {
	t.Parallel()
	h := newHarness(t, reviewUpstreamEcho("prod"))

	request := fmt.Sprintf(reviewTemplate, `"verb":"list","resource":"namespaces"`)
	resp, body := h.postReview(t, []byte(request), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var object map[string]any
	if err := json.Unmarshal(body, &object); err != nil {
		t.Fatalf("parse the response %q: %v", body, err)
	}
	status, ok := object["status"].(map[string]any)
	if !ok {
		t.Fatalf("the response has no status object: %q", body)
	}
	if status["allowed"] != true {
		t.Errorf("allowed = %v, want true", status["allowed"])
	}
	spec, ok := object["spec"].(map[string]any)
	if !ok {
		t.Fatalf("the response has no spec object: %q", body)
	}
	attributes, ok := spec["resourceAttributes"].(map[string]any)
	if !ok {
		t.Fatalf("the response has no resourceAttributes: %q", body)
	}
	if attributes["verb"] != "list" || attributes["resource"] != "namespaces" {
		t.Errorf("resourceAttributes = %v, want the namespace list review", attributes)
	}
}

// TestReviewGrantsProtobufOnEchoedNamespaceList checks that the protobuf
// review path is still granted, with the echoed spec present in the answer.
func TestReviewGrantsProtobufOnEchoedNamespaceList(t *testing.T) {
	t.Parallel()
	h := newHarness(t, reviewUpstreamEcho("prod"))

	header := http.Header{
		"Content-Type": []string{protobufContentType},
		"Accept":       []string{protobufContentType + ", */*"},
	}
	request := protobufReview(protoAttributes{verb: "list", resource: "namespaces"})
	resp, body := h.postReview(t, request, header)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var object map[string]any
	if err := json.Unmarshal(body, &object); err != nil {
		t.Fatalf("parse the response %q: %v", body, err)
	}
	status, ok := object["status"].(map[string]any)
	if !ok {
		t.Fatalf("the response has no status object: %q", body)
	}
	if status["allowed"] != true {
		t.Errorf("allowed = %v, want true", status["allowed"])
	}
	spec, ok := object["spec"].(map[string]any)
	if !ok {
		t.Fatalf("the response has no spec object: %q", body)
	}
	if _, ok := spec["resourceAttributes"].(map[string]any); !ok {
		t.Errorf("the response has no resourceAttributes: %q", body)
	}
}

func TestCollectionList(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		spec reviewSpec
		want bool
	}{
		{
			name: "wildcard resource",
			spec: reviewSpec{resource: &resourceAttributes{Verb: "list", Resource: "*"}},
			want: false,
		},
		{
			name: "wildcard group",
			spec: reviewSpec{resource: &resourceAttributes{Verb: "list", Resource: "pods", Group: "*"}},
			want: false,
		},
		{
			name: "plain namespaced resource",
			spec: reviewSpec{resource: &resourceAttributes{Verb: "list", Resource: "pods"}},
			want: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := collectionList(test.spec); got != test.want {
				t.Errorf("collectionList(%+v) = %v, want %v", test.spec, got, test.want)
			}
		})
	}
}
