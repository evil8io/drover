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

func (h *harness) postReview(t *testing.T, body []byte, header http.Header) (*http.Response, []byte) {
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
	return h.do(t, h.request(t, http.MethodPost, reviewPath, bytes.NewReader(body), all))
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
			h := newHarness(t, reviewUpstream(deniedAnswer))

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

			sent := h.upstream.all()
			if len(sent) != 1 {
				t.Fatalf("upstream requests = %d, want 1", len(sent))
			}
			if string(sent[0].body) != request {
				t.Errorf("upstream body = %q, want %q", sent[0].body, request)
			}
			if got := sent[0].header.Get("Accept-Encoding"); got != "" {
				t.Errorf("upstream Accept-Encoding = %q, want no header", got)
			}
		})
	}
}

func TestReviewKeepsAllowed(t *testing.T) {
	t.Parallel()
	h := newHarness(t, reviewUpstream(allowedAnswer))

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
			h := newHarness(t, reviewUpstream(deniedAnswer))

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

			sent := h.upstream.all()
			if len(sent) != 1 {
				t.Fatalf("upstream requests = %d, want 1", len(sent))
			}
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
			h := newHarness(t, reviewUpstream(deniedAnswer))

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
