package filter

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestListWithoutTokenFile(t *testing.T) {
	t.Parallel()
	h := newHarnessWithoutToken(t, listUpstream(steveHandler("a"), namespaceListHandler))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}

	var status struct {
		Kind    string `json:"kind"`
		Reason  string `json:"reason"`
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatalf("parse status body %q: %v", body, err)
	}
	if status.Kind != "Status" {
		t.Errorf("kind = %q, want Status", status.Kind)
	}
	if status.Code != http.StatusBadGateway {
		t.Errorf("code = %d, want 502", status.Code)
	}
	want := "drover: the token file is not available yet"
	if status.Message != want {
		t.Errorf("message = %q, want %q", status.Message, want)
	}

	if strings.Contains(h.logs.String(), "level=ERROR") {
		t.Errorf("logs contain an ERROR line, want WARN: %s", h.logs.String())
	}
	if !strings.Contains(h.logs.String(), "level=WARN") {
		t.Errorf("logs have no WARN line: %s", h.logs.String())
	}
}

func TestListRecoversWhenTokenFileAppears(t *testing.T) {
	t.Parallel()
	h := newHarnessWithoutToken(t, listUpstream(steveHandler("a"), namespaceListHandler))

	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("first status = %d, want 502", resp.StatusCode)
	}

	writeToken(t, h.tokenFile, "service")

	resp, _ = h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("second status = %d, want 200", resp.StatusCode)
	}
}

func TestListWithoutTokenFileNative200(t *testing.T) {
	t.Parallel()
	h := newHarnessWithoutToken(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == stevePath {
			t.Error("the upstream got an allowed set request")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"kind":"NamespaceList","items":[{"metadata":{"name":"a"}}]}`)
	})

	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestPassThroughWithoutTokenFile(t *testing.T) {
	t.Parallel()
	h := newHarnessWithoutToken(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	resp, _ := h.do(t, h.request(t, http.MethodGet, "/v3/settings", nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHealthzWithoutTokenFile(t *testing.T) {
	t.Parallel()
	h := newHarnessWithoutToken(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("the upstream got the health request")
	})

	resp, body := h.do(t, h.request(t, http.MethodGet, "/healthz", nil, nil))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != "ok" {
		t.Errorf("body = %q, want %q", body, "ok")
	}
}
