package filter

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"
)

func allowAll(string, map[string]string) bool { return true }

func TestReadyzBeforeAndAfterDrain(t *testing.T) {
	t.Parallel()
	h := newHarness(t, namespaceListHandler)

	resp, body := h.do(t, h.request(t, http.MethodGet, "/readyz", nil, nil))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != "ok" {
		t.Errorf("body = %q, want %q", body, "ok")
	}

	if ended := h.svc.StartDrain(); ended != 0 {
		t.Errorf("StartDrain = %d, want 0", ended)
	}

	resp, body = h.do(t, h.request(t, http.MethodGet, "/readyz", nil, nil))
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if string(body) != "draining" {
		t.Errorf("body = %q, want %q", body, "draining")
	}
}

func TestHealthzIgnoresDrain(t *testing.T) {
	t.Parallel()
	h := newHarness(t, namespaceListHandler)
	h.svc.StartDrain()

	resp, body := h.do(t, h.request(t, http.MethodGet, "/healthz", nil, nil))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != "ok" {
		t.Errorf("body = %q, want %q", body, "ok")
	}
}

// closeSignal wraps a ReadCloser, and closes ch once Close runs.
type closeSignal struct {
	io.ReadCloser
	ch chan struct{}
}

func (c closeSignal) Close() error {
	err := c.ReadCloser.Close()
	close(c.ch)
	return err
}

// TestDrainEndsWatchWithEOF checks that a reader gets io.EOF, not another
// error, after a drain.
func TestDrainEndsWatchWithEOF(t *testing.T) {
	t.Parallel()
	upstreamReader, upstreamWriter := io.Pipe()
	goroutineEnded := make(chan struct{})
	upstream := closeSignal{ReadCloser: upstreamReader, ch: goroutineEnded}

	registry := newWatchRegistry()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reader := filterWatchBody(context.Background(), upstream, allowAll, logger, registry)

	if got := registry.len(); got != 1 {
		t.Fatalf("registry length = %d, want 1", got)
	}

	if got := registry.closeAll(); got != 1 {
		t.Errorf("closeAll = %d, want 1", got)
	}

	if _, err := reader.Read(make([]byte, 1)); err != io.EOF {
		t.Errorf("read error = %v, want io.EOF", err)
	}

	// Unblock the goroutine's read of upstream, so it can remove itself.
	_ = upstreamWriter.Close()
	select {
	case <-goroutineEnded:
	case <-time.After(5 * time.Second):
		t.Fatal("the goroutine did not end after the drain")
	}

	if got := registry.len(); got != 0 {
		t.Errorf("registry length after the goroutine ends = %d, want 0", got)
	}
}
