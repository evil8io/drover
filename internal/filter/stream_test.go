package filter

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type readResult struct {
	line string
	err  error
}

func readLine(reader *bufio.Reader) <-chan readResult {
	out := make(chan readResult, 1)
	go func() {
		line, err := reader.ReadString('\n')
		out <- readResult{line: line, err: err}
	}()
	return out
}

func TestListWatchStreams(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	defer releaseOnce()

	const (
		added    = `{"type":"ADDED","object":{"metadata":{"name":"a"}}}` + "\n"
		modified = `{"type":"MODIFIED","object":{"metadata":{"name":"a"}}}` + "\n"
	)

	h := newHarness(t, listUpstream(steveHandler("a"), func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("the upstream response writer has no flusher")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, added)
		flusher.Flush()
		<-release
		_, _ = io.WriteString(w, modified)
		flusher.Flush()
	}))

	resp, err := h.proxy.Client().Do(h.request(t, http.MethodGet, listPath+"?watch=true", nil, callerHeader()))
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	reader := bufio.NewReader(resp.Body)
	select {
	case got := <-readLine(reader):
		if got.err != nil {
			t.Fatalf("read the first event: %v", got.err)
		}
		if got.line != added {
			t.Fatalf("first event = %q, want the ADDED event", got.line)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the first event did not arrive before the second write")
	}

	releaseOnce()

	select {
	case got := <-readLine(reader):
		if got.err != nil {
			t.Fatalf("read the second event: %v", got.err)
		}
		if got.line != modified {
			t.Errorf("second event = %q, want the MODIFIED event", got.line)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the second event did not arrive")
	}

	privileged := h.upstream.all()[3]
	if got := privileged.query.Get("watch"); got != "true" {
		t.Errorf("privileged watch = %q, want true", got)
	}
	if got := privileged.query.Get("labelSelector"); got != "" {
		t.Errorf("privileged labelSelector = %q, want no selector", got)
	}
	if got := privileged.header.Get("Accept"); got != "application/json" {
		t.Errorf("privileged Accept = %q, want application/json", got)
	}
}

func TestWatchDropsDisallowedNamespace(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler("a"), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"type":"ADDED","object":{"metadata":{"name":"a"}}}`+"\n")
		_, _ = io.WriteString(w, `{"type":"ADDED","object":{"metadata":{"name":"z"}}}`+"\n")
	}))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	want := `{"type":"ADDED","object":{"metadata":{"name":"a"}}}` + "\n"
	if string(body) != want {
		t.Errorf("body = %q, want %q", body, want)
	}
}

func TestWatchAllowsProjectMember(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstreamWithProjects(steveHandler("a"), projectsHandler("p-1"), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"type":"ADDED","object":{"metadata":{"name":"z","labels":{"field.cattle.io/projectId":"p-1"}}}}`+"\n")
	}))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	want := `{"type":"ADDED","object":{"metadata":{"name":"z","labels":{"field.cattle.io/projectId":"p-1"}}}}` + "\n"
	if string(body) != want {
		t.Errorf("body = %q, want %q", body, want)
	}
}

func TestWatchPassesBookmark(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler(), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"type":"BOOKMARK","object":{"metadata":{"resourceVersion":"99"}}}`+"\n")
	}))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	want := `{"type":"BOOKMARK","object":{"metadata":{"resourceVersion":"99"}}}` + "\n"
	if string(body) != want {
		t.Errorf("body = %q, want %q", body, want)
	}
}

func TestListWatchWebsocketUpgrade(t *testing.T) {
	t.Parallel()
	echoed := make(chan struct{})
	defer close(echoed)

	h := newHarness(t, listUpstream(steveHandler("a"), func(w http.ResponseWriter, r *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Error("the upstream response writer has no hijacker")
			return
		}
		conn, buffered, err := hijacker.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = buffered.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		if err := buffered.Flush(); err != nil {
			t.Errorf("write the upgrade response: %v", err)
			return
		}
		line, err := buffered.ReadString('\n')
		if err != nil {
			t.Errorf("read from the client: %v", err)
			return
		}
		_, _ = buffered.WriteString(line)
		if err := buffered.Flush(); err != nil {
			t.Errorf("write the echo: %v", err)
			return
		}
		<-echoed
	}))

	conn, err := net.Dial("tcp", strings.TrimPrefix(h.proxy.URL, "http://"))
	if err != nil {
		t.Fatalf("dial the proxy: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set the deadline: %v", err)
	}

	req := h.request(t, http.MethodGet, listPath+"?watch=true", nil, http.Header{
		"Authorization":         []string{callerToken},
		"Connection":            []string{"Upgrade"},
		"Upgrade":               []string{"websocket"},
		"Sec-WebSocket-Key":     []string{"dGhlIHNhbXBsZSBub25jZQ=="},
		"Sec-WebSocket-Version": []string{"13"},
	})
	if err := req.Write(conn); err != nil {
		t.Fatalf("write the request: %v", err)
	}

	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		t.Fatalf("read the response: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	if got := resp.Header.Get("Upgrade"); !strings.EqualFold(got, "websocket") {
		t.Errorf("Upgrade = %q, want websocket", got)
	}

	if _, err := conn.Write([]byte("ping\n")); err != nil {
		t.Fatalf("write to the upgraded connection: %v", err)
	}
	echo, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read the echo: %v", err)
	}
	if echo != "ping\n" {
		t.Errorf("echo = %q, want %q", echo, "ping\n")
	}

	requests := h.upstream.all()
	if len(requests) != 4 {
		t.Fatalf("upstream requests = %d, want 4", len(requests))
	}
	privileged := requests[3]
	if got := privileged.header.Get("Authorization"); got != serviceAuth {
		t.Errorf("privileged Authorization = %q, want %q", got, serviceAuth)
	}
	if got := privileged.query.Get("labelSelector"); got != "" {
		t.Errorf("privileged labelSelector = %q, want no selector", got)
	}
	if got := privileged.header.Get("Upgrade"); got != "websocket" {
		t.Errorf("privileged Upgrade = %q, want websocket", got)
	}
	if got := privileged.header.Get("Connection"); !strings.EqualFold(got, "Upgrade") {
		t.Errorf("privileged Connection = %q, want Upgrade", got)
	}
}
