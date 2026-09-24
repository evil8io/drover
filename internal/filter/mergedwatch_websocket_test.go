package filter

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

const (
	// handshakeKey and handshakeAccept are the handshake example of RFC 6455,
	// section 1.3. The accept value is the base64 text of the SHA-1 of the key
	// and the constant of the protocol.
	handshakeKey    = "dGhlIHNhbXBsZSBub25jZQ=="
	handshakeAccept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
)

// dialMergedWatch opens a cluster-wide watch over a raw connection, with the
// websocket handshake of the caller. It returns the answer of the filter, a
// reader of the frames that follow it, and the connection, so a test writes
// the frames of the client on it.
func dialMergedWatch(t *testing.T, h *harness, target, offer string) (*http.Response, *bufio.Reader, net.Conn) {
	t.Helper()
	conn, err := net.Dial("tcp", strings.TrimPrefix(h.proxy.URL, "http://"))
	if err != nil {
		t.Fatalf("dial the proxy: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatalf("set the deadline: %v", err)
	}

	header := http.Header{
		"Authorization":            []string{callerToken},
		"Connection":               []string{"Upgrade"},
		"Upgrade":                  []string{"websocket"},
		"Sec-WebSocket-Key":        []string{handshakeKey},
		"Sec-WebSocket-Version":    []string{"13"},
		"Sec-WebSocket-Extensions": []string{"permessage-deflate"},
	}
	if offer != "" {
		header.Set("Sec-WebSocket-Protocol", offer)
	}

	req := h.request(t, http.MethodGet, target, nil, header)
	if err := req.Write(conn); err != nil {
		t.Fatalf("write the request: %v", err)
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		t.Fatalf("read the response: %v", err)
	}
	return resp, reader, conn
}

// nextFrame returns the next frame that reaches the client.
func nextFrame(t *testing.T, reader *bufio.Reader) wsTestFrame {
	t.Helper()
	frame, err := readWSFrame(reader)
	if err != nil {
		t.Fatalf("read a frame: %v", err)
	}
	return frame
}

// frameEvent checks that frame is one final text frame, and returns its
// payload, the event as plain JSON.
func frameEvent(t *testing.T, frame wsTestFrame) string {
	t.Helper()
	if !frame.fin {
		t.Fatalf("frame = %+v, want a final frame", frame)
	}
	if frame.opcode != opcodeText {
		t.Fatalf("opcode = %#x, want the text opcode %#x", frame.opcode, opcodeText)
	}
	return string(frame.payload)
}

// upgradedMergeHarness answers the watch of one namespace with one event, and
// it keeps that upstream watch open until release closes.
func upgradedMergeHarness(t *testing.T, release <-chan struct{}) *harness {
	t.Helper()
	return newHarnessOpt(t, collectionUpstream(
		steveHandler("a"),
		namespaceWatches(release, map[string][]string{"a": {podEvent("a")}}),
	), withFanout)
}

// TestMergedWatchUpgradeSwitchesProtocols checks the handshake that the merge
// answers itself, and that every subprotocol gets a text frame with plain
// JSON, as the API server sends it. The merge has no single upstream
// connection, so it has no 101 answer to relay.
func TestMergedWatchUpgradeSwitchesProtocols(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		offer    string
		protocol string
	}{
		{"binary", binarySubprotocol, binarySubprotocol},
		{"base64", base64Subprotocol, base64Subprotocol},
		{"binary first of the offer", binarySubprotocol + ", " + base64Subprotocol, binarySubprotocol},
		{"base64 first of the offer", base64Subprotocol + ", " + binarySubprotocol, base64Subprotocol},
		{"an offer that the merge does not name", "v4.channel.k8s.io", ""},
		{"no offer", "", ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			release := make(chan struct{})
			defer close(release)

			h := upgradedMergeHarness(t, release)
			resp, reader, _ := dialMergedWatch(t, h, podsPath+"?watch=true", test.offer)
			if resp.StatusCode != http.StatusSwitchingProtocols {
				t.Fatalf("status = %d, want 101", resp.StatusCode)
			}
			if got := resp.Header.Get("Upgrade"); !strings.EqualFold(got, "websocket") {
				t.Errorf("Upgrade = %q, want websocket", got)
			}
			if got := resp.Header.Get("Connection"); !strings.EqualFold(got, "Upgrade") {
				t.Errorf("Connection = %q, want Upgrade", got)
			}
			if got := resp.Header.Get("Sec-Websocket-Accept"); got != handshakeAccept {
				t.Errorf("Sec-Websocket-Accept = %q, want %q", got, handshakeAccept)
			}
			if got := resp.Header.Get("Sec-Websocket-Protocol"); got != test.protocol {
				t.Errorf("Sec-Websocket-Protocol = %q, want %q", got, test.protocol)
			}
			if got := resp.Header.Get(extensionsHeader); got != "" {
				t.Errorf("%s = %q, want no extension", extensionsHeader, got)
			}

			if got := frameEvent(t, nextFrame(t, reader)); got != podEvent("a") {
				t.Errorf("event = %q, want %q", got, podEvent("a"))
			}
		})
	}
}

// TestMergedWatchUpgradeDropsTheHandshakeHeaders checks that each upstream
// watch takes the chunked transport, and that it keeps the query of the
// caller.
func TestMergedWatchUpgradeDropsTheHandshakeHeaders(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	h := upgradedMergeHarness(t, release)
	resp, reader, _ := dialMergedWatch(t, h, podsPath+"?watch=true&resourceVersion=42", binarySubprotocol)
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	if got := frameEvent(t, nextFrame(t, reader)); got != podEvent("a") {
		t.Fatalf("event = %q, want %q", got, podEvent("a"))
	}

	records := namespacedRecords(h.upstream)
	if len(records) != 1 {
		t.Fatalf("namespaced requests = %d, want 1", len(records))
	}
	request := records[0]
	for _, name := range []string{
		"Connection",
		"Upgrade",
		"Sec-Websocket-Key",
		"Sec-Websocket-Version",
		"Sec-Websocket-Protocol",
		extensionsHeader,
	} {
		if got := request.header.Get(name); got != "" {
			t.Errorf("%s = %q, want no handshake header on the upstream watch", name, got)
		}
	}
	if got := request.query.Get("watch"); got != "true" {
		t.Errorf("watch = %q, want true", got)
	}
	if got := request.query.Get("resourceVersion"); got != "42" {
		t.Errorf("resourceVersion = %q, want 42", got)
	}
}

// TestMergedWatchUpgradeEndsOnClientCloseFrame checks that a close frame of
// the client ends the merged stream, and that the client reads a close frame
// before the end.
func TestMergedWatchUpgradeEndsOnClientCloseFrame(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	h := upgradedMergeHarness(t, release)
	resp, reader, conn := dialMergedWatch(t, h, podsPath+"?watch=true", binarySubprotocol)
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	if got := frameEvent(t, nextFrame(t, reader)); got != podEvent("a") {
		t.Fatalf("event = %q, want %q", got, podEvent("a"))
	}

	if _, err := conn.Write(wsMaskedFrame(opcodeClose, [4]byte{1, 2, 3, 4}, []byte{0x03, 0xe8})); err != nil {
		t.Fatalf("write the close frame: %v", err)
	}

	frame := nextFrame(t, reader)
	if frame.opcode != opcodeClose || !bytes.Equal(frame.payload, []byte{0x03, 0xe8}) {
		t.Errorf("frame = %+v, want the close frame with status 1000", frame)
	}
	if _, err := reader.ReadByte(); !errors.Is(err, io.EOF) {
		t.Errorf("read after the close frame = %v, want io.EOF", err)
	}
}

// TestMergedWatchUpgradeAnswersPingWithPong checks that the merge answers a
// ping of the client itself. It has no upstream connection that answers it.
func TestMergedWatchUpgradeAnswersPingWithPong(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	h := upgradedMergeHarness(t, release)
	resp, reader, conn := dialMergedWatch(t, h, podsPath+"?watch=true", binarySubprotocol)
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	if got := frameEvent(t, nextFrame(t, reader)); got != podEvent("a") {
		t.Fatalf("event = %q, want %q", got, podEvent("a"))
	}

	if _, err := conn.Write(wsMaskedFrame(opcodePing, [4]byte{5, 6, 7, 8}, []byte("keepalive"))); err != nil {
		t.Fatalf("write the ping frame: %v", err)
	}

	frame := nextFrame(t, reader)
	if frame.opcode != opcodePong || string(frame.payload) != "keepalive" {
		t.Errorf("frame = %+v, want the pong frame with the payload of the ping", frame)
	}
}

// TestMaxWatchesPerCallerCapsAnUpgradedMergedWatch checks that an upgraded
// merged watch holds a slot of its caller while it runs, and that the end of
// the stream frees the slot.
func TestMaxWatchesPerCallerCapsAnUpgradedMergedWatch(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a"),
		namespaceWatches(release, map[string][]string{"a": {podEvent("a")}}),
	), withFanout, func(cfg *Config) { cfg.MaxWatchesPerCaller = 1 })

	resp, reader, conn := dialMergedWatch(t, h, podsPath+"?watch=true", binarySubprotocol)
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	if got := frameEvent(t, nextFrame(t, reader)); got != podEvent("a") {
		t.Fatalf("event = %q, want %q", got, podEvent("a"))
	}

	capped, _, _ := dialMergedWatch(t, h, podsPath+"?watch=true", binarySubprotocol)
	body, err := io.ReadAll(capped.Body)
	if err != nil {
		t.Fatalf("read the capped answer: %v", err)
	}
	wantWatchCapped(t, capped, body, "per-caller limit of 1")

	if _, err := conn.Write(wsMaskedFrame(opcodeClose, [4]byte{1, 2, 3, 4}, []byte{0x03, 0xe8})); err != nil {
		t.Fatalf("write the close frame: %v", err)
	}
	if !waitFor(func() bool { return h.svc.watches.reservedCount() == 0 }) {
		t.Fatalf("reserved slots after the end of the stream = %d, want 0", h.svc.watches.reservedCount())
	}
	again, _, _ := dialMergedWatch(t, h, podsPath+"?watch=true", binarySubprotocol)
	if again.StatusCode != http.StatusSwitchingProtocols {
		t.Errorf("status after the end of the stream = %d, want 101", again.StatusCode)
	}
}

// TestMergedWatchUpgradeAddsAGainedNamespace checks that a namespace that the
// caller gains joins a running upgraded merge, with the ADDED event of its
// existing object and then its live event, each as one text frame, and that
// the stream stays open.
func TestMergedWatchUpgradeAddsAGainedNamespace(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)
	live := make(chan struct{})

	namespaces := newNamespaceSet(steveNamespace{name: "a"})
	h := newHarnessOpt(t, collectionUpstream(
		namespaces.handler(), gainedNamespaceWatches(release, live),
	), withFanout, shortTTL)

	resp, reader, conn := dialMergedWatch(t, h, podsPath+"?watch=true&resourceVersion=42", binarySubprotocol)
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	if got := frameEvent(t, nextFrame(t, reader)); got != podEvent("a") {
		t.Fatalf("event = %q, want %q", got, podEvent("a"))
	}

	namespaces.set(steveNamespace{name: "a"}, steveNamespace{name: "b"})
	h.clock.advance(time.Minute)
	if got := frameEvent(t, nextFrame(t, reader)); got != podEvent("b") {
		t.Fatalf("event = %q, want the ADDED event %q of the gained namespace", got, podEvent("b"))
	}
	close(live)
	if got := frameEvent(t, nextFrame(t, reader)); got != modifiedEvent("b") {
		t.Fatalf("event = %q, want the live event %q", got, modifiedEvent("b"))
	}
	if records := namespaceRecords(h.upstream, "b"); len(records) != 1 || records[0].query.Has("resourceVersion") {
		t.Errorf("watch requests of namespace b = %+v, want one without a resourceVersion", records)
	}

	before := h.upstream.countPath(stevePath)
	h.clock.advance(time.Minute)
	if !waitFor(func() bool { return h.upstream.countPath(stevePath) > before }) {
		t.Fatal("the ticker did not re-read the allowed set of the caller")
	}
	if err := conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatalf("set the read deadline: %v", err)
	}
	var timeout net.Error
	if _, err := reader.ReadByte(); !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Errorf("read after the tick = %v, want a timeout of an open stream", err)
	}
}
