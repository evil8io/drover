package filter

import (
	"bufio"
	"bytes"
	"encoding/base64"
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

// frameEvent returns the event of one message frame, in the encoding that the
// opcode names. A text frame has the base64 text of the event.
func frameEvent(t *testing.T, frame wsTestFrame, opcode byte) string {
	t.Helper()
	if !frame.fin {
		t.Fatalf("frame = %+v, want a final frame", frame)
	}
	if frame.opcode != opcode {
		t.Fatalf("opcode = %#x, want %#x", frame.opcode, opcode)
	}
	if opcode != opcodeText {
		return string(frame.payload)
	}
	decoded, err := base64.StdEncoding.DecodeString(string(frame.payload))
	if err != nil {
		t.Fatalf("decode the message %q: %v", frame.payload, err)
	}
	return string(decoded)
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
// answers itself, and the frame encoding of each subprotocol. The merge has
// no single upstream connection, so it has no 101 answer to relay.
func TestMergedWatchUpgradeSwitchesProtocols(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		offer    string
		protocol string
		opcode   byte
	}{
		{"binary", binarySubprotocol, binarySubprotocol, opcodeBinary},
		{"base64", base64Subprotocol, base64Subprotocol, opcodeText},
		{"binary first of the offer", binarySubprotocol + ", " + base64Subprotocol, binarySubprotocol, opcodeBinary},
		{"base64 first of the offer", base64Subprotocol + ", " + binarySubprotocol, base64Subprotocol, opcodeText},
		{"an offer that the merge does not encode", "v4.channel.k8s.io", "", opcodeBinary},
		{"no offer", "", "", opcodeBinary},
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

			if got := frameEvent(t, nextFrame(t, reader), test.opcode); got != podEvent("a") {
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
	if got := frameEvent(t, nextFrame(t, reader), opcodeBinary); got != podEvent("a") {
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
	if got := frameEvent(t, nextFrame(t, reader), opcodeBinary); got != podEvent("a") {
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
	if got := frameEvent(t, nextFrame(t, reader), opcodeBinary); got != podEvent("a") {
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
