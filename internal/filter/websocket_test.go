package filter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	opcodeContinuation = 0x0
	opcodeText         = 0x1
	opcodeBinary       = 0x2
	opcodePing         = 0x9
)

// wsFrame builds one RFC 6455 frame with no mask, like a frame of a server.
func wsFrame(fin bool, opcode byte, payload []byte) []byte {
	first := opcode
	if fin {
		first |= 0x80
	}
	frame := []byte{first}
	switch {
	case len(payload) < 126:
		frame = append(frame, byte(len(payload)))
	case len(payload) <= 0xffff:
		frame = append(frame, 126, byte(len(payload)>>8), byte(len(payload)))
	default:
		extended := make([]byte, 8)
		binary.BigEndian.PutUint64(extended, uint64(len(payload)))
		frame = append(append(frame, 127), extended...)
	}
	return append(frame, payload...)
}

// wsMaskedFrame builds one frame with the mask bit and the mask key. A client
// sends this form, and a server does not.
func wsMaskedFrame(opcode byte, mask [4]byte, payload []byte) []byte {
	masked := make([]byte, len(payload))
	for i := range payload {
		masked[i] = payload[i] ^ mask[i%4]
	}
	frame := wsFrame(true, opcode, masked)
	header := len(frame) - len(payload)
	frame[1] |= 0x80
	return append(append(frame[:header:header], mask[:]...), masked...)
}

// wsTestFrame is one unmasked frame that a test reads from the filter.
type wsTestFrame struct {
	fin     bool
	opcode  byte
	payload []byte
}

func readWSFrame(r *bufio.Reader) (wsTestFrame, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(r, header); err != nil {
		return wsTestFrame{}, err
	}
	frame := wsTestFrame{fin: header[0]&0x80 != 0, opcode: header[0] & 0x0f}
	length := uint64(header[1] & 0x7f)
	switch length {
	case 126:
		extended := make([]byte, 2)
		if _, err := io.ReadFull(r, extended); err != nil {
			return wsTestFrame{}, err
		}
		length = uint64(binary.BigEndian.Uint16(extended))
	case 127:
		extended := make([]byte, 8)
		if _, err := io.ReadFull(r, extended); err != nil {
			return wsTestFrame{}, err
		}
		length = binary.BigEndian.Uint64(extended)
	}
	frame.payload = make([]byte, length)
	if _, err := io.ReadFull(r, frame.payload); err != nil {
		return wsTestFrame{}, err
	}
	return frame, nil
}

// addedEvent returns the ADDED watch event of the namespace.
func addedEvent(name string) []byte {
	return []byte(`{"type":"ADDED","object":{"metadata":{"name":"` + name + `"}}}`)
}

// allowNames returns an allow function that accepts the given names only.
func allowNames(names ...string) func(string, map[string]string) bool {
	return func(name string, _ map[string]string) bool {
		for _, allowed := range names {
			if name == allowed {
				return true
			}
		}
		return false
	}
}

// recordingMetrics returns the filter metrics on a manual reader, so a test
// can assert on a counter.
func recordingMetrics(t *testing.T) (*metrics, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	m, err := newMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	return m, reader
}

// fakeUpgrade is the body of an upgraded response in a test. Read returns the
// bytes of the source, Write collects the bytes of the caller, and Close
// closes done once.
type fakeUpgrade struct {
	source io.Reader
	once   sync.Once
	done   chan struct{}

	mu     sync.Mutex
	writes bytes.Buffer
}

func (c *fakeUpgrade) Read(p []byte) (int, error) { return c.source.Read(p) }

func (c *fakeUpgrade) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes.Write(p)
}

func (c *fakeUpgrade) Close() error {
	c.once.Do(func() { close(c.done) })
	return nil
}

func (c *fakeUpgrade) written() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes.String()
}

// upgradeHarness runs filterWatchUpgrade over the frames of source.
type upgradeHarness struct {
	body     io.ReadWriteCloser
	reader   *bufio.Reader
	upstream *fakeUpgrade
	registry *watchRegistry
}

func newUpgradeHarness(t *testing.T, source io.Reader, allow func(string, map[string]string) bool, m *metrics, subprotocol string) *upgradeHarness {
	t.Helper()
	upstream := &fakeUpgrade{source: source, done: make(chan struct{})}
	resp := &http.Response{
		StatusCode: http.StatusSwitchingProtocols,
		Header:     http.Header{},
		Body:       upstream,
	}
	if subprotocol != "" {
		resp.Header.Set("Sec-WebSocket-Protocol", subprotocol)
	}
	registry := newWatchRegistry()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	filterWatchUpgrade(context.Background(), resp, allow, logger, registry, m, "c-1")

	body, ok := resp.Body.(io.ReadWriteCloser)
	if !ok {
		t.Fatalf("the filtered body has type %T, want an io.ReadWriteCloser", resp.Body)
	}
	t.Cleanup(func() { _ = body.Close() })
	return &upgradeHarness{body: body, reader: bufio.NewReader(body), upstream: upstream, registry: registry}
}

// next reads the next frame that reaches the caller.
func (h *upgradeHarness) next(t *testing.T) wsTestFrame {
	t.Helper()
	frame, err := readWSFrame(h.reader)
	if err != nil {
		t.Fatalf("read a frame: %v", err)
	}
	return frame
}

// wantEnd checks that no further frame reaches the caller.
func (h *upgradeHarness) wantEnd(t *testing.T) {
	t.Helper()
	if _, err := h.reader.ReadByte(); err != io.EOF {
		t.Fatalf("read after the last frame = %v, want io.EOF", err)
	}
}

func TestUpgradeForwardsAllowedFrame(t *testing.T) {
	t.Parallel()
	event := addedEvent("a")
	h := newUpgradeHarness(t, bytes.NewReader(wsFrame(true, opcodeBinary, event)), allowNames("a"), testMetrics(t), "")

	frame := h.next(t)
	if frame.opcode != opcodeBinary || !frame.fin {
		t.Errorf("opcode = %#x, fin = %v, want %#x and true", frame.opcode, frame.fin, opcodeBinary)
	}
	if string(frame.payload) != string(event) {
		t.Errorf("payload = %q, want %q", frame.payload, event)
	}
	h.wantEnd(t)
}

func TestUpgradeDropsDeniedFrame(t *testing.T) {
	t.Parallel()
	allowed := addedEvent("a")
	source := bytes.NewReader(append(
		wsFrame(true, opcodeBinary, addedEvent("z")),
		wsFrame(true, opcodeBinary, allowed)...))
	h := newUpgradeHarness(t, source, allowNames("a"), testMetrics(t), "")

	if got := string(h.next(t).payload); got != string(allowed) {
		t.Errorf("payload = %q, want the allowed event %q", got, allowed)
	}
	h.wantEnd(t)
}

// TestUpgradeUnmasksFrame checks that the filter reads the payload of a
// masked frame, and that it forwards the frame bytes as they arrived.
func TestUpgradeUnmasksFrame(t *testing.T) {
	t.Parallel()
	frame := wsMaskedFrame(opcodeBinary, [4]byte{0x11, 0x22, 0x33, 0x44}, addedEvent("a"))
	h := newUpgradeHarness(t, bytes.NewReader(frame), allowNames("a"), testMetrics(t), "")

	got := make([]byte, len(frame))
	if _, err := io.ReadFull(h.reader, got); err != nil {
		t.Fatalf("read the frame: %v", err)
	}
	if !bytes.Equal(got, frame) {
		t.Errorf("frame = %x, want the original bytes %x", got, frame)
	}
	h.wantEnd(t)
}

func TestUpgradeDecodesBase64Subprotocol(t *testing.T) {
	t.Parallel()
	allowed := []byte(base64.StdEncoding.EncodeToString(addedEvent("a")))
	denied := []byte(base64.StdEncoding.EncodeToString(addedEvent("z")))
	source := bytes.NewReader(append(
		wsFrame(true, opcodeText, denied),
		wsFrame(true, opcodeText, allowed)...))
	h := newUpgradeHarness(t, source, allowNames("a"), testMetrics(t), base64Subprotocol)

	if got := string(h.next(t).payload); got != string(allowed) {
		t.Errorf("payload = %q, want the base64 bytes of the allowed event %q", got, allowed)
	}
	h.wantEnd(t)
}

// TestUpgradeAssemblesContinuationFrame checks that the filter decides on the
// whole message, and that it forwards every frame of a message that passes.
func TestUpgradeAssemblesContinuationFrame(t *testing.T) {
	t.Parallel()
	denied, allowed := addedEvent("z"), addedEvent("a")
	var source []byte
	for _, event := range [][]byte{denied, allowed} {
		source = append(source, wsFrame(false, opcodeText, event[:10])...)
		source = append(source, wsFrame(true, opcodeContinuation, event[10:])...)
	}
	h := newUpgradeHarness(t, bytes.NewReader(source), allowNames("a"), testMetrics(t), "")

	first := h.next(t)
	if first.fin || first.opcode != opcodeText || string(first.payload) != string(allowed[:10]) {
		t.Errorf("first frame = %+v, want the head of the allowed event", first)
	}
	second := h.next(t)
	if !second.fin || second.opcode != opcodeContinuation || string(second.payload) != string(allowed[10:]) {
		t.Errorf("second frame = %+v, want the tail of the allowed event", second)
	}
	h.wantEnd(t)
}

// TestUpgradeForwardsPingWhileBuffering checks that a ping frame reaches the
// caller while the filter still assembles a message.
func TestUpgradeForwardsPingWhileBuffering(t *testing.T) {
	t.Parallel()
	denied := addedEvent("z")
	source := wsFrame(false, opcodeText, denied[:10])
	source = append(source, wsFrame(true, opcodePing, []byte("keepalive"))...)
	source = append(source, wsFrame(true, opcodeContinuation, denied[10:])...)
	h := newUpgradeHarness(t, bytes.NewReader(source), allowNames("a"), testMetrics(t), "")

	frame := h.next(t)
	if frame.opcode != opcodePing || string(frame.payload) != "keepalive" {
		t.Errorf("frame = %+v, want the ping frame", frame)
	}
	h.wantEnd(t)
}

// TestUpgradeForwardsMessageAboveTheCap checks that a message above 1 MiB
// reaches the caller without a decision, and that the filter counts it.
func TestUpgradeForwardsMessageAboveTheCap(t *testing.T) {
	t.Parallel()
	event := []byte(`{"type":"ADDED","object":{"metadata":{"name":"z","annotations":{"pad":"` +
		strings.Repeat("x", maxMessage) + `"}}}}`)
	m, reader := recordingMetrics(t)
	h := newUpgradeHarness(t, bytes.NewReader(wsFrame(true, opcodeText, event)), allowNames("a"), m, "")

	frame := h.next(t)
	if !bytes.Equal(frame.payload, event) {
		t.Errorf("payload has %d bytes, want the %d bytes of the message", len(frame.payload), len(event))
	}
	h.wantEnd(t)

	wantUnfiltered(t, reader, 1)
}

// TestUpgradeForwardsUnparsedMessage checks that a message that is no watch
// event reaches the caller, and that the filter counts it.
func TestUpgradeForwardsUnparsedMessage(t *testing.T) {
	t.Parallel()
	m, reader := recordingMetrics(t)
	h := newUpgradeHarness(t, bytes.NewReader(wsFrame(true, opcodeText, []byte("not json"))), allowNames("a"), m, "")

	if got := string(h.next(t).payload); got != "not json" {
		t.Errorf("payload = %q, want the unchanged message", got)
	}
	h.wantEnd(t)

	wantUnfiltered(t, reader, 1)
}

// TestUpgradeEndsOnCloseFrame checks that the close frame reaches the caller,
// and that the stream ends there.
func TestUpgradeEndsOnCloseFrame(t *testing.T) {
	t.Parallel()
	source := append(wsFrame(true, opcodeClose, []byte{0x03, 0xe8}), wsFrame(true, opcodeText, addedEvent("a"))...)
	h := newUpgradeHarness(t, bytes.NewReader(source), allowNames("a"), testMetrics(t), "")

	frame := h.next(t)
	if frame.opcode != opcodeClose || !bytes.Equal(frame.payload, []byte{0x03, 0xe8}) {
		t.Errorf("frame = %+v, want the close frame", frame)
	}
	h.wantEnd(t)
}

// TestUpgradeWritesToUpstream checks that the bytes of the caller reach the
// upstream unchanged.
func TestUpgradeWritesToUpstream(t *testing.T) {
	t.Parallel()
	source, sourceWriter := io.Pipe()
	t.Cleanup(func() { _ = sourceWriter.Close() })
	h := newUpgradeHarness(t, source, allowNames("a"), testMetrics(t), "")

	frame := wsMaskedFrame(opcodePing, [4]byte{1, 2, 3, 4}, []byte("hello"))
	if _, err := h.body.Write(frame); err != nil {
		t.Fatalf("write to the upstream: %v", err)
	}
	if got := h.upstream.written(); got != string(frame) {
		t.Errorf("upstream got %x, want %x", got, frame)
	}
}

// TestDrainEndsUpgradedWatch checks that a drain writes a close frame, that
// the stream then ends with io.EOF, and that the goroutine leaves the
// registry.
func TestDrainEndsUpgradedWatch(t *testing.T) {
	t.Parallel()
	source, sourceWriter := io.Pipe()
	h := newUpgradeHarness(t, source, allowNames("a"), testMetrics(t), "")

	if got := h.registry.len(); got != 1 {
		t.Fatalf("registry length = %d, want 1", got)
	}
	if got := h.registry.closeAll(); got != 1 {
		t.Errorf("closeAll = %d, want 1", got)
	}

	frame := h.next(t)
	if frame.opcode != opcodeClose || !bytes.Equal(frame.payload, []byte{0x03, 0xe8}) {
		t.Errorf("frame = %+v, want the close frame with status 1000", frame)
	}
	h.wantEnd(t)

	// Unblock the read of the goroutine, so it can remove itself.
	_ = sourceWriter.Close()
	select {
	case <-h.upstream.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the goroutine did not end after the drain")
	}
	if got := h.registry.len(); got != 0 {
		t.Errorf("registry length after the goroutine ends = %d, want 0", got)
	}
}

// wantUnfiltered checks the value of drover.filter.frames.unfiltered.
func wantUnfiltered(t *testing.T, reader *sdkmetric.ManualReader, want int64) {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	sum := findSum(t, data, "drover.filter.frames.unfiltered")
	if len(sum.DataPoints) != 1 {
		t.Fatalf("data points = %d, want 1", len(sum.DataPoints))
	}
	if got := sum.DataPoints[0].Value; got != want {
		t.Errorf("drover.filter.frames.unfiltered = %d, want %d", got, want)
	}
}
