package filter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
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

// opcodePing is the ping opcode of RFC 6455. The filter forwards a ping
// frame, and it sends none itself.
const opcodePing = 0x9

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

// wsMessage builds the frame of one message that has slice, a part of the
// watch stream. The API server sends at most 2048 bytes of the stream per
// message, on no event boundary.
func wsMessage(subprotocol string, slice []byte) []byte {
	if subprotocol == base64Subprotocol {
		return wsFrame(true, opcodeText, []byte(base64.StdEncoding.EncodeToString(slice)))
	}
	return wsFrame(true, opcodeBinary, slice)
}

// wsStream builds the frames of one message per slice.
func wsStream(subprotocol string, slices ...[]byte) []byte {
	var out []byte
	for _, slice := range slices {
		out = append(out, wsMessage(subprotocol, slice)...)
	}
	return out
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

// eventLine returns the ADDED event of the namespace, with the newline that
// separates two events in the watch stream.
func eventLine(name string) []byte {
	return append(addedEvent(name), '\n')
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

// subprotocols are the two payload encodings of a watch upgrade. An empty
// value is the binary case, where the message has the stream bytes as they
// are.
var subprotocols = []string{"", base64Subprotocol}

// subprotocolName names a subprotocol for a subtest.
func subprotocolName(subprotocol string) string {
	if subprotocol == "" {
		return "binary"
	}
	return "base64"
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
	return newUpgradeHarnessWith(t, source, allow, m, subprotocol, "")
}

// newUpgradeHarnessWith is newUpgradeHarness with the websocket extensions of
// the 101 answer.
func newUpgradeHarnessWith(t *testing.T, source io.Reader, allow func(string, map[string]string) bool, m *metrics, subprotocol, extensions string) *upgradeHarness {
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
	if extensions != "" {
		resp.Header.Set(extensionsHeader, extensions)
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

// nextMessage reads the next message that reaches the caller, and returns the
// event in it. The filter sends one final frame per event.
func (h *upgradeHarness) nextMessage(t *testing.T, subprotocol string) string {
	t.Helper()
	frame := h.next(t)
	if !frame.fin {
		t.Fatalf("frame = %+v, want a final frame", frame)
	}
	if subprotocol != base64Subprotocol {
		if frame.opcode != opcodeBinary {
			t.Fatalf("opcode = %#x, want the binary opcode %#x", frame.opcode, opcodeBinary)
		}
		return string(frame.payload)
	}
	if frame.opcode != opcodeText {
		t.Fatalf("opcode = %#x, want the text opcode %#x", frame.opcode, opcodeText)
	}
	decoded, err := base64.StdEncoding.DecodeString(string(frame.payload))
	if err != nil {
		t.Fatalf("decode the message: %v", err)
	}
	return string(decoded)
}

// wantEnd checks that no further frame reaches the caller.
func (h *upgradeHarness) wantEnd(t *testing.T) {
	t.Helper()
	if _, err := h.reader.ReadByte(); err != io.EOF {
		t.Fatalf("read after the last frame = %v, want io.EOF", err)
	}
}

// TestUpgradeSplitsOneMessageWithTwoEvents checks that the filter decides per
// event when one message has two events.
func TestUpgradeSplitsOneMessageWithTwoEvents(t *testing.T) {
	t.Parallel()
	for _, subprotocol := range subprotocols {
		t.Run(subprotocolName(subprotocol), func(t *testing.T) {
			t.Parallel()
			allowed := eventLine("a")
			message := append(eventLine("z"), allowed...)
			h := newUpgradeHarness(t, bytes.NewReader(wsStream(subprotocol, message)), allowNames("a"), testMetrics(t), subprotocol)

			if got := h.nextMessage(t, subprotocol); got != string(allowed) {
				t.Errorf("message = %q, want the allowed event %q", got, allowed)
			}
			h.wantEnd(t)
		})
	}
}

// TestUpgradeJoinsEventAcrossThreeMessages checks that the filter keeps the
// bytes of an event that three messages carry.
func TestUpgradeJoinsEventAcrossThreeMessages(t *testing.T) {
	t.Parallel()
	for _, subprotocol := range subprotocols {
		t.Run(subprotocolName(subprotocol), func(t *testing.T) {
			t.Parallel()
			allowed := eventLine("a")
			source := wsStream(subprotocol, allowed[:10], allowed[10:30], allowed[30:])
			h := newUpgradeHarness(t, bytes.NewReader(source), allowNames("a"), testMetrics(t), subprotocol)

			if got := h.nextMessage(t, subprotocol); got != string(allowed) {
				t.Errorf("message = %q, want the whole event %q", got, allowed)
			}
			h.wantEnd(t)
		})
	}
}

// TestUpgradeDropsEventAcrossMessages checks that the filter drops an event
// whose name a message boundary splits.
func TestUpgradeDropsEventAcrossMessages(t *testing.T) {
	t.Parallel()
	for _, subprotocol := range subprotocols {
		t.Run(subprotocolName(subprotocol), func(t *testing.T) {
			t.Parallel()
			denied, allowed := eventLine("z"), eventLine("a")
			stream := append(denied, allowed...)
			cut := bytes.Index(stream, []byte(`"z"`)) + 1
			h := newUpgradeHarness(t, bytes.NewReader(wsStream(subprotocol, stream[:cut], stream[cut:])), allowNames("a"), testMetrics(t), subprotocol)

			if got := h.nextMessage(t, subprotocol); got != string(allowed) {
				t.Errorf("message = %q, want the allowed event %q", got, allowed)
			}
			h.wantEnd(t)
		})
	}
}

// TestUpgradeForwardsPingWhileBuffering checks that a ping frame reaches the
// caller while the filter still waits for the rest of an event.
func TestUpgradeForwardsPingWhileBuffering(t *testing.T) {
	t.Parallel()
	allowed := eventLine("a")
	source := wsMessage("", allowed[:12])
	source = append(source, wsFrame(true, opcodePing, []byte("keepalive"))...)
	source = append(source, wsMessage("", allowed[12:])...)
	h := newUpgradeHarness(t, bytes.NewReader(source), allowNames("a"), testMetrics(t), "")

	frame := h.next(t)
	if frame.opcode != opcodePing || string(frame.payload) != "keepalive" {
		t.Errorf("frame = %+v, want the ping frame", frame)
	}
	if got := h.nextMessage(t, ""); got != string(allowed) {
		t.Errorf("message = %q, want the whole event %q", got, allowed)
	}
	h.wantEnd(t)
}

// TestUpgradePassesEventWithoutADecision checks that a BOOKMARK event and a
// value that is no watch event reach the caller.
func TestUpgradePassesEventWithoutADecision(t *testing.T) {
	t.Parallel()
	bookmark := `{"type":"BOOKMARK","object":{"metadata":{"resourceVersion":"7"}}}`
	other := `["not an event"]`
	stream := []byte(bookmark + "\n" + other + "\n")
	h := newUpgradeHarness(t, bytes.NewReader(wsStream("", stream)), allowNames(), testMetrics(t), "")

	for _, want := range []string{bookmark, other} {
		if got := h.nextMessage(t, ""); got != want+"\n" {
			t.Errorf("message = %q, want %q", got, want+"\n")
		}
	}
	h.wantEnd(t)
}

// TestUpgradeAssemblesContinuationFrame checks that the filter reads a
// message that arrives as a first frame plus a continuation frame.
func TestUpgradeAssemblesContinuationFrame(t *testing.T) {
	t.Parallel()
	allowed := eventLine("a")
	source := wsFrame(false, opcodeBinary, allowed[:10])
	source = append(source, wsFrame(true, opcodeContinuation, allowed[10:])...)
	h := newUpgradeHarness(t, bytes.NewReader(source), allowNames("a"), testMetrics(t), "")

	if got := h.nextMessage(t, ""); got != string(allowed) {
		t.Errorf("message = %q, want the whole event %q", got, allowed)
	}
	h.wantEnd(t)
}

// TestUpgradeUnmasksFrame checks that the filter reads the payload of a
// masked frame. A server sends no masked frame, so this is a tolerance.
func TestUpgradeUnmasksFrame(t *testing.T) {
	t.Parallel()
	allowed := eventLine("a")
	frame := wsMaskedFrame(opcodeBinary, [4]byte{0x11, 0x22, 0x33, 0x44}, allowed)
	h := newUpgradeHarness(t, bytes.NewReader(frame), allowNames("a"), testMetrics(t), "")

	if got := h.nextMessage(t, ""); got != string(allowed) {
		t.Errorf("message = %q, want the event %q", got, allowed)
	}
	h.wantEnd(t)
}

// TestUpgradeReadsSlicesOfTheStream checks a long stream that arrives in
// 2048-byte slices, the slice size of the API server.
func TestUpgradeReadsSlicesOfTheStream(t *testing.T) {
	t.Parallel()
	var stream []byte
	var names, want []string
	for i := range 200 {
		name := fmt.Sprintf("z-%03d", i)
		if i%2 == 0 {
			name = fmt.Sprintf("a-%03d", i)
			names = append(names, name)
			want = append(want, string(eventLine(name)))
		}
		stream = append(stream, eventLine(name)...)
	}

	var source []byte
	for start := 0; start < len(stream); start += 2048 {
		source = append(source, wsMessage("", stream[start:min(start+2048, len(stream))])...)
	}
	h := newUpgradeHarness(t, bytes.NewReader(source), allowNames(names...), testMetrics(t), "")

	for _, event := range want {
		if got := h.nextMessage(t, ""); got != event {
			t.Fatalf("message = %q, want %q", got, event)
		}
	}
	h.wantEnd(t)
}

// TestUpgradeEndsOnWebsocketExtension checks that an answer with an extension
// ends the stream with a close frame, that no event reaches the caller, and
// that the filter counts the stream.
func TestUpgradeEndsOnWebsocketExtension(t *testing.T) {
	t.Parallel()
	m, reader := recordingMetrics(t)
	source := bytes.NewReader(wsStream("", eventLine("z")))
	h := newUpgradeHarnessWith(t, source, allowNames("a"), m, "", "permessage-deflate")

	frame := h.next(t)
	if frame.opcode != opcodeClose || !bytes.Equal(frame.payload, []byte{0x03, 0xe8}) {
		t.Errorf("frame = %+v, want the close frame with status 1000", frame)
	}
	h.wantEnd(t)

	wantRejected(t, reader, 1)
}

// TestUpgradeEndsAboveTheBufferBound checks that a stream that never
// completes an event ends once the buffer passes the bound.
func TestUpgradeEndsAboveTheBufferBound(t *testing.T) {
	t.Parallel()
	head := []byte(`{"type":"ADDED","object":{"metadata":{"name":"`)
	pad := bytes.Repeat([]byte("x"), 400*1024)
	source := wsStream("", append(head, pad...), pad, pad)
	h := newUpgradeHarness(t, bytes.NewReader(source), allowNames("a"), testMetrics(t), "")

	_, err := h.reader.ReadByte()
	if err == nil || !strings.Contains(err.Error(), "without a complete event") {
		t.Fatalf("read = %v, want the error of the buffer bound", err)
	}
}

// TestUpgradeEndsOnCloseFrame checks that the close frame reaches the caller,
// and that the stream ends there.
func TestUpgradeEndsOnCloseFrame(t *testing.T) {
	t.Parallel()
	source := append(wsFrame(true, opcodeClose, []byte{0x03, 0xe8}), wsStream("", eventLine("a"))...)
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

// wantRejected checks the value of drover.filter.watches.rejected.
func wantRejected(t *testing.T, reader *sdkmetric.ManualReader, want int64) {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	sum := findSum(t, data, "drover.filter.watches.rejected")
	if len(sum.DataPoints) != 1 {
		t.Fatalf("data points = %d, want 1", len(sum.DataPoints))
	}
	if got := sum.DataPoints[0].Value; got != want {
		t.Errorf("drover.filter.watches.rejected = %d, want %d", got, want)
	}
}
