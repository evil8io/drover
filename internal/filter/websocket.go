package filter

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"time"
)

const (
	// base64Subprotocol is the websocket subprotocol whose message payload is
	// base64 text. Every other subprotocol sends the message unencoded.
	base64Subprotocol = "base64.binary.k8s.io"

	// opcodeClose is the close opcode of RFC 6455. It is also the lowest
	// control opcode, so an opcode from this value up is a close, a ping, or
	// a pong frame.
	opcodeClose = 0x8

	// maxMessage is the largest message that the filter assembles, in bytes.
	maxMessage = 1 << 20
	// maxControlPayload is the payload limit of RFC 6455 for a control frame.
	maxControlPayload = 125

	// closeFrameWait bounds the write of closeFrame into the stream, because
	// that write waits for the reader.
	closeFrameWait = 5 * time.Second
)

// closeFrame is an unmasked close frame of RFC 6455 with status 1000, a
// normal closure. The service writes it when it ends an upgraded stream
// itself, so the client reports a clean end of the stream.
var closeFrame = []byte{0x80 | opcodeClose, 0x02, 0x03, 0xe8}

// upgradedWatch is the body of a namespace watch that Rancher answers with a
// protocol switch. Read returns the frames of the upstream that pass the
// filter. Write passes the bytes of the caller to the upstream unchanged.
type upgradedWatch struct {
	*io.PipeReader
	upstream io.ReadWriteCloser
}

func (u *upgradedWatch) Write(p []byte) (int, error) {
	return u.upstream.Write(p)
}

// Close ends the read side, and closes the upstream connection.
func (u *upgradedWatch) Close() error {
	_ = u.PipeReader.Close()
	return u.upstream.Close()
}

// filterWatchUpgrade replaces the body of an upgraded namespace watch, so
// every websocket message gets the event filter of a chunked watch. It
// returns the write side of the filtered stream. It registers that write side
// in registry while the goroutine runs, so a drain and a project change can
// end the stream. It raises drover.filter.watches.open while the stream is
// open. The body stays unchanged, and the return is nil, when the body is not
// an io.ReadWriteCloser, because httputil.ReverseProxy needs that interface
// for an upgraded connection.
func filterWatchUpgrade(ctx context.Context, resp *http.Response, allow func(name string, labels map[string]string) bool, logger *slog.Logger, registry *watchRegistry, metrics *metrics, cluster string) *io.PipeWriter {
	upstream, ok := resp.Body.(io.ReadWriteCloser)
	if !ok {
		logger.ErrorContext(ctx, "the upgraded watch gets no filter, because its body is read-only",
			"cluster", cluster, "body_type", fmt.Sprintf("%T", resp.Body))
		return nil
	}

	reader, writer := io.Pipe()
	registry.add(writer, true)
	metrics.watchOpened(ctx)

	filter := &frameFilter{
		ctx:         ctx,
		src:         bufio.NewReader(upstream),
		dst:         writer,
		subprotocol: resp.Header.Get("Sec-WebSocket-Protocol"),
		allow:       allow,
		logger:      logger,
		metrics:     metrics,
		cluster:     cluster,
	}

	go func() {
		defer func() {
			registry.remove(writer)
			metrics.watchClosed(ctx)
			_ = upstream.Close()
		}()
		_ = writer.CloseWithError(filter.run())
	}()

	resp.Body = &upgradedWatch{PipeReader: reader, upstream: upstream}
	return writer
}

// frameFilter reads the RFC 6455 frames of a namespace watch, and writes the
// frames that pass to dst. It buffers a text or a binary frame, and its
// continuation frames, until the FIN bit. It then decides on the assembled
// message, and it forwards every buffered frame in order, or none of them.
type frameFilter struct {
	ctx         context.Context
	src         *bufio.Reader
	dst         io.Writer
	subprotocol string
	allow       func(name string, labels map[string]string) bool
	logger      *slog.Logger
	metrics     *metrics
	cluster     string

	raw      []byte
	payload  []byte
	oversize bool
}

// run reads frames until the upstream ends, the write side fails, or the
// upstream sends a close frame. A close frame gives a nil error, so the
// caller of the stream gets a plain end of the stream.
func (f *frameFilter) run() error {
	for {
		header, err := readFrameHeader(f.src)
		if err != nil {
			return err
		}
		if header.opcode >= opcodeClose {
			if err := f.forwardControl(header); err != nil {
				return err
			}
			if header.opcode == opcodeClose {
				return nil
			}
			continue
		}
		if err := f.readData(header); err != nil {
			return err
		}
	}
}

// forwardControl passes a close, a ping, or a pong frame to the caller at
// once, also while a message is still incomplete.
func (f *frameFilter) forwardControl(header frameHeader) error {
	if header.length > maxControlPayload {
		return fmt.Errorf("websocket control frame has %d payload bytes, above the limit of %d", header.length, maxControlPayload)
	}
	payload := make([]byte, header.length)
	if _, err := io.ReadFull(f.src, payload); err != nil {
		return err
	}
	return f.write(append(header.raw, payload...))
}

// readData buffers one text, binary, or continuation frame. It decides on the
// message once the frame has the FIN bit.
func (f *frameFilter) readData(header frameHeader) error {
	if f.oversize || uint64(len(f.payload))+header.length > maxMessage {
		return f.forwardOversize(header)
	}

	payload := make([]byte, header.length)
	if _, err := io.ReadFull(f.src, payload); err != nil {
		return err
	}
	f.raw = append(append(f.raw, header.raw...), payload...)
	f.payload = append(f.payload, unmask(payload, header)...)
	if !header.fin {
		return nil
	}
	return f.decide()
}

// forwardOversize passes a message above maxMessage to the caller without a
// decision. It writes the buffered frames first, and it copies the payload of
// every later frame of the message straight through, so the filter never
// buffers the whole message.
func (f *frameFilter) forwardOversize(header frameHeader) error {
	if !f.oversize {
		f.oversize = true
		f.unfiltered("the message is above the size limit")
		buffered := f.raw
		f.raw, f.payload = nil, nil
		if err := f.write(buffered); err != nil {
			return err
		}
	}
	if err := f.write(header.raw); err != nil {
		return err
	}
	if _, err := io.CopyN(f.dst, f.src, int64(header.length)); err != nil {
		return err
	}
	if header.fin {
		f.reset()
	}
	return nil
}

// decide applies the event filter to the assembled message, and forwards the
// buffered frames when the message passes. An ADDED, a MODIFIED, and a
// DELETED event needs a namespace that allow accepts. Every other event
// passes, and so does a message that the filter cannot read.
func (f *frameFilter) decide() error {
	raw, payload := f.raw, f.payload
	f.reset()

	message, err := decodeMessage(f.subprotocol, payload)
	if err != nil {
		f.unfiltered("the base64 payload does not decode")
		return f.write(raw)
	}
	event, ok := parseWatchEvent(message)
	if !ok {
		f.unfiltered("the payload is not a watch event")
		return f.write(raw)
	}

	switch event.Type {
	case watchAdded, watchModified, watchDeleted:
		if !eventAllowed(event, f.allow) {
			f.metrics.eventDropped(f.ctx, f.cluster)
			f.logger.DebugContext(f.ctx, "dropped a watch event",
				"kind", event.Object.Kind, "namespace", event.Object.Metadata.Name)
			return nil
		}
	}
	return f.write(raw)
}

func (f *frameFilter) reset() {
	f.raw, f.payload, f.oversize = nil, nil, false
}

func (f *frameFilter) write(p []byte) error {
	if len(p) == 0 {
		return nil
	}
	_, err := f.dst.Write(p)
	return err
}

// unfiltered counts one message that reaches the caller without a decision,
// and names the reason.
func (f *frameFilter) unfiltered(reason string) {
	f.metrics.frameUnfiltered(f.ctx, f.cluster)
	f.logger.WarnContext(f.ctx, "forwarded a websocket message without a filter decision",
		"cluster", f.cluster, "reason", reason)
}

// decodeMessage returns the bytes to parse. The base64 subprotocol encodes
// the message, and every other subprotocol sends it as it is. The filter
// forwards the original bytes in both cases.
func decodeMessage(subprotocol string, payload []byte) ([]byte, error) {
	if subprotocol != base64Subprotocol {
		return payload, nil
	}
	decoded, err := base64.StdEncoding.AppendDecode(nil, payload)
	if err == nil {
		return decoded, nil
	}
	return base64.RawStdEncoding.AppendDecode(nil, payload)
}

// parseWatchEvent reads a watch event from a message. It drops one leading
// NUL byte and tries again, because a channel byte can precede the JSON.
func parseWatchEvent(message []byte) (watchEvent, bool) {
	var event watchEvent
	if json.Unmarshal(message, &event) == nil {
		return event, true
	}
	if len(message) > 0 && message[0] == 0x00 {
		if json.Unmarshal(message[1:], &event) == nil {
			return event, true
		}
	}
	return watchEvent{}, false
}

// frameHeader is the header of one RFC 6455 frame.
type frameHeader struct {
	// raw is the header as it arrived, so the filter can forward the frame
	// unchanged.
	raw    []byte
	opcode byte
	fin    bool
	masked bool
	mask   [4]byte
	length uint64
}

// readFrameHeader reads the FIN bit, the opcode, the mask, and the payload
// length of the next frame.
func readFrameHeader(r *bufio.Reader) (frameHeader, error) {
	first := make([]byte, 2)
	if _, err := io.ReadFull(r, first); err != nil {
		return frameHeader{}, err
	}
	header := frameHeader{
		raw:    first,
		opcode: first[0] & 0x0f,
		fin:    first[0]&0x80 != 0,
		masked: first[1]&0x80 != 0,
		length: uint64(first[1] & 0x7f),
	}

	switch header.length {
	case 126:
		extended := make([]byte, 2)
		if _, err := io.ReadFull(r, extended); err != nil {
			return frameHeader{}, err
		}
		header.raw = append(header.raw, extended...)
		header.length = uint64(binary.BigEndian.Uint16(extended))
	case 127:
		extended := make([]byte, 8)
		if _, err := io.ReadFull(r, extended); err != nil {
			return frameHeader{}, err
		}
		header.raw = append(header.raw, extended...)
		header.length = binary.BigEndian.Uint64(extended)
	}
	if header.length > math.MaxInt64 {
		return frameHeader{}, fmt.Errorf("websocket frame length %d is not valid", header.length)
	}

	if header.masked {
		if _, err := io.ReadFull(r, header.mask[:]); err != nil {
			return frameHeader{}, err
		}
		header.raw = append(header.raw, header.mask[:]...)
	}
	return header, nil
}

// unmask returns the payload of the frame after the XOR with its mask key. A
// server sends no masked frame, so this is a tolerance only.
func unmask(payload []byte, header frameHeader) []byte {
	if !header.masked {
		return payload
	}
	out := make([]byte, len(payload))
	for i := range payload {
		out[i] = payload[i] ^ header.mask[i%4]
	}
	return out
}
