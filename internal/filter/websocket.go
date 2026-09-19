package filter

import (
	"bufio"
	"bytes"
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

	// extensionsHeader names the websocket extensions of a connection. An
	// extension such as permessage-deflate compresses the payload, and the
	// filter reads no compressed payload.
	extensionsHeader = "Sec-WebSocket-Extensions"

	opcodeContinuation = 0x0
	opcodeText         = 0x1
	opcodeBinary       = 0x2

	// opcodeClose is the close opcode of RFC 6455. It is also the lowest
	// control opcode, so an opcode from this value up is a close, a ping, or
	// a pong frame.
	opcodeClose = 0x8

	// maxBuffer is the largest number of bytes that the filter holds for one
	// stream, in the message it assembles and in the stream buffer.
	maxBuffer = 1 << 20
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
// protocol switch. Read returns the events of the upstream that pass the
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
// every watch event gets the filter of a chunked watch. It returns the write
// side of the filtered stream, or nil when no stream follows. It registers
// that write side in registry while the goroutine runs, so a drain and a
// project change can end the stream. It raises drover.filter.watches.open
// while the stream is open.
//
// The body stays unchanged, and the return is nil, when the body is not an
// io.ReadWriteCloser, because httputil.ReverseProxy needs that interface for
// an upgraded connection. The stream ends at once, with a close frame and no
// event, when the answer names a websocket extension.
func filterWatchUpgrade(ctx context.Context, resp *http.Response, allow func(name string, labels map[string]string) bool, logger *slog.Logger, registry *watchRegistry, metrics *metrics, cluster string) *io.PipeWriter {
	upstream, ok := resp.Body.(io.ReadWriteCloser)
	if !ok {
		logger.ErrorContext(ctx, "the upgraded watch gets no filter, because its body is read-only",
			"cluster", cluster, "body_type", fmt.Sprintf("%T", resp.Body))
		return nil
	}

	subprotocol := resp.Header.Get("Sec-WebSocket-Protocol")
	extensions := resp.Header.Get(extensionsHeader)
	logger.DebugContext(ctx, "opened an upgraded namespace watch",
		"cluster", cluster, "subprotocol", subprotocol, "extensions", extensions)

	reader, writer := io.Pipe()
	registry.add(writer, true)
	metrics.watchOpened(ctx)
	resp.Body = &upgradedWatch{PipeReader: reader, upstream: upstream}

	end := func() {
		registry.remove(writer)
		metrics.watchClosed(ctx)
		_ = upstream.Close()
	}

	if extensions != "" {
		logger.ErrorContext(ctx, "ended an upgraded namespace watch, because the answer names a websocket extension",
			"cluster", cluster, "extensions", extensions)
		metrics.watchRejected(ctx, cluster)
		go func() {
			defer end()
			registry.end(writer)
		}()
		return nil
	}

	filter := &frameFilter{
		ctx:     ctx,
		src:     bufio.NewReader(upstream),
		dst:     writer,
		base64:  subprotocol == base64Subprotocol,
		allow:   allow,
		logger:  logger,
		metrics: metrics,
		cluster: cluster,
	}

	go func() {
		defer end()
		_ = writer.CloseWithError(filter.run())
	}()

	return writer
}

// frameFilter reads the RFC 6455 frames of a namespace watch, and writes the
// events that pass to dst.
//
// A message of the API server is an arbitrary slice of the newline-delimited
// JSON watch stream, not one event. The filter therefore assembles a message
// from its frames, decodes it, and appends the bytes to a stream buffer. It
// then takes one complete event at a time from that buffer, and it writes
// each event that passes as a new message of its own.
type frameFilter struct {
	ctx     context.Context
	src     *bufio.Reader
	dst     io.Writer
	base64  bool
	allow   func(name string, labels map[string]string) bool
	logger  *slog.Logger
	metrics *metrics
	cluster string

	message []byte
	buffer  []byte
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

// readData buffers one text, binary, or continuation frame. It reads the
// message once the frame has the FIN bit.
func (f *frameFilter) readData(header frameHeader) error {
	if uint64(len(f.message))+header.length > maxBuffer {
		return fmt.Errorf("websocket message is above the limit of %d bytes", maxBuffer)
	}
	payload := make([]byte, header.length)
	if _, err := io.ReadFull(f.src, payload); err != nil {
		return err
	}
	f.message = append(f.message, unmask(payload, header)...)
	if !header.fin {
		return nil
	}
	message := f.message
	f.message = nil
	return f.consume(message)
}

// consume decodes one complete message, appends its bytes to the stream
// buffer, and writes every complete event that the buffer now holds. A
// message that does not decode ends the stream, because the position in the
// stream is then lost.
func (f *frameFilter) consume(message []byte) error {
	chunk := message
	if f.base64 {
		decoded, err := base64.StdEncoding.AppendDecode(nil, message)
		if err != nil {
			return fmt.Errorf("decode a base64 websocket message: %w", err)
		}
		chunk = decoded
	}
	if len(f.buffer)+len(chunk) > maxBuffer {
		return fmt.Errorf("websocket watch stream buffered more than %d bytes without a complete event", maxBuffer)
	}
	f.buffer = append(f.buffer, chunk...)
	return f.drain()
}

// drain takes every complete JSON value from the stream buffer, and keeps the
// bytes that the decoder does not consume. Those bytes are the head of the
// next event, and the next message completes them.
func (f *frameFilter) drain() error {
	decoder := json.NewDecoder(bytes.NewReader(f.buffer))
	var consumed int64
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			break
		}
		consumed = decoder.InputOffset()
		if err := f.emit(raw); err != nil {
			return err
		}
	}
	if consumed > 0 {
		f.buffer = append(f.buffer[:0], f.buffer[consumed:]...)
	}
	return nil
}

// emit applies the event filter to one event, and writes the event when it
// passes. An ADDED, a MODIFIED, and a DELETED event needs a namespace that
// allow accepts. Every other event passes, and so does a value that is no
// watch event.
func (f *frameFilter) emit(raw json.RawMessage) error {
	var event watchEvent
	if json.Unmarshal(raw, &event) == nil {
		switch event.Type {
		case watchAdded, watchModified, watchDeleted:
			if !eventAllowed(event, f.allow) {
				f.metrics.eventDropped(f.ctx, f.cluster)
				f.logger.DebugContext(f.ctx, "dropped a watch event",
					"kind", event.Object.Kind, "namespace", event.Object.Metadata.Name)
				return nil
			}
		}
	}

	line := make([]byte, 0, len(raw)+1)
	line = append(line, raw...)
	line = append(line, '\n')
	return f.write(encodeMessage(f.base64, line))
}

// encodeMessage returns one final unmasked frame with the event. The base64
// subprotocol takes a text frame with the base64 text of the event, and every
// other subprotocol takes a binary frame with the event itself. A
// server-to-client frame has no mask.
func encodeMessage(base64Encode bool, event []byte) []byte {
	opcode := byte(opcodeBinary)
	payload := event
	if base64Encode {
		opcode = opcodeText
		payload = []byte(base64.StdEncoding.EncodeToString(event))
	}

	frame := []byte{0x80 | opcode}
	switch {
	case len(payload) < 126:
		frame = append(frame, byte(len(payload)))
	case len(payload) <= math.MaxUint16:
		frame = append(frame, 126)
		frame = binary.BigEndian.AppendUint16(frame, uint16(len(payload)))
	default:
		frame = append(frame, 127)
		frame = binary.BigEndian.AppendUint64(frame, uint64(len(payload)))
	}
	return append(frame, payload...)
}

func (f *frameFilter) write(p []byte) error {
	if len(p) == 0 {
		return nil
	}
	_, err := f.dst.Write(p)
	return err
}

// frameHeader is the header of one RFC 6455 frame.
type frameHeader struct {
	// raw is the header as it arrived, so the filter can forward a control
	// frame unchanged.
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
