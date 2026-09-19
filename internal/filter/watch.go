package filter

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"time"
)

const (
	watchAdded    = "ADDED"
	watchModified = "MODIFIED"
	watchDeleted  = "DELETED"

	// tableKind is the kind of a server-side table response, for example the
	// response to the Accept that kubectl sends.
	tableKind = "Table"
)

// watchEvent is the part of a watch event that the filter reads. The filter
// re-emits the raw bytes of a passed event, not a re-encoding of this struct.
// A table event has kind Table, and one row per object. Any other event has
// its name and its labels directly under the object.
type watchEvent struct {
	Type   string `json:"type"`
	Object struct {
		Kind     string `json:"kind"`
		Metadata struct {
			Name   string            `json:"name"`
			Labels map[string]string `json:"labels"`
		} `json:"metadata"`
		Rows []struct {
			Object struct {
				Metadata struct {
					Name   string            `json:"name"`
					Labels map[string]string `json:"labels"`
				} `json:"metadata"`
			} `json:"object"`
		} `json:"rows"`
	} `json:"object"`
}

// eventAllowed reports whether allow accepts every row of a table event, or
// the object of any other event. A table event with no row passes, because no
// name reaches the caller.
func eventAllowed(event watchEvent, allow func(name string, labels map[string]string) bool) bool {
	if event.Object.Kind != tableKind {
		return allow(event.Object.Metadata.Name, event.Object.Metadata.Labels)
	}
	for _, row := range event.Object.Rows {
		if !allow(row.Object.Metadata.Name, row.Object.Metadata.Labels) {
			return false
		}
	}
	return true
}

// watchStream is one open watch stream. upgraded marks a stream of a
// websocket connection, which needs a close frame before the pipe ends.
type watchStream struct {
	upgraded bool
	done     chan struct{}
}

// watchRegistry is the set of open watch streams of a Service. A stream joins
// when it starts, and it leaves when its goroutine ends.
type watchRegistry struct {
	mu      sync.Mutex
	streams map[*io.PipeWriter]*watchStream
}

func newWatchRegistry() *watchRegistry {
	return &watchRegistry{streams: make(map[*io.PipeWriter]*watchStream)}
}

func (r *watchRegistry) add(w *io.PipeWriter, upgraded bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.streams[w] = &watchStream{upgraded: upgraded, done: make(chan struct{})}
}

func (r *watchRegistry) remove(w *io.PipeWriter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if stream, ok := r.streams[w]; ok {
		close(stream.done)
		delete(r.streams, w)
	}
}

// done returns a channel that closes once the stream leaves the registry. A
// writer that is not in the registry gets a channel that is closed already.
func (r *watchRegistry) done(w *io.PipeWriter) <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	if stream, ok := r.streams[w]; ok {
		return stream.done
	}
	ended := make(chan struct{})
	close(ended)
	return ended
}

func (r *watchRegistry) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.streams)
}

// end ends one registered stream with a plain EOF, that is writer.Close,
// never CloseWithError. An upgraded stream gets a websocket close frame
// first, because a connection that ends without one gives the client an error
// instead of a clean end of the stream. The write of that frame waits for the
// reader, so a timer ends the stream even when the client reads nothing.
func (r *watchRegistry) end(w *io.PipeWriter) {
	r.mu.Lock()
	stream, ok := r.streams[w]
	r.mu.Unlock()
	if !ok {
		return
	}
	if stream.upgraded {
		timer := time.AfterFunc(closeFrameWait, func() { _ = w.Close() })
		defer timer.Stop()
		_, _ = w.Write(closeFrame)
	}
	_ = w.Close()
}

// closeAll ends every registered stream, and returns the count of streams it
// ends. Each stream ends in its own goroutine, because the close frame of an
// upgraded stream waits for the reader.
func (r *watchRegistry) closeAll() int {
	r.mu.Lock()
	writers := make([]*io.PipeWriter, 0, len(r.streams))
	for w := range r.streams {
		writers = append(writers, w)
	}
	r.mu.Unlock()

	for _, w := range writers {
		go r.end(w)
	}
	return len(writers)
}

// filterWatchBody reads a namespace watch stream from upstream, and returns a
// stream with only the events that allow lets through, plus the write side of
// that stream. A BOOKMARK, an ERROR, and any event the filter cannot parse
// always pass. It registers the write side in registry while the goroutine
// runs, so a drain and a project change can end the stream. The goroutine
// ends, and closes upstream, in two cases: a read of upstream fails, for
// example because ctx cancels, or a write to the pipe fails because the
// reader closed it or the registry ended it. It raises
// drover.filter.watches.open while the stream is open, and counts a dropped
// event on cluster in drover.filter.events.dropped.
func filterWatchBody(ctx context.Context, upstream io.ReadCloser, allow func(name string, labels map[string]string) bool, logger *slog.Logger, registry *watchRegistry, metrics *metrics, cluster string) (io.ReadCloser, *io.PipeWriter) {
	reader, writer := io.Pipe()
	registry.add(writer, false)
	metrics.watchOpened(ctx)

	go func() {
		defer func() {
			registry.remove(writer)
			metrics.watchClosed(ctx)
			_ = upstream.Close()
		}()

		decoder := json.NewDecoder(upstream)
		for {
			var raw json.RawMessage
			if err := decoder.Decode(&raw); err != nil {
				_ = writer.CloseWithError(err)
				return
			}

			var event watchEvent
			pass := true
			if json.Unmarshal(raw, &event) == nil {
				switch event.Type {
				case watchAdded, watchModified, watchDeleted:
					pass = eventAllowed(event, allow)
					if !pass {
						metrics.eventDropped(ctx, cluster)
						logger.DebugContext(ctx, "dropped a watch event", "kind", event.Object.Kind, "namespace", event.Object.Metadata.Name)
					}
				}
			}
			if !pass {
				continue
			}

			if _, err := writer.Write(raw); err != nil {
				return
			}
			if _, err := io.WriteString(writer, "\n"); err != nil {
				return
			}
		}
	}()

	return reader, writer
}
