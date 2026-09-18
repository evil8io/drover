package filter

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
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

// watchRegistry is the set of open watch streams of a Service. filterWatchBody
// adds a stream when it starts, and removes it when its goroutine ends.
type watchRegistry struct {
	mu      sync.Mutex
	writers map[*io.PipeWriter]struct{}
}

func newWatchRegistry() *watchRegistry {
	return &watchRegistry{writers: make(map[*io.PipeWriter]struct{})}
}

func (r *watchRegistry) add(w *io.PipeWriter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writers[w] = struct{}{}
}

func (r *watchRegistry) remove(w *io.PipeWriter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.writers, w)
}

func (r *watchRegistry) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.writers)
}

// closeAll ends every registered stream with a plain EOF, that is
// writer.Close, never CloseWithError. It returns the count of streams it ends.
func (r *watchRegistry) closeAll() int {
	r.mu.Lock()
	writers := make([]*io.PipeWriter, 0, len(r.writers))
	for w := range r.writers {
		writers = append(writers, w)
	}
	r.mu.Unlock()

	for _, w := range writers {
		_ = w.Close()
	}
	return len(writers)
}

// filterWatchBody reads a namespace watch stream from upstream, and returns a
// stream with only the events that allow lets through. A BOOKMARK, an ERROR,
// and any event the filter cannot parse always pass. It registers writer in
// registry while the goroutine runs, so a drain can end the stream. The
// goroutine ends, and closes upstream, in two cases: a read of upstream fails,
// for example because ctx cancels, or a write to the pipe fails because the
// reader closed it or a drain ended it.
func filterWatchBody(ctx context.Context, upstream io.ReadCloser, allow func(name string, labels map[string]string) bool, logger *slog.Logger, registry *watchRegistry) io.ReadCloser {
	reader, writer := io.Pipe()
	registry.add(writer)

	go func() {
		defer func() {
			registry.remove(writer)
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

	return reader
}
