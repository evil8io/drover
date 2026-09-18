package filter

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
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

// filterWatchBody reads a namespace watch stream from upstream, and returns a
// stream with only the events that allow lets through. A BOOKMARK, an ERROR,
// and any event the filter cannot parse always pass. The goroutine ends, and
// closes upstream, in three cases: a read of upstream fails, a write to the
// pipe fails because the reader closed it, or ctx cancels and so stops the
// read of upstream, because upstream is the body of a request with that same
// context.
func filterWatchBody(ctx context.Context, upstream io.ReadCloser, allow func(name string, labels map[string]string) bool, logger *slog.Logger) io.ReadCloser {
	reader, writer := io.Pipe()

	go func() {
		defer func() { _ = upstream.Close() }()

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
