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
)

// watchEvent is the part of a watch event that the filter reads. The filter
// re-emits the raw bytes of a passed event, not a re-encoding of this struct.
type watchEvent struct {
	Type   string `json:"type"`
	Object struct {
		Metadata struct {
			Name   string            `json:"name"`
			Labels map[string]string `json:"labels"`
		} `json:"metadata"`
	} `json:"object"`
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
					name := event.Object.Metadata.Name
					pass = allow(name, event.Object.Metadata.Labels)
					if !pass {
						logger.DebugContext(ctx, "dropped a watch event", "namespace", name)
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
