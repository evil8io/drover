package filter

import (
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
)

// errNotCollection marks a body that is not a List and not a Table.
var errNotCollection = errors.New("the answer is not a collection")

// collectionHeader is the part of a List or a Table that stands before the
// elements. arrayKey is items for a List, and rows for a Table. It is empty
// when the body has neither key.
type collectionHeader struct {
	kind            string
	apiVersion      string
	resourceVersion string
	columns         json.RawMessage
	arrayKey        string
}

// collectionScanner reads one List or Table body element by element. The peak
// allocation is one element, not the whole body, because a merged answer of
// many namespaces does not fit in the memory of the container.
type collectionScanner struct {
	decoder *json.Decoder
	body    io.ReadCloser
	header  collectionHeader
	inArray bool
}

// openCollection reads the fields that stand before the elements, and leaves
// the scanner at the first element. The caller closes the scanner.
func openCollection(body io.ReadCloser) (*collectionScanner, error) {
	decoder := json.NewDecoder(body)
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return nil, errNotCollection
	}

	scanner := &collectionScanner{decoder: decoder, body: body}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, errNotCollection
		}
		switch key {
		case "kind":
			err = decoder.Decode(&scanner.header.kind)
		case "apiVersion":
			err = decoder.Decode(&scanner.header.apiVersion)
		case "columnDefinitions":
			err = decoder.Decode(&scanner.header.columns)
		case "metadata":
			err = scanner.readMetadata()
		case "items", "rows":
			scanner.header.arrayKey = key
			return scanner, scanner.openArray()
		default:
			var skipped json.RawMessage
			err = decoder.Decode(&skipped)
		}
		if err != nil {
			return nil, err
		}
	}
	return scanner, nil
}

func (c *collectionScanner) readMetadata() error {
	var metadata struct {
		ResourceVersion string `json:"resourceVersion"`
	}
	if err := c.decoder.Decode(&metadata); err != nil {
		return err
	}
	c.header.resourceVersion = metadata.ResourceVersion
	return nil
}

// openArray reads the token after the items or rows key. A null value gives an
// empty collection.
func (c *collectionScanner) openArray() error {
	token, err := c.decoder.Token()
	if err != nil {
		return err
	}
	if token == nil {
		return nil
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '[' {
		return errNotCollection
	}
	c.inArray = true
	return nil
}

// next returns the next element. The second result is false at the end of the
// elements.
func (c *collectionScanner) next() (json.RawMessage, bool, error) {
	if !c.inArray {
		return nil, false, nil
	}
	if !c.decoder.More() {
		if _, err := c.decoder.Token(); err != nil {
			return nil, false, err
		}
		c.inArray = false
		return nil, false, nil
	}
	var element json.RawMessage
	if err := c.decoder.Decode(&element); err != nil {
		return nil, false, err
	}
	return element, true, nil
}

// finish reads the fields that stand after the elements, so a body that puts
// metadata last also gives its resourceVersion. It ignores a read error,
// because the elements are already through.
func (c *collectionScanner) finish() {
	for c.decoder.More() {
		token, err := c.decoder.Token()
		if err != nil {
			return
		}
		key, ok := token.(string)
		if !ok {
			return
		}
		if key == "metadata" {
			if err := c.readMetadata(); err != nil {
				return
			}
			continue
		}
		var skipped json.RawMessage
		if err := c.decoder.Decode(&skipped); err != nil {
			return
		}
	}
}

func (c *collectionScanner) close() {
	_ = c.body.Close()
}

// writeCollectionHeader writes the start of the merged answer, up to and
// including the open bracket of the elements.
func writeCollectionHeader(w io.Writer, header collectionHeader) error {
	var fields []string
	if header.kind != "" {
		fields = append(fields, strconv.Quote("kind")+":"+strconv.Quote(header.kind))
	}
	if header.apiVersion != "" {
		fields = append(fields, strconv.Quote("apiVersion")+":"+strconv.Quote(header.apiVersion))
	}
	if len(header.columns) > 0 {
		fields = append(fields, strconv.Quote("columnDefinitions")+":"+string(header.columns))
	}
	key := header.arrayKey
	if key == "" {
		key = "items"
	}
	fields = append(fields, strconv.Quote(key)+":[")

	_, err := io.WriteString(w, "{"+strings.Join(fields, ","))
	return err
}

// writeCollectionFooter closes the elements and writes the metadata of the
// merged answer. The metadata stands after the elements, because the
// resourceVersion of the merge is the highest of the answers, and the merge
// streams, so that value is known at the end only. A JSON object has no
// significant key order, and every Kubernetes client parses it as such.
func writeCollectionFooter(w io.Writer, resourceVersion string) error {
	out := []byte(`],"metadata":{`)
	if resourceVersion != "" {
		out = append(out, []byte(`"resourceVersion":`+strconv.Quote(resourceVersion))...)
	}
	out = append(out, '}', '}')
	_, err := w.Write(out)
	return err
}

// maxResourceVersion returns the higher of two resource versions. A Kubernetes
// resourceVersion is the etcd revision, which counts up over the whole
// cluster, so the highest of the answers is the revision that every answer
// reached.
func maxResourceVersion(a, b string) string {
	left, leftErr := strconv.ParseUint(a, 10, 64)
	right, rightErr := strconv.ParseUint(b, 10, 64)
	switch {
	case leftErr != nil && rightErr != nil:
		if a != "" {
			return a
		}
		return b
	case leftErr != nil:
		return b
	case rightErr != nil:
		return a
	case left >= right:
		return a
	default:
		return b
	}
}

// apiResourceList is the part of a discovery answer that the empty collection
// needs.
type apiResourceList struct {
	GroupVersion string `json:"groupVersion"`
	Resources    []struct {
		Name       string `json:"name"`
		Kind       string `json:"kind"`
		Namespaced bool   `json:"namespaced"`
	} `json:"resources"`
}

// emptyCollectionJSON returns an empty List of the kind, with no element.
func emptyCollectionJSON(kind, apiVersion string) []byte {
	return []byte(`{"kind":` + strconv.Quote(kind) +
		`,"apiVersion":` + strconv.Quote(apiVersion) +
		`,"metadata":{},"items":[]}`)
}
