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

// finish reads the fields that stand after the elements. A custom resource
// serializes as an unstructured object, whose keys are in alphabetical
// order, so apiVersion comes first and items comes before kind and metadata.
// The kind of such a list is therefore known only after its elements. It
// ignores a read error, because the elements are already through.
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
		switch key {
		case "kind":
			err = c.decoder.Decode(&c.header.kind)
		case "apiVersion":
			err = c.decoder.Decode(&c.header.apiVersion)
		case "columnDefinitions":
			err = c.decoder.Decode(&c.header.columns)
		case "metadata":
			err = c.readMetadata()
		default:
			var skipped json.RawMessage
			err = c.decoder.Decode(&skipped)
		}
		if err != nil {
			return
		}
	}
}

func (c *collectionScanner) close() {
	_ = c.body.Close()
}

// writeCollectionHeader writes the start of the merged answer: the open
// brace and the open bracket of the elements. Every other field stands after
// the elements, see writeCollectionFooter.
func writeCollectionHeader(w io.Writer, arrayKey string) error {
	if arrayKey == "" {
		arrayKey = "items"
	}
	_, err := io.WriteString(w, "{"+strconv.Quote(arrayKey)+":[")
	return err
}

// writeCollectionFooter closes the elements and writes every other field of
// the merged answer. They all stand after the elements, for two reasons. The
// resourceVersion of the merge is the lowest of the answers, and the merge
// streams, so that value is known at the end only. The kind of a custom
// resource list also stands after its elements upstream, because an
// unstructured object serializes its keys in alphabetical order, so the
// merge knows it at the end as well. A JSON object has no significant key
// order, and every Kubernetes client parses it as such.
func writeCollectionFooter(w io.Writer, header collectionHeader) error {
	fields := []string{}
	if header.kind != "" {
		fields = append(fields, strconv.Quote("kind")+":"+strconv.Quote(header.kind))
	}
	if header.apiVersion != "" {
		fields = append(fields, strconv.Quote("apiVersion")+":"+strconv.Quote(header.apiVersion))
	}
	if len(header.columns) > 0 {
		fields = append(fields, strconv.Quote("columnDefinitions")+":"+string(header.columns))
	}
	metadata := "{}"
	if header.resourceVersion != "" {
		metadata = "{" + strconv.Quote("resourceVersion") + ":" + strconv.Quote(header.resourceVersion) + "}"
	}
	fields = append(fields, strconv.Quote("metadata")+":"+metadata)

	_, err := io.WriteString(w, "],"+strings.Join(fields, ",")+"}")
	return err
}

// lowestResourceVersion returns the lower of two resource versions. A
// Kubernetes resourceVersion is the etcd revision, which counts up over the
// whole cluster. The answers of a merge arrive at different revisions, and a
// client starts its watch from the revision of the merged answer. A watch from
// the highest of them loses every change that a namespace of a lower revision
// got in between. A watch from the lowest loses nothing, and it repeats the
// changes of that window for the other namespaces. A client takes a repeated
// change as an update of an object it holds already, and a lost change leaves
// it with a stale object, so the lowest revision is the safe one.
func lowestResourceVersion(a, b string) string {
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
	case left <= right:
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
