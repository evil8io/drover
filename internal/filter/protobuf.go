package filter

import (
	"bytes"
	"errors"
	"fmt"
	"mime"
)

// protobufContentType is the media type of a Kubernetes protobuf body.
const protobufContentType = "application/vnd.kubernetes.protobuf"

// maxProtobufDepth limits the nested messages that the reader enters.
const maxProtobufDepth = 8

const (
	wireVarint  = 0
	wireFixed64 = 1
	wireBytes   = 2
	wireFixed32 = 5
)

// protobufPrefix starts a Kubernetes protobuf body. A runtime.Unknown message
// follows it.
var protobufPrefix = []byte("k8s\x00")

var errProtobufTruncated = errors.New("the protobuf message is truncated")

func isProtobuf(contentType string) bool {
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false
	}
	return mediaType == protobufContentType
}

// protobufReader reads the protocol buffer wire format.
type protobufReader struct {
	data []byte
	pos  int
}

func (r *protobufReader) done() bool {
	return r.pos >= len(r.data)
}

func (r *protobufReader) varint() (uint64, error) {
	var value uint64
	for shift := uint(0); shift < 64; shift += 7 {
		if r.done() {
			return 0, errProtobufTruncated
		}
		b := r.data[r.pos]
		r.pos++
		value |= uint64(b&0x7f) << shift
		if b < 0x80 {
			return value, nil
		}
	}
	return 0, errors.New("the protobuf varint is longer than 10 bytes")
}

func (r *protobufReader) tag() (number, wire int, err error) {
	tag, err := r.varint()
	if err != nil {
		return 0, 0, err
	}
	number, wire = int(tag>>3), int(tag&0x7)
	if number < 1 {
		return 0, 0, fmt.Errorf("the protobuf field number %d is not valid", number)
	}
	return number, wire, nil
}

// bytes reads a length-delimited value.
func (r *protobufReader) bytes(wire int) ([]byte, error) {
	if wire != wireBytes {
		return nil, fmt.Errorf("the protobuf wire type %d has no length", wire)
	}
	length, err := r.varint()
	if err != nil {
		return nil, err
	}
	if length > uint64(len(r.data)-r.pos) {
		return nil, errProtobufTruncated
	}
	start := r.pos
	r.pos += int(length)
	return r.data[start:r.pos], nil
}

func (r *protobufReader) text(wire int) (string, error) {
	value, err := r.bytes(wire)
	return string(value), err
}

func (r *protobufReader) skip(wire int) error {
	switch wire {
	case wireVarint:
		_, err := r.varint()
		return err
	case wireFixed64:
		return r.advance(8)
	case wireBytes:
		_, err := r.bytes(wire)
		return err
	case wireFixed32:
		return r.advance(4)
	}
	return fmt.Errorf("the protobuf wire type %d is not supported", wire)
}

func (r *protobufReader) advance(n int) error {
	if len(r.data)-r.pos < n {
		return errProtobufTruncated
	}
	r.pos += n
	return nil
}

// fields reads the fields of one message in order. visit reads the value of a
// field that it handles, and it returns false for a field that fields skips.
func fields(message []byte, depth int, visit func(number, wire int, r *protobufReader) (bool, error)) error {
	if depth > maxProtobufDepth {
		return fmt.Errorf("the protobuf message is nested deeper than %d messages", maxProtobufDepth)
	}
	r := &protobufReader{data: message}
	for !r.done() {
		number, wire, err := r.tag()
		if err != nil {
			return err
		}
		read, err := visit(number, wire, r)
		if err != nil {
			return err
		}
		if read {
			continue
		}
		if err := r.skip(wire); err != nil {
			return err
		}
	}
	return nil
}

// decodeProtobufReview reads the spec of a SelfSubjectAccessReview from a
// Kubernetes protobuf body. The field numbers are in the generated.proto file of
// k8s.io/apimachinery/pkg/runtime and of k8s.io/api/authorization/v1.
func decodeProtobufReview(body []byte) (reviewSpec, error) {
	if !bytes.HasPrefix(body, protobufPrefix) {
		return reviewSpec{}, errors.New("the body has no Kubernetes protobuf prefix")
	}

	// runtime.Unknown: raw = 2, contentEncoding = 3.
	var spec reviewSpec
	err := fields(body[len(protobufPrefix):], 0, func(number, wire int, r *protobufReader) (bool, error) {
		switch number {
		case 2:
			raw, err := r.bytes(wire)
			if err != nil {
				return false, err
			}
			spec, err = decodeReviewObject(raw, 1)
			return true, err
		case 3:
			encoding, err := r.text(wire)
			if err != nil {
				return false, err
			}
			if encoding != "" {
				return false, fmt.Errorf("the protobuf content encoding %q is not supported", encoding)
			}
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return reviewSpec{}, err
	}
	return spec, nil
}

// decodeReviewObject reads the spec field of a SelfSubjectAccessReview message.
func decodeReviewObject(object []byte, depth int) (reviewSpec, error) {
	var spec reviewSpec
	err := fields(object, depth, func(number, wire int, r *protobufReader) (bool, error) {
		if number != 2 {
			return false, nil
		}
		value, err := r.bytes(wire)
		if err != nil {
			return false, err
		}
		spec, err = decodeReviewSpec(value, depth+1)
		return true, err
	})
	if err != nil {
		return reviewSpec{}, err
	}
	return spec, nil
}

// decodeReviewSpec reads a SelfSubjectAccessReviewSpec message: resourceAttributes
// = 1, nonResourceAttributes = 2.
func decodeReviewSpec(data []byte, depth int) (reviewSpec, error) {
	var spec reviewSpec
	err := fields(data, depth, func(number, wire int, r *protobufReader) (bool, error) {
		switch number {
		case 1:
			value, err := r.bytes(wire)
			if err != nil {
				return false, err
			}
			attributes, err := decodeResourceAttributes(value, depth+1)
			if err != nil {
				return false, err
			}
			spec.resource = &attributes
			return true, nil
		case 2:
			if _, err := r.bytes(wire); err != nil {
				return false, err
			}
			spec.nonResource = true
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return reviewSpec{}, err
	}
	return spec, nil
}

// decodeResourceAttributes reads a ResourceAttributes message: namespace = 1,
// verb = 2, group = 3, resource = 5, subresource = 6, name = 7.
func decodeResourceAttributes(data []byte, depth int) (resourceAttributes, error) {
	var attributes resourceAttributes
	targets := map[int]*string{
		1: &attributes.Namespace,
		2: &attributes.Verb,
		3: &attributes.Group,
		5: &attributes.Resource,
		6: &attributes.Subresource,
		7: &attributes.Name,
	}
	err := fields(data, depth, func(number, wire int, r *protobufReader) (bool, error) {
		target, ok := targets[number]
		if !ok {
			return false, nil
		}
		value, err := r.text(wire)
		if err != nil {
			return false, err
		}
		*target = value
		return true, nil
	})
	if err != nil {
		return resourceAttributes{}, err
	}
	return attributes, nil
}
