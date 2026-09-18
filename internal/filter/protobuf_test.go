package filter

import (
	"encoding/binary"
	"strings"
	"testing"
)

func varintBytes(value uint64) []byte {
	var out []byte
	for value >= 0x80 {
		out = append(out, byte(value)|0x80)
		value >>= 7
	}
	return append(out, byte(value))
}

func varintField(number int, value uint64) []byte {
	return append(varintBytes(uint64(number)<<3|wireVarint), varintBytes(value)...)
}

func bytesField(number int, value []byte) []byte {
	out := append(varintBytes(uint64(number)<<3|wireBytes), varintBytes(uint64(len(value)))...)
	return append(out, value...)
}

func stringField(number int, value string) []byte {
	return bytesField(number, []byte(value))
}

func fixed32Field(number int, value uint32) []byte {
	return binary.LittleEndian.AppendUint32(varintBytes(uint64(number)<<3|wireFixed32), value)
}

func fixed64Field(number int, value uint64) []byte {
	return binary.LittleEndian.AppendUint64(varintBytes(uint64(number)<<3|wireFixed64), value)
}

func concat(parts ...[]byte) []byte {
	var out []byte
	for _, part := range parts {
		out = append(out, part...)
	}
	return out
}

// protoAttributes builds a ResourceAttributes message.
type protoAttributes struct {
	namespace   string
	verb        string
	group       string
	resource    string
	subresource string
	name        string
}

func (a protoAttributes) encode() []byte {
	var out []byte
	for _, field := range []struct {
		number int
		value  string
	}{
		{1, a.namespace},
		{2, a.verb},
		{3, a.group},
		{5, a.resource},
		{6, a.subresource},
		{7, a.name},
	} {
		if field.value != "" {
			out = append(out, stringField(field.number, field.value)...)
		}
	}
	return out
}

// protobufReview returns the body that kubectl sends for this review: a
// SelfSubjectAccessReview with an empty metadata and an empty status.
func protobufReview(attributes protoAttributes) []byte {
	return protobufEnvelope(concat(
		bytesField(1, bytesField(8, nil)),
		bytesField(2, bytesField(1, attributes.encode())),
		bytesField(3, nil),
	))
}

// protobufEnvelope wraps an object in the runtime.Unknown message of the
// Kubernetes protobuf format.
func protobufEnvelope(object []byte) []byte {
	typeMeta := concat(
		stringField(1, "authorization.k8s.io/v1"),
		stringField(2, "SelfSubjectAccessReview"),
	)
	return concat(protobufPrefix, bytesField(1, typeMeta), bytesField(2, object))
}

func TestDecodeProtobufReview(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		body        []byte
		want        resourceAttributes
		nonResource bool
	}{
		{
			name: "list namespaces",
			body: protobufReview(protoAttributes{verb: "list", resource: "namespaces"}),
			want: resourceAttributes{Verb: "list", Resource: "namespaces"},
		},
		{
			name: "every attribute",
			body: protobufReview(protoAttributes{
				namespace: "a", verb: "get", group: "apps", resource: "deployments",
				subresource: "status", name: "b",
			}),
			want: resourceAttributes{
				Namespace: "a", Verb: "get", Group: "apps", Resource: "deployments",
				Subresource: "status", Name: "b",
			},
		},
		{
			name: "unknown fields",
			body: protobufEnvelope(concat(
				varintField(7, 1),
				bytesField(2, concat(
					fixed32Field(9, 2),
					bytesField(1, concat(
						stringField(2, "watch"),
						stringField(4, "v1"),
						stringField(5, "namespaces"),
						bytesField(9, stringField(1, "app=a")),
					)),
				)),
			)),
			want: resourceAttributes{Verb: "watch", Resource: "namespaces"},
		},
		{
			name: "non resource attributes",
			body: protobufEnvelope(bytesField(2, bytesField(2, concat(
				stringField(1, "/healthz"),
				stringField(2, "get"),
			)))),
			nonResource: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			spec, err := decodeProtobufReview(test.body)
			if err != nil {
				t.Fatalf("decode the review: %v", err)
			}
			if spec.nonResource != test.nonResource {
				t.Errorf("nonResource = %v, want %v", spec.nonResource, test.nonResource)
			}
			if test.nonResource {
				return
			}
			if spec.resource == nil {
				t.Fatal("the spec has no resourceAttributes")
			}
			if *spec.resource != test.want {
				t.Errorf("resourceAttributes = %+v, want %+v", *spec.resource, test.want)
			}
		})
	}
}

func TestDecodeProtobufReviewRejects(t *testing.T) {
	t.Parallel()
	review := protobufReview(protoAttributes{verb: "list", resource: "namespaces"})
	tests := []struct {
		name string
		body []byte
	}{
		{name: "no prefix", body: review[len(protobufPrefix):]},
		{name: "json body", body: []byte(`{"spec":{}}`)},
		{name: "empty body", body: nil},
		{name: "truncated envelope", body: review[:len(review)-4]},
		{name: "truncated prefix", body: protobufPrefix[:3]},
		{
			name: "content encoding",
			body: concat(protobufEnvelope(nil), stringField(3, "gzip")),
		},
		{
			name: "wrong wire type",
			body: concat(protobufPrefix, varintField(2, 1)),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := decodeProtobufReview(test.body); err == nil {
				t.Error("decode the review returns no error")
			}
		})
	}
}

func TestProtobufFieldsSkipsUnknown(t *testing.T) {
	t.Parallel()
	message := concat(
		varintField(1, 300),
		fixed64Field(2, 7),
		stringField(3, "skip this"),
		fixed32Field(4, 9),
		bytesField(5, concat(varintField(1, 1), stringField(2, "nested"))),
	)

	var got string
	err := fields(message, 0, func(number, wire int, r *protobufReader) (bool, error) {
		if number != 5 {
			return false, nil
		}
		value, err := r.bytes(wire)
		if err != nil {
			return false, err
		}
		return true, fields(value, 1, func(number, wire int, r *protobufReader) (bool, error) {
			if number != 2 {
				return false, nil
			}
			text, textErr := r.text(wire)
			got = text
			return true, textErr
		})
	})
	if err != nil {
		t.Fatalf("read the message: %v", err)
	}
	if got != "nested" {
		t.Errorf("the nested string = %q, want nested", got)
	}
}

func TestProtobufFieldsStops(t *testing.T) {
	t.Parallel()
	deep := stringField(1, "value")
	for range maxProtobufDepth + 1 {
		deep = bytesField(1, deep)
	}
	tests := []struct {
		name    string
		message []byte
	}{
		{name: "truncated varint", message: []byte{0x08, 0x80}},
		{name: "truncated value", message: []byte{0x0a, 0x05, 0x61}},
		{name: "truncated fixed 32", message: fixed32Field(1, 1)[:3]},
		{name: "truncated fixed 64", message: fixed64Field(1, 1)[:5]},
		{name: "field number zero", message: []byte{0x00, 0x01}},
		{name: "group wire type", message: []byte{0x0b}},
		{name: "nested too deep", message: deep},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := fields(test.message, 0, nestedReader(0))
			if err == nil {
				t.Error("fields returns no error")
			}
		})
	}
}

// nestedReader enters field 1 of every message, to reach the depth limit.
func nestedReader(depth int) func(number, wire int, r *protobufReader) (bool, error) {
	return func(number, wire int, r *protobufReader) (bool, error) {
		if number != 1 || wire != wireBytes {
			return false, nil
		}
		value, err := r.bytes(wire)
		if err != nil {
			return false, err
		}
		return true, fields(value, depth+1, nestedReader(depth+1))
	}
}

func TestIsProtobuf(t *testing.T) {
	t.Parallel()
	tests := []struct {
		contentType string
		want        bool
	}{
		{contentType: protobufContentType, want: true},
		{contentType: protobufContentType + ";charset=utf-8", want: true},
		{contentType: strings.ToUpper(protobufContentType), want: true},
		{contentType: jsonContentType, want: false},
		{contentType: "", want: false},
		{contentType: "application/", want: false},
	}
	for _, test := range tests {
		if got := isProtobuf(test.contentType); got != test.want {
			t.Errorf("isProtobuf(%q) = %v, want %v", test.contentType, got, test.want)
		}
	}
}
