package telemetry

import "testing"

func TestParseEndpoint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		raw      string
		endpoint string
		secure   bool
	}{
		{"host and port", "host:4317", "host:4317", false},
		{"http URL", "http://host:4317", "host:4317", false},
		{"https URL", "https://host:4317", "host:4317", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			endpoint, secure, err := parseEndpoint(test.raw)
			if err != nil {
				t.Fatalf("parseEndpoint(%q): %v", test.raw, err)
			}
			if endpoint != test.endpoint {
				t.Errorf("endpoint = %q, want %q", endpoint, test.endpoint)
			}
			if secure != test.secure {
				t.Errorf("secure = %v, want %v", secure, test.secure)
			}
		})
	}
}
