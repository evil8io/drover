package projectsync

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
	"testing"
)

func TestParseKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		list    string
		want    []string
		wantErr bool
	}{
		{name: "one key", list: "cost-center", want: []string{"cost-center"}},
		{name: "two keys with a space", list: "cost-center, example.com/owner",
			want: []string{"cost-center", "example.com/owner"}},
		{name: "no key", list: ""},
		{name: "space only", list: "  "},
		{name: "an empty key between two keys", list: "cost-center,,owner", wantErr: true},
		{name: "a comma at the end", list: "cost-center,", wantErr: true},
		{name: "a prefix without a name", list: "example.com/", wantErr: true},
		{name: "the project id label", list: "field.cattle.io/projectId", wantErr: true},
		{name: "a Rancher key", list: "cattle.io/creator", wantErr: true},
		{name: "a Kubernetes key", list: "kubernetes.io/metadata.name", wantErr: true},
		{name: "a k8s.io key", list: "k8s.io/cluster-name", wantErr: true},
		{name: "a denied key after a valid key", list: "cost-center,kubernetes.io/metadata.name", wantErr: true},
		{name: "the managed labels annotation", list: managedLabelsKey, wantErr: true},
		{name: "the managed annotations annotation", list: managedAnnotationsKey, wantErr: true},
		{name: "a subdomain of kubernetes.io", list: "pod-security.kubernetes.io/enforce", wantErr: true},
		{name: "a subdomain of cattle.io", list: "management.cattle.io/project", wantErr: true},
		{name: "a subdomain of k8s.io", list: "topology.k8s.io/zone", wantErr: true},
		{name: "a domain that only ends in the reserved text", list: "mykubernetes.io/owner",
			want: []string{"mykubernetes.io/owner"}},
		{name: "a name with a space", list: "cost center", wantErr: true},
		{name: "a name that ends in a dash", list: "cost-", wantErr: true},
		{name: "a name above 63 characters", list: strings.Repeat("a", 64), wantErr: true},
		{name: "a prefix with a second slash", list: "example.com/team/owner", wantErr: true},
		{name: "a prefix with an upper-case letter", list: "Example.com/team", wantErr: true},
		{name: "a prefix with an underscore", list: "example_com/team", wantErr: true},
		{name: "a prefix that starts with a dot", list: ".example.com/team", wantErr: true},
		{name: "a prefix with an empty label", list: "example..com/team", wantErr: true},
		{name: "a prefix that ends with a dash", list: "example.com-/team", wantErr: true},
		{name: "a name with the allowed inner characters", list: "example.com/Owner_1.a-b",
			want: []string{"example.com/Owner_1.a-b"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			keys, err := ParseKeys(test.list)
			if test.wantErr {
				if err == nil {
					t.Fatalf("ParseKeys(%q) returned no error", test.list)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseKeys(%q): %v", test.list, err)
			}
			if !slices.Equal(keys, test.want) {
				t.Errorf("ParseKeys(%q) = %v, want %v", test.list, keys, test.want)
			}
		})
	}
}

func TestNewChecksTheKeys(t *testing.T) {
	t.Parallel()
	target, err := url.Parse("https://rancher.example.com")
	if err != nil {
		t.Fatalf("parse the URL: %v", err)
	}

	tests := []struct {
		name        string
		labels      []string
		annotations []string
	}{
		{name: "no key"},
		{name: "a denied label key", labels: []string{"field.cattle.io/projectId"}},
		{name: "a denied annotation key", annotations: []string{"cattle.io/creator"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(Config{
				RancherURL:  target,
				TokenFile:   tokenFile(t, serviceToken),
				Labels:      test.labels,
				Annotations: test.annotations,
			})
			if err == nil {
				t.Error("New returned no error")
			}
		})
	}
}

func TestNewAcceptsANameKeyAlone(t *testing.T) {
	t.Parallel()
	target, err := url.Parse("https://rancher.example.com")
	if err != nil {
		t.Fatalf("parse the URL: %v", err)
	}

	tests := []struct {
		name           string
		nameLabel      string
		nameAnnotation string
	}{
		{name: "a name label alone", nameLabel: "example.com/project-name"},
		{name: "a name annotation alone", nameAnnotation: "example.com/project-name"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(Config{
				RancherURL:     target,
				TokenFile:      tokenFile(t, serviceToken),
				NameLabel:      test.nameLabel,
				NameAnnotation: test.nameAnnotation,
			})
			if err != nil {
				t.Errorf("New returned an error: %v", err)
			}
		})
	}
}

// longKey returns a key of 317 characters, the longest valid key: a prefix
// of 253 characters, and a name of 63 characters.
func longKey(i int) string {
	prefix := fmt.Sprintf("k%02d.", i) + strings.Repeat("a", 62) + "." + strings.Repeat("b", 62) + "." +
		strings.Repeat("c", 62) + "." + strings.Repeat("d", 60)
	return prefix + "/" + strings.Repeat("n", 63)
}

// TestNewRejectsKeysWhoseRecordIsAboveTheBound checks that the ownership
// record of the keys fits in maxRecordValue. The prune drops a longer record,
// and the service then never removes a key.
func TestNewRejectsKeysWhoseRecordIsAboveTheBound(t *testing.T) {
	t.Parallel()
	target, err := url.Parse("https://rancher.example.com")
	if err != nil {
		t.Fatalf("parse the URL: %v", err)
	}
	if got := len(longKey(0)); got != 317 {
		t.Fatalf("length of the long key = %d, want 317", got)
	}
	keys := func(n int) []string {
		var out []string
		for i := range n {
			out = append(out, longKey(i))
		}
		return out
	}

	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{name: "12 label keys", cfg: Config{Labels: keys(12)}},
		{name: "13 label keys", cfg: Config{Labels: keys(13)}, wantErr: true},
		{name: "12 label keys and a name label", cfg: Config{Labels: keys(12), NameLabel: longKey(12)}, wantErr: true},
		{name: "12 annotation keys and a name annotation", cfg: Config{Annotations: keys(12), NameAnnotation: longKey(12)}, wantErr: true},
		{name: "12 label keys and 12 annotation keys", cfg: Config{Labels: keys(12), Annotations: keys(12)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg := test.cfg
			cfg.RancherURL = target
			cfg.TokenFile = tokenFile(t, serviceToken)
			_, err := New(cfg)
			if test.wantErr && err == nil {
				t.Error("New returned no error")
			}
			if !test.wantErr && err != nil {
				t.Errorf("New returned an error: %v", err)
			}
		})
	}
}
