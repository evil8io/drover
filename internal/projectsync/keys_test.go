package projectsync

import (
	"net/url"
	"slices"
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
