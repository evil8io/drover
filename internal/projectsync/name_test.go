package projectsync

import (
	"strings"
	"testing"
)

func TestSanitizeLabelValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "no change needed", in: "cost-center-team", want: "cost-center-team"},
		{name: "a space", in: "My Project", want: "My-Project"},
		{name: "a leading digit", in: "9lives", want: "9lives"},
		{name: "longer than 63 characters", in: strings.Repeat("a", 70), want: strings.Repeat("a", 63)},
		{name: "sanitises to an empty value", in: "!!!", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := sanitizeLabelValue(test.in); got != test.want {
				t.Errorf("sanitizeLabelValue(%q) = %q, want %q", test.in, got, test.want)
			}
		})
	}
}
