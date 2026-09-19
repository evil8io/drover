package projectsync

import "strings"

// maxLabelValueLength is the length limit of a Kubernetes label value.
const maxLabelValueLength = 63

// sanitizeLabelValue turns a project display name into a valid Kubernetes
// label value. It replaces every character outside [A-Za-z0-9._-] with -,
// truncates the result to 63 characters, then trims every leading and
// trailing character that is not alphanumeric. The result is empty when the
// name has no alphanumeric character within the limit.
func sanitizeLabelValue(name string) string {
	var b strings.Builder
	for _, r := range name {
		if isLabelValueChar(r) {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	value := b.String()
	if len(value) > maxLabelValueLength {
		value = value[:maxLabelValueLength]
	}
	return strings.TrimFunc(value, func(r rune) bool { return !isAlphanumeric(r) })
}

func isLabelValueChar(r rune) bool {
	return r == '-' || r == '.' || r == '_' || isAlphanumeric(r)
}

func isAlphanumeric(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}
