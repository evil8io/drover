package projectsync

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// deniedPrefixes are the key prefixes of Rancher and of Kubernetes. The service
// never writes a key under one of them.
var deniedPrefixes = []string{"field.cattle.io/", "cattle.io/", "kubernetes.io/", "k8s.io/"}

const (
	// managedLabelsKey and managedAnnotationsKey are the annotations that hold
	// the label keys and the annotation keys that the service wrote on a
	// namespace. The service removes a key of that list once the project of the
	// namespace no longer sets it. A key outside the list belongs to the tenant,
	// and the service never removes it.
	managedLabelsKey      = "drover-managed-labels"
	managedAnnotationsKey = "drover-managed-annotations"
)

// ParseKeys splits a comma-separated list of label or annotation keys. A list
// without a key returns no key and no error.
func ParseKeys(list string) ([]string, error) {
	if strings.TrimSpace(list) == "" {
		return nil, nil
	}
	parts := strings.Split(list, ",")
	keys := make([]string, 0, len(parts))
	for _, part := range parts {
		key := strings.TrimSpace(part)
		if err := checkKey(key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func checkKey(key string) error {
	if key == "" {
		return errors.New("a metadata key is empty")
	}
	if strings.HasSuffix(key, "/") {
		return fmt.Errorf("the metadata key %q has no name after the prefix", key)
	}
	if slices.Contains([]string{managedLabelsKey, managedAnnotationsKey}, key) {
		return fmt.Errorf("the metadata key %q is reserved, because the service writes it itself", key)
	}
	for _, prefix := range deniedPrefixes {
		if strings.HasPrefix(key, prefix) {
			return fmt.Errorf("the metadata key %q is under %s, and Rancher or Kubernetes owns that prefix", key, prefix)
		}
	}
	return nil
}
