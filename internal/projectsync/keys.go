package projectsync

import (
	"errors"
	"fmt"
	"strings"
)

// deniedPrefixes are the key prefixes of Rancher and of Kubernetes. The service
// never writes a key under one of them.
var deniedPrefixes = []string{"field.cattle.io/", "cattle.io/", "kubernetes.io/", "k8s.io/"}

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
	for _, prefix := range deniedPrefixes {
		if strings.HasPrefix(key, prefix) {
			return fmt.Errorf("the metadata key %q is under %s, and Rancher or Kubernetes owns that prefix", key, prefix)
		}
	}
	return nil
}
