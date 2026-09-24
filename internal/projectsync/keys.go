package projectsync

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// reservedDomains are the key prefixes of Rancher and of Kubernetes. Each
// covers its subdomains. The service never writes a key under one of them.
var reservedDomains = []string{"cattle.io", "kubernetes.io", "k8s.io"}

// qualifiedName is the name part of a Kubernetes label or annotation key.
var qualifiedName = regexp.MustCompile(`^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$`)

const (
	maxKeyNameLength   = 63
	maxKeyPrefixLength = 253
)

const (
	// managedLabelsKey and managedAnnotationsKey are the annotations that hold
	// the label keys and the annotation keys that the service wrote on a
	// namespace. The service removes a key of that list once the project of the
	// namespace no longer sets it. A key outside the list belongs to the tenant,
	// and the service never removes it.
	managedLabelsKey      = "drover-managed-labels"
	managedAnnotationsKey = "drover-managed-annotations"
)

// namespaceKeys returns the label keys and the annotation keys of a namespace
// that the sync reads.
func namespaceKeys(cfg Config) (labels, annotations []string) {
	labels = slices.Concat([]string{projectLabel}, cfg.Labels)
	annotations = slices.Concat([]string{managedLabelsKey, managedAnnotationsKey}, cfg.Annotations)
	if cfg.NameLabel != "" {
		labels = append(labels, cfg.NameLabel)
	}
	if cfg.NameAnnotation != "" {
		annotations = append(annotations, cfg.NameAnnotation)
	}
	return labels, annotations
}

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
	if slices.Contains([]string{managedLabelsKey, managedAnnotationsKey}, key) {
		return fmt.Errorf("the metadata key %q is reserved, because the service writes it itself", key)
	}
	prefix, name, hasPrefix := strings.Cut(key, "/")
	if !hasPrefix {
		prefix, name = "", key
	}
	if name == "" {
		return fmt.Errorf("the metadata key %q has no name after the prefix", key)
	}
	if len(name) > maxKeyNameLength || !qualifiedName.MatchString(name) {
		return fmt.Errorf("the metadata key %q has no valid name: at most %d characters, alphanumeric at both ends, with - _ . inside", key, maxKeyNameLength)
	}
	if hasPrefix && (prefix == "" || len(prefix) > maxKeyPrefixLength || strings.ContainsAny(prefix, " /")) {
		return fmt.Errorf("the metadata key %q has no valid prefix", key)
	}
	for _, domain := range reservedDomains {
		if prefix == domain || strings.HasSuffix(prefix, "."+domain) {
			return fmt.Errorf("the metadata key %q is under %s, and Rancher or Kubernetes owns that domain", key, domain)
		}
	}
	return nil
}
