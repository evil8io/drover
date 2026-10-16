package projectsync

import (
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
)

// reservedDomains are the key prefixes of Rancher and of Kubernetes. Each
// covers its subdomains. The service never writes a key under one of them.
var reservedDomains = []string{"cattle.io", "kubernetes.io", "k8s.io"}

// qualifiedName is the name part of a Kubernetes label or annotation key.
var qualifiedName = regexp.MustCompile(`^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$`)

// dnsSubdomain is the prefix part of a Kubernetes label or annotation key.
var dnsSubdomain = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

const (
	maxKeyNameLength   = 63
	maxKeyPrefixLength = 253
)

const (
	// managedDomain is the domain of every key that the service writes on a
	// Kubernetes object. An admission policy of the chart protects the keys
	// under managedPrefix on every kind. A key under another prefix has no
	// protection, so the service writes no such key.
	managedDomain = "managed.drover.evil8.io"
	managedPrefix = managedDomain + "/"
)

const (
	// managedLabelsKey and managedAnnotationsKey are the annotations that hold
	// the label keys and the annotation keys that the service wrote on a
	// namespace. The service removes a key of that list once the project of the
	// namespace no longer sets it. A key outside the list belongs to the tenant,
	// and the service never removes it.
	managedLabelsKey      = managedPrefix + "synced-labels"
	managedAnnotationsKey = managedPrefix + "synced-annotations"
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
	annotations = append(annotations, projectAnnotation)
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

// checkKey returns an error when key is not a valid key for a copy list or
// a name key. Such a key is a valid Kubernetes key outside the reserved
// domains and outside managedDomain.
func checkKey(key string) error {
	prefix, err := checkKeySyntax(key)
	if err != nil {
		return err
	}
	if underDomain(prefix, managedDomain) {
		return fmt.Errorf("the metadata key %q is under %s, and the service writes its own keys under that domain", key, managedDomain)
	}
	return nil
}

// checkKeySyntax returns the prefix of key, and an error when key is not a
// valid Kubernetes label or annotation key, or when key is under a reserved
// domain. The prefix of a key without a slash is empty.
func checkKeySyntax(key string) (string, error) {
	if key == "" {
		return "", errors.New("a metadata key is empty")
	}
	prefix, name, hasPrefix := strings.Cut(key, "/")
	if !hasPrefix {
		prefix, name = "", key
	}
	if name == "" {
		return "", fmt.Errorf("the metadata key %q has no name after the prefix", key)
	}
	if len(name) > maxKeyNameLength || !qualifiedName.MatchString(name) {
		return "", fmt.Errorf("the metadata key %q has no valid name: at most %d characters, alphanumeric at both ends, with - _ . inside", key, maxKeyNameLength)
	}
	if hasPrefix && (len(prefix) > maxKeyPrefixLength || !dnsSubdomain.MatchString(prefix)) {
		return "", fmt.Errorf("the metadata key %q has no valid prefix: a DNS subdomain in lower case, at most %d characters", key, maxKeyPrefixLength)
	}
	for _, domain := range reservedDomains {
		if underDomain(prefix, domain) {
			return "", fmt.Errorf("the metadata key %q is under %s, and Rancher or Kubernetes owns that domain", key, domain)
		}
	}
	return prefix, nil
}

// underDomain reports whether prefix is domain or a subdomain of domain.
func underDomain(prefix, domain string) bool {
	return prefix == domain || strings.HasSuffix(prefix, "."+domain)
}

// checkRecord returns an error when the ownership record of keys and nameKey
// is longer than maxRecordValue. The prune drops a longer record, and the
// service then never removes a key of it. kind names the keys in the error.
func checkRecord(kind string, keys []string, nameKey string) error {
	set := make(map[string]struct{}, len(keys)+1)
	for _, key := range keys {
		set[key] = struct{}{}
	}
	if nameKey != "" {
		set[nameKey] = struct{}{}
	}
	record := strings.Join(slices.Sorted(maps.Keys(set)), ",")
	if len(record) > maxRecordValue {
		return fmt.Errorf("the %s keys need an ownership record of %d bytes, and the limit is %d bytes", kind, len(record), maxRecordValue)
	}
	return nil
}
