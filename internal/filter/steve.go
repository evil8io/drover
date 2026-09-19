package filter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	steveTimeout    = 30 * time.Second
	maxStevePages   = 100
	maxSteveBody    = 32 << 20
	maxAllowedNames = 20000
	maxCacheEntries = 1000

	projectsPath = "/v3/projects"
	// projectLabel is the label of Rancher on a namespace of a project. Its
	// value is the part of the project id after the colon.
	projectLabel = "field.cattle.io/projectId"
)

// steveCollection is the part of a Steve collection that the allowed set needs.
type steveCollection struct {
	Continue string `json:"continue"`
	Data     []struct {
		ID       string `json:"id"`
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
	} `json:"data"`
}

// projectCollection is the part of a Rancher project list that the allowed
// set needs.
type projectCollection struct {
	Data []struct {
		ID string `json:"id"`
	} `json:"data"`
}

// allowedSet is the cached view of one caller: the namespace names it may
// list, and the ids of the projects it may see.
type allowedSet struct {
	names    []string
	projects []string
}

// allowedNamespaces returns the namespaces that the caller can get. A non-nil
// response is the 401 or 403 answer of Steve, for the caller.
func (s *Service) allowedNamespaces(ctx context.Context, cluster string, header http.Header) ([]string, *http.Response, error) {
	set, denied, err := s.allowed(ctx, cluster, header)
	return set.names, denied, err
}

// allowedNamespace reports whether the caller may see the namespace: its name
// is in the cached name set, or its project label matches one of the cached
// project ids of the caller. A cache error or a denial answers false.
func (s *Service) allowedNamespace(ctx context.Context, cluster string, header http.Header, name string, labels map[string]string) bool {
	set, denied, err := s.allowed(ctx, cluster, header)
	if denied != nil {
		_ = denied.Body.Close()
	}
	if denied != nil || err != nil {
		return false
	}
	if slices.Contains(set.names, name) {
		return true
	}
	project := labels[projectLabel]
	return project != "" && slices.Contains(set.projects, project)
}

// allowed returns the cached allowed set of the caller. A non-nil response is
// the 401 or 403 answer of Steve or of the project list, for the caller.
func (s *Service) allowed(ctx context.Context, cluster string, header http.Header) (allowedSet, *http.Response, error) {
	auth := header.Get("Authorization")
	cookie := header.Get("Cookie")
	sum := sha256.Sum256([]byte(auth + "\n" + cookie))
	key := cluster + "\n" + hex.EncodeToString(sum[:])

	return s.cache.do(ctx, key, func() (allowedSet, *http.Response, error) {
		return s.fetchAllowed(ctx, cluster, auth, cookie)
	})
}

// fetchAllowed reads the namespace names and the project ids of the caller,
// over Steve and the Rancher project API.
func (s *Service) fetchAllowed(ctx context.Context, cluster, auth, cookie string) (allowedSet, *http.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, steveTimeout)
	defer cancel()

	names, denied, err := s.fetchNamespaceNames(ctx, cluster, auth, cookie)
	if denied != nil || err != nil {
		return allowedSet{}, denied, err
	}

	projects, denied, err := s.fetchProjectIDs(ctx, auth, cookie)
	if denied != nil || err != nil {
		return allowedSet{}, denied, err
	}

	return allowedSet{names: names, projects: projects}, nil, nil
}

func (s *Service) fetchNamespaceNames(ctx context.Context, cluster, auth, cookie string) ([]string, *http.Response, error) {
	names := make(map[string]struct{})
	token := ""
	for page := 0; page < maxStevePages; page++ {
		target := *s.upstream
		target.Path = "/k8s/clusters/" + cluster + "/v1/namespaces"
		query := url.Values{"exclude": []string{"metadata.managedFields"}}
		if token != "" {
			query.Set("continue", token)
		}
		target.RawQuery = query.Encode()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
		if err != nil {
			return nil, nil, err
		}
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		if cookie != "" {
			req.Header.Set("Cookie", cookie)
		}
		req.Header.Set("Accept", "application/json")

		resp, err := s.base.RoundTrip(req)
		if err != nil {
			return nil, nil, fmt.Errorf("allowed set request failed: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			body, tooLarge, err := readLimited(resp.Body, maxSteveBody)
			_ = resp.Body.Close()
			if err != nil {
				return nil, nil, fmt.Errorf("allowed set response: %w", err)
			}
			if tooLarge {
				return nil, nil, fmt.Errorf("allowed set response is larger than %d bytes", maxSteveBody)
			}
			switch resp.StatusCode {
			case http.StatusUnauthorized, http.StatusForbidden:
				resp.Body = io.NopCloser(bytes.NewReader(body))
				resp.ContentLength = int64(len(body))
				return nil, resp, nil
			default:
				return nil, nil, fmt.Errorf("allowed set request returned %s", resp.Status)
			}
		}

		var collection steveCollection
		decodeErr := json.NewDecoder(io.LimitReader(resp.Body, maxSteveBody)).Decode(&collection)
		_ = resp.Body.Close()
		if decodeErr != nil {
			if errors.Is(decodeErr, io.ErrUnexpectedEOF) {
				return nil, nil, fmt.Errorf("allowed set response is larger than %d bytes", maxSteveBody)
			}
			return nil, nil, fmt.Errorf("allowed set response: %w", decodeErr)
		}
		for _, item := range collection.Data {
			name := item.Metadata.Name
			if name == "" {
				name = item.ID
			}
			if name != "" {
				names[name] = struct{}{}
			}
		}
		if len(names) > maxAllowedNames {
			return nil, nil, fmt.Errorf("allowed set has more than %d names", maxAllowedNames)
		}
		if collection.Continue == "" {
			return slices.Sorted(maps.Keys(names)), nil, nil
		}
		token = collection.Continue
	}
	return nil, nil, fmt.Errorf("allowed set has more than %d pages", maxStevePages)
}

// fetchProjectIDs reads the projects that the caller may see, as the part of
// the project id after the colon. A non-nil response is the 401 or 403 answer
// of Rancher, for the caller.
func (s *Service) fetchProjectIDs(ctx context.Context, auth, cookie string) ([]string, *http.Response, error) {
	target := *s.upstream
	target.Path = projectsPath

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, nil, err
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	if cookie != "" {
		req.Header.Set("Cookie", cookie)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := s.base.RoundTrip(req)
	if err != nil {
		return nil, nil, fmt.Errorf("project list request failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		body, tooLarge, err := readLimited(resp.Body, maxSteveBody)
		_ = resp.Body.Close()
		if err != nil {
			return nil, nil, fmt.Errorf("project list response: %w", err)
		}
		if tooLarge {
			return nil, nil, fmt.Errorf("project list response is larger than %d bytes", maxSteveBody)
		}
		switch resp.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			resp.Body = io.NopCloser(bytes.NewReader(body))
			resp.ContentLength = int64(len(body))
			return nil, resp, nil
		default:
			return nil, nil, fmt.Errorf("project list request returned %s", resp.Status)
		}
	}

	var collection projectCollection
	decodeErr := json.NewDecoder(io.LimitReader(resp.Body, maxSteveBody)).Decode(&collection)
	_ = resp.Body.Close()
	if decodeErr != nil {
		if errors.Is(decodeErr, io.ErrUnexpectedEOF) {
			return nil, nil, fmt.Errorf("project list response is larger than %d bytes", maxSteveBody)
		}
		return nil, nil, fmt.Errorf("project list response: %w", decodeErr)
	}
	ids := make(map[string]struct{}, len(collection.Data))
	for _, item := range collection.Data {
		id := item.ID
		if _, after, ok := strings.Cut(id, ":"); ok {
			id = after
		}
		if id != "" {
			ids[id] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(ids)), nil, nil
}

type cacheEntry struct {
	set     allowedSet
	expires time.Time
}

type cache struct {
	ttl      time.Duration
	now      func() time.Time
	mu       sync.Mutex
	entries  map[string]cacheEntry
	inflight map[string]chan struct{}
}

func newCache(ttl time.Duration, now func() time.Time) *cache {
	return &cache{
		ttl:      ttl,
		now:      now,
		entries:  make(map[string]cacheEntry),
		inflight: make(map[string]chan struct{}),
	}
}

// do returns the cached set for the key. Concurrent misses of one key do one
// fetch. An error and a response for the caller are not cached.
func (c *cache) do(ctx context.Context, key string, fetch func() (allowedSet, *http.Response, error)) (allowedSet, *http.Response, error) {
	for {
		c.mu.Lock()
		if entry, ok := c.entries[key]; ok && c.now().Before(entry.expires) {
			c.mu.Unlock()
			return entry.set, nil, nil
		}
		if done, ok := c.inflight[key]; ok {
			c.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return allowedSet{}, nil, ctx.Err()
			}
		}
		done := make(chan struct{})
		c.inflight[key] = done
		c.mu.Unlock()

		set, resp, err := fetch()

		c.mu.Lock()
		delete(c.inflight, key)
		if err == nil && resp == nil {
			if len(c.entries) > maxCacheEntries {
				c.removeExpired()
			}
			c.entries[key] = cacheEntry{set: set, expires: c.now().Add(c.ttl)}
		}
		c.mu.Unlock()
		close(done)
		return set, resp, err
	}
}

func (c *cache) removeExpired() {
	now := c.now()
	for key, entry := range c.entries {
		if !now.Before(entry.expires) {
			delete(c.entries, key)
		}
	}
}
