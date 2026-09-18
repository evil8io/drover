package filter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"
)

const (
	steveTimeout    = 30 * time.Second
	maxStevePages   = 100
	maxSteveBody    = 32 << 20
	maxCacheEntries = 1000
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

// allowedNamespaces returns the namespaces that the caller can get. A non-nil
// response is the 401 or 403 answer of Steve, for the caller.
func (s *service) allowedNamespaces(ctx context.Context, cluster string, header http.Header) ([]string, *http.Response, error) {
	auth := header.Get("Authorization")
	cookie := header.Get("Cookie")
	sum := sha256.Sum256([]byte(auth + "\n" + cookie))
	key := cluster + "\n" + hex.EncodeToString(sum[:])

	return s.cache.do(ctx, key, func() ([]string, *http.Response, error) {
		return s.fetchAllowed(ctx, cluster, auth, cookie)
	})
}

func (s *service) fetchAllowed(ctx context.Context, cluster, auth, cookie string) ([]string, *http.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, steveTimeout)
	defer cancel()

	names := make(map[string]struct{})
	token := ""
	for page := 0; page < maxStevePages; page++ {
		target := *s.upstream
		target.Path = "/k8s/clusters/" + cluster + "/v1/namespaces"
		if token != "" {
			target.RawQuery = url.Values{"continue": []string{token}}.Encode()
		}
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
		body, tooLarge, err := readLimited(resp.Body, maxSteveBody)
		_ = resp.Body.Close()
		if err != nil {
			return nil, nil, fmt.Errorf("allowed set response: %w", err)
		}
		if tooLarge {
			return nil, nil, fmt.Errorf("allowed set response is larger than %d bytes", maxSteveBody)
		}

		switch resp.StatusCode {
		case http.StatusOK:
		case http.StatusUnauthorized, http.StatusForbidden:
			resp.Body = io.NopCloser(bytes.NewReader(body))
			resp.ContentLength = int64(len(body))
			return nil, resp, nil
		default:
			return nil, nil, fmt.Errorf("allowed set request returned %s", resp.Status)
		}

		var collection steveCollection
		if err := json.Unmarshal(body, &collection); err != nil {
			return nil, nil, fmt.Errorf("allowed set response: %w", err)
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
		if collection.Continue == "" {
			return slices.Sorted(maps.Keys(names)), nil, nil
		}
		token = collection.Continue
	}
	return nil, nil, fmt.Errorf("allowed set has more than %d pages", maxStevePages)
}

type cacheEntry struct {
	names   []string
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
func (c *cache) do(ctx context.Context, key string, fetch func() ([]string, *http.Response, error)) ([]string, *http.Response, error) {
	for {
		c.mu.Lock()
		if entry, ok := c.entries[key]; ok && c.now().Before(entry.expires) {
			c.mu.Unlock()
			return entry.names, nil, nil
		}
		if done, ok := c.inflight[key]; ok {
			c.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
		}
		done := make(chan struct{})
		c.inflight[key] = done
		c.mu.Unlock()

		names, resp, err := fetch()

		c.mu.Lock()
		delete(c.inflight, key)
		if err == nil && resp == nil {
			if len(c.entries) > maxCacheEntries {
				c.removeExpired()
			}
			c.entries[key] = cacheEntry{names: names, expires: c.now().Add(c.ttl)}
		}
		c.mu.Unlock()
		close(done)
		return names, resp, err
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
