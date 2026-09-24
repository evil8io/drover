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

	defaultMaxCacheEntries    = 1000
	defaultFetchRate          = 50
	defaultFetchRatePerCaller = 5
	fetchWaitCap              = 5 * time.Second

	// limitShared and limitCaller name the limit that refuses a fetch or a
	// watch slot: the limit of the service, or the limit of one caller.
	limitShared  = "shared"
	limitCaller  = "caller"
	noCredential = "none"

	projectsPath = "/v3/projects"
	// projectLabel is the label of Rancher on a namespace of a project. Its
	// value is the part of the project id after the colon.
	projectLabel = "field.cattle.io/projectId"
	// sessionCookie is the cookie that Rancher reads when the first
	// Authorization value is empty or absent.
	sessionCookie = "R_SESS"

	maxKnownClusters = 1024
)

// steveCollection is the part of a Steve collection that the allowed set needs.
type steveCollection struct {
	Continue string `json:"continue"`
	Data     []struct {
		ID       string `json:"id"`
		Metadata struct {
			Name   string            `json:"name"`
			Labels map[string]string `json:"labels"`
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

// allowedSet is the cached view of one caller. It has the namespace names
// the caller may list. It also has the ids of the projects that contain at
// least one of those names. Rancher grants project visibility and namespace
// access as two separate rights, so a visible project with no allowed
// namespace is not in this set. extras is the subset of names whose project
// label is not one of these project ids. user is the Rancher user id of the
// caller, from a SelfSubjectReview, or empty when the lookup failed.
type allowedSet struct {
	names    []string
	projects []string
	extras   []string
	user     string
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
// the 401 or 403 answer of Steve or of the project list, or the 429 answer of
// a fetch rate limit, for the caller.
func (s *Service) allowed(ctx context.Context, cluster string, header http.Header) (allowedSet, *http.Response, error) {
	auth := header.Get("Authorization")
	cookie := strings.Join(header.Values("Cookie"), "; ")
	caller := callerHash(header)
	key := cluster + "\n" + caller

	return s.cache.do(ctx, key, func() (allowedSet, *http.Response, error) {
		// The caller limit runs first, so a throttled caller takes no shared token.
		if throttled, err := s.waitFetch(ctx, s.callers.get(caller), limitCaller); throttled != nil || err != nil {
			return allowedSet{}, throttled, err
		}
		if throttled, err := s.waitFetch(ctx, s.limiter, limitShared); throttled != nil || err != nil {
			return allowedSet{}, throttled, err
		}
		return s.fetchAllowed(ctx, cluster, auth, cookie)
	})
}

// waitFetch takes one token of l, for the fetch rate limit named limit. It
// returns a 429 answer when l has no free token inside fetchWaitCap.
func (s *Service) waitFetch(ctx context.Context, l *limiter, limit string) (*http.Response, error) {
	err := l.wait(ctx, fetchWaitCap)
	if err == nil {
		return nil, nil
	}
	if !errors.Is(err, errFetchThrottled) {
		return nil, err
	}
	s.metrics.fetchThrottled(ctx, limit)
	message := serviceName + ": the fetch rate limit has no free token"
	if limit == limitCaller {
		message = serviceName + ": the fetch rate limit per caller has no free token"
	}
	return statusResponse(nil, http.StatusTooManyRequests, reasonThrottled, message), nil
}

// callerHash returns the sha256 hex of the credential key of the headers.
func callerHash(header http.Header) string {
	sum := sha256.Sum256([]byte(credentialKey(header)))
	return hex.EncodeToString(sum[:])
}

// credentialKey returns the credential that Rancher reads from the headers:
// the first Authorization value, or the first R_SESS cookie as net/http
// parses it when that value is empty. It returns a fixed key when the headers
// have neither. A second value and another cookie do not change the key, so
// one credential gives one key.
func credentialKey(header http.Header) string {
	if auth := header.Get("Authorization"); auth != "" {
		return "authorization\n" + auth
	}
	if session, err := (&http.Request{Header: header}).Cookie(sessionCookie); err == nil {
		return "cookie\n" + session.Value
	}
	return noCredential
}

// fetchAllowed reads the namespace names and the project ids of the caller,
// over Steve and the Rancher project API.
func (s *Service) fetchAllowed(ctx context.Context, cluster, auth, cookie string) (allowedSet, *http.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, steveTimeout)
	defer cancel()

	names, projectOf, denied, err := s.fetchNamespaceNames(ctx, cluster, auth, cookie)
	if denied != nil || err != nil {
		return allowedSet{}, denied, err
	}
	s.clusters.add(cluster)

	visible, denied, err := s.fetchProjectIDs(ctx, cluster, auth, cookie)
	if denied != nil || err != nil {
		return allowedSet{}, denied, err
	}
	projects := allowedProjects(names, projectOf, visible)

	user := s.fetchCallerName(ctx, cluster, auth, cookie)

	return allowedSet{
		names:    names,
		projects: projects,
		extras:   extraNames(names, projectOf, projects),
		user:     user,
	}, nil, nil
}

// allowedProjects returns the projects in visible that contain a namespace
// from names, by the project label in projectOf. The result is sorted with
// no duplicates.
func allowedProjects(names []string, projectOf map[string]string, visible []string) []string {
	set := make(map[string]struct{})
	for _, name := range names {
		project := projectOf[name]
		if project != "" && slices.Contains(visible, project) {
			set[project] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(set))
}

// extraNames returns the names among names whose project label, from
// projectOf, is not one of projects. A name with no project label counts as
// outside.
func extraNames(names []string, projectOf map[string]string, projects []string) []string {
	var extras []string
	for _, name := range names {
		project := projectOf[name]
		if project == "" || !slices.Contains(projects, project) {
			extras = append(extras, name)
		}
	}
	return extras
}

// fetchNamespaceNames reads the namespace names of the caller, and the
// project label of each name that has one.
func (s *Service) fetchNamespaceNames(ctx context.Context, cluster, auth, cookie string) ([]string, map[string]string, *http.Response, error) {
	names := make(map[string]struct{})
	projectOf := make(map[string]string)
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
			return nil, nil, nil, err
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
			return nil, nil, nil, fmt.Errorf("allowed set request failed: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			body, tooLarge, err := readLimited(resp.Body, maxSteveBody)
			_ = resp.Body.Close()
			if err != nil {
				return nil, nil, nil, fmt.Errorf("allowed set response: %w", err)
			}
			if tooLarge {
				return nil, nil, nil, fmt.Errorf("allowed set response is larger than %d bytes", maxSteveBody)
			}
			switch resp.StatusCode {
			case http.StatusUnauthorized, http.StatusForbidden:
				resp.Body = io.NopCloser(bytes.NewReader(body))
				resp.ContentLength = int64(len(body))
				return nil, nil, resp, nil
			default:
				return nil, nil, nil, fmt.Errorf("allowed set request returned %s", resp.Status)
			}
		}

		var collection steveCollection
		decodeErr := json.NewDecoder(io.LimitReader(resp.Body, maxSteveBody)).Decode(&collection)
		_ = resp.Body.Close()
		if decodeErr != nil {
			if errors.Is(decodeErr, io.ErrUnexpectedEOF) {
				return nil, nil, nil, fmt.Errorf("allowed set response is larger than %d bytes", maxSteveBody)
			}
			return nil, nil, nil, fmt.Errorf("allowed set response: %w", decodeErr)
		}
		for _, item := range collection.Data {
			name := item.Metadata.Name
			if name == "" {
				name = item.ID
			}
			if name != "" {
				names[name] = struct{}{}
				if project := item.Metadata.Labels[projectLabel]; project != "" {
					projectOf[name] = project
				}
			}
		}
		if len(names) > maxAllowedNames {
			return nil, nil, nil, fmt.Errorf("allowed set has more than %d names", maxAllowedNames)
		}
		if collection.Continue == "" {
			return slices.Sorted(maps.Keys(names)), projectOf, nil, nil
		}
		token = collection.Continue
	}
	return nil, nil, nil, fmt.Errorf("allowed set has more than %d pages", maxStevePages)
}

// fetchProjectIDs reads the projects of the cluster that the caller may see,
// as the part of the project id after the colon. The project label of a
// namespace has that part only, and the part is unique inside one cluster,
// so a project of another cluster must not reach the selector. The result is
// the full visible set. fetchAllowed keeps only the visible projects that
// contain an allowed namespace, because project visibility alone grants no
// namespace access. A non-nil response is the 401 or 403 answer of Rancher,
// for the caller.
func (s *Service) fetchProjectIDs(ctx context.Context, cluster, auth, cookie string) ([]string, *http.Response, error) {
	target := *s.upstream
	target.Path = projectsPath
	target.RawQuery = url.Values{"clusterId": []string{cluster}}.Encode()

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
		if id, ok := strings.CutPrefix(item.ID, cluster+":"); ok && id != "" {
			ids[id] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(ids)), nil, nil
}

// clusterSet is the bounded set of cluster ids that Steve answered a list
// for. A metric attribute takes a cluster id from this set only, because the
// path of a request names any string, also before authentication.
type clusterSet struct {
	mu  sync.Mutex
	ids map[string]struct{}
}

func newClusterSet() *clusterSet {
	return &clusterSet{ids: make(map[string]struct{})}
}

func (c *clusterSet) add(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.ids) < maxKnownClusters {
		c.ids[id] = struct{}{}
	}
}

// attribute returns id when the set has it, and unknown otherwise.
func (c *clusterSet) attribute(id string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.ids[id]; ok {
		return id
	}
	return "unknown"
}

// selfSubjectReviewBody is the fixed SelfSubjectReview request that resolves
// the Rancher user id of the caller. Kubernetes 1.28 and later serve the
// resource. The built-in system:basic-user role lets every authenticated
// caller create it.
const selfSubjectReviewBody = `{"apiVersion":"authentication.k8s.io/v1","kind":"SelfSubjectReview"}`

// fetchCallerName reads the Rancher user id of the caller, through a
// SelfSubjectReview. The lookup never fails the allowed set fetch. Each of
// these leaves the name empty, with one debug line for the reason:
//
//   - a transport error
//   - a status other than 200 or 201
//   - a body that does not parse
//   - an empty username
func (s *Service) fetchCallerName(ctx context.Context, cluster, auth, cookie string) string {
	target := *s.upstream
	target.Path = "/k8s/clusters/" + cluster + "/apis/authentication.k8s.io/v1/selfsubjectreviews"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), strings.NewReader(selfSubjectReviewBody))
	if err != nil {
		s.logger.DebugContext(ctx, "caller identity lookup", "cluster", cluster, "reason", err.Error())
		return ""
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	if cookie != "" {
		req.Header.Set("Cookie", cookie)
	}
	req.Header.Set("Content-Type", jsonContentType)
	req.Header.Set("Accept", jsonContentType)

	resp, err := s.base.RoundTrip(req)
	if err != nil {
		s.logger.DebugContext(ctx, "caller identity lookup", "cluster", cluster, "reason", err.Error())
		return ""
	}
	body, tooLarge, err := readLimited(resp.Body, maxSteveBody)
	_ = resp.Body.Close()
	if err != nil {
		s.logger.DebugContext(ctx, "caller identity lookup", "cluster", cluster, "reason", err.Error())
		return ""
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		s.logger.DebugContext(ctx, "caller identity lookup", "cluster", cluster, "reason", "status "+resp.Status)
		return ""
	}
	if tooLarge {
		s.logger.DebugContext(ctx, "caller identity lookup", "cluster", cluster, "reason", "the response is larger than the limit")
		return ""
	}

	var review struct {
		Status struct {
			UserInfo struct {
				Username string `json:"username"`
			} `json:"userInfo"`
		} `json:"status"`
	}
	if err := json.Unmarshal(body, &review); err != nil {
		s.logger.DebugContext(ctx, "caller identity lookup", "cluster", cluster, "reason", err.Error())
		return ""
	}
	if review.Status.UserInfo.Username == "" {
		s.logger.DebugContext(ctx, "caller identity lookup", "cluster", cluster, "reason", "the answer has no username")
		return ""
	}
	return review.Status.UserInfo.Username
}

type cacheEntry struct {
	set     allowedSet
	expires time.Time
}

type cache struct {
	ttl        time.Duration
	maxEntries int
	now        func() time.Time
	mu         sync.Mutex
	entries    map[string]cacheEntry
	inflight   map[string]chan struct{}
}

func newCache(ttl time.Duration, maxEntries int, now func() time.Time) *cache {
	return &cache{
		ttl:        ttl,
		maxEntries: maxEntries,
		now:        now,
		entries:    make(map[string]cacheEntry),
		inflight:   make(map[string]chan struct{}),
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
			c.removeExpired()
			for len(c.entries) > 0 && len(c.entries) >= c.maxEntries {
				c.evictEarliest()
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

// evictEarliest deletes the cache entry with the earliest expires. The
// entries map has at least one entry.
func (c *cache) evictEarliest() {
	var oldestKey string
	var oldest time.Time
	first := true
	for key, entry := range c.entries {
		if first || entry.expires.Before(oldest) {
			oldestKey, oldest = key, entry.expires
			first = false
		}
	}
	delete(c.entries, oldestKey)
}

// errFetchThrottled marks a wait that ends with no free token inside the cap.
var errFetchThrottled = errors.New("fetch rate limit: no free token")

// limiter is a token bucket that bounds the rate of a fetch. It is
// hand-written, because the module has no external rate package.
type limiter struct {
	rate  float64 // tokens added per second
	burst float64 // maximum tokens held
	now   func() time.Time

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

// newLimiter returns a limiter with a full bucket of burst tokens.
func newLimiter(rate, burst float64, now func() time.Time) *limiter {
	return &limiter{rate: rate, burst: burst, now: now, tokens: burst, last: now()}
}

// wait takes one token. When no token is free, it blocks up to waitCap,
// against ctx. A nil error means the wait took a token.
func (l *limiter) wait(ctx context.Context, waitCap time.Duration) error {
	l.mu.Lock()
	l.refill()
	if l.tokens >= 1 {
		l.tokens--
		l.mu.Unlock()
		return nil
	}
	need := time.Duration((1 - l.tokens) / l.rate * float64(time.Second))
	l.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	if need > waitCap {
		return errFetchThrottled
	}

	timer := time.NewTimer(need)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return ctx.Err()
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l.refill()
	if l.tokens < 1 {
		return errFetchThrottled
	}
	l.tokens--
	return nil
}

// refill adds the tokens that the time since the last refill earns, up to
// burst. The caller holds mu.
func (l *limiter) refill() {
	now := l.now()
	if elapsed := now.Sub(l.last); elapsed > 0 {
		l.tokens = min(l.burst, l.tokens+elapsed.Seconds()*l.rate)
		l.last = now
	}
}

// lastRefill returns the time of the last refill.
func (l *limiter) lastRefill() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.last
}

// callerLimiters has one fetch limiter per caller credential, keyed on the
// callerHash of the headers. The set has at most maxEntries limiters.
type callerLimiters struct {
	rate       float64
	burst      float64
	maxEntries int
	now        func() time.Time

	mu       sync.Mutex
	limiters map[string]*limiter
}

func newCallerLimiters(rate, burst float64, maxEntries int, now func() time.Time) *callerLimiters {
	return &callerLimiters{
		rate:       rate,
		burst:      burst,
		maxEntries: maxEntries,
		now:        now,
		limiters:   make(map[string]*limiter),
	}
}

// get returns the limiter of the caller key. A new key gets a full bucket.
// At the bound, the limiter with the oldest refill goes first.
func (c *callerLimiters) get(key string) *limiter {
	c.mu.Lock()
	defer c.mu.Unlock()
	if l, ok := c.limiters[key]; ok {
		return l
	}
	for len(c.limiters) > 0 && len(c.limiters) >= c.maxEntries {
		c.evictOldest()
	}
	l := newLimiter(c.rate, c.burst, c.now)
	c.limiters[key] = l
	return l
}

// evictOldest deletes the limiter with the oldest refill. The caller holds
// mu, and the limiters map has at least one entry.
func (c *callerLimiters) evictOldest() {
	var oldestKey string
	var oldest time.Time
	first := true
	for key, l := range c.limiters {
		if last := l.lastRefill(); first || last.Before(oldest) {
			oldestKey, oldest = key, last
			first = false
		}
	}
	delete(c.limiters, oldestKey)
}
