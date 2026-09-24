# api-filter

Human doc: `docs/api-filter.md`. Update it with a behaviour change.

## Transport

- The retry logic is an `http.RoundTripper` under `httputil.ReverseProxy`, with `FlushInterval` -1 and HTTP/1.1 only to the upstream, because a websocket upgrade needs HTTP/1.1. Keep `ForceAttemptHTTP2` off and `TLSNextProto` empty in `internal/rancherclient`.
- The upstream is the in-cluster Rancher Service, never the public hostname, because the route sends the filtered paths back to the filter.
- Strip `Accept-Encoding` from a request whose answer the filter parses, because the filter reads the upstream body as plain JSON.
- `steveTimeout` in `steve.go` is 30 s. A tenant read that hangs 30 s and returns 502 with `allowed set request failed: context deadline exceeded` means that Rancher is down or cold, for example after an OOMKill while its Steve cache fills. It does not mean that the fan-out is slow. Check the Rancher pods first, then `duration_ms` per outcome.

## Allowed set

- Steve (`/k8s/clusters/<id>/v1/namespaces`) returns the RBAC-filtered list in its own shape, without `labelSelector` and without watch. It is the source of the allowed set only.
- The cache key is the cluster plus the credential that Rancher reads: the `Authorization` header, else the `R_SESS` cookie. Another cookie must not enter the key.
- The caller name comes from `POST .../selfsubjectreviews` with the caller credentials, cached in `allowedSet.user`, and it fails open. Put it in a log line and a span only, never in a metric attribute, because the value set is unbounded.
- Rancher accepts a ServiceAccount JWT on `/k8s/clusters/<id>/` only, per cluster, behind a `ClusterProxyConfig` with `enabled: true` (Rancher 2.9.0 and later).
- A ServiceAccount JWT caller gets 401 from Steve, because the Steve cluster proxy runs before the ServiceAccount authenticator in the Rancher handler chain, and the filter returns that 401. An RBAC leg through a `SelfSubjectRulesReview` is the open design for that caller. Do not derive its project from a label on the ServiceAccount namespace, because a project owner can set that label.

## Watch

- The watch selector is exact only: match-nothing for an empty set, the project selector when every allowed namespace is in a project of the caller, and no selector otherwise. Never build a union of names and projects, because a cluster-wide `get namespaces` binding puts nearly every namespace into the extra set.
- The timer per watch, `endOnSelectorChange` in `namespaces.go`, compares the selector, not the project set. A `get` on one namespace outside the projects of the caller changes the selector case without a project change, and a project-set comparison held such a watch open for the full stream lifetime. Live check: a Role and a RoleBinding with `get` on `namespaces` in a namespace of a second project; the stream ends within about 5 s.
- Every watch transport passes `eventAllowed`. Version 0.8.0 filtered a 200 stream and let a 101 upgrade through without a filter, and a test asserted the wrong thing. A websocket message from the API server is a slice of at most 2048 bytes of the newline-delimited stream, never one event, so `websocket.go` buffers the bytes and splits the events with a `json.Decoder` and `InputOffset`.
- Strip `Sec-WebSocket-Extensions` from the privileged request, and end a stream whose 101 still names an extension, because the filter reads no compressed frame.
- Verified clients: kubectl, k9s, OpenLens, Headlamp (websocket), and Aptakube (chunked). No integration test drives the websocket path, so a change to `websocket.go` needs a live check with Headlamp.
- Aptakube fills its namespace picker once per connection, and it re-watches without a list when a stream ends. A picker that disagrees with the Namespaces view is not a filter defect. Reconnect Aptakube first.

## Access review

- kubectl and client-go send the SelfSubjectAccessReview as `application/vnd.kubernetes.protobuf`. `protobuf.go` decodes it, and the answer goes back as JSON.
- kubectl sends the namespace of the kubeconfig context in a review for the cluster-scoped `namespaces` resource. Ignore that attribute.
- Grant on the spec that the API server echoes in its answer, never on the request body alone, because the two parsers differ on key case.

## Fan-out

- The merged answer writes `metadata` after the elements, and its `resourceVersion` is the lowest of the answers, because a watch from the lowest value loses no event. An earlier version wrote the highest and lost the events of a namespace listed at a lower revision. The value is known at the end of the stream only.
- The dispatcher takes a concurrency slot before it starts a request, and it starts the requests in name order, so the in-order reader never waits for a request that has no slot. That order is the deadlock fix. Keep it.
- A 404 from one namespace stops the whole fan-out, because a cluster-scoped kind answers 404 in every namespace. `kubectl get nodes` then costs at most the concurrency plus one requests.
- A caller that may see no object gets an empty collection, not the native 403. The kind then comes from the discovery document with the caller credentials, because no answer names it. A Rancher user with zero projects reaches Steve and gets an empty collection, so that caller takes this path too.
- `kubectl netshoot run` warns `couldn't attach` on a direct cluster connection too. It is not a routing defect.
