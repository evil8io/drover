# api-filter

Human doc: `docs/api-filter.md`. Update it with a behaviour change.

## Transport

- The retry logic is an `http.RoundTripper` under `httputil.ReverseProxy`, with `FlushInterval` -1 and HTTP/1.1 only to the upstream, because a websocket upgrade needs HTTP/1.1. Keep `ForceAttemptHTTP2` off and `TLSNextProto` empty in `internal/rancherclient`.
- The upstream is the in-cluster Rancher Service, never the public hostname, because the route sends the filtered paths back to the filter.
- Strip `Accept-Encoding` from a request whose answer the filter parses, because the filter reads the upstream body as plain JSON.
- `steveTimeout` in `steve.go` is 30 s. A tenant read that hangs 30 s and returns 502 with `allowed set request failed: context deadline exceeded` means that Rancher is down or cold, for example after an OOMKill while its Steve cache fills. It does not mean that the fan-out is slow. Check the Rancher pods first, then `duration_ms` per outcome.

## Allowed set

- Steve (`/k8s/clusters/<id>/v1/namespaces`) returns the RBAC-filtered list in its own shape, without `labelSelector` and without watch. It is the source of the allowed set only.
- A project enters the allowed set only when it contains an allowed namespace. A custom role can show a project with no namespace right, so project visibility alone grants no namespace access.
- Key the allowed set on the cluster plus the first `Authorization` value, else the first `R_SESS` cookie as net/http parses it, because Rancher reads exactly that credential. A second header value or cookie must not create a second key, because every key costs a token of the shared fetch limit.
- Key the fetch limit per caller on the credential alone, without the cluster. One caller with many clusters is one caller, and a key per cluster multiplies its rate by the cluster count.
- The caller name comes from `POST .../selfsubjectreviews` with the caller credentials, cached in `allowedSet.user`, and it fails open. Put it in a log line and a span only, never in a metric attribute, because the value set is unbounded.
- Rancher accepts a ServiceAccount JWT on `/k8s/clusters/<id>/` only, per cluster, behind a `ClusterProxyConfig` with `enabled: true` (Rancher 2.9.0 and later). Steve answers 401 to that caller, because the Steve cluster proxy runs before the ServiceAccount authenticator in the Rancher handler chain.
- A bearer JWT whose `sub` starts with `system:serviceaccount:` takes the RBAC leg in `rulesreview.go`: a `SelfSubjectRulesReview` with the credentials of the caller, in the namespace of the ServiceAccount. Rancher applies the same test to the token. Keep the review in that namespace, because no tenant binds a role there, and a RoleBinding to `view` or `edit` in the review namespace shows `get` on `namespaces` without names. Do not derive the project of the caller from a label on the ServiceAccount namespace, because a project owner can set that label.
- A rule without `resourceNames`, or no rule that grants `get` on `namespaces`, is a denied set, and the caller gets the native 403, not an empty list. The cache keeps the denied set for one TTL, because an uncached denial costs one review per request and takes tokens of the shared fetch limit from every other caller.
- On a 401 or a 403 from the allowed-set fetch, answer the buffered native 403 of the request, through `denialResponse`. A client that gets a 401 for a valid token authenticates again, and the native answer has the RBAC message. Pass a 429 of the fetch limit through as is.
- No unit test drives a real ServiceAccount token through Rancher. A change to the RBAC leg needs a live check with kubectl and a ServiceAccount token, on a cluster with JWT authentication on.

## Watch

- The watch selector is exact only: match-nothing for an empty set, the project selector when every allowed namespace is in a project of the caller, and no selector otherwise. Never build a union of names and projects, because a cluster-wide `get namespaces` binding puts nearly every namespace into the extra set.
- Keep a namespace watch open across a change of the allowed set. Headlamp opens no new watch after a clean end of a stream, and its view then shows no change until a reload. `followAllowedSet` in `namespaces.go` writes a `DELETED` event per lost name, and it swaps the upstream of a watch with a selector when the selector changes. A swap replays the namespaces under the selector for every open stream of the caller, so it takes a token of the per-caller fetch limit. A watch without a selector gets one privileged `GET` and one `ADDED` event per gained name instead. A swap without a selector replays every namespace of the cluster.
- On a watch that takes no event of the service and has a project selector, read a lost namespace once before the 410 end. Keep the stream open when the read answers 404, or a project label outside the selector, because the upstream watch sent the `DELETED` event then. End it on every other answer, so the client keeps no stale namespace. Do not read for a watch that takes events of the service. Its `DELETED` event does no harm after the upstream event, and it covers an upstream event that the event filter dropped.
- Trigger the swap on a change of the selector, not of the project set. A `get` on one namespace outside the projects of the caller changes the selector case without a project change. A new namespace in a project of the caller keeps the selector, and the running upstream sends its event, so it needs no swap and no token. Live check: open the Namespaces view of Headlamp with an empty allowed set. Create the first namespace of a project of the caller. The namespace appears in the view within one cache TTL, without a reload.
- Reserve the watch slot under the registry mutex before the upstream request. Release it on every path that fails before `add` binds it to a stream. The registry counts reserved slots, not open streams, because a check before the request and a count after it let a parallel burst pass the limit. A leaked slot stays counted, so enough leaked slots answer 503 to every watch of every caller.
- Every watch transport passes `eventAllowed`. Version 0.8.0 filtered a 200 stream and let a 101 upgrade through without a filter, and a test asserted the wrong thing. The API server sends one event per text frame, as plain JSON under every subprotocol, also under `base64.binary.k8s.io`. A proxy can re-slice the stream or send base64 text, so decide the form per message and answer in the same form. Version 0.14.1 decoded every message under `base64.binary.k8s.io` as base64, and Headlamp got no event. Keep the stream buffer in `websocket.go` and the split of the events with a `json.Decoder` and `InputOffset`, because a re-sliced message is not one event.
- Strip `Sec-WebSocket-Extensions` from the privileged request, and end a stream whose 101 still names an extension, because the filter reads no compressed frame.
- Verified clients: kubectl, k9s, OpenLens, Aptakube (chunked), and Headlamp (websocket, on 0.14.2 against Kubernetes 1.35: a new namespace in the Namespaces view, and a new pod in the Pods view of all namespaces). No integration test drives the websocket path, so a change to `websocket.go` or to the merged websocket watch needs that live check with Headlamp again.
- Aptakube fills its namespace picker once per connection, and it re-watches without a list when a stream ends. A picker that disagrees with the Namespaces view is not a filter defect. Reconnect Aptakube first.

## Access review

- kubectl and client-go send the SelfSubjectAccessReview as `application/vnd.kubernetes.protobuf`. `protobuf.go` decodes it, and the answer goes back as JSON.
- kubectl sends the namespace of the kubeconfig context in a review for the cluster-scoped `namespaces` resource. Ignore that attribute.
- Grant on the spec that the API server echoes in its answer, never on the request body alone, because the two parsers differ on key case.

## Fan-out

- The merged answer writes `metadata` after the elements, and its `resourceVersion` is the lowest of the answers, because a watch from the lowest value loses no event. An earlier version wrote the highest and lost the events of a namespace listed at a lower revision. The value is known at the end of the stream only.
- The dispatcher takes a concurrency slot before it starts a request, and it starts the requests in name order, so the in-order reader never waits for a request that has no slot. That order is the deadlock fix. Keep it.
- The global in-flight cap waits in the dispatcher only, after the per-request slot, so the in-order reader never waits on a request without a slot.
- A gained namespace joins a running merged watch through an upstream watch without a `resourceVersion`, so the client gets the objects of that namespace as `ADDED` events. Do not end the stream on a gain. A client re-watches from its last `resourceVersion` without a list, so an object created before that reconnect stays hidden. Headlamp, kubectl, k9s, OpenLens, and Aptakube all re-watch that way. A lost namespace ends the stream, because the client would keep stale objects.
- End the stream on a lost namespace, or on a gain above `--fanout-max-watch-namespaces`, after an `ERROR` event with code 410 and reason `Expired`. A client that re-watches from its last `resourceVersion` keeps the objects of a lost namespace, and 410 is the signal of the watch protocol for a new list. Send no such event when an upstream watch ends on its own. The namespaces of the stream do not change then, and a re-watch from the last `resourceVersion` covers them.
- A 404 from one namespace stops the whole fan-out, because a cluster-scoped kind answers 404 in every namespace. `kubectl get nodes` then costs at most the concurrency plus one requests.
- A caller that may see no object gets an empty collection, not the native 403. The kind then comes from the discovery document with the caller credentials, because no answer names it. A Rancher user with zero projects reaches Steve and gets an empty collection, so that caller takes this path too.
- `kubectl netshoot run` warns `couldn't attach` on a direct cluster connection too. It is not a routing defect.
