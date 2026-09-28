# api-filter

The human document of this component is `docs/api-filter.md`. Update it together with a behaviour change.

## Transport

- The retry logic is an `http.RoundTripper` under `httputil.ReverseProxy`. The proxy uses `FlushInterval` -1. The transport connects to the upstream with HTTP/1.1 only, because a websocket upgrade needs HTTP/1.1. In `internal/rancherclient`, keep `ForceAttemptHTTP2` off and `TLSNextProto` empty.
- Use the in-cluster Rancher Service as the upstream. Never use the public hostname, because the route sends the filtered paths back to the filter.
- Strip `Accept-Encoding` from a request whose answer the filter parses, because the filter reads the upstream body as plain JSON.
- `steveTimeout` in `steve.go` is 30 s. A 502 with `allowed set request failed: context deadline exceeded` after 30 s on a tenant read means that Rancher is down, or that its Steve cache is not full yet. This occurs, for example, after an OOMKill, while Rancher fills its Steve cache. This 502 does not mean that the fan-out (one request per allowed namespace) is slow. Check the Rancher pods first. Then check `duration_ms` per outcome.

## Allowed set

- Steve (`/k8s/clusters/<id>/v1/namespaces`) returns the RBAC-filtered list in its own format. Steve has no `labelSelector` and no watch. The filter uses Steve only as the source of the allowed set. The allowed set of a caller has the namespaces that the caller can see, and their projects.
- A project is in the allowed set only when it contains an allowed namespace. A custom role can make a project visible with no right on a namespace. So a caller gets no namespace access from project visibility alone.
- Key the allowed set on the cluster plus the first `Authorization` value. Without an `Authorization` value, use the first `R_SESS` cookie, as net/http parses it. In both cases, Rancher reads exactly that credential. Make no second key for a second header value or cookie. Each key costs a token of the shared fetch limit.
- Key the fetch limit per caller on the credential alone, without the cluster. A caller with more than one cluster is one caller. A key per cluster multiplies the rate of that caller by the number of clusters.
- The filter gets the caller name from `POST .../selfsubjectreviews` with the caller credentials, and it caches the name in `allowedSet.user`. The lookup fails open. Put the name only in a log line and a span. Never put it in a metric attribute, because the set of values is unbounded.
- Rancher accepts a ServiceAccount JWT only on `/k8s/clusters/<id>/`, per cluster, when the cluster has a `ClusterProxyConfig` with `enabled: true` (Rancher 2.9.0 and later). Steve answers 401 to that caller, because the Steve cluster proxy runs before the ServiceAccount authenticator in the Rancher handler chain.
- For a bearer JWT whose `sub` starts with `system:serviceaccount:`, the filter uses the RBAC code path in `rulesreview.go`. There, the filter sends a `SelfSubjectRulesReview` with the credentials of the caller, in the namespace of the ServiceAccount. Rancher applies the same test to the token. Keep the review in that namespace, because no tenant binds a role there. With a RoleBinding to `view` or `edit` in the review namespace, the review returns `get` on `namespaces` without names. Never read the caller's project from a label on the ServiceAccount namespace, because a project owner can set that label.
- The review result is a denied set when a rule has no `resourceNames`, or when no rule grants `get` on `namespaces`. The caller then gets the native 403, not an empty list. The cache keeps the denied set for one TTL. Without it, a denied caller costs one review per request, and it takes tokens of the shared fetch limit from every other caller.
- On a 401 or a 403 from the allowed-set fetch, answer the buffered native 403 of the request, through `denialResponse`. A client that gets a 401 for a valid token authenticates again, and the native answer has the RBAC message. Pass a 429 of the fetch limit through unchanged.
- No unit test sends a real ServiceAccount token through Rancher. After a change to the RBAC code path, do a live check with kubectl and a ServiceAccount token. Do the check on a cluster with JWT authentication on.

## Watch

- Use only an exact watch selector:
  - For an empty allowed set, use a selector that matches nothing.
  - When every allowed namespace is in a project of the caller, use the project selector.
  - In every other case, use no selector.

  Never build a union of names and projects. The extra set is the set of allowed namespaces outside the projects of the caller. With a cluster-wide `get namespaces` binding, nearly every namespace is in the extra set.
- Keep a namespace watch open across a change of the allowed set. Headlamp opens no new watch after a clean end of a stream, and its view then shows no change until a reload.
- A lost namespace is no longer in the allowed set, and a gained namespace is new in the allowed set. `followAllowedSet` in `namespaces.go` writes a `DELETED` event per lost name, and it swaps the upstream of a watch with a selector when the selector changes. After a swap, the new upstream watch replays the namespaces under the selector, for every open stream of the caller. So the filter takes a token of the per-caller fetch limit for each swap. For a watch without a selector, the filter sends one privileged `GET` and writes one `ADDED` event per gained name instead. A new upstream watch without a selector replays every namespace of the cluster.
- A service event is an event that the filter writes itself, not an event of the upstream watch. On a watch with a project selector and no service events, read a lost namespace once before the 410 end. Keep the stream open when the read gets 404 or a project label outside the selector. In both cases, the upstream watch sent the `DELETED` event. End the stream on every other answer, so that the client keeps no stale namespace.
- Do not read a lost namespace for a watch that takes service events. The `DELETED` service event is harmless after the upstream event, and it replaces an upstream event that the event filter dropped.
- Trigger the swap on a change of the selector, not on a change of the project set. When the caller gets the `get` right on one namespace outside its projects, the selector case changes, but the project set does not change. The selector stays the same for a new namespace in a project of the caller. The open upstream watch sends the event of that namespace, so the filter needs no swap and no token for it. For the live check, do these steps:
  1. Open the Namespaces view of Headlamp with an empty allowed set.
  2. Create the first namespace of a project of the caller.
  3. Check that the namespace appears in the view within one cache TTL, without a reload.
- Reserve the watch slot under the registry mutex before the upstream request. Release it on every code path that fails before `add` binds it to a stream. The registry counts reserved slots, not open streams. With a check before the request and a count after it, a parallel burst passes the limit. A leaked slot stays counted. When the leaked slots reach the limit, the filter answers 503 to every watch of every caller.
- Check every event with `eventAllowed`, on every watch transport. Version 0.8.0 filtered a 200 stream and let a 101 upgrade through without a filter, and a test asserted the wrong thing.
- The API server sends one event per text frame, as plain JSON under every subprotocol, also under `base64.binary.k8s.io`. A proxy can re-slice the stream into other frames, or send base64 text. So decide the form per message. Answer in the same form. Version 0.14.1 decoded every message under `base64.binary.k8s.io` as base64, and Headlamp got no event.
- Keep the stream buffer in `websocket.go`, because a re-sliced message is not one event. For the same reason, keep the code that splits the events with a `json.Decoder` and `InputOffset`.
- Strip `Sec-WebSocket-Extensions` from the privileged request, because the filter reads no compressed frame. For the same reason, end a stream whose 101 still names an extension.
- These clients are verified: kubectl, k9s, OpenLens, Aptakube (chunked), and Headlamp (websocket). Headlamp is verified on 0.14.2 against Kubernetes 1.35. In that check, Headlamp showed a new namespace in the Namespaces view, and a new pod in the Pods view of all namespaces. No integration test uses the websocket path. So after a change to `websocket.go` or to the merged websocket watch, do that live check with Headlamp again.
- Aptakube fills its namespace picker once per connection, and it re-watches without a list when a stream ends. When the picker shows other namespaces than the Namespaces view, the cause is not a defect in the filter. Reconnect Aptakube first.

## Access review

- kubectl and client-go send the SelfSubjectAccessReview as `application/vnd.kubernetes.protobuf`. The filter decodes it in `protobuf.go`, and it sends the answer back as JSON.
- kubectl sends the namespace of the kubeconfig context in a review for the cluster-scoped `namespaces` resource. Ignore that attribute.
- Base a grant on the spec that the API server returns in its answer. Never base it on the request body alone, because the two parsers differ on key case.

## Fan-out

- The filter writes the `metadata` of the merged answer after the elements. The `resourceVersion` of the merged answer is the lowest value of all answers, because a watch from the lowest value loses no event. An earlier version wrote the highest value, and it lost the events of a namespace that it listed at a lower `resourceVersion`. The filter has the value only at the end of the stream.
- The dispatcher takes a concurrency slot before it starts a request, and it starts the requests in name order. So the in-order reader never waits for a request that has no slot. With that order, the deadlock does not occur. Keep that order.
- Only the dispatcher waits for the global in-flight cap. It waits after it takes the per-request slot, so the in-order reader never waits on a request without a slot.
- For a gained namespace, the filter adds an upstream watch of that namespace, without a `resourceVersion`, to the open merged watch. So the client gets the objects of that namespace as `ADDED` events. Do not end the stream on a gain. A client re-watches from its last `resourceVersion` without a list, so an object created before that reconnect stays hidden. Headlamp, kubectl, k9s, OpenLens, and Aptakube all re-watch that way. The filter ends the stream on a lost namespace, because the client would otherwise keep stale objects.
- End the stream on a lost namespace, or on a gain above `--fanout-max-watch-namespaces`. Before you end it, send an `ERROR` event with code 410 and reason `Expired`. A client that re-watches from its last `resourceVersion` keeps the objects of a lost namespace. In the watch protocol, 410 is the signal for a new list. Send no such event when an upstream watch ends on its own. In that case, the namespaces of the stream do not change, and a re-watch from the last `resourceVersion` includes them.
- The filter stops the whole fan-out on a 404 for one namespace, because the upstream answers 404 for a cluster-scoped kind in every namespace. So `kubectl get nodes` costs at most one request more than the concurrency.
- A caller that can see no object gets an empty collection, not the native 403. The filter then reads the kind from the discovery document with the caller credentials, because no answer contains the kind. A Rancher user with zero projects reaches Steve and gets an empty collection, so the filter serves that caller on this path too.
- `kubectl netshoot run` warns `couldn't attach` on a direct cluster connection too. The cause of this warning is not a routing defect.
