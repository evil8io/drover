# api-filter

The human document of this component is `docs/api-filter.md`. Read it first, and update it together with a behaviour change. Do not repeat its content here.

## Transport

- The filter logic is an `http.RoundTripper` under `httputil.ReverseProxy`. The proxy uses `FlushInterval` -1. The transport connects to the upstream with HTTP/1.1 only, because a websocket upgrade needs HTTP/1.1. In `internal/rancherclient`, keep `ForceAttemptHTTP2` off and `TLSNextProto` empty.
- Strip `Accept-Encoding` from a request whose answer the filter parses, because the filter reads the upstream body as plain JSON.
- `steveTimeout` in `steve.go` is 30 s. A 502 after 30 s on a tenant read means that Rancher is down, or that its Steve cache is not full yet. Its log line then has `allowed set request failed: context deadline exceeded` in the `error` field. This occurs, for example, after an OOMKill, while Rancher fills its Steve cache. This 502 does not mean that the fan-out (one request per allowed namespace) is slow. Check the Rancher pods first. Then check `duration_ms` per outcome.

## Allowed set

- Steve (`/k8s/clusters/<id>/v1/namespaces`) returns the RBAC-filtered list in its own format. Steve has no `labelSelector` and no watch. Use Steve only as the source of the allowed set.
- Keep a project in the allowed set only when every namespace under its label is an allowed namespace (`completeProjects`). A custom role can make a project visible with a right on some of its namespaces only. The project selector and the event filter would then pass the other namespaces of that project.
- A new namespace makes its project incomplete until Rancher binds the caller in it. A watch with a project selector then ends with 410. Do not keep an incomplete project in the set to prevent that end. The project selector would then show a namespace that the caller cannot get.
- Key the allowed set on the cluster plus the token that Rancher reads. `tokenValue` reads it as `GetTokenAuthFromRequest` in `pkg/auth/tokens/token_util.go` of Rancher 2.14.6 does. A key on the raw header gives each spelling of one token its own cache entry, its own fetch limiter, and its own watch slots. Each key costs a token of the shared fetch limit.
- Key the fetch limit per caller on the credential alone, without the cluster. A key per cluster multiplies the rate of a caller by the number of its clusters.
- Key the watch slots and the in-flight slots of the fan-out on `allowedSet.user`, because a user can make more tokens. The fetch limit per caller keeps the token key, because it runs before the lookup of the user.
- Steve answers 401 to a ServiceAccount JWT, because the Steve cluster proxy runs before the ServiceAccount authenticator in the Rancher handler chain.
- Never read the project of a ServiceAccount caller from a label on its namespace, because a project owner can set that label. Use the RBAC code path in `rulesreview.go`.
- Send the `SelfSubjectRulesReview` in a namespace name that no namespace can have (`clusterRulesNamespace`). A project owner can bind a Role that names another namespace to a ServiceAccount in its own namespace. A review in that namespace returns the rules of that Role. The API server answers 400 to an empty namespace, and it accepts every other name without a check. This is verified in `pkg/registry/authorization/selfsubjectrulesreview/rest.go` of Kubernetes 1.30.0 and 1.37.1.
- Keep the denied set of a ServiceAccount caller in the cache for one TTL. Without it, a denied caller costs one review per request, and it takes tokens of the shared fetch limit from every other caller.
- On the tick of an open watch, take a 401 or a 403 of the allowed-set fetch as the empty set (`trackedSet`). Do not retry on these answers. A retry keeps the stream open until the upstream watch times out. Without a `timeoutSeconds` from the client, the API server times out a watch after 30 to 60 minutes. The fetch returns 403 for a ServiceAccount whose rules name no namespace, and Steve answers 401 for a deleted token.
- No unit test sends a real ServiceAccount token through Rancher. After a change to the RBAC code path, do a live check with kubectl and a ServiceAccount token. Do the check on a cluster with JWT authentication on.

## Watch

- Never build a union selector of names and projects for a namespace watch. The extra set is the set of allowed namespaces outside the projects of the caller. With a cluster-wide `get namespaces` binding, nearly every namespace is in the extra set.
- Trigger the swap of the upstream watch on a change of the selector, not on a change of the project set. When the caller gets the `get` right on one namespace outside its projects, the selector case changes, but the project set does not change. For the live check, do these steps:
  1. Open the Namespaces view of Headlamp with an empty allowed set.
  2. Create the first namespace of a project of the caller.
  3. Check that the namespace appears in the view within two cache TTLs, without a reload.
- Reserve the watch slot under the registry mutex before the upstream request. Release it on every code path that fails before `add` binds it to a stream. A leaked slot stays counted. When the leaked slots reach the limit, the filter answers 503 to every watch of every caller.
- Check every upstream event with `filterEvent`, on every watch transport. Before 0.8.0, the filter let a 101 upgrade through without a filter, and a test asserted that wrong behaviour.
- Keep the stream buffer in `websocket.go`, and keep the split of the events with a `json.Decoder` and `InputOffset`. A proxy can re-slice the stream, so a message is not one event. In 0.14.1, the filter decoded every message under `base64.binary.k8s.io` as base64, and Headlamp got no event.
- These clients are verified: kubectl, k9s, OpenLens, Aptakube (chunked), and Headlamp 0.14.2 (websocket) against Kubernetes 1.35. No integration test uses the websocket path. After a change to `websocket.go` or to the merged websocket watch, check Headlamp live. A new namespace must appear in the Namespaces view, and a new pod in the Pods view of all namespaces.
- Aptakube fills its namespace picker once per connection, and it re-watches without a list when a stream ends. When the picker shows other namespaces than the Namespaces view, the cause is not a defect in the filter. Reconnect Aptakube first.

## Access review

- kubectl and client-go send the SelfSubjectAccessReview as `application/vnd.kubernetes.protobuf`, so keep the decoder in `protobuf.go`.

## Fan-out

- The dispatcher takes a concurrency slot before it starts a request, and it starts the requests in name order. Only the dispatcher waits for the in-flight slots, after it takes the slot of the request. So the in-order reader never waits for a request that has no slot, and the deadlock does not occur. Keep that order.
- Take the in-flight slot of the caller before the global slot, on every path. With both orders in use, two requests can each hold the slot that the other waits for.
- Do not add a write deadline for a client that no longer reads its answers. `--fanout-max-inflight-per-caller` limits the slots that such a client keeps. A watch stream uses the same writer and can be idle for minutes, so a deadline needs a reset around each write.
- A merged watch returns the in-flight slot of an upstream open when the upstream answers. A slot for the life of the stream would block every fan-out list.
- Do not end a merged watch on a gained namespace. Headlamp, kubectl, k9s, OpenLens, and Aptakube re-watch from the last `resourceVersion` without a list, so an object created before that reconnect stays hidden.
- `kubectl netshoot run` warns `couldn't attach` on a direct cluster connection too. The cause of this warning is not a routing defect.
