# api-filter

The api-filter is a filter in front of Rancher. It lets a tenant list the namespaces of its projects with kubectl, k9s, and other Kubernetes clients. The list contains only those namespaces. With `--fanout` on, the api-filter also lists and watches a namespaced kind across those namespaces, cluster-wide, in one call.

Rancher grants a project member `get` on the namespaces of its projects, and no `list`. A `kubectl get ns` request through a Rancher kubeconfig gets a 403 error, but `kubectl get ns <name>` works. This service is an HTTP reverse proxy in front of Rancher. A Gateway or an Ingress routes the paths in Requirements to the service, and it routes every other path to Rancher directly.

## How it works

The service handles two request patterns from a Rancher kubeconfig. With `--fanout` on, it also handles the requests in Fan-out. It passes every other request to Rancher unchanged.

**List namespaces**

1. The service sends the request to Rancher with the caller's own credentials.
2. When Rancher answers with a status other than 403, the service returns that answer to the client unchanged.
3. On a 403 error, the service reads the allowed namespaces of the caller. These namespaces are the allowed set of the caller. For a caller with a Rancher token or a session cookie, the service gets them from Steve, the Rancher API server in the cluster agent. For a caller with a ServiceAccount token, the service gets them from the RBAC rules of the caller. See the paragraphs after this list.
4. For a plain list, the service makes a new request with the service token, with no cookie, and with a label selector. The service token is the API token of the Rancher service user (see Requirements). The selector matches `field.cattle.io/projectId` on the projects of the caller that contain an allowed namespace. The service uses this selector when the label of every allowed namespace names one of those projects. In every other case, the selector matches the allowed namespace names. When the caller can see no namespace, this name selector matches no namespace.
5. For a watch (`?watch=true`), the service makes a new request with the service token, with no cookie, and with the caller's own query. The service gives the watch a project selector, a selector that matches no namespace, or no selector at all. A project selector matches a new namespace of a project automatically. A name selector needs a new upstream watch for each new namespace. The service merges the selector of the caller into that selector, as it does on a list. These three cases apply:
   - A caller with no namespace and no project gets the selector that matches no namespace.
   - A caller whose namespaces are all in its own projects gets a `field.cattle.io/projectId` selector on those projects. A new namespace of such a project matches that selector automatically.
   - A caller with a namespace outside its own projects gets no selector. No single label selector matches that namespace and the projects of the caller at the same time.

   The service keeps every Accept entry of the caller with the media type `application/json` in the new request, for example a table request from kubectl. It drops every other entry, for example protobuf or CBOR. When no entry remains, the service sets `application/json`.
6. The service sends the new request to Rancher, and it streams the answer to the client. The event filter runs on every watch, also on a watch with a selector. The event filter passes an event when every namespace in the event is in the allowed set. It also passes an event when the `field.cattle.io/projectId` label of the event matches a project of the caller that contains an allowed namespace. The event filter drops every other `ADDED`, `MODIFIED`, or `DELETED` event. It checks a `BOOKMARK` event with a name in the same way, and it passes a `BOOKMARK` event without a name. It passes an `ERROR` event only when the object of the event is a `Status`. It drops an event of another type, and a value that is not a watch event.

   A server-side table event has one row per namespace, and the event filter checks the name and the labels of each row. For a watch with no selector, the event filter is the only check. For a watch with a selector, the event filter is a second check after the selector.

A request is a watch when its `watch` parameter is present with a value other than `0` or `false`. The API server reads the parameter in the same way. The answer to a watch request is a stream over chunked HTTP, or over a websocket connection after a protocol switch. When Rancher grants a namespace to the caller after the start of the watch, the namespace becomes visible in one of three ways:

- A project selector matches the new namespace at once, when the project of the caller already contains an allowed namespace.
- A watch with no selector also gets the event, when the cached allowed set contains the namespace at the time of the event.
- In the other cases, the service itself changes the stream, as described below.

The cache of allowed sets has one entry per cluster and per credential that Rancher reads. That credential is the first `Authorization` value. When that value is empty or absent, the credential is the first `R_SESS` cookie. A second `Authorization` value, a second `R_SESS` cookie, and any other cookie are not part of the cache key.

To fetch the allowed set, the service reads from Steve, from the project list, or from the rules review. When one of these reads gets a 401 or 403 error, the caller gets the native 403 error of its own request, with its message. The native 403 error is the answer of Rancher to the original request of the caller. A client that gets a 401 error for a valid token authenticates again. The native answer names the permission that the caller does not have. The service returns a 429 error of a fetch rate limit to the caller unchanged.

A ServiceAccount token is a bearer JWT whose subject starts with `system:serviceaccount:`. Rancher 2.9.0 and later accepts such a token on `/k8s/clusters/<id>/` when the `ClusterProxyConfig` object of the cluster has `enabled: true`. Rancher verifies the token against the cluster and keeps the identity of the caller, so the RBAC of that cluster controls the access. Such a caller is not a Rancher user. It has no project, and Steve answers it with a 401 error.

The service therefore reads the allowed namespaces of such a caller from a `SelfSubjectRulesReview`. It creates the review with the credentials of the caller, in the namespace `drover:cluster-rules`. No namespace can have a name with a colon, so the review contains only the rules of the ClusterRoleBindings of the caller. A RoleBinding adds no namespace to the allowed set. A Role grants its rules in its own namespace only, so a Role that names another namespace does not let the caller get that namespace. The built-in `system:basic-user` role grants every authenticated caller the permission to create that review. The allowed set is the union of the `resourceNames` of the rules that grant `get` on `namespaces` in the core group. A wildcard in the verbs, the groups, or the resources also matches. This allowed set has no project, so a list gets a name selector, and a watch gets only the event filter, with no selector.

A rule that grants `get` on `namespaces` without names does not separate tenants. A caller with no such rule has no allowed namespace. In both cases, the caller gets the native 403 error of its own request, with its message, and not an empty list. The service caches the denied result for one cache TTL, so it does one review per TTL for such a caller.

To see the rules that the service reads, run `kubectl auth can-i --list` with the token of the ServiceAccount, in a namespace where the ServiceAccount has no RoleBinding.

The stream of a namespace watch stays open when the allowed set of the caller changes. Headlamp opens no new watch after a clean end of a stream. After such an end, the view of Headlamp gets no updates until the user reloads it. A timer re-reads the allowed set of the caller once per cache TTL. The timer reads the set from the cache of a plain list, so it adds at most one fetch per cache TTL. When the re-read gets a 401 or 403 error, the service takes the allowed set as empty. In that case, the caller lost its last namespace or its credential. When the re-read gets a 429 error or another error, the service keeps the stream as it is until the next re-read. The service then compares the new names with the names of the stream, and it builds the selector again from the new set:

1. For each name that the caller loses, the service sends a `DELETED` event with the object `{"kind":"Namespace","apiVersion":"v1","metadata":{"name":"<name>"}}`, before any other change. The caller listed that name before, so the event contains no new information for the caller. The event can come after the `DELETED` event of the upstream watch. A client accepts that second event without a problem. The event of the service also takes the place of an upstream event that the event filter dropped.
2. A watch that takes no event of the service ends instead, as described in the list below. For such a watch with a project selector, the service first reads each lost namespace once with the service token. The upstream watch sends the `DELETED` event of a deleted namespace. It also sends that event for a namespace whose `field.cattle.io/projectId` label no longer matches the selector. The service therefore keeps the stream open when the read gets a 404 answer. It also keeps the stream open when the `field.cattle.io/projectId` label of the namespace does not match the selector.

   The service counts a read with another status as a namespace under the selector. The service then ends the stream, so that the client keeps no stale namespace. The service stops the reads at the first name that ends the stream.
3. A watch with a selector gets a new upstream watch when the selector changes. The new request has the new selector, and no `resourceVersion`, `resourceVersionMatch`, or `sendInitialEvents`. The API server starts such a watch with an `ADDED` event for each namespace under the selector. The client thus gets the namespaces that the caller gains. A client treats the `ADDED` event of a namespace that it already has as an update. The service then closes the old upstream watch.
4. A new upstream watch takes one token of the fetch rate limit per caller. The caller decides when its allowed set changes. Each new upstream watch replays the namespaces under the selector for every open stream of that caller.
5. A watch without a selector keeps its upstream watch, because a new upstream watch without a selector replays every namespace of the cluster. For each name that the caller gains, the service reads that namespace with the service token. The stream then gets an `ADDED` event with the object of the answer. The allowed set of the caller contains that name, so the caller is allowed to read it. When the read gets a status other than 200, the service tries again at the next re-read.

A new namespace in a project of the caller that already contains an allowed namespace does not change the selector. The service then keeps the upstream watch, and the upstream watch sends the event of that namespace. A chunked stream gets an event of the service as one line. An upgraded stream, a stream over a websocket connection, gets it as one text frame with plain JSON. On a websocket connection, the service does the handshake of the new upstream watch itself, with a new key.

The service ends the stream after an `ERROR` event in the cases below. The event has a Status of code 410 and reason `Expired`. The client then lists again before it watches again.

- The Accept header of the client contains an `as` parameter, for example `as=Table` for a server-side table. In addition, the caller loses a name, or the watch has no selector and the caller gains a name. Under item 2, the service keeps the stream open for a lost name whose `DELETED` event the upstream watch sent. The objects of such a stream have a form other than a namespace, and a table event also needs the columns of the stream. The service therefore writes no event of its own on that stream.
- The query of the client contains a `labelSelector` or a `fieldSelector`, in the same cases. The service does not apply that selector to an event of its own.
- The allowed set of a watch with a selector changes, and the new set needs a watch without a selector.
- The request for the new upstream watch gets a status other than 200, or other than 101 on a websocket connection.
- The caller has no free token for the new upstream watch.

An upgraded stream gets a websocket close frame with status 1000 after the `ERROR` event, so the client reports a normal closure. A stream also ends when the service drains at shutdown, when its upstream watch ends, and when the client closes the connection.

On a websocket connection, the service gets the watch stream as RFC 6455 frames. The API server sends one event per text frame, as plain JSON, under every subprotocol, also under `base64.binary.k8s.io`. A proxy between the API server and the service can split the stream into messages at other points, or send base64 text. A message then contains a part of an event, several events, or the end of one event and the start of the next. The service therefore decides the form of each message separately. A message whose first byte after white space is `{` is plain JSON, and under `base64.binary.k8s.io` only, every other message is base64 text.

The service assembles a text or a binary message from its continuation frames, and it decodes a base64 message with `base64.StdEncoding`. It appends the bytes to a stream buffer. It applies the event filter of the chunked path to each complete event in that buffer. It writes each event that passes as a new message of its own, with a newline after the event. That message has the opcode of the upstream message that completes the event. It is base64 text only when that upstream message is base64 text.

The service forwards a close, a ping, and a pong frame unchanged, also while a message is incomplete.

The service offers no websocket extension in the upgrade, because it reads no compressed payload. The service deletes `Sec-WebSocket-Extensions` from the privileged request, the request with the service token. When the 101 answer still contains an extension, the service ends the stream at once with a close frame. It counts that stream in `drover.filter.watches.rejected`.

The service also ends the stream with a close frame when it cannot decode a message. It does the same when the buffer grows past 1 MiB without a complete event. In both cases, the position in the stream is lost, and the service writes a `warn` line.

**Review access**

The service reads the body of a `selfsubjectaccessreviews` request. It intercepts a review of a list or a watch on the namespaces resource. A review can contain a namespace, because kubectl sends the namespace of the kubeconfig context, also for this cluster-scoped resource. The service ignores that attribute. With `--fanout` on, the service also intercepts a review of a cluster-wide list or watch of another resource. The service sends an intercepted review to Rancher with the caller's own credentials.

When Rancher denies an intercepted review, the service sets `allowed: true` in the answer, but only when the caller has at least one allowed namespace. The service passes every other review through unchanged. It reads a JSON review or a Kubernetes protobuf review, and it answers an intercepted review in JSON. The answer of Rancher contains a copy of the spec of the review. The service grants a review only when that copy is still for that list or that watch. The parser of the service ignores the case of JSON keys, but the parser of the API server does not.

## Fan-out

With `--fanout` on, the service answers a cluster-wide list of a namespaced kind, for example `kubectl get pods -A`, with one request per allowed namespace. It merges the answers into one collection.

1. A Rancher project member has the list permission inside the namespaces of its projects only. It has no list permission at cluster scope. A cluster-wide list then gets a 403 error from Rancher.
2. The service sends the native request first. The fan-out starts only after that request gets a 403 error.
3. The service sends each namespaced request of the fan-out with the credentials of the caller, not with the service token. RBAC checks each request separately. The caller thus gets no permission beyond the permissions that it already has.
4. When the request for a namespace gets a status other than 200, the service leaves that namespace out of the merge. The service stops the fan-out at a 404 error, because every namespace gives that same answer. The caller then gets the native 403 error. For a cluster-scoped kind, for example `nodes`, the fan-out always ends in this way.
5. A caller that can see no object of the kind gets an empty collection, not the native 403 error. This applies to a caller with no allowed namespace. It also applies to a caller for which RBAC denies the list in every allowed namespace. A client then shows an empty view instead of an error, as it does for the namespace list.

   The service reads the kind and the scope of the resource from the discovery document of the api path, with the credentials of the caller. The service needs discovery here, because the merge has no answer to take the kind and the scope from. For a cluster-scoped kind, the caller still gets the 403 error. The caller also gets the 403 error for a resource that is not in the discovery document.
6. The service streams the merged answer to the client. The service keeps the elements of at most `--fanout-concurrency` answers in memory at the same time. For a tenant with hundreds of namespaces, the service then needs no more than that count of answers in memory. The service limits the count of namespaced requests of all fan-outs together to `--fanout-max-inflight`. No caller can thus multiply the load on Rancher beyond that count.
7. The service puts the `kind`, the `apiVersion`, and the `metadata` fields after the elements in the merged answer. The `resourceVersion` of the merge is the lowest value of the answers, so the service has that value only after the last answer. The API server serializes the keys of an unstructured object in alphabetical order. The `kind` of a custom resource list is thus also after its elements upstream. A JSON object has no fixed field order, so this position is valid. The service asks discovery for the kind when no answer contains one.
8. The merged answer contains no `continue` token. The service also drops the `limit` and the `continue` parameters of the caller. A `continue` token is valid for one namespace only. A client that reads a merged answer reads one page, and it stops there.
9. The service merges a `List` and a `Table` in the same way. In a merged `Table`, the service keeps the `columnDefinitions` field of the first answer. kubectl then shows the table view with its columns.

When a caller has more allowed namespaces than `--fanout-max-namespaces`, a cluster-wide list of that caller gets a 403 error. The service then runs no fan-out.

On the `selfsubjectaccessreviews` path, the service grants a cluster-wide `list` and a cluster-wide `watch` of any named resource. It does this when `--fanout` is on and the caller has at least one allowed namespace. A review of the resource `*` or of the group `*` gets its native answer. RBAC still checks the real permission on the list and on the watch. When the caller has no real permission for a granted list or watch, the caller gets an empty answer, not an error.

**Merged watch**

With `--fanout` on, the service also answers a cluster-wide watch, for example `kubectl get pods -A --watch`, with one upstream watch per allowed namespace. It merges the upstream watches into one stream.

1. The service opens every upstream watch at the same time, on the namespaced path of the kind, with the credentials of the caller. `--fanout-max-watch-namespaces` is the maximum size of that set of watches. A caller with more allowed namespaces than that maximum gets a 403 error, and the service opens no upstream watch. By default, that maximum is lower than `--fanout-max-namespaces`. For a list, one upstream request is open for the length of that request. For a watch, one upstream connection stays open until the stream ends, and each client of the caller has its own set of connections.
2. At the start of the stream, the service keeps the query of the caller on each upstream watch, with its `resourceVersion` and its `timeoutSeconds`. The merged list has the lowest `resourceVersion` of its answers, so a watch from that value loses no event. When the list answer of a namespace had a higher revision, the upstream watch of that namespace repeats the events between the two revisions. A client treats a repeated event as an update of an object that it already has.
3. When the upstream watch of a namespace gets a status other than 200, the service leaves that namespace out of the stream. Item 7 describes the new tries of that watch. For a 404 error, the caller gets the native 403 error, because the kind then has no namespace scope. When RBAC denies the watch in every namespace of the allowed set, the caller also gets the native answer. The reason is that an open stream without an upstream watch does not show that the caller really has no permission.
4. A caller that can see no namespace gets an open stream without an event. On the list path, such a caller gets a collection without an element.
5. The service sends no `BOOKMARK` event on the merged stream, and it drops `allowWatchBookmarks` from the upstream requests. A bookmark contains the revision of one namespace. A client that resumes the merge from that bookmark loses the events of every other namespace. A watch-list, a watch with `sendInitialEvents=true`, is the exception. For a watch-list, the service keeps `allowWatchBookmarks` on each upstream watch, and the client gets the initial events of each upstream watch.

   After every upstream watch sent its own `BOOKMARK`, the service sends one `BOOKMARK` on the merged stream. That `BOOKMARK` has the annotation `k8s.io/initial-events-end` and the lowest `resourceVersion` of the upstream bookmarks. A caller with no allowed namespace gets that `BOOKMARK` at once, with no `resourceVersion`.
6. A timer re-reads the allowed set of the caller once per cache TTL. The timer reads the set from the cache of a plain list, so it adds at most one fetch per cache TTL. The timer takes a 401 or 403 error of the re-read as an empty allowed set, as on a namespace watch.
7. The service adds a namespace that the caller gains to the open stream. The upstream watch of that namespace has no `resourceVersion`. The client therefore gets an `ADDED` event for each object that exists in that namespace. After these events, the client gets the live events of that namespace. The service also drops `resourceVersionMatch`, `sendInitialEvents`, and `allowWatchBookmarks` from that request. The stream does not end when the caller gains a namespace.

   After an end of the stream, a client watches again from its last `resourceVersion`, without a new list. The client then misses an object that was in the namespace before that new watch. When the watch of a namespace does not open, the service tries again at the next re-reads. The service makes at most 3 tries per namespace and per stream. This limit applies to a namespace at the start of the stream and to a gained namespace alike. The service tries again because Rancher creates the role bindings of a new namespace some seconds after the namespace. The limit keeps the cost of a namespace that the caller cannot watch at 3 requests per stream.
8. When the caller loses a namespace, the service ends the stream. Otherwise, the client keeps the objects of that namespace. The service also ends the stream when the caller gains a namespace and the allowed set goes above `--fanout-max-watch-namespaces`. The next watch of the client then gets the 403 error of item 1. Before the service ends the stream in either case, it sends an `ERROR` event with a Status of code 410 and reason `Expired`. A client with an informer treats that event as the end of its `resourceVersion` window, and it lists again before it watches again.
9. When one upstream watch ends, the service ends the merged stream. Otherwise, the client gets stale data for that namespace, and it gets no report about the problem. The service sends no `ERROR` event before this end. The namespaces of the stream do not change, and a new watch from the last `resourceVersion` includes all of them.
10. On this path, the service controls the transport to the client, because it relays no single upstream stream. A chunked client gets the events as a newline-delimited JSON stream. A client that asks for a websocket gets the protocol switch from the service itself.

    Such a client gets one text frame with plain JSON per event under every subprotocol, as the API server sends it. In the 101 answer, the service names the first subprotocol of the offer that is `binary.k8s.io` or `base64.binary.k8s.io`. When the client sends a close frame, the service ends the stream. The service answers a ping frame with a pong.
11. The service uses the chunked transport for every upstream watch. The service deletes the handshake headers of the caller from the upstream request.

## Requirements

1. A Rancher service user with `get`, `list`, and `watch` on `namespaces` in every cluster whose tenants use the service. A `cluster-owner` binding also grants these permissions.
2. An API token of that service user. The token has no scope.
3. The token in a Secret that is mounted into the service.
4. Route rules on the Rancher hostname for these requests:

   | Match | Method | Path | Target |
   | --- | --- | --- | --- |
   | `Exact` | GET | `/k8s/clusters/<id>/api/v1/namespaces` | the service |
   | `PathPrefix` | GET | `/k8s/clusters/<id>/api/v1/namespaces` | Rancher |
   | `PathPrefix` | GET | `/k8s/clusters/<id>/api/v1` | the service |
   | `PathPrefix` | GET | `/k8s/clusters/<id>/apis` | the service |
   | `Exact` | POST | `/k8s/clusters/<id>/apis/authorization.k8s.io/v1/selfsubjectaccessreviews` | the service |

   A Gateway selects the rule by the specificity of the match, not by the order of the rules. An `Exact` match has precedence over a `PathPrefix` match, and a longer prefix has precedence over a shorter prefix. The rule with the namespaces prefix therefore matches every namespaced read. The Gateway thus sends `exec`, `attach`, `portforward`, and a log stream to Rancher, and not to the service. Give the Rancher rule the same unlimited request timeout as the service rule, because those streams stay open for minutes.
5. For a ServiceAccount caller, JWT authentication on the cluster. This authentication needs a `ClusterProxyConfig` object with `enabled: true`, in Rancher 2.9.0 and later. A ClusterRoleBinding of the caller binds a role with a rule that grants `get` on `namespaces`. The namespaces of the caller are in the `resourceNames` of that rule.

With these routes, the service becomes the data path for most reads of a tenant. The tenant then depends on the availability and the latency of the service for those reads.

## Configuration

Run `drover api-filter [flags]` to start the service.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--listen` | `:8080` | The address that the service listens on. |
| `--upstream` | (required) | The URL of Rancher. Use `http://` or `https://`. The path must be empty or `/`. |
| `--upstream-ca-file` | | The PEM bundle that the service uses to verify an `https` upstream. |
| `--upstream-insecure-skip-verify` | `false` | When the flag is on, the service does not verify the certificate of an `https` upstream. |
| `--token-file` | (required) | The file with the API token of the Rancher service user. |
| `--cache-ttl` | `15s` | The lifetime of a cached allowed set. |
| `--max-cache-entries` | `1000` | The hard limit on the count of cached allowed sets. |
| `--fetch-rate` | `50` | The shared rate limit for the fetch of an allowed set, in fetches per second. The burst is twice the rate. |
| `--fetch-rate-per-caller` | `5` | The rate limit of one caller credential for the fetch of an allowed set, in fetches per second. The burst is twice the rate. |
| `--fanout` | `false` | When the flag is on, the service answers a cluster-wide list of a namespaced kind with one request per allowed namespace. |
| `--fanout-max-namespaces` | `200` | The count of allowed namespaces above which the service answers a cluster-wide list of a namespaced kind with a 403 error. |
| `--fanout-concurrency` | `16` | The count of namespaced requests of one fan-out that run at the same time. The service also uses this count to limit the memory of one fan-out. |
| `--fanout-max-inflight` | `64` | The count of namespaced requests of all fan-outs that run at the same time. |
| `--fanout-max-watch-namespaces` | `50` | The count of allowed namespaces above which the service answers a cluster-wide watch with a 403 error. |
| `--max-watches` | `1000` | The count of open watch streams above which the service answers a new watch with a 503 error. |
| `--max-watches-per-caller` | `100` | The count of open watch streams of one caller above which the service answers a new watch of that caller with a 503 error. A caller is one credential that Rancher reads. |
| `--log-level` | `info` | The log level. The value is one of `debug`, `info`, `warn`, or `error`. |
| `--shutdown-grace` | `20s` | The grace period for the shutdown after SIGTERM or SIGINT. |
| `--otlp-endpoint` | `$OTEL_EXPORTER_OTLP_ENDPOINT` | The OTLP gRPC endpoint, as `host:port` or as a URL. When the value is empty, telemetry is off. |
| `--otlp-traces` | `true` | When the flag is on, the service sends traces to the OTLP endpoint. |
| `--otlp-metrics` | `true` | When the flag is on, the service sends metrics to the OTLP endpoint. |
| `--service-name` | `$OTEL_SERVICE_NAME`, or `drover` | The `service.name` resource attribute. With an empty value, `--service-name=`, the service sets no `service.name`, so that a collector can derive it. The SDK still takes a `service.name` from `OTEL_SERVICE_NAME` or `OTEL_RESOURCE_ATTRIBUTES`. |

`--upstream-insecure-skip-verify` is for an upstream inside the cluster with a certificate from a private CA. Use the flag when the deployment of the service has no copy of that CA. Then limit the path to Rancher with a network policy.

The service counts each watch against `--max-watches` and `--max-watches-per-caller` from before its upstream request until its stream ends. A burst of parallel watches then cannot go above a limit. With the per-caller limit, one caller cannot take every stream of the service. A merged watch has one upstream connection per allowed namespace. One caller then has at most `--max-watches-per-caller` times `--fanout-max-watch-namespaces` upstream watch connections. With the defaults, this maximum is 5,000 connections.

The upstream is the Rancher Service inside the cluster, for example `http://rancher.cattle-system.svc`. The public hostname is not a valid upstream, because the Gateway or the Ingress routes the filtered paths on that hostname back to the service.

The service also starts without a token file. Until the token file contains a token, the service returns a 502 Status on a filtered request.

The service always answers `GET /healthz` with status 200 and body `ok`. It answers `GET /readyz` with status 200 and body `ok` in normal operation. After the shutdown starts, it answers `GET /readyz` with status 503 and body `draining`.

## Behaviour differences

| Difference | Detail |
| --- | --- |
| Cache delay | A role change becomes visible after the delay of Rancher plus the cache TTL. On an open watch, the delay of the service can reach two cache TTLs, because the timer can read a cached set that is one TTL old. |
| Field selector | The answer to a field selector on a name outside the allowed set is an empty list. The answer to a `get` on that name is Forbidden. |
| Namespace cap | A caller with more than 20,000 allowed namespace names gets an error, not a list. |
| Fan-out cap | A cluster-wide list gets a 403 error, with no fan-out, when the caller has more than `--fanout-max-namespaces` allowed namespaces. |
| Watch cap | A cluster-wide watch gets a 403 error, with no merge, when the caller has more than `--fanout-max-watch-namespaces` allowed namespaces. |
| Stream cap | A namespace watch or a cluster-wide watch gets a 503 error when the service has `--max-watches` open watch streams. It also gets a 503 error when the caller has `--max-watches-per-caller` open watch streams. The client retries. |
| Merged watch end | A merged cluster-wide watch ends when one upstream watch ends. It also ends when the caller loses an allowed namespace, or when the caller gains a namespace and the allowed set goes above `--fanout-max-watch-namespaces`. In these two cases, the service first sends an `ERROR` event with the Status 410 `Expired`, so that a client with an informer lists again. The service adds a namespace that the caller gains to the open stream, with an `ADDED` event for each object that exists in it. |
| Merged watch bookmark | The service sends no `BOOKMARK` event on a merged cluster-wide watch, also when the client asks for one. A watch-list gets the one `BOOKMARK` event that marks the end of its initial events. |
| Empty answer | A cluster-wide list of a namespaced kind gets an empty collection, not Forbidden, when the caller can see no object of that kind. |
| Self-check | For `kubectl auth can-i list namespaces`, the answer is yes when the caller has at least one allowed namespace. The answer of RBAC is no. With `--fanout` on, the same is true for a cluster-wide `list` and `watch` of any named resource. This includes a cluster-scoped resource, but a list of that resource still gets its 403 error. |
| ServiceAccount caller | A caller with a ServiceAccount token gets the namespaces in the `resourceNames` of the RBAC rules of its ClusterRoleBindings that grant `get` on `namespaces`. A RoleBinding adds no namespace. When such a rule has no names, the caller gets the native 403 error, not an empty list. The same applies when the caller has no such rule. |
| Denied fetch | When the fetch of the allowed set gets a 401 or 403 error, the caller gets the native 403 error of its request. The caller also gets the message of that error. The caller does not get the answer of Steve or of the rules review. |
| Fetch rate | Above `--fetch-rate`, the service waits up to 5 s for a free token before a fetch of an allowed set. When no token is free within 5 s, the caller gets a 429 Status. The service applies the limit per caller, `--fetch-rate-per-caller`, first, with the same wait and the same 429 Status. That limit is per credential, across all clusters. The service takes no token of the shared limit for a fetch that it throttles on the limit per caller. |
| Trust level | The service is a privileged component. It uses the service token for the filtered namespace list and for the watch stream. |
| Project scope | A project selector contains the projects of the requested cluster only. The project part of a Rancher project id is unique inside one cluster, and the `field.cattle.io/projectId` label of a namespace contains that part only. |

## Logging

The service writes JSON logs to stderr. It writes one line per intercepted request.

A log line for a namespace list and a log line for a collection have the fields `cluster`, `outcome`, `status`, `count`, `watch`, and `duration_ms`. The value of `outcome` is `native`, `filtered`, `passthrough`, `denied`, `fanout`, `empty`, `capped`, or `error`. The field `count` is present only for the outcome `filtered`, `fanout`, `empty`, or `capped`. A Status body that the service writes for an error contains no internal address and no file path. The log line contains the full error. A log line for a collection also has the field `resource`, with the kind of the requested collection.

A log line for a review has the fields `cluster`, `outcome`, and `status`. The value of `outcome` is `passthrough`, `native`, `granted`, or `error`.

Both kinds of log line have the field `user`, with the Rancher user id of the caller from a SelfSubjectReview. The field is present only when the service resolves that id. For a ServiceAccount caller, `user` is the subject of the token, for example `system:serviceaccount:<namespace>:<name>`. The API server verified that subject with the rules review. No metric attribute contains the user name, because a user name has an unbounded value set. An attribute with an unbounded value set makes the cardinality of the metric unbounded.

The service never logs a token, a cookie, a header value, or a request body. It logs a namespace name at the `debug` level. At the `info` level, it logs a namespace name only in these cases:

- The service adds a namespace to a merged watch.
- A merged watch ends because an upstream watch ended.

The service writes an `info` line with `cluster`, `lost`, and `gained` when it gives a namespace watch a new upstream watch. The fields `lost` and `gained` are the counts of the changed names. The service writes an `info` line with `cluster` when it ends a namespace watch after the allowed set of the caller changes. That line has the field `error` when the new upstream watch failed.

The service writes a `debug` line with `cluster`, `type`, and `namespace` for each `ADDED` or `DELETED` event that it writes itself. It writes a `debug` line with `cluster`, `namespace`, and `status` when it keeps a watch open on a lost namespace. This applies to a watch that takes no event of the service, when the upstream watch sent the `DELETED` event.

The service writes a `warn` line when the privileged list request gets a 403 error. The cause is that the service user has no permission to list namespaces.

The service writes a `debug` line with `cluster` and `bounded` when the rules of a ServiceAccount caller contain no namespace name. The field `bounded` is false when a rule grants `get` on `namespaces` without names. The service writes a `debug` line with `cluster` and `reason` when the API server marks the result of the rules review as incomplete. For example, the API server does this when an authorizer other than RBAC runs in the cluster.

The service writes a `warn` line with `cluster` and `error` when an upgraded namespace watch ends because of an error of the upstream stream. Examples of such an error are a message that is neither JSON nor base64 text, and a reset of the upstream connection.

A log line has `trace_id` and `span_id` when the request has a span.

## Telemetry

When a request has a `traceparent` header, the service continues that trace. When the header is absent, the service starts a span. A request span has a child span for the Steve call, the project list, the rules review, the privileged list, and the privileged watch.

The service names a span after the method and a path template, for example `GET /k8s/clusters/{cluster}/api/v1/pods`. The server span has that template in the attribute `http.route`. A collector that builds a span name again from the semantic conventions reads that attribute. Without the attribute, such a collector gives the span the method alone as its name. With the attribute, the name from the collector and the name from the service agree.

The service replaces the cluster id, a namespace name, and an object name with a placeholder, because each of them has an unbounded value set. The service keeps the api group, the version, and the resource name in the template. These parts identify what the request asks for, and the api surface of a cluster is bounded.

Search a trace on `resource.service.name`, not on the span name. The service takes the value of that attribute from `--service-name`.

The service exports `url.full` on a client span without its query, so the trace backend never gets a namespace name from the privileged list.

With `--otlp-endpoint` set, the service also exports these metrics:

| Metric | Kind | Unit | Attributes |
| --- | --- | --- | --- |
| `drover.filter.requests` | Counter | `1` | `path`, `outcome`, `cluster`, `watch` |
| `drover.filter.request.duration` | Histogram | `s` | `path`, `outcome`, `cluster`, `watch` |
| `drover.filter.watches.open` | UpDownCounter | `1` | |
| `drover.filter.watches.rejected` | Counter | `1` | `cluster`, `limit` |
| `drover.filter.events.dropped` | Counter | `1` | `cluster` |
| `drover.filter.fetch.throttled` | Counter | `1` | `limit` |
| `drover.filter.fanout.namespaces` | Histogram | `1` | `cluster` |
| `drover.filter.fanout.capped` | Counter | `1` | `cluster` |
| `drover.filter.fanout.skipped` | Counter | `1` | `cluster` |

In `drover.filter.watches.rejected`, the service counts each watch that it refuses with a 503 error because of a watch limit. The `limit` attribute is `shared` for `--max-watches`, and `caller` for `--max-watches-per-caller`. The service also counts in this metric an upgraded stream that it ends because of a websocket extension. The service records that count without a `limit` attribute.

The value of the `cluster` attribute of the two request metrics is the cluster id only for a known cluster. A cluster is known after Steve answers a namespace list for it, or after a rules review names a namespace in it. For every other request, the value is `unknown`, because a client can put any string in the path, also before authentication. A metric attribute with an unbounded value set makes the cardinality of the metric unbounded.

The `limit` attribute of `drover.filter.fetch.throttled` is `caller` for the limit per caller, and `shared` for the shared limit.

## Shutdown

On SIGTERM or SIGINT, the service drains before it stops:

1. The service marks itself not ready. From that point, the service answers `/readyz` with status 503.
2. The service ends every open watch stream with a clean end of the stream. An upgraded stream first gets a websocket close frame with status 1000. A client then lists and watches again, with no error.
3. The service stops the HTTP server within the `--shutdown-grace` period. For an upgraded stream, the service waits at most 5 s until the client reads the close frame, before the server stops.

The exit code is non-zero only when the server does not stop in time.
