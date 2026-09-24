# api-filter

A filter in front of Rancher that lets a tenant list only the namespaces of its projects, with kubectl, k9s, and other Kubernetes clients. With `--fanout` on, it also lists and watches a namespaced kind across those namespaces, cluster-wide, in one call.

Rancher grants a project member `get` on the namespaces of its projects, and no `list`. A `kubectl get ns` request through a Rancher kubeconfig gets a 403 error, while `kubectl get ns <name>` works. This service is an HTTP reverse proxy in front of Rancher. A Gateway or an Ingress routes the paths in Requirements to the service, and every other path goes to Rancher directly.

## How it works

The service handles two request patterns from a Rancher kubeconfig. It passes every other request to Rancher unchanged.

**List namespaces**

1. The service sends the request to Rancher with the caller's own credentials.
2. A status other than 403 goes back to the client unchanged.
3. On a 403 error, the service requests the caller's allowed namespaces from Steve, the Rancher API server in the cluster agent.
4. A plain list gets a new request with a service token, no cookie, and a label selector. The selector matches `field.cattle.io/projectId` on the projects of the caller that contain an allowed namespace, when every allowed namespace has that label. It matches the allowed namespace names in every other case. That case includes the selector that matches no namespace, when the caller may see none.
5. A watch (`?watch=true`) gets a new request with a service token, no cookie, and the caller's own query. The selector of a watch does not change while the stream runs. A name selector on a watch hides a namespace that Rancher puts in a project of the caller later. The watch therefore gets a project selector, or no selector at all. The selector of the caller merges into it, as it does on a list. Three cases apply:
   - A caller with no namespace and no project gets the selector that matches no namespace.
   - A caller whose namespaces are all in its own projects gets a `field.cattle.io/projectId` selector on those projects. A new namespace of such a project matches that selector by itself.
   - A caller with a namespace outside its own projects gets no selector, because no selector holds that namespace and the projects of the caller at the same time.

   The request keeps every Accept entry of the caller whose media type is `application/json`, for example a table request from kubectl, and drops every other entry, for example protobuf or CBOR. It sets `application/json` when no entry remains.
6. The service sends the new request to Rancher, and it streams the response to the client. The event filter runs on every watch, also on a watch with a selector. An event passes only when every namespace in it is in the allowed set. It also passes when its `field.cattle.io/projectId` label matches a project of the caller that contains an allowed namespace. A server-side table event has one row per namespace, and the filter checks the name and the labels of each row. The filter is the only gate for a watch with no selector. It is the second gate for a watch with a selector.

A request is a watch when its `watch` parameter is present with a value other than `0` or `false`, as the API server reads it. A watch request streams over chunked HTTP, or over a websocket connection after a protocol switch. A namespace that Rancher grants after the start of the watch becomes visible in one of three ways. A project selector matches a new namespace at once, when the project of the caller already contains an allowed namespace. A watch with no selector gets the event too, because the event filter reads the allowed set through the same cache as a plain list. In every other case the service ends the watch, and the client sees the namespace on its next watch.

The cache of an allowed set has one entry per credential that Rancher reads: the first `Authorization` value, or the first `R_SESS` cookie when that value is empty or absent. A second value, a second `R_SESS` cookie, and another cookie are not part of the key.

The service ends a watch with a selector when the selector of the caller changes. A timer re-reads the allowed set of the caller once per cache TTL, from the cache of a plain list, so it adds no request. It builds the selector again from that set. A different selector, or an allowed set that gets no selector at all, ends the stream with a clean end of the stream. The client then re-lists and re-watches, and the new watch gets the selector of the new allowed set. An upgraded stream gets a websocket close frame with status 1000 first, so the client reports a normal closure. A new namespace in a project of the caller that already contains an allowed namespace keeps the selector the same, so the stream stays open. A watch with no selector needs no timer, because the event filter reads the new allowed set by itself.

On a websocket connection the watch stream arrives as RFC 6455 frames. One message is a slice of at most 2048 bytes of the stream, and not one event. A message contains a part of an event, several events, or the tail of one event and the head of the next.

The filter assembles a text or a binary message from its continuation frames, decodes it, and appends the bytes to a stream buffer. It takes one complete event at a time from that buffer, and it applies the rule of the chunked path to each event. It writes each event that passes as a new message of its own, with a newline after the event. With the `base64.binary.k8s.io` subprotocol it decodes each message with `base64.StdEncoding`, and it encodes each event that it sends. It forwards a close, a ping, and a pong frame unchanged, also while a message is incomplete.

The upgrade offers no websocket extension, because the filter reads no compressed payload. The service deletes `Sec-WebSocket-Extensions` from the privileged request. When the 101 answer still names an extension, the service ends the stream at once with a close frame. It counts that stream in `drover.filter.watches.rejected`. The service also ends the stream when a message does not decode, or when the buffer passes 1 MiB without a complete event. The position in the stream is lost in both cases.

**Review access**

The service reads the body of a `selfsubjectaccessreviews` request. It intercepts a review that asks about a list or a watch on the namespaces resource. A review may name a namespace, because kubectl sends the namespace of the kubeconfig context even for this cluster-scoped resource. The service ignores that attribute. With `--fanout` on, the service also intercepts a review that asks about a cluster-wide list or watch of another resource. An intercepted review goes to Rancher with the caller's own credentials. A denied response gets `allowed: true` only when the caller has at least one allowed namespace. Every other review passes through unchanged. The service reads a JSON review or a Kubernetes protobuf review, and it answers an intercepted review in JSON. The service grants a review only when the spec that the answer echoes still names that list or watch. Its own parser matches JSON keys without case, and the API server's parser matches them with case.

## Fan-out

With `--fanout` on, the service answers a cluster-wide list of a namespaced kind, for example `kubectl get pods -A`, with one request per allowed namespace, merged into one collection.

1. A Rancher project member has the list permission inside the namespaces of its projects only, and none at cluster scope. A cluster-wide list then gets a 403 error from Rancher.
2. The service sends the native request first. The fan-out starts only after that request gets a 403 error.
3. Each namespaced request of the fan-out uses the credentials of the caller, not the service token. RBAC checks each request on its own. The fan-out then grants no permission beyond what the caller already has.
4. A namespace that answers with a status other than 200 drops out of the merge. A 404 error stops the fan-out, because every namespace gives that same answer, and the native 403 error then stands. A cluster-scoped kind, for example `nodes`, always takes that path.
5. A caller that may see no object of the kind gets an empty collection, not the native 403 error. This covers a caller with no allowed namespace, and a caller whose namespaces all deny the list. A client then shows an empty view instead of an error, as it does for the namespace list. The service reads the kind and the scope of the resource from the discovery document of the api path, with the credentials of the caller, because the merge has no answer to take them from. A cluster-scoped kind keeps its 403 error, and so does a resource that discovery does not name.
6. The merged answer streams to the client. It holds the elements of at most `--fanout-concurrency` answers at a time. A tenant with hundreds of namespaces then needs no more than that count of answers in memory.
7. The merged answer puts the `kind`, the `apiVersion` and the `metadata` fields after the elements. The `resourceVersion` of the merge is the lowest value of the answers, so the service knows it only after the last answer. The `kind` of a custom resource list also stands after its elements upstream, because an unstructured object serializes its keys in alphabetical order. A JSON object has no fixed field order, so this position is valid. The service asks discovery for the kind when no answer names one.
8. The merge names no `continue` token. It also drops the `limit` and `continue` parameters of the caller. A `continue` token belongs to one namespace only. A client that reads a merged answer reads one page, and it stops there.
9. The service merges a `List` and a `Table` alike. A merged `Table` keeps the `columnDefinitions` field of the first answer. A table view in kubectl then keeps its columns.

A caller may see more allowed namespaces than `--fanout-max-namespaces` allows. That cluster-wide list then gets a 403 error, and the service runs no fan-out.

The `selfsubjectaccessreviews` path grants a cluster-wide `list` and a cluster-wide `watch` of any named resource when `--fanout` is on and the caller has at least one allowed namespace. A review of the resource `*` or the group `*` keeps its native answer. The list and the watch apply the real permission on their own. A grant that the permission does not cover then gives an empty answer, not an error.

**Merged watch**

With `--fanout` on, the service also answers a cluster-wide watch, for example `kubectl get pods -A --watch`, with one upstream watch per allowed namespace, merged into one stream.

1. The service opens every upstream watch at the same time, on the namespaced path of the kind, with the credentials of the caller. `--fanout-max-watch-namespaces` bounds that set. A caller above the bound gets a 403 error, and the service opens no upstream watch. The bound is below `--fanout-max-namespaces`. A list keeps one upstream request open for the length of that request. A watch keeps one upstream connection open for the whole life of the stream, and each client of the caller has its own set.
2. Each upstream watch keeps the query of the caller, with its `resourceVersion` and its `timeoutSeconds`. The merged list reports the lowest `resourceVersion` of its answers, so a watch from that value loses no event. A namespace that answered the list at a higher revision repeats the events between the two revisions. A client takes a repeated event as an update of an object that it has already.
3. A namespace that answers a status other than 200 drops out of the stream. A 404 error gives the native 403 error, because the kind then has no namespace scope. An allowed set whose namespaces all deny the watch gives the native answer too, because a stream without an upstream watch hides a real permission gap.
4. A caller that may see no namespace gets an open stream without an event, as it gets a collection without an element on the list path.
5. The merged stream sends no `BOOKMARK` event, and the service drops `allowWatchBookmarks` from the upstream requests. A bookmark names the revision of one namespace, and a client that resumes the merge from it loses the events of every other namespace. A watch-list, a watch with `sendInitialEvents=true`, is the exception: each upstream watch keeps `allowWatchBookmarks`, its initial events reach the client, and the merged stream sends one `BOOKMARK` with the annotation `k8s.io/initial-events-end` once every upstream watch sent its own, with the lowest `resourceVersion` of them. A caller with no allowed namespace gets that `BOOKMARK` at once, with no `resourceVersion`.
6. The set of upstream watches is fixed while the stream runs. A timer re-reads the allowed set of the caller once per cache TTL, and a set that changed ends the stream. The client re-lists and re-watches, and the new stream covers the new set. The re-read goes through the cache of a plain list, so it adds no request.
7. An upstream watch that ends ends the merged stream. The client re-lists, which repairs the gap. A stream that stays open with one dead namespace shows stale data and reports nothing about it.
8. The service owns the transport of the client on this path, because it relays no single upstream stream. A chunked client gets the events as a newline-delimited JSON stream. A client that asks for a websocket gets the protocol switch from the service itself, and one frame per event: a binary frame for the `binary.k8s.io` subprotocol, and a text frame with base64 text for `base64.binary.k8s.io`. A close frame of the client ends the stream, and a ping frame gets a pong.
9. Every upstream watch takes the chunked transport. The service deletes the handshake headers of the caller from the upstream request.

## Requirements

1. A Rancher service user with `list` and `watch` on `namespaces` in every cluster whose tenants use the filter. A `cluster-owner` binding also covers that.
2. An API token of that service user. The token has no scope.
3. The token in a Secret, mounted into the service.
4. Route rules on the Rancher hostname for these requests:

   | Match | Method | Path | Target |
   | --- | --- | --- | --- |
   | `Exact` | GET | `/k8s/clusters/<id>/api/v1/namespaces` | the service |
   | `PathPrefix` | GET | `/k8s/clusters/<id>/api/v1/namespaces` | Rancher |
   | `PathPrefix` | GET | `/k8s/clusters/<id>/api/v1` | the service |
   | `PathPrefix` | GET | `/k8s/clusters/<id>/apis` | the service |
   | `Exact` | POST | `/k8s/clusters/<id>/apis/authorization.k8s.io/v1/selfsubjectaccessreviews` | the service |

   A Gateway picks the rule by the specificity of the match, not by the order of the rules. An `Exact` match wins over a `PathPrefix` match, and a longer prefix wins over a shorter one. The namespaces prefix is therefore the rule of every namespaced read, and it keeps `exec`, `attach`, `portforward`, and a log stream on Rancher, off the service. Give the Rancher rule the same unlimited request timeout as the service rule, because those streams stay open for minutes.

With these routes, the service becomes the data path for most reads of a tenant. The tenant then depends on the availability and the latency of the service for those reads.

## Configuration

`drover api-filter [flags]` starts the service.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--listen` | `:8080` | Address the service listens on. |
| `--upstream` | (required) | URL of Rancher. Use `http://` or `https://`. The path must be empty or `/`. |
| `--upstream-ca-file` | | PEM bundle that verifies an `https` upstream. |
| `--upstream-insecure-skip-verify` | `false` | Skip the certificate verification of an `https` upstream. |
| `--token-file` | (required) | File with the API token of the Rancher service user. |
| `--cache-ttl` | `15s` | Lifetime of a cached allowed set. |
| `--max-cache-entries` | `1000` | Hard bound on the cached allowed sets. |
| `--fetch-rate` | `50` | Fetches per second that the shared rate limit allows, for a fetch of an allowed set. The burst is twice the rate. |
| `--fetch-rate-per-caller` | `5` | Fetches per second that the rate limit of one caller credential allows, for a fetch of an allowed set. The burst is twice the rate. |
| `--fanout` | `false` | Answer a cluster-wide list of a namespaced kind with one request per allowed namespace. |
| `--fanout-max-namespaces` | `200` | Count of allowed namespaces above which such a list answers 403. |
| `--fanout-concurrency` | `16` | Namespaced requests of one fan-out that run at a time. It also bounds the memory of one fan-out. |
| `--fanout-max-watch-namespaces` | `50` | Count of allowed namespaces above which a cluster-wide watch answers 403. |
| `--max-watches` | `1000` | Count of open watch streams above which a new watch answers 503. |
| `--max-watches-per-caller` | `100` | Count of open watch streams of one caller above which a new watch of that caller answers 503. A caller is one credential that Rancher reads. |
| `--log-level` | `info` | One of `debug`, `info`, `warn`, or `error`. |
| `--shutdown-grace` | `20s` | Grace period for the shutdown after SIGTERM or SIGINT. |
| `--otlp-endpoint` | `$OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC endpoint, `host:port` or a URL. Empty turns telemetry off. |
| `--otlp-traces` | `true` | Send traces to the OTLP endpoint. |
| `--otlp-metrics` | `true` | Send metrics to the OTLP endpoint. |
| `--service-name` | `$OTEL_SERVICE_NAME`, or `drover` | `service.name` resource attribute. |

`--upstream-insecure-skip-verify` is for an upstream inside the cluster whose certificate comes from a private CA that the deployment does not copy. A network policy must then limit the path to Rancher.

`--max-watches` and `--max-watches-per-caller` count each watch from before its upstream request until its stream ends. A burst of parallel watches then cannot pass a limit. With the per-caller limit, one caller cannot take every stream of the service. A merged watch has one upstream connection per allowed namespace. One caller then has at most `--max-watches-per-caller` times `--fanout-max-watch-namespaces` upstream watch connections, 5,000 with the defaults.

The upstream is the Rancher Service inside the cluster, for example `http://rancher.cattle-system.svc`. The public hostname is not a valid upstream, because the route sends the filtered paths back to the service.

The service starts with no token file and returns a 502 Status on a filtered request until the file gets a token.

`GET /healthz` returns status 200 with body `ok`, always. `GET /readyz` returns status 200 with body `ok` in normal operation, and status 503 with body `draining` after the shutdown starts.

## Behaviour differences

| Difference | Detail |
| --- | --- |
| Cache delay | A role change becomes visible after the cache TTL, on top of Rancher's own delay. |
| Field selector | A field selector on a name outside the allowed set returns an empty list. A `get` on that name returns Forbidden. |
| Namespace cap | A caller with more than 20,000 allowed namespace names gets an error, not a list. |
| Fan-out cap | A cluster-wide list gets a 403 error, with no fan-out, when the caller has more than `--fanout-max-namespaces` allowed namespaces. |
| Watch cap | A cluster-wide watch gets a 403 error, with no merge, when the caller has more than `--fanout-max-watch-namespaces` allowed namespaces. |
| Stream cap | A namespace watch or a cluster-wide watch gets a 503 error when the service has `--max-watches` open watch streams, or when the caller has `--max-watches-per-caller` open watch streams. The client retries. |
| Merged watch end | A merged cluster-wide watch ends when one upstream watch ends, and when the allowed set of the caller changes. The client re-lists and re-watches. |
| Merged watch bookmark | A merged cluster-wide watch sends no `BOOKMARK` event, also when the client asks for one. A watch-list gets the one `BOOKMARK` that ends its initial events. |
| Empty answer | A cluster-wide list of a namespaced kind gets an empty collection, not Forbidden, when the caller may see no object of that kind. |
| Self-check | `kubectl auth can-i list namespaces` returns yes when the caller has at least one allowed namespace, while RBAC returns no. With `--fanout` on, the same applies to `list` and `watch` on any named resource, cluster-wide, also a cluster-scoped one, whose list then keeps its 403 error. |
| Fetch rate | A fetch of an allowed set past `--fetch-rate` waits up to 5 s for a free token, then gets a 429 Status. The limit per caller, `--fetch-rate-per-caller`, applies first with the same wait and the same 429 Status, for one credential across all clusters. A fetch that it throttles takes no token of the shared limit. |
| Trust level | The service is a privileged component. It uses the service token for the filtered namespace list and for the watch stream. |
| Project scope | A project selector has the projects of the requested cluster only. The project part of a Rancher project id is unique inside one cluster, and the namespace label has that part alone. |

## Logging

The service writes JSON logs to stderr, one line per intercepted request.

A namespace list log line and a collection log line have these fields: `cluster`, `outcome` (`native`, `filtered`, `passthrough`, `denied`, `fanout`, `empty`, `capped`, or `error`), `status`, `count` (on `filtered`, `fanout`, `empty`, or `capped` only), `watch`, and `duration_ms`. A Status body that the service writes for an error names no internal address and no file path. The log line has the full error. A collection log line also has `resource`, the kind of the requested collection.

A review log line has these fields: `cluster`, `outcome` (`passthrough`, `native`, or `granted`), and `status`.

Both log lines have the field `user`, the caller's Rancher user id from a SelfSubjectReview, only when the service resolves it. No metric attribute has the name, because a user name has an unbounded value set and that shape is a cardinality fault.

The service never logs a token, a cookie, a header value, or a request body. It logs a namespace name at the `debug` level only.

The service writes a `warn` line when the privileged list request gets a 403 error. The cause is a namespace list permission that the service user does not have.

A log line has `trace_id` and `span_id` when the request has a span.

## Telemetry

The service continues an incoming `traceparent` on every request. It starts a span when the header is absent. A request span has a child span for the Steve call, the project list, the privileged list, and the privileged watch.

A span of the service is named after the method and a path template, for example `GET /k8s/clusters/{cluster}/api/v1/pods`, and the server span carries that template as `http.route`. A collector that rebuilds a span name from the semantic conventions reads that attribute and gives the span the method alone without it, so the two names agree. The cluster id, a namespace name and an object name each become a placeholder, because their value set is unbounded. The api group, the version and the resource name stay, because they say what the request asks for and the api surface of a cluster is bounded. Search a trace on `resource.service.name`, which `--service-name` sets, not on the span name.

A client span records `url.full` without its query, because the query of the privileged list names every allowed namespace of the caller.

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

`drover.filter.watches.rejected` counts a watch that a watch limit refuses with a 503 error. Its `limit` attribute is `shared` for `--max-watches`, and `caller` for `--max-watches-per-caller`. The metric also counts an upgraded stream that the service ends for a websocket extension, without a `limit` attribute.

The `cluster` attribute of the two request metrics names a cluster once Steve answered a namespace list for it. Every other request records `unknown`, because the path of a request names any string, also before authentication, and a metric attribute with an unbounded value set is a cardinality fault.

The `limit` attribute of `drover.filter.fetch.throttled` is `caller` for the limit per caller, and `shared` for the shared limit.

## Shutdown

On SIGTERM or SIGINT, the service drains before it stops:

1. It marks itself not ready. `/readyz` answers 503 from that point on.
2. It ends every open watch stream with a clean end of the stream. An upgraded stream gets a websocket close frame with status 1000 first. A client re-lists and re-watches, with no error.
3. It stops the HTTP server, within the `--shutdown-grace` period. The close frame of an upgraded stream waits at most 5 s for the client, before the server stops.

The exit code is non-zero only when the server does not stop in time.
