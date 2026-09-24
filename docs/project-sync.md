# project-sync

This service copies labels and annotations of a Rancher project to every namespace of that project, because Rancher does not copy them. The `--labels` and `--annotations` flags are the allow list of keys. The value of the project wins, and the service overwrites a different value on the namespace. The service polls Rancher at every `--interval`, and it also keeps one namespace watch per cluster open, so that a new namespace gets its keys within a few seconds. One replica is enough, because every run is a full reconcile. A namespace list that returns status 403 means that the service user may not list namespaces on that cluster. The service writes a warning with the cluster id and continues with the next cluster.

## Requirements

1. A Rancher service user with `get`, `list`, `watch`, and `patch` on `namespaces`, and with `get`, `list`, and `watch` on `projects` of `management.cattle.io`, in every cluster whose namespaces the service syncs. A `cluster-owner` binding also covers that.
2. An API token of that service user, in a Secret that the service mounts.

## Configuration

`drover project-sync [flags]` starts the service.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--rancher-url` | (required) | URL of Rancher. Use `http://` or `https://`. The path must be empty or `/`. |
| `--rancher-ca-file` | | PEM bundle that verifies an `https` URL. |
| `--rancher-insecure-skip-verify` | `false` | Skip the certificate verification of an `https` Rancher URL. |
| `--token-file` | (required) | File with the API token of the Rancher service user. |
| `--labels` | | Comma-separated label keys of a project to copy. |
| `--annotations` | | Comma-separated annotation keys of a project to copy. |
| `--name-label` | | Label key on the namespace that gets the display name of the project. |
| `--name-annotation` | | Annotation key on the namespace that gets the display name of the project. |
| `--interval` | `60s` | Time between two runs. |
| `--patch-rate` | `10` | Namespace patches per second that the watch of one cluster sends. |
| `--listen` | `:8080` | Address the service listens on. |
| `--log-level` | `info` | One of `debug`, `info`, `warn`, or `error`. |
| `--otlp-endpoint` | `$OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC endpoint, `host:port` or a URL. Empty turns telemetry off. |
| `--otlp-traces` | `true` | Send traces to the OTLP endpoint. |
| `--otlp-metrics` | `true` | Send metrics to the OTLP endpoint. |
| `--service-name` | `$OTEL_SERVICE_NAME`, or `drover` | `service.name` resource attribute. |

The flags need at least one label key, annotation key, name label key, or name annotation key. A key must be a valid Kubernetes label or annotation key. A key whose prefix is `cattle.io`, `kubernetes.io`, or `k8s.io`, or a subdomain of one of them, is not valid, because Rancher and Kubernetes own those domains.

The `--name-annotation` value is the raw display name of the project. The `--name-label` value is a sanitised copy: the service replaces every character outside `[A-Za-z0-9._-]` with `-`, and cuts the result to 63 characters. It then trims the leading and trailing characters that are not alphanumeric. A name that sanitises to an empty value gets no label, and the service writes one warning line for that project in that run.

The name path owns its key. A key that `--labels` or `--annotations` names again gets the display name, never a label or an annotation of the Project object. A namespace can therefore have no name key, because the display name has no valid label value, or because the namespace is in no project.

The service starts with no token file. It skips the run until the file has a token, and it writes one warning per state change of the file.

`GET /healthz` returns status 200 with body `ok`.

## Ownership of a key

The service records the keys that it set on a namespace in two annotations of that namespace: `drover-managed-labels` and `drover-managed-annotations`. Each value is a comma-separated list of keys.

The service removes a key that one of these lists names once the project of the namespace no longer sets it. A namespace that moves to another project thus loses the keys of the old project. A key outside these lists belongs to the tenant, and the service never removes it. The lists are on the namespace, so a tenant can edit them. The service therefore removes only a key that `--labels`, `--annotations`, `--name-label`, or `--name-annotation` names, and it ignores every other key of the lists. The `--labels` and `--annotations` flags reject `drover-managed-labels` and `drover-managed-annotations`, because the service writes them itself.

## Design

The service polls `GET /v3/projects` at every interval. One call per interval returns every project that the service user sees, because Rancher filters that list by RBAC. The cluster set comes from the `clusterId` of those projects, so the service needs no cluster list of its own. One run syncs at most 8 clusters at the same time.

The service lists the namespaces of a cluster in pages of 500, with the `limit` and `continue` parameters of the Kubernetes API. A `continue` token that is too old returns status 410, and the service then starts the list again from the first page, once. The service decodes each page as a stream, one namespace at a time.

Each namespace keeps only the keys that the sync reads. These are the project label, the two ownership annotations, and the keys that `--labels`, `--annotations`, `--name-label`, and `--name-annotation` name. Metadata under other keys uses memory only while the service decodes its namespace. A tenant can fill an ownership annotation up to the annotation limit of the API server, 256 KiB. The service therefore treats an ownership annotation above 4096 bytes as absent, because a list that the service writes is a short key list. The next patch then writes that annotation again.

The project list asks for pages of 500 too, and each project keeps only the keys of `--labels` and `--annotations`. The service reads at most 32 MiB of one page of either list. A larger namespace page fails the run of that cluster with an error that names the limit in bytes, and the other clusters continue. A larger project page fails the whole run.

The service also keeps one namespace watch per cluster open, on `GET /k8s/clusters/<id>/api/v1/namespaces?watch=true`. Rancher sets the project label about three seconds after the namespace create, so the service acts on an `ADDED` event and on a `MODIFIED` event. A stream that ends starts again, after a backoff that grows from 1 s to 30 s. An expired resource version starts the stream again without one. The periodic reconcile stays the backstop: it catches a missed event, a dropped watch, and every change on the project side.

The watch puts the namespace of an event into the queue of its cluster, by namespace name. One worker per cluster patches the namespaces of that queue, one at a time. A second event of a namespace replaces the first one in the queue, so a storm of events on one namespace gives one patch. The `--patch-rate` flag bounds the patches per second of one cluster, and the burst equals the rate.

A patch of a namespace that no longer exists, or of a namespace in `Terminating`, is not an error. The service skips it, and the next reconcile run repeats the work.

## Telemetry

With `--otlp-endpoint` set, one reconcile run produces a span named `reconcile`, and one patched namespace produces a span named `patch_namespace`. That span has the attributes `drover.cluster`, `drover.origin`, and `k8s.namespace.name`. The `drover.origin` value is `reconcile` or `watch`. The service exports these metrics:

| Metric | Kind | Unit | Attributes |
| --- | --- | --- | --- |
| `drover.sync.reconciles` | Counter | `1` | `outcome` |
| `drover.sync.namespaces.patched` | Counter | `1` | `origin` |
| `drover.sync.errors` | Counter | `1` | |
| `drover.sync.duration` | Histogram | `s` | |
| `drover.sync.events` | Counter | `1` | `cluster`, `type` |
| `drover.sync.watches.open` | UpDownCounter | `1` | `cluster` |
