# project-sync

This service copies labels and annotations of a Rancher project to every namespace of that project, because Rancher does not copy them. The `--labels` and `--annotations` flags are the allow list of keys. The value of the project wins, and the service overwrites a different value on the namespace. The service polls Rancher at every `--interval`, and it also keeps one namespace watch per cluster open, so that a new namespace gets its keys within a few seconds. It keeps one project watch too, so a project label, annotation, or display name change reaches its namespaces within seconds. One replica is enough, because every run is a full reconcile. A namespace list that returns status 403 means that the service user may not list namespaces on that cluster. The service writes a warning with the cluster id and continues with the next cluster.

With `--service-accounts`, the service also keeps three ServiceAccounts per Rancher project, one per project role. A CI job of a project authenticates with a token of such a ServiceAccount, through the Rancher proxy. See [Service accounts](#service-accounts).

With `--openbao-address` as well, the service writes the Kubernetes secrets engine config of every cluster into OpenBao, so that OpenBao creates short-lived tokens of these ServiceAccounts. See [OpenBao config](#openbao-config).

## Requirements

1. A Rancher service user with `get`, `list`, `watch`, and `patch` on `namespaces`, in every cluster whose namespaces the service syncs. A `cluster-owner` binding also covers that.
2. The same service user with `list` and `watch` on `projects` of `management.cattle.io`, cluster-wide, in the Rancher cluster. The Rancher cluster is the cluster that Rancher itself runs in, cluster id `local`. A `ClusterRoleTemplateBinding` on that cluster, with a role template that has those two verbs, gives this permission. The bindings on the other clusters do not give it. Without it, a project watch request answers with status 403. The service then writes one warning per attempt, with the backoff between attempts, and the periodic reconcile run continues on its own.
3. An API token of that service user, in a Secret that the service mounts.
4. With `--service-accounts`, the same service user also needs these rights in every cluster, and in the Rancher cluster through a `ClusterRoleTemplateBinding`:
   - every verb on `namespaces`, and `create` and `manage-namespaces` on `projects` of `management.cattle.io`. With these rights the user holds every rule of the Rancher ClusterRoles `<project>-namespaces-edit` and `<project>-namespaces-readonly`, so it binds them without `bind`.
   - `get`, `list`, `watch`, `create`, and `delete` on `serviceaccounts`.
   - `get`, `list`, `watch`, `create`, `update`, and `delete` on `rolebindings` and `clusterrolebindings`.
   - `bind` on the ClusterRoles `admin`, `edit`, `view`, and `create-ns`, with `resourceNames`. Do not grant `bind` on every ClusterRole, because that equals cluster admin.

   Give the service its own service user. The API filter is on the request path of every tenant, and its user needs only the namespace list.
5. With `--openbao-address`, the same service user also needs these rights in every cluster, and in the Rancher cluster through a `ClusterRoleTemplateBinding`:
   - `create` on `serviceaccounts/token`, with the `resourceNames` `openbao`, `project-owner`, `project-member`, and `read-only`.
   - `get`, `list`, `watch`, `create`, `update`, and `delete` on `roles`.

   With these rights and the `get` on `serviceaccounts`, the user holds every rule of the Role `drover-openbao`, so it creates and binds that Role without `escalate` and without `bind`. The user also needs `get` on `settings` of `management.cattle.io`, to read the Rancher setting `cacerts`. The GlobalRole `user-base` gives this right.
6. With `--openbao-address`, OpenBao needs:
   - a Kubernetes auth mount at `--openbao-auth-path`, with the role `--openbao-role` for the ServiceAccount of the pod of the service.
   - a policy on that role with `update` on `<prefix>/<cluster id>/config` of every cluster. Without it, a write answers 403.
   - a Kubernetes secrets engine mount at `<prefix>/<cluster id>` per cluster. Another component creates the mount and its roles.

   OpenBao reaches each cluster through the Rancher proxy, with a ServiceAccount token of that cluster. Rancher accepts such a token only for a cluster with a `ClusterProxyConfig` that has `enabled: true`. See [the API filter document](api-filter.md).

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
| `--service-accounts` | `false` | Keep three ServiceAccounts per project, one per project role. |
| `--openbao-address` | | URL of OpenBao. Use `http://` or `https://`. The path must be empty or `/`. Empty turns the OpenBao config off. |
| `--openbao-auth-path` | `kubernetes` | Kubernetes auth mount of OpenBao that the service logs in to. |
| `--openbao-role` | `project-sync` | Role of that auth mount. |
| `--openbao-jwt-file` | | File with the ServiceAccount token of the pod, for the login. Required with `--openbao-address`. |
| `--openbao-mount-prefix` | `kubernetes` | Path prefix of the secrets engine mounts. The config of a cluster is at `<prefix>/<cluster id>/config`. |
| `--openbao-rancher-url` | | URL of Rancher that OpenBao uses. Use `https://`. The path must be empty or `/`. Required with `--openbao-address`. |
| `--openbao-token-ttl` | `24h` | Requested lifetime of the token of a cluster. The minimum is `10m`. The API server can shorten it. |
| `--interval` | `60s` | Time between two runs. |
| `--patch-rate` | `10` | Namespace patches and project namespace lists per second that the watches of one cluster send, together. |
| `--listen` | `:8080` | Address the service listens on. |
| `--log-level` | `info` | One of `debug`, `info`, `warn`, or `error`. |
| `--otlp-endpoint` | `$OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC endpoint, `host:port` or a URL. Empty turns telemetry off. |
| `--otlp-traces` | `true` | Send traces to the OTLP endpoint. |
| `--otlp-metrics` | `true` | Send metrics to the OTLP endpoint. |
| `--service-name` | `$OTEL_SERVICE_NAME`, or `drover` | `service.name` resource attribute. An empty value, `--service-name=`, sets no `service.name`, so that a collector can derive it. The SDK still takes a `service.name` from `OTEL_SERVICE_NAME` or `OTEL_RESOURCE_ATTRIBUTES`. |

The flags need at least one label key, annotation key, name label key, or name annotation key, or `--service-accounts`. Every `--openbao-` flag needs `--service-accounts`. A key must be a valid Kubernetes label or annotation key. A key whose prefix is `cattle.io`, `kubernetes.io`, or `k8s.io`, or a subdomain of one of them, is not valid, because Rancher and Kubernetes own those domains.

The `--name-annotation` value is the raw display name of the project. The `--name-label` value is a sanitised copy: the service replaces every character outside `[A-Za-z0-9._-]` with `-`, and cuts the result to 63 characters. It then trims the leading and trailing characters that are not alphanumeric. A name that sanitises to an empty value gets no label, and the service writes one warning line for that project in that run.

The name path owns its key. A key that `--labels` or `--annotations` names again gets the display name, never a label or an annotation of the Project object. A namespace can therefore have no name key, because the display name has no valid label value, or because the namespace is in no project.

The service starts with no token file. It skips the run until the file has a token, and it writes one warning per state change of the file.

`GET /healthz` returns status 200 with body `ok`.

## Ownership of a key

The service records the keys that it set on a namespace in two annotations of that namespace: `drover-managed-labels` and `drover-managed-annotations`. Each value is a comma-separated list of keys.

The service removes a key that one of these lists names once the project of the namespace no longer sets it. A namespace that moves to another project thus loses the keys of the old project. A namespace in no project loses every key of the lists, and the lists. The service reads the project of a namespace from the annotation `field.cattle.io/projectId`, not from the label of the same name, because Rancher keeps the label when a namespace leaves its project. A key outside these lists belongs to the tenant, and the service never removes it. The lists are on the namespace, so a tenant can edit them. The service therefore removes only a key that `--labels`, `--annotations`, `--name-label`, or `--name-annotation` names, and it ignores every other key of the lists. The `--labels` and `--annotations` flags reject `drover-managed-labels` and `drover-managed-annotations`, because the service writes them itself.

## Design

The service polls `GET /v3/projects` at every interval. One call per interval returns every project that the service user sees, because Rancher filters that list by RBAC. The cluster set comes from the `clusterId` of those projects, so the service needs no cluster list of its own. One run syncs at most 8 clusters at the same time.

The reconcile run lists every namespace of a cluster, also a namespace without a project label, so it finds a namespace that left its project together with its label. It lists in pages of 500, with the `limit` and `continue` parameters of the Kubernetes API. A `continue` token that is too old returns status 410, and the service then starts the list again from the first page, once. The service decodes each page as a stream, one namespace at a time.

Each namespace keeps only the keys that the sync reads. These are the project label, the project annotation, the two ownership annotations, and the keys that `--labels`, `--annotations`, `--name-label`, and `--name-annotation` name. Metadata under other keys uses memory only while the service decodes its namespace. A tenant can fill an ownership annotation up to the annotation limit of the API server, 256 KiB. The service therefore treats an ownership annotation above 4096 bytes as absent, because a list that the service writes is a short key list. The next patch then writes that annotation again.

The project list asks for pages of 500 too, and each project keeps only the keys of `--labels` and `--annotations`. The service reads at most 32 MiB of one page of either list. A larger namespace page fails the run of that cluster with an error that names the limit in bytes, and the other clusters continue. A larger project page fails the whole run.

The service also keeps one namespace watch per cluster open, on `GET /k8s/clusters/<id>/api/v1/namespaces?watch=true`. The watch selects the namespaces with the project label. Rancher sets that label about three seconds after the namespace create, so the service acts on an `ADDED` event and on a `MODIFIED` event. A namespace that loses the label leaves the selection, and the watch sends a `DELETED` event for it, with the namespace from before the change. Unless the namespace is in deletion, the service reads the namespace again and handles the current state. A stream that ends starts again, after a backoff that grows from 1 s to 30 s. An expired resource version starts the stream again without one. The periodic reconcile stays the backstop: it catches a missed event, a dropped watch, and a new cluster.

The watch puts the namespace of an event into the queue of its cluster, by namespace name. One worker per cluster patches the namespaces of that queue, one at a time. A second event of a namespace replaces the first one in the queue, so a storm of events on one namespace gives one patch. The `--patch-rate` flag bounds, together, the patches and the project namespace lists per second of one cluster, and the burst equals the rate.

A patch of a namespace that no longer exists, or of a namespace in `Terminating`, is not an error. The service skips it, and the next reconcile run repeats the work.

The service also keeps one project watch open, on `GET /k8s/clusters/local/apis/management.cattle.io/v3/projects?watch=true`, with the same timeout and backoff as the namespace watch. The watch decodes a Project object into the same fields as the project list. These are the project id, the cluster id, the display name, and the allow-listed labels and annotations. A restart without a resource version reads the full project list again, as an `ADDED` event per project. Each such event is an in-memory compare only, so the restart costs no request.

A project of a cluster that the last reconcile run did not see is skipped. The next run takes it up, so a new cluster waits for the next run, as it did before the project watch. A Project object whose `metadata.namespace` differs from its `spec.clusterName` is skipped, with a warning. A project whose name is not a valid label value is skipped, because no namespace can carry it in its project label.

A change updates the project that the service holds from the last reconcile run, and puts its name on the queue of its cluster. A second event of the same project replaces the first one in the queue, so a storm of events on one project gives one list. One lister per cluster takes a project from that queue. It lists the namespaces of the cluster with the label selector `field.cattle.io/projectId=<name>`, in pages of 500, as the reconcile list does. It puts each such namespace on the patch queue of the cluster, with the origin `project`. The worker of the cluster then patches these namespaces as it patches the namespaces that the namespace watch finds.

A display name change clears the name cache of the worker first, so the worker computes the new label value. A list that fails is a warning, and the next reconcile run repeats the work. A status 403 gives the same warning as a failed namespace list of the reconcile run.

A project owner edits the labels, the annotations, and the display name of its own project. The values that the sync copies are tenant input, as they were with the reconcile run. The project watch only shortens the delay before a change reaches its namespaces. The queue and the shared limiter of a cluster bound the requests that a storm of project edits causes. The project watch never starts a reconcile run. The service logs project ids and key names, never a display name or a value.

## Service accounts

With `--service-accounts`, the service keeps these objects in every cluster that the reconcile run syncs:

1. One account project, with the display name `drover`, the label `drover-service-accounts: "true"`, and the service user in the annotation `field.cattle.io/creatorId`. Rancher then binds the service user as its owner, and no tenant gets a role in it. The service creates the project when the cluster has none.
2. Per tenant project, the account namespace `drover-<project>` in the account project. `<project>` is the project name, for example `p-n52j9`.
3. In the account namespace, the ServiceAccounts `project-owner`, `project-member`, and `read-only`. The user name of such a ServiceAccount is `system:serviceaccount:drover-<project>:<role>`.
4. In every namespace of the tenant project, the RoleBindings `drover-project-owner`, `drover-project-member`, and `drover-read-only`, to the ClusterRoles `admin`, `edit`, and `view`.
5. Per role, the ClusterRoleBinding `drover-<project>-<role>-namespaces`, to the Rancher ClusterRole `<project>-namespaces-edit` for `project-owner` and `project-member`, and to `<project>-namespaces-readonly` for `read-only`. Rancher keeps the `resourceNames` of these ClusterRoles equal to the project namespaces, so the API filter answers the namespace list of the ServiceAccount from them.
6. For `project-owner` and `project-member`, the ClusterRoleBinding `drover-<project>-<role>-create-ns`, to the Rancher ClusterRole `create-ns`.

Every object has the labels `drover-project: <project>` and `drover-role: <role>`, and every binding has one subject. The account namespace owns the bindings, so the garbage collector deletes them with it.

A tenant project is every project but the System project, the Default project, and an account project. The service reads the System and the Default project from the labels `authz.management.cattle.io/system-project` and `authz.management.cattle.io/default-project`.

The ServiceAccounts get the Kubernetes rights of a project role through `admin`, `edit`, and `view`. They do not get the extra rules of the Rancher role templates, for example the monitoring resources or the read of nodes.

The service applies these rules:

- A namespace that joins a project gets the three RoleBindings. A namespace that moves to another project gets the subjects and the owner of the new project. A namespace that leaves its project loses the RoleBindings. The namespace watch starts this work within seconds.
- A new project gets its account namespace, ServiceAccounts, and ClusterRoleBindings within seconds, through the project watch.
- The reconcile run corrects a binding that differs and creates a missing object again. A tenant can edit or delete the RoleBindings in its namespaces, and the next run restores them.
- The service uses an account namespace only in an account project. A tenant can create the name `drover-<project>` first in its own project. The service then writes a warning, gives the project no accounts, and deletes the bindings of the project.
- A deleted account project leaves its namespaces in no project, because Rancher deletes only the namespaces with the annotation `field.cattle.io/creatorId`. The reconcile run creates a new account project, and it moves such a namespace into it when a ClusterRoleBinding of the service names the namespace as its owner.
- The reconcile run deletes an account namespace whose project is gone, after Rancher answers 404 for that project. It deletes the bindings of the project first.
- To make every token of a ServiceAccount invalid, delete the ServiceAccount. The next reconcile run creates it again. Do not delete the account namespace for this. A binding names its ServiceAccount by namespace and name, and the garbage collector deletes the bindings only some time after the namespace. Until then, a tenant who creates a namespace of that name gets the rights of the bindings.

## OpenBao config

With `--openbao-address`, the service keeps these objects in every cluster that the reconcile run syncs, the Rancher cluster included:

1. The namespace `drover-openbao` in the account project, with the label `drover-role: openbao`.
2. In that namespace, the ServiceAccount `openbao`, with the same label.
3. In every account namespace `drover-<project>`, the Role `drover-openbao`. It grants `get` on `serviceaccounts` and `create` on `serviceaccounts/token`, both with the `resourceNames` `project-owner`, `project-member`, and `read-only`.
4. In every account namespace, the RoleBinding `drover-openbao` to that Role, with the one subject `drover-openbao/openbao`. The namespace `drover-openbao` owns the RoleBinding, so the garbage collector deletes it with that namespace.

The Role and the RoleBinding have the labels `drover-project: <project>` and `drover-role: openbao`. The service applies these rules:

- The service uses the namespace `drover-openbao` only in an account project, as it does with an account namespace. A tenant can create the name first in its own project. The service then writes a warning, writes no config for that cluster, and deletes every RoleBinding `drover-openbao` of the cluster.
- A namespace `drover-openbao` in no project moves into the account project only when a RoleBinding `drover-openbao` in an account namespace names it as owner.
- A project that gets no accounts, or whose account namespace the reconcile run deletes, loses its RoleBinding `drover-openbao` together with its other bindings.
- The reconcile run corrects a Role or a RoleBinding that differs, and creates a missing one again. The project watch does not create them, so a new project waits for the next run.
- A project with the name `openbao` gets no accounts, because its account namespace is `drover-openbao`.
- The service deletes none of these objects when `--openbao-address` goes empty. It also keeps the config of a removed cluster in OpenBao.
- To make the token in OpenBao invalid, delete the ServiceAccount `openbao`. The next run creates it again, and writes a new token. Do not delete the namespace `drover-openbao` for this, for the reason in [Service accounts](#service-accounts).

For each cluster, the service writes the config in three steps:

1. It requests a token of `drover-openbao/openbao` with the TokenRequest API, through the Rancher proxy: `POST /k8s/clusters/<cluster id>/api/v1/namespaces/drover-openbao/serviceaccounts/openbao/token`. The request has `spec.expirationSeconds` from `--openbao-token-ttl`, and no audiences. The API server can shorten the lifetime, for example to 24 hours on EKS, so the service uses `status.expirationTimestamp` of the answer.
2. It logs in with `POST <address>/v1/auth/<auth path>/login`, with the role and the content of `--openbao-jwt-file`. It reads the file at every login, because the kubelet replaces the token. The service logs in once per run, and only when a cluster needs a write. It does not keep the client token after the run.
3. It writes `POST <address>/v1/<prefix>/<cluster id>/config`, with the client token in the header `X-Vault-Token`, and the body `{"kubernetes_host": "<openbao rancher url>/k8s/clusters/<cluster id>", "kubernetes_ca_cert": "<rancher ca>", "service_account_jwt": "<token>", "disable_local_ca_jwt": true}`.

The Kubernetes secrets engine of OpenBao cannot skip the TLS verification of `kubernetes_host`. It verifies Rancher only with `kubernetes_ca_cert` when that field has a value, and with the system roots otherwise. The service therefore reads the Rancher setting `cacerts` with `GET /v3/settings/cacerts`, once per run that has a cluster to keep, and sends its value as `kubernetes_ca_cert`. Rancher puts the CA chain of a private certificate into that setting, and leaves it empty for a public certificate. The service always sends the field, so an empty value removes a CA that an earlier write stored. A failed read gives an error line, and the run writes nothing, so that no write removes the CA of a private certificate.

OpenBao keeps a written token and never renews it. The service therefore keeps in memory, per cluster, the expiry of the token of the last successful write. The reconcile run writes the config of a cluster again when less than half of the lifetime of that token remains, when the ServiceAccount has a new uid, or when the Rancher CA differs from the CA of that write. A new Rancher CA thus reaches OpenBao in the next run. After a restart, the service has no expiry, so the first run writes the config of every cluster. A failed write keeps the old expiry, and the next run tries again.

OpenBao checks the policy before the mount. With `update` on the path, OpenBao answers 404 with the message `no handler for route` to a write under a mount that does not exist, and an older server answers 400 with the same message. The service then writes an info line with the cluster id, counts the write with the outcome `missing_mount`, and tries again in the next run. Every other failure gives an error line, and the next run tries again. The service logs no token and no JWT.

## Telemetry

With `--otlp-endpoint` set, one reconcile run produces a span named `reconcile`, and one patched namespace produces a span named `patch_namespace`. That span has the attributes `drover.cluster`, `drover.origin`, and `k8s.namespace.name`. The `drover.origin` value is `reconcile`, `watch`, or `project`. With `--openbao-address`, the OpenBao login produces a span named `openbao_login`, and the config write of one cluster produces a span named `openbao_config`, with the attribute `drover.cluster`. The service exports these metrics:

| Metric | Kind | Unit | Attributes |
| --- | --- | --- | --- |
| `drover.sync.reconciles` | Counter | `1` | `outcome` |
| `drover.sync.namespaces.patched` | Counter | `1` | `origin` |
| `drover.sync.errors` | Counter | `1` | |
| `drover.sync.duration` | Histogram | `s` | |
| `drover.sync.events` | Counter | `1` | `cluster`, `kind`, `type` |
| `drover.sync.watches.open` | UpDownCounter | `1` | `cluster`, `kind` |
| `drover.sync.projects.changed` | Counter | `1` | `cluster` |
| `drover.sync.accounts.changes` | Counter | `1` | `kind`, `action` |
| `drover.sync.openbao.writes` | Counter | `1` | `cluster`, `outcome` |

The `kind` attribute of the watch metrics is `namespace` or `project`. For the project watch, `cluster` is always `local`. The `kind` attribute of `drover.sync.accounts.changes` is `project`, `namespace`, `serviceaccount`, `role`, `rolebinding`, or `clusterrolebinding`, and `action` is `create`, `update`, `move`, or `delete`. With `--service-accounts`, the summary line of a run has the field `accounts_changed`, and every write has a line `account object changed`. The `outcome` attribute of `drover.sync.openbao.writes` is `ok`, `missing_mount`, or `error`. A failed login counts one `error` per cluster that needed a write. A successful write has the line `the OpenBao config is written`, with the fields `cluster` and `expires`.
