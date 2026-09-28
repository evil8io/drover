# project-sync

This service copies labels and annotations of a Rancher project to every namespace of that project, because Rancher does not copy them. The `--labels` and `--annotations` flags are the allow list of keys. When a namespace has a different value for a key, the service overwrites that value with the value of the project.

The service polls Rancher at every `--interval`, in a reconcile run. It also keeps one namespace watch per cluster open, so that a new namespace gets its keys within a few seconds. It keeps one project watch open too. When a label, an annotation, or the display name of a project changes, the namespaces of the project get the change within seconds. One replica is enough, because every reconcile run is a full reconcile.

Status 403 for the namespace list of a cluster means that the service user does not have the right to list namespaces on that cluster. The service then writes a warning with the cluster id and continues with the next cluster.

With `--service-accounts`, the service also keeps three ServiceAccounts per Rancher project, one per project role. A CI job of a project authenticates with a token of such a ServiceAccount, through the Rancher proxy. See [Service accounts](#service-accounts).

With `--openbao-address` as well, the service writes the OpenBao state that follows from the Rancher objects. This state contains a mount of the Kubernetes secrets engine per cluster, with the config of that mount. It also contains a role and an ACL policy per project role. OpenBao then creates short-lived tokens of these ServiceAccounts. See [OpenBao config](#openbao-config).

## Requirements

1. The service needs a Rancher service user with `get`, `list`, `watch`, and `patch` on `namespaces`, in every cluster whose namespaces the service syncs. With a `cluster-owner` binding, the user also has these rights.
2. The same service user needs `list` and `watch` on `projects` of `management.cattle.io`, cluster-wide, in the Rancher cluster. The Rancher cluster is the cluster that Rancher itself runs in. Its cluster id is `local`. The user gets these rights from a `ClusterRoleTemplateBinding` on that cluster, with a role template that has those two verbs. The user does not get them from the bindings on the other clusters.

   Without these rights, Rancher answers a request of the project watch with status 403. The service then writes one warning per attempt, with the backoff between attempts. The periodic reconcile run continues without the project watch.
3. The service needs an API token of that service user, in a Secret that the service mounts.
4. With `--service-accounts`, the same service user also needs these rights in every cluster, and in the Rancher cluster through a `ClusterRoleTemplateBinding`:
   - every verb on `namespaces`, and `create` and `manage-namespaces` on `projects` of `management.cattle.io`. With these rights, the user has every rule of the Rancher ClusterRoles `<project>-namespaces-edit` and `<project>-namespaces-readonly`, so it can bind them without `bind`.
   - `get`, `list`, `watch`, `create`, and `delete` on `serviceaccounts`.
   - `get`, `list`, `watch`, `create`, `update`, and `delete` on `rolebindings` and `clusterrolebindings`.
   - `bind` on the ClusterRoles `admin`, `edit`, `view`, and `create-ns`, with `resourceNames`. Do not grant `bind` on every ClusterRole, because that right is equal to the rights of a cluster admin.

   Give the service its own service user. The API filter is on the request path of every tenant. The service user of the API filter needs only the rights for the namespace list.
5. With `--openbao-address`, the same service user also needs these rights in every cluster, and in the Rancher cluster through a `ClusterRoleTemplateBinding`:
   - `create` on `serviceaccounts/token`, with the `resourceNames` `openbao`, `project-owner`, `project-member`, and `read-only`.
   - `get`, `list`, `watch`, `create`, `update`, and `delete` on `roles`.

   With these rights and the `get` right on `serviceaccounts`, the user has every rule of the Role `drover-openbao`. The user can therefore create and bind that Role without `escalate` and without `bind`. The user also needs `get` on `settings` of `management.cattle.io`, to read the Rancher setting `cacerts`. The GlobalRole `user-base` includes this right.
6. With `--openbao-address`, OpenBao needs a Kubernetes auth mount at `--openbao-auth-path`, with the role `--openbao-role` for the ServiceAccount of the pod of the service. That role needs the ACL policy that follows. This policy is for the default prefix `kubernetes`. None of the paths needs the `sudo` capability. A LIST request also matches a path without the slash at its end.

   ```hcl
   path "sys/mounts/kubernetes/+" {
     capabilities = ["read", "update"]
   }
   path "kubernetes/+/config" {
     capabilities = ["update"]
   }
   path "kubernetes/+/roles/*" {
     capabilities = ["create", "read", "update", "delete", "list"]
   }
   path "sys/policies/acl" {
     capabilities = ["list"]
   }
   path "sys/policies/acl/kubernetes-*" {
     capabilities = ["read", "update", "delete"]
   }
   ```

   OpenBao connects to each cluster through the Rancher proxy, with a ServiceAccount token of that cluster. Rancher accepts such a token only for a cluster with `enabled: true` in its `ClusterProxyConfig`. See [the API filter document](api-filter.md).

## Configuration

Start the service with `drover project-sync [flags]`.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--rancher-url` | (required) | URL of Rancher. Use `http://` or `https://`. The path must be empty or `/`. |
| `--rancher-ca-file` | | PEM bundle that the service uses to verify an `https` URL. |
| `--rancher-insecure-skip-verify` | `false` | When `true`, the service does not verify the certificate of an `https` Rancher URL. |
| `--token-file` | (required) | File with the API token of the Rancher service user. |
| `--labels` | | Comma-separated label keys of a project to copy. |
| `--annotations` | | Comma-separated annotation keys of a project to copy. |
| `--name-label` | | Label key on the namespace that gets the display name of the project. |
| `--name-annotation` | | Annotation key on the namespace that gets the display name of the project. |
| `--service-accounts` | `false` | When `true`, the service keeps three ServiceAccounts per project, one per project role. |
| `--openbao-address` | | URL of OpenBao. Use `http://` or `https://`. The path must be empty or `/`. When the value is empty, the OpenBao config is off. |
| `--openbao-auth-path` | `kubernetes` | Kubernetes auth mount of OpenBao that the service logs in to. |
| `--openbao-role` | `project-sync` | Role of that auth mount. |
| `--openbao-jwt-file` | | File with the ServiceAccount token of the pod, for the login. This flag is required with `--openbao-address`. |
| `--openbao-mount-prefix` | `kubernetes` | Path prefix of the secrets engine mounts. The config of a cluster is at `<prefix>/<cluster id>/config`. |
| `--openbao-rancher-url` | | URL of Rancher that OpenBao uses. Use `https://`. The path must be empty or `/`. This flag is required with `--openbao-address`. |
| `--openbao-token-ttl` | `24h` | Requested lifetime of the token of a cluster. The minimum is `10m`. The API server can shorten the lifetime. |
| `--openbao-credential-ttl` | `15m` | Default lifetime of a credential of a project role. This value is the `token_default_ttl` of the role in OpenBao for that project role. The minimum is `1s`. |
| `--openbao-credential-max-ttl` | `2h` | Longest lifetime of a credential of a project role. This value is the `token_max_ttl` of the role in OpenBao for that project role. It must not be shorter than `--openbao-credential-ttl`. |
| `--interval` | `60s` | Time between two reconcile runs. |
| `--patch-rate` | `10` | Limit for the namespace patches and the project namespace lists per second that the watches of one cluster send together. |
| `--listen` | `:8080` | Address that the service listens on. |
| `--log-level` | `info` | The value is one of `debug`, `info`, `warn`, or `error`. |
| `--otlp-endpoint` | `$OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC endpoint, as `host:port` or as a URL. When the value is empty, telemetry is off. |
| `--otlp-traces` | `true` | When `true`, the service sends traces to the OTLP endpoint. |
| `--otlp-metrics` | `true` | When `true`, the service sends metrics to the OTLP endpoint. |
| `--service-name` | `$OTEL_SERVICE_NAME`, or `drover` | Value of the `service.name` resource attribute. With an empty value, `--service-name=`, the service sets no `service.name`, so that a collector can derive it. The OpenTelemetry SDK still takes a `service.name` from `OTEL_SERVICE_NAME` or `OTEL_RESOURCE_ATTRIBUTES`. |

The flags need at least one label key, annotation key, name label key, or name annotation key, or `--service-accounts`. Every `--openbao-` flag needs `--service-accounts`. A key must be a valid Kubernetes label or annotation key. A key is not valid when its prefix is `cattle.io`, `kubernetes.io`, or `k8s.io`, or a subdomain of one of them. The reason is that Rancher and Kubernetes own those domains.

The `--name-annotation` value is the raw display name of the project. The `--name-label` value is a sanitised copy of the display name. The service replaces every character outside `[A-Za-z0-9._-]` with `-`, and cuts the result to 63 characters. It then removes the characters that are not alphanumeric from the start and the end of the result. When the sanitised name is empty, the service writes no label, and it writes one warning line for that project in that reconcile run.

A name key is the key of `--name-label` or `--name-annotation`. The service writes only the display name to a name key. That key can also be a key of `--labels` or `--annotations`. The service then still writes the display name to it, never a label or an annotation of the Project object. A namespace can therefore have no name key, because the display name has no valid label value, or because the namespace is in no project.

The service also starts when there is no token file. It skips the reconcile runs until the file contains a token. It writes one warning per state change of the file.

The service answers `GET /healthz` with status 200 and the body `ok`.

## Ownership of a key

The service records the keys that it set on a namespace in two annotations of that namespace, `drover-managed-labels` and `drover-managed-annotations`. These two annotations are the ownership annotations. The value of each annotation is a comma-separated list of keys.

The service removes a key in one of these lists when the project of the namespace no longer has that key. The service thus removes the keys of the old project from a namespace that moves to another project. From a namespace in no project, it removes every key of the lists, and the two ownership annotations. The service reads the project of a namespace from the annotation `field.cattle.io/projectId`, the project annotation. It does not use the label of the same name, the project label, because Rancher keeps that label when a namespace leaves its project.

A key outside these lists belongs to the tenant, and the service never removes it. The lists are on the namespace, so a tenant can edit them. The service therefore removes only a key of `--labels`, `--annotations`, `--name-label`, or `--name-annotation`. It ignores every other key of the lists. The service does not accept `drover-managed-labels` and `drover-managed-annotations` in `--labels` and `--annotations`, because it writes these keys itself.

## Design

The service polls `GET /v3/projects` at every interval. With one call per interval, the service gets every project that the service user can see, because Rancher filters that list by RBAC. The service takes the set of clusters from the `clusterId` of those projects, so it needs no cluster list of its own. One reconcile run syncs at most 8 clusters at the same time.

The reconcile run lists every namespace of a cluster, also a namespace without the project label. It thus finds a namespace that left its project and no longer has the project label. It lists in pages of 500, with the `limit` and `continue` parameters of the Kubernetes API. When a `continue` token is too old, the API server answers with status 410. The service then starts the list again from the first page, once. The service decodes each page as a stream, one namespace at a time.

For each namespace, the service keeps in memory only the keys that it reads for the sync. These are the project label, the project annotation, the two ownership annotations, and the keys of `--labels`, `--annotations`, `--name-label`, and `--name-annotation`. The service keeps the metadata under other keys in memory only while it decodes that namespace. A tenant can fill an ownership annotation up to the size limit of the API server for annotations, 256 KiB. The service therefore treats an ownership annotation above 4096 bytes as absent, because a list that the service writes is a short list of keys. The service then writes that annotation again in the next patch.

The service also lists the projects in pages of 500. For each project, it keeps only the keys of `--labels` and `--annotations`. The service reads at most 32 MiB of one page of either list. When a namespace page is larger, the reconcile run fails for that cluster, with an error that contains the limit in bytes. The reconcile run continues for the other clusters. When a project page is larger, the whole reconcile run fails.

The service also keeps one namespace watch per cluster open, on `GET /k8s/clusters/<id>/api/v1/namespaces?watch=true`. The watch selects only the namespaces with the project label. Rancher sets that label about three seconds after the namespace is created, so the service acts on an `ADDED` event and on a `MODIFIED` event. When the project label is removed from a namespace, the namespace is no longer in the selection. The API server then sends a `DELETED` event for it, with the namespace from before the change. Unless the namespace is in deletion, the service reads the namespace again and handles its current state.

When a stream ends, the service starts it again after a backoff. The backoff increases from 1 s to 30 s. When the resource version is expired, the service starts the stream again without a resource version. The periodic reconcile run still handles a missed event, a dropped watch, and a new cluster.

The watch puts the namespace of an event into the patch queue of its cluster, with the namespace name as the key. One worker per cluster patches the namespaces of that queue, one at a time. After a second event of a namespace, the queue contains only the second event for that namespace. For several events on one namespace before its patch, the worker thus sends one patch. The service limits the patches and the project namespace lists of one cluster, together, to `--patch-rate` per second. The burst is equal to the rate.

A patch of a namespace that no longer exists, or of a namespace in `Terminating`, is not an error. The service skips it, and the next reconcile run repeats the work.

The service also keeps one project watch open, on `GET /k8s/clusters/local/apis/management.cattle.io/v3/projects?watch=true`, with the same timeout and backoff as the namespace watch. The watch decodes a Project object into the same fields that the service reads from the project list. These are the project id, the cluster id, the display name, and the allow-listed labels and annotations. When the watch starts again without a resource version, it gets the full project list again, as an `ADDED` event per project. For each such event, the service only compares the project with its copy in memory, so the service sends no request because of the restart.

The service skips a project of a cluster that the last reconcile run did not see. The next reconcile run handles that project. The service thus syncs a new cluster only in the next reconcile run, as it did before the project watch was added. The service skips a Project object whose `metadata.namespace` differs from its `spec.clusterName`, and it writes a warning. The service also skips a project whose name is not a valid label value, because no namespace can have that name in its project label.

When a project changes, the service updates the copy of the project that it keeps from the last reconcile run. It then puts the project name on the project queue of the cluster. After a second event of the same project, the queue contains only the second event for that project. For several events on one project before the lister takes it, the lister thus sends one list.

One lister per cluster takes a project from the project queue. It lists the namespaces of the cluster with the label selector `field.cattle.io/projectId=<name>`, in pages of 500, as the reconcile run does. It puts each such namespace on the patch queue of the cluster, with the origin `project`. The worker of the cluster then patches these namespaces as it patches the namespaces that the namespace watch finds.

When the display name changes, the service first clears the name cache of the worker, so that the worker computes the new label value. When a list fails, the service writes a warning, and the next reconcile run repeats the work. For status 403, the service writes the same warning as for a failed namespace list of the reconcile run.

A project owner can edit the labels, the annotations, and the display name of their own project. The values that the service copies are tenant input, as they were with the reconcile run. The project watch only shortens the delay before the namespaces of a project get a change. The queue and the shared rate limiter of a cluster limit the requests from many project edits in a short time. The project watch never starts a reconcile run. The service logs project ids and key names, never a display name or a value.

## Service accounts

With `--service-accounts`, the service keeps these objects in every cluster that the reconcile run syncs:

1. One account project, with the display name `drover`, the label `drover-service-accounts: "true"`, and the service user in the annotation `field.cattle.io/creatorId`. Rancher then binds the service user as the owner of the project, and no tenant gets a role in it. The service creates the project when the cluster does not have an account project.
2. Per tenant project, the account namespace `drover-<project>` in the account project. `<project>` is the name of the project, for example `p-n52j9`.
3. In the account namespace, the ServiceAccounts `project-owner`, `project-member`, and `read-only`. The user name of such a ServiceAccount is `system:serviceaccount:drover-<project>:<role>`.
4. In every namespace of the tenant project, the RoleBindings `drover-project-owner`, `drover-project-member`, and `drover-read-only`, to the ClusterRoles `admin`, `edit`, and `view`.
5. Per project role, the ClusterRoleBinding `drover-<project>-<role>-namespaces`, to the Rancher ClusterRole `<project>-namespaces-edit` for `project-owner` and `project-member`, and to `<project>-namespaces-readonly` for `read-only`. Rancher keeps the `resourceNames` of these ClusterRoles equal to the namespaces of the project. The API filter uses these `resourceNames` to answer a namespace list of the ServiceAccount.
6. For `project-owner` and `project-member`, the ClusterRoleBinding `drover-<project>-<role>-create-ns`, to the Rancher ClusterRole `create-ns`.

Every object has the labels `drover-project: <project>` and `drover-role: <role>`, and every binding has one subject. The account namespace is the owner of the bindings, so the garbage collector deletes them together with the namespace.

A tenant project is a project that is not the System project, the Default project, or an account project. The service finds the System project and the Default project by the labels `authz.management.cattle.io/system-project` and `authz.management.cattle.io/default-project`.

The ServiceAccounts get the Kubernetes rights of a project role through `admin`, `edit`, and `view`. They do not get the extra rules of the Rancher role templates. Examples are the rules for the monitoring resources and the rule to read nodes.

The service applies these rules:

- When a namespace joins a project, the service creates the three RoleBindings in it. When a namespace moves to another project, the service changes the subjects and the owner of its RoleBindings to those of the new project. When a namespace leaves its project, the service deletes the RoleBindings. The namespace watch starts this work within seconds.
- For a new project, the service creates the account namespace, the ServiceAccounts, and the ClusterRoleBindings within seconds, through the project watch.
- The reconcile run corrects a binding that differs, and it creates an object again when the object does not exist. A tenant can edit or delete the RoleBindings in its namespaces, and the next reconcile run restores them.
- The service uses an account namespace only in an account project. A tenant can create a namespace with the name `drover-<project>` first, in its own project. The service then writes a warning, gives the project no accounts, and deletes the bindings of the project.
- When an account project is deleted, its namespaces are in no project, because Rancher deletes only the namespaces with the annotation `field.cattle.io/creatorId`. The reconcile run then creates a new account project. It moves such a namespace into the new account project when the namespace is the owner of a ClusterRoleBinding of the service.
- The reconcile run deletes an account namespace whose project no longer exists, after Rancher answers 404 for that project. It deletes the bindings of the project first.
- To make every token of a ServiceAccount invalid, delete the ServiceAccount. The next reconcile run creates it again. Do not delete the account namespace for this. The subject of a binding contains the namespace and the name of its ServiceAccount. The garbage collector deletes the bindings only some time after the namespace is deleted. Until then, a tenant who creates a namespace with that name gets the rights of the bindings.

## OpenBao config

With `--openbao-address`, the service keeps these objects in every cluster that the reconcile run syncs, also in the Rancher cluster:

1. The namespace `drover-openbao` in the account project, with the label `drover-role: openbao`.
2. In that namespace, the ServiceAccount `openbao`, with the same label.
3. In every account namespace `drover-<project>`, the Role `drover-openbao`. Its rules are `get` on `serviceaccounts` and `create` on `serviceaccounts/token`, both with the `resourceNames` `project-owner`, `project-member`, and `read-only`.
4. In every account namespace, the RoleBinding `drover-openbao` to that Role, with only one subject, `drover-openbao/openbao`. The namespace `drover-openbao` is the owner of the RoleBinding, so the garbage collector deletes the RoleBinding together with that namespace.

The Role and the RoleBinding have the labels `drover-project: <project>` and `drover-role: openbao`. The service applies these rules:

- The service uses the namespace `drover-openbao` only in an account project, as it does with an account namespace. A reconcile run trusts such a namespace after it checks that the namespace is in an account project. A tenant can create a namespace with that name first, in its own project. The service then writes a warning, writes no config for that cluster, and deletes every RoleBinding `drover-openbao` in the cluster.
- The service moves a namespace `drover-openbao` that is in no project into the account project. It does this only when the namespace is the owner of a RoleBinding `drover-openbao` in an account namespace.
- When a project gets no accounts, the service deletes the RoleBinding `drover-openbao` of the project together with its other bindings. The service does the same when the reconcile run deletes the account namespace of the project.
- For a new or changed project, the service writes the Role and the RoleBinding within seconds, through the project watch, together with the ServiceAccounts. The project watch writes them only after a reconcile run trusted the namespace `drover-openbao` of that cluster. The reason is that the RoleBinding needs the uid of that namespace as its owner. Until then, the next reconcile run creates them.
- The reconcile run corrects a Role or a RoleBinding that differs, and it creates such an object again when the object does not exist.
- A project with the name `openbao` gets no accounts, because the name of its account namespace is `drover-openbao`.
- The service deletes none of these objects when `--openbao-address` is set to an empty value. It also keeps the config of a removed cluster in OpenBao.
- To make the token in OpenBao invalid, delete the ServiceAccount `openbao`. The next reconcile run creates it again and writes a new token. Do not delete the namespace `drover-openbao` for this. The reason is the same as in [Service accounts](#service-accounts).

For each cluster, the service writes the config in three steps:

1. It requests a token of `drover-openbao/openbao` with the TokenRequest API, through the Rancher proxy. The request is `POST /k8s/clusters/<cluster id>/api/v1/namespaces/drover-openbao/serviceaccounts/openbao/token`. The request contains `spec.expirationSeconds` from `--openbao-token-ttl`, and no audiences. The API server can shorten the lifetime, for example to 24 hours on EKS, so the service uses `status.expirationTimestamp` from the answer.
2. It logs in with `POST <address>/v1/auth/<auth path>/login`, with the role and the content of the file in `--openbao-jwt-file`. It reads the file at every login, because the kubelet replaces the token. The service keeps the client token for the reconcile run and the project watch, until a fifth of its lifetime remains. After a 403 answer, the service logs in again once and repeats the request.
3. It sends `POST <address>/v1/<prefix>/<cluster id>/config`, with the client token in the header `X-Vault-Token`. The body is `{"kubernetes_host": "<openbao rancher url>/k8s/clusters/<cluster id>", "kubernetes_ca_cert": "<rancher ca>", "service_account_jwt": "<token>", "disable_local_ca_jwt": true}`.

The Kubernetes secrets engine of OpenBao cannot skip the TLS verification of `kubernetes_host`. When `kubernetes_ca_cert` is not empty, the engine verifies Rancher only with that field. When the field is empty, the engine verifies Rancher with the root certificates of the system. The service therefore reads the Rancher setting `cacerts` with `GET /v3/settings/cacerts`, once per reconcile run that has a cluster to keep. It sends the value of that setting as `kubernetes_ca_cert`. Rancher puts the CA chain of a private certificate into that setting, and leaves it empty for a public certificate.

The service always sends the field. When the value is empty, OpenBao thus removes a CA that the service wrote earlier. When the read fails, the service writes an error line, and the reconcile run writes nothing. The service does this so that it does not remove the CA of a private certificate.

OpenBao keeps the token that the service writes, and it never renews that token. The service therefore keeps in memory, per cluster, the expiry of the token of the last successful write. The reconcile run writes the config of a cluster again when one of these conditions is true:

- Less than half of the lifetime of that token remains.
- The ServiceAccount `openbao` has a new uid.
- The Rancher CA differs from the CA of that write.

The service thus writes a new Rancher CA to OpenBao in the next reconcile run. After a restart, the service has no expiry in memory, so the first reconcile run writes the config of every cluster. After a failed write, the service keeps the old expiry, and the next reconcile run tries again.

OpenBao checks the ACL policy before it checks the mount. When the service has `update` on the path and writes under a mount that does not exist, OpenBao answers 404 with the message `no handler for route`. An older OpenBao server answers 400 with the same message. The service creates the mount before it writes the config, so it handles this answer only as a fallback.

For this answer, the service writes an info line with the cluster id, and counts the write with the outcome `missing_mount`. It tries again in the next reconcile run. For every other failure, the service writes an error line, and the next reconcile run tries again. The service logs no token and no JWT.

### Mounts, roles, and policies

Before the reconcile run writes the config of a cluster, it sends `GET <address>/v1/sys/mounts/<prefix>/<cluster id>`. When the mount does not exist, OpenBao answers 400 with `No secret engine mount at`. The service then enables the mount with `POST <address>/v1/sys/mounts/<prefix>/<cluster id>` and the body `{"type": "kubernetes"}`. A mount of another type is an error. The service keeps the mount of a removed cluster.

For every tenant project with a trusted account namespace, and for each project role, the service keeps these objects:

1. The role `<prefix>/<cluster id>/roles/<project>-<role>`, with `service_account_name` set to the project role, `allowed_kubernetes_namespaces` set to `drover-<project>`, and `token_default_ttl` and `token_max_ttl` from the two credential flags. The service also sends `allowed_kubernetes_namespace_selector`, `kubernetes_role_name`, `generated_role_rules`, and `token_default_audiences` as empty values. The reason is that OpenBao keeps the old value of a field that is not in a write.
2. The ACL policy `<prefix>-<cluster id>-<project>-<role>`, with every slash of the prefix replaced by a dash. The text of the policy is `path "<prefix>/<cluster id>/creds/<project>-<role>" { capabilities = ["update"] }`, on three lines.

The names are the same as the names of the earlier operator objects, so a client keeps its paths and policies. The service applies these rules:

- The project watch writes the roles and the policies of a new or changed project within seconds, together with its ServiceAccounts and its Role `drover-openbao`. It does not read an object before it writes it. It skips an object that the service read or wrote in the last 10 minutes. The service thus sends no requests when many project events arrive in a short time.
- Each reconcile run lists the roles of every mount and the ACL policies. It creates a role or a policy that does not exist. At most once per 10 minutes, it reads a role or a policy that exists, and it corrects the object when it differs. When the service compares a policy, it ignores the white space around the text of the policy.
- The reconcile run deletes the policies and the roles of a project that the cluster no longer has. It does this only after it read the full project list, and after Rancher answers 404 for that project. The reason is that the project list of a reconcile run can be older than a new project. The service deletes the policy before the role. The service finds these objects from the role names in the mount of the cluster, and from the names of the ACL policies.
- A policy name has the form `<prefix>-<cluster id>-<project>-<role>`. From such a name, the service takes as the cluster id the longest id of a cluster of the reconcile run that fits. It takes the rest, up to the role, as the project. The service thus also deletes a policy whose role does not exist. Such a policy can remain after a failed write, or from an earlier writer. The service does not delete a policy of a cluster outside the reconcile run, or a policy whose name does not have this form.
- When the list of the roles of a mount fails, the service skips the roles of that cluster. It also deletes no OpenBao object of that cluster. When the list of the policies fails, the service skips the policies, and it deletes no OpenBao object.

## Telemetry

When `--otlp-endpoint` is set, the service produces a span named `reconcile` for each reconcile run. It produces a span named `patch_namespace` for each patched namespace. That span has the attributes `drover.cluster`, `drover.origin`, and `k8s.namespace.name`. The `drover.origin` value is `reconcile`, `watch`, or `project`.

With `--openbao-address`, the service also produces a span named `openbao_login` for the OpenBao login. It produces a span named `openbao_config` for the config write of one cluster, with the attribute `drover.cluster`. The service exports these metrics:

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
| `drover.sync.openbao.changes` | Counter | `1` | `kind`, `action` |

The `kind` attribute of the watch metrics is `namespace` or `project`. For the project watch, `cluster` is always `local`.

The `kind` attribute of `drover.sync.accounts.changes` is `project`, `namespace`, `serviceaccount`, `role`, `rolebinding`, or `clusterrolebinding`. Its `action` attribute is `create`, `update`, `move`, or `delete`. With `--service-accounts`, the summary line of a reconcile run contains the field `accounts_changed`. For every write of an account object, the service writes the line `account object changed`.

The `outcome` attribute of `drover.sync.openbao.writes` is `ok`, `missing_mount`, or `error`. For a failed login, the service counts one `error` for each cluster that needed a write. For a successful write, the service writes the line `the OpenBao config is written`, with the fields `cluster` and `expires`.

The `kind` attribute of `drover.sync.openbao.changes` is `mount`, `role`, or `policy`. Its `action` attribute is `create`, `update`, `delete`, or `write`. The action `write` is a write by the project watch, which does not read the object first. For every change, the service writes the line `OpenBao object changed`.
