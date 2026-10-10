# project-sync

The human document of this component is `docs/project-sync.md`. Read it first, and update it together with a behaviour change. Do not repeat its content here.

- Read the project of a namespace from the annotation `field.cattle.io/projectId`, because Rancher grants the project roles by the annotation. The label stays when kubectl removes the annotation, and when a project is deleted.
- When a user moves a namespace to "no project" in the Rancher UI, Rancher removes the label and the annotation in one write. The `DELETED` event of the watch then has the object from before the change (verified on Kubernetes 1.35). Its object names the project that the namespace left, so never act on it without a new read.
- Remove from every decoded namespace each key that project-sync does not read. A tenant controls the size of its namespace metadata, and the pod has a fixed memory limit.
- Reject in `New` a key set whose own ownership record is longer than `maxRecordValue`. The prune drops such a record, and project-sync then never removes a key of it.
- The worker and the lister read the token file per queue item, because the token changes at a rotation while the stream stays open.
- After an `ERROR` event with a code other than 410, keep the resource version. A start without one sends an `ADDED` event for every namespace again.
- When a patch gets 404, 409, or a 403 whose message names the terminating state, project-sync skips the patch and counts no error.
- After an upgrade with a new record key, the first reconcile run patches every namespace once. Expect one patch per namespace in the metrics after such a release.
- A deployment tool that reads back every label of a namespace shows the keys of project-sync as drift. Name a new key in `docs/project-sync.md`, so that a deployer can ignore it.
- Start the project watch in the reconcile run, after the first snapshot. Before that, the watch has no snapshot to compare against. Also, the watch would log a warning per attempt when the token file is missing.
- Never let the project watch start a reconcile run. A cluster member can create Projects, so a trigger in the project watch would let that member start runs at any time.
- Take a token from the cluster limiter before each request of the worker and the lister, not per queue item. A project owner can edit its Project in a loop. A start of the watch without a resource version puts every namespace on the queue, and most of them need no request.
- Replace the snapshot maps of a cluster on every change. Never mutate them in place, because a worker and the reconcile run read them without a lock.
- A reconcile run that reads the project list before a Project change can overwrite that change in the snapshot. It can then patch the old values back. This gap is known and accepted, because the time window lasts one list request, and the next event or run ends it.

## Service accounts

The facts in this section are verified live against Rancher 2.14.5, and against the source of Rancher 2.14.6 and its webhook 0.10.12.

- Trust an account project only by the label `drover-service-accounts` together with the annotation `field.cattle.io/creatorId` equal to the service user. A project owner can set the label on its own project. On an update, the webhook denies a request that changes or adds the creator annotation, also from an admin. A cluster member can create a project with the service user as creator, because no webhook compares the annotation with the requester. But Rancher then binds only the service user as owner, so that member gets no role in the project.
- Create the account project without `field.cattle.io/no-creator-rbac`. The webhook rejects that annotation together with a creator, and the trust check rejects a project without a creator.
- Move a namespace in no project into the account project only when a ClusterRoleBinding of project-sync names it as owner by uid. The same rule applies to a delete in the sweep. The namespace webhook checks `manage-namespaces` on the new project only, and it checks nothing when a request removes the project annotation. So a project owner can put its own namespace, with its own RoleBindings, into no project.
- Do not set `field.cattle.io/creatorId` on an account namespace. Rancher deletes a namespace with that annotation together with its project. The ServiceAccounts of that namespace are then deleted too, and every token of those ServiceAccounts becomes invalid.
- Put the uid of the namespace into the key of the worker cache. A CI job can delete a namespace and create it again within one interval. The queue can then merge the `DELETED` item into the next `ADDED` item.
- When the new project of a moved namespace has no trusted account namespace yet, delete the RoleBindings of the old project at once. Otherwise, the ServiceAccounts of the old project keep their rights in a namespace of another project until the next reconcile run.
- Never set `blockOwnerDeletion` on an owner reference. It needs a right on the owner that the service user does not have.
- Rancher creates `<project>-namespaces-edit` and `<project>-namespaces-readonly` shortly after the project is created, also when the project has no member and no namespace.
- Send a PATCH as `application/merge-patch+json`. The API server answers 415 to `application/json`.

## OpenBao

The facts about the answer for a mount that does not exist are from the source of OpenBao 2.7.0. The facts about mounts, roles, policies, and ACLs are from OpenBao 2.6.3. On OpenBao 2.7.1, the mount create and the config write are checked live. No other fact of this section is checked live.

- project-sync writes every OpenBao object that it derives from a Rancher object: the mounts, their config, the roles, and the ACL policies. Do not move this work back to an operator. The operator added a delay to each new project, it did not correct drift, and an uninstall hung.
- Refresh the config of a cluster when less than half of the lifetime of its token remains. The other half is the time for the retries of a failed write.
- Read the setting `cacerts` also in a run without a due token, so that OpenBao gets a rotated CA in one run.
- Keep `--openbao-rancher-url` separate from `--rancher-url`. Through the Rancher Service, project-sync can reach Rancher with `--rancher-insecure-skip-verify`, but OpenBao verifies the certificate of Rancher.
- Never take an OpenBao RoleBinding outside a trusted account namespace as proof for `drover-openbao`. A tenant can write a binding with that owner reference in its own namespace.
- Delete every RoleBinding `drover-openbao` when the namespace `drover-openbao` is not trusted. Otherwise, a tenant who creates `drover-openbao` and a ServiceAccount `openbao` gets the tokens of every project role.
- Read `drover-openbao` in the lister before each write of the OpenBao Role and RoleBinding of a project. A tenant can delete the namespace and create it again in its own project within one interval. The lister creates each object first and reads it only after a 409, so it needs no list call.
- Check the `type` in the answer of `GET sys/mounts/<path>`, because that read also matches a parent mount (`vault/logical_system.go`, `handleReadMount`).
- Put `list` on `kubernetes/+/roles/*`, not on `kubernetes/+/roles` alone. OpenBao adds a slash to the end of the path of a LIST request (`http/logical.go`). The ACL matches the path without the slash only when no rule matches the path with it (`vault/policy/acl.go`). A rule on `kubernetes/+/roles` alone gave 403 on OpenBao 2.6.3.
- Only one of `service_account_name`, `kubernetes_role_name`, and `generated_role_rules` of a role can have a value.
- Do not change the separator of the policy name. A Rancher cluster id is `local`, `c-` and five characters, or `c-m-` and eight characters. A project name is `p-` and five characters, and the role names are a fixed set. So the split at the longest cluster id finds one pair for each name that project-sync writes. A new separator renames every policy that exists, and every client that names a policy then fails.
- The name of a policy does not prove the writer. A policy `kubernetes-c-1-grafana-read-only` of an operator parses as the project `grafana` of `c-1`. Accept only a project part that is a label value, because it goes into the path of a Rancher request.
- Delete the policies of a cluster or a project before its roles and its mount. The role or the mount then stays as the key for a retry.
- The Kubernetes API checks the right before it looks for the object. So without a right on `clusters`, the Kubernetes API answers 403, never 404, and the mount of that cluster stays.
- Keep the `allowed_parameters` of the `creds/<role>` policy at `kubernetes_namespace` and `ttl` (`credsParameters` in `openbao_roles.go`). The endpoint also accepts `audiences` and `cluster_role_binding` (`builtin/logical/kubernetes/path_creds.go`). A token with another audience is valid for other services than the API server, for example the OIDC federation of a cloud provider.
- Keep the client token until a fifth of its lifetime remains. OpenBao can give batch tokens for a login role, for example with a lifetime of 5 minutes. A batch token cannot be renewed.
- After a 403, log in again at most once per reconcile run. A second 403 in the run means that a right is absent, and another login gets the same rights.
- Skip `drover-openbao` in the sweep of the account namespaces. It has the account prefix and no project, so the sweep would delete it after its 404 check.

## Login roles

`trust.go` has the rules, the parser of the trust annotation, and the format of the status annotation. `trust_roles.go` has the OpenBao writes and the status write. The integration tests of the private repository run a copy of `testdata/trust/` against the admission policy of the chart. Change a case in both places.

- Never log the value of the trust annotation or of the status annotation. A tenant writes the document, and its claims can contain the names of private repositories or accounts.
- Keep the status rules of `docs/project-sync.md`, sections "The status annotation" and "Rules of the service", against a loop of status writes. After each status write, the project watch gets a `MODIFIED` event of the Project. When the code breaks one rule, the service writes the status again at every event.
- Store the status in the snapshot, and exclude it from the change compare of the project watch. Otherwise, at the next event the service compares against an old status and writes the status again.
- Do not change the separator `_` of a login role name again. With a dash, the project `p` fits every role `p-xxxxx-<statement>`. Then every such statement gets `WriteFailed`, and the reconcile run deletes its role. The service keeps every role with another name form. After a change of the separator, each old role thus stays in OpenBao, and a login with it still works.
- Keep the conditions for the delete of a role of the last status, in `docs/project-sync.md`, "Rules of the service". A tenant can edit the status when the admission policy is off. Without these checks, the service deletes the role of another project when a tenant adds that role to the status.
- Read at most `maxStatements` + 1 entries of the last status. A tenant can write a status with thousands of entries, and each entry can cause a delete request and a log line.
- In the trust step of the reconcile run, take each project from the snapshot, not from the project list of the run. After the project list, the project watch can store a narrower document. With the listed document, the run writes the role of a removed statement again.
- In the reconcile run, delete a role by its name only while the snapshot has the trust value that the run checked. After the project list, the project watch can get a project event with a new statement. Without this check, the run deletes the role of the new statement.
- Keep the role of a statement whose write failed. It is the last role that works.
- Write a login role again after the 10-minute window without a read. The read answer of a JWT role has other value types than the write body, for example seconds for `token_ttl`. A compare then needs a conversion per field.
- Keep `drover.sync.trust.statements.ready` an observable gauge whose callback reads the data points of each project from a map. With cumulative temporality, the OpenTelemetry SDK exports at each collection every attribute set that a synchronous gauge ever recorded. So the data point of a removed statement would stay in the export with its last value.
- Keep `gaugeCardinalityLimit` in `cmd/drover/project_sync.go` above the SDK default of 2000, because a tenant controls the count of data points of `drover.sync.trust.statements.ready`.
