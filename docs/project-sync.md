# project-sync

project-sync applies the state of the Rancher projects to the clusters that Rancher manages. It has four jobs. You can use the project metadata job and the service accounts job separately. The short-lived tokens need the service accounts. The login roles need the short-lived tokens.

| Job | Flag | Result |
| --- | --- | --- |
| [Project metadata](#project-metadata) | `--labels`, `--annotations`, `--name-label`, `--name-annotation` | project-sync copies the labels, the annotations, and the display name of a project to each namespace of the project. |
| [Service accounts](#service-accounts) | `--service-accounts` | project-sync creates three ServiceAccounts for each project, one for each project role. |
| [Short-lived tokens](#short-lived-tokens) | `--openbao-address` | OpenBao creates short-lived tokens of those ServiceAccounts on request. |
| [Login roles](#login-roles) | `--trust-file` | A project owner selects the CI jobs and the cloud identities that can request such a token. |

project-sync does a full reconcile run at each `--interval`, 60 seconds by default. It also watches the namespaces and the projects, so project-sync applies most changes in seconds. One replica is sufficient.

## Project metadata

Rancher does not copy the labels and the annotations of a project to the namespaces of the project. project-sync copies them, for the keys in `--labels` and `--annotations`. The metadata of a project, for example a cost centre or an owner, is then available to policies, network rules, and dashboards.

- **The value of the project has priority.** When a namespace has a different value for a key in the list, project-sync overwrites that value.
- **Removal.** project-sync removes a key from a namespace when the key is removed from the project. It also removes the key when the namespace is moved to another project. project-sync removes only the keys that it wrote. It records the label keys that it wrote in the annotation `managed.drover.evil8.io/synced-labels` of the namespace, as a comma-separated list. It records the annotation keys in `managed.drover.evil8.io/synced-annotations`.
- **The display name as an annotation.** Set `--name-annotation` to an annotation key. project-sync then writes the display name of the project to that annotation, on each namespace of the project.
- **The display name as a label.** Set `--name-label` to a label key. project-sync then writes the display name to that label. Fewer characters are permitted in a label value than in a display name, so project-sync replaces each character outside `[A-Za-z0-9._-]` with `-`. It also cuts the value to 63 characters. When the display name does not contain a letter or a digit, project-sync does not write the label.
- **Refused keys.** project-sync refuses a key under `cattle.io`, `kubernetes.io`, `k8s.io`, or `managed.drover.evil8.io`.

project-sync also writes the number of namespaces of each project to the annotation `managed.drover.evil8.io/namespace-count` on the Project object. A policy in the drover chart depends on this annotation. With that policy, Kyverno denies the delete of a project that still has namespaces. When your deployment tool reports this annotation as drift, configure the tool to ignore the key.

## Service accounts

A CI job, or another client outside the cluster, is not a Rancher user. It thus cannot be a member of a project. With `--service-accounts`, project-sync creates three ServiceAccounts for each tenant project. Each ServiceAccount has the rights of one project role:

| ServiceAccount | Kubernetes role in each namespace of the project |
| --- | --- |
| `project-owner` | `admin` |
| `project-member` | `edit` |
| `read-only` | `view` |

project-sync creates the ServiceAccounts in the cluster of the project, in the namespace `drover-<cluster id>-<project>`. Each cluster has one hidden Rancher project with the name `drover`, and that namespace is in it. Tenants do not have a role in that hidden project. Each of the three ServiceAccounts can list the namespaces of its project. `project-owner` and `project-member` can also create namespaces in the project. A client can use a token of such a ServiceAccount through the Rancher proxy, for example with kubectl.

Notes:

- The ServiceAccounts have the Kubernetes roles only. They do not have the extra rules of the Rancher role templates, for example the rules for monitoring resources or for the read of nodes. Add such rules with `--bind-cluster-role` and `--bind-role`.
- A role for `--bind-cluster-role` must not grant `get` on `namespaces` without `resourceNames`. With such a rule, the api-filter refuses each namespace list of each project ServiceAccount.
- project-sync does not create ServiceAccounts for the System project, the Default project, and the hidden `drover` project.
- In the next reconcile run, project-sync restores a binding that was edited or deleted.
- To revoke each token of a ServiceAccount, delete the ServiceAccount. project-sync creates it again. Do not delete its namespace for this purpose.

## Short-lived tokens

With `--openbao-address`, project-sync configures the Kubernetes secrets engine of [OpenBao](https://openbao.org) for each cluster and each project. OpenBao can then create a token of a project ServiceAccount on request.

- Each cluster has its own mount, `kubernetes/<cluster id>`. The mount has a role and an ACL policy for each project role of each project. To change the `kubernetes` part, set `--openbao-mount-prefix`.
- OpenBao connects to a cluster through the Rancher proxy. For this connection, project-sync gives OpenBao a ServiceAccount token. project-sync replaces that token when half of its lifetime remains.
- A credential is valid for 15 minutes by default. A client can request a maximum of 2 hours. The flags are `--openbao-credential-ttl` and `--openbao-credential-max-ttl`.
- When a project or a cluster is removed from Rancher, project-sync removes its roles, its policies, and its mount from OpenBao.

A client requests a token with this command:

```sh
bao write kubernetes/<cluster id>/creds/<project>-<role> \
  kubernetes_namespace=drover-<cluster id>-<project>
```

## Login roles

A client must log in to OpenBao before it can request a token. With `--trust-file`, the project owner controls this login. The owner lists the trusted identities in the annotation `drover.evil8.io/trust-policy` on the Rancher project. Each entry of the list is a statement.

```json
{
  "statements": [
    {
      "name": "deploy-main",
      "jwt": {
        "issuer": "github",
        "claims": {"repository_id": "123456789", "ref": "refs/heads/main"}
      },
      "role": "project-member"
    },
    {
      "name": "rotator",
      "aws": {"arn": "arn:aws:iam::123456789012:role/rotator"},
      "role": "read-only"
    }
  ]
}
```

project-sync writes one login role in OpenBao for each valid statement. For a `jwt` statement, OpenBao compares the claims in the OIDC token of a CI system with the claims in the statement. For an `aws` statement, OpenBao accepts the IAM role in the statement. A client that logs in with a login role gets an OpenBao token with a short lifetime. With that token, the client can only request a credential for the project role in the statement.

project-sync reports the result of each statement in the annotation `managed.drover.evil8.io/trust-policy-status`. A valid statement has `ready: true` and the login path. A statement that is not valid has `ready: false` and a reason, for example `UnknownIssuer` or `MissingRequiredClaim`.

The platform team sets the limits for all projects in the file of `--trust-file`:

```json
{
  "annotation": "drover.evil8.io/trust-policy",
  "statusAnnotation": "managed.drover.evil8.io/trust-policy-status",
  "maxStatements": 20,
  "audience": "https://openbao.example.com",
  "loginTokenTTL": "5m",
  "jwtIssuers": {
    "github": {"requiredClaims": ["repository_id"], "allowedClaims": {"ref": ["refs/heads/main"]}}
  },
  "aws": {"enabled": true, "allowedAccounts": ["123456789012"]}
}
```

- `jwtIssuers` contains the JWT issuers that are permitted in a statement. Each issuer needs a JWT auth mount at `auth/jwt/<issuer>` in OpenBao. The AWS method needs an auth mount at `auth/aws`. project-sync does not create auth mounts. With the drover chart, a Helm hook creates them: one JWT mount for each entry of `global.broker.jwtIssuers`, by default GitHub Actions and GitLab.com. The hook creates the AWS mount when `global.broker.aws.enabled` is true.
- `requiredClaims` contains the claims that each statement of the issuer must contain, for example the repository id. `allowedClaims` contains the permitted values of a claim.
- `audience` is the audience that a JWT must have.
- Set `aws.enabled` to true to enable the AWS method. The AWS method is limited to the accounts in `aws.allowedAccounts`.

When you add the ServiceAccount tokens of a cluster as an issuer, use the claim `/kubernetes.io/serviceaccount/uid` together with `sub`. The `sub` claim contains only a namespace and a name. Another tenant can create a namespace and a ServiceAccount with those names.

## Requirements

Each item contains a note on whether the [drover chart](https://github.com/evil8io/charts/tree/main/charts/drover) satisfies it.

**Always**

- A separate Rancher service user for project-sync. Do not use the service user of the api-filter. The api-filter is on the request path of each tenant, so its user must have as few rights as possible. project-sync needs many more rights. With the chart, Helm creates this user.
- An API token of that user in a mounted file (`--token-file`). A Rancher token expires, so it must be renewed. With the chart, a CronJob runs [rotate-token](rotate-token.md) for this token.
- A network path to Rancher (`--rancher-url`). With the chart, set the value `rancher.url` to the Rancher Service in the cluster.
- For an `https` URL with a private certificate, the CA of Rancher (`--rancher-ca-file`). With the chart, a CA file is not mounted. When project-sync cannot verify the certificate, set the value `rancher.insecureSkipVerify`.
- These rights for the service user: `get`, `list`, `watch`, and `patch` on `namespaces` in each downstream cluster. The user also needs `list`, `watch`, and `patch` on the Rancher projects in the `local` cluster. With the chart, Helm creates the Rancher roles and bindings for these rights.
- An admission policy that denies a tenant write to a label or an annotation under `managed.drover.evil8.io/`. project-sync trusts its own keys under that prefix. The drover chart does not contain this policy. Install the [drover-policies chart](https://github.com/evil8io/charts/tree/main/charts/drover-policies), which contains it.

**For service accounts**

- More rights for the service user in each cluster. The user needs every verb on `namespaces`, and `create` on Rancher projects. The user also needs the rights to manage ServiceAccounts, RoleBindings, and ClusterRoleBindings. The user also needs `bind` on each role that project-sync binds. These roles are `admin`, `edit`, `view`, `create-ns`, and each role of `--bind-cluster-role` and `--bind-role`. With the chart, Helm adds these rights when `projectSync.serviceAccounts.enabled` is true.
- Rancher 2.9.0 or later. Without it, Rancher does not accept the tokens of the ServiceAccounts. The chart does not contain Rancher.
- A `ClusterProxyConfig` with `enabled: true` for each cluster. With the chart, Kyverno creates it for each downstream cluster.
- The [api-filter](api-filter.md), when the ServiceAccounts must list their namespaces. The api-filter is part of the chart.

**For short-lived tokens**

- An OpenBao server that project-sync can connect to (`--openbao-address`). With the chart, Helm installs OpenBao when `projectSync.serviceAccounts.enabled` is true.
- An `https` URL of Rancher that OpenBao can connect to (`--openbao-rancher-url`). With the chart, the URL is the value `global.broker.rancherUrl`, or the first entry of `httpRoute.hostnames`.
- A certificate on that URL that OpenBao can verify. For a private CA, the Rancher setting `cacerts` must contain the CA. With the chart, that setting is not written.
- A Kubernetes auth mount in OpenBao, with a role for the ServiceAccount of the project-sync pod. The flags are `--openbao-auth-path`, `--openbao-role`, and `--openbao-jwt-file`. The role needs an ACL policy for the secrets engine mounts, their roles, and the policies under the mount prefix. With the chart, OpenBao creates the auth mount at its first start. A Helm hook writes the role and the policy.
- More rights for the Rancher service user. The user needs `create` on the tokens of the ServiceAccounts, and the rights to manage Roles. The user also needs `get` on the Rancher clusters and on the Rancher setting `cacerts`. With the chart, Helm adds these rights.

**For login roles**

- The file of `--trust-file`. With the chart, Helm writes this file from the values under `global.broker`.
- A JWT auth mount at `auth/jwt/<issuer>` for each issuer in that file, with the configuration of that issuer. With the chart, a Helm hook creates one mount for each entry of `global.broker.jwtIssuers`.
- For AWS identities, an AWS auth mount at `auth/aws`. With the chart, a Helm hook creates it when `global.broker.aws.enabled` is true.
- An ACL policy for project-sync that also includes the roles of those auth mounts. With the chart, the same Helm hook writes it.
- A network path from the clients, for example CI runners, to OpenBao and to Rancher. With the chart, set `openbao.server.gateway.httpRoute` for the route to OpenBao. The chart does not contain the Gateway.

The permission lists and the OpenBao policies are in [.claude/docs/project-sync.md](../.claude/docs/project-sync.md).

## How to run it

```sh
drover project-sync --rancher-url https://rancher.cattle-system --token-file /var/run/drover/token \
  --labels example.com/cost-center --name-annotation example.com/project-name
```

`drover project-sync --help` shows each flag with its default. project-sync answers on `/healthz`. It writes JSON logs to stderr. With `--otlp-endpoint`, it also sends OpenTelemetry traces and the `drover.sync.*` metrics. project-sync does not log a token, a display name, or the content of a trust annotation.
