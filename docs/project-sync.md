# project-sync

project-sync keeps the clusters that Rancher manages in line with the Rancher projects. It has four jobs. Project metadata and service accounts can each be used alone. The last two build on the one before: short-lived tokens need the service accounts, and login roles need the short-lived tokens.

| Job | Flag | Result |
| --- | --- | --- |
| [Project metadata](#project-metadata) | `--labels`, `--annotations`, `--name-label`, `--name-annotation` | Every namespace gets the labels, annotations, and display name of its project. |
| [Service accounts](#service-accounts) | `--service-accounts` | Every project gets three ServiceAccounts, one per project role. |
| [Short-lived tokens](#short-lived-tokens) | `--openbao-address` | OpenBao can hand out short-lived tokens of those ServiceAccounts. |
| [Login roles](#login-roles) | `--trust-file` | A project owner decides which CI jobs or cloud identities may get such a token. |

The service does a full reconcile run every `--interval` (60 seconds by default). It also watches namespaces and projects, so most changes arrive within seconds. One replica is enough.

## Project metadata

Rancher does not copy the labels and annotations of a project to its namespaces. project-sync does, for the keys you list in `--labels` and `--annotations`. This makes project metadata such as a cost centre or an owner usable in policies, network rules, and dashboards.

- The project wins. If a namespace has a different value for a listed key, the service overwrites it.
- The service removes a key when the project no longer has it, or when the namespace moves to another project. It only removes keys that it wrote itself; it remembers them in two annotations on the namespace.
- `--name-annotation` gets the display name of the project. `--name-label` gets a copy that is a valid label value: other characters become `-`, and the value is cut to 63 characters. A display name without a single letter or digit gets no label.
- Keys under `cattle.io`, `kubernetes.io`, `k8s.io`, and `managed.drover.evil8.io` are refused.

The service also writes the number of namespaces of each project into the annotation `managed.drover.evil8.io/namespace-count` on the Project. A policy in the drover chart uses it to block the delete of a project that still has namespaces. If your deployment tool reports this annotation as drift, tell it to ignore the key.

## Service accounts

A CI job or another client outside the cluster is not a Rancher user, so it cannot be a member of a project. With `--service-accounts`, every tenant project gets three ServiceAccounts that stand in for the three project roles:

| ServiceAccount | Kubernetes role in every namespace of the project |
| --- | --- |
| `project-owner` | `admin` |
| `project-member` | `edit` |
| `read-only` | `view` |

They live in a namespace `drover-<cluster id>-<project>`, inside a hidden Rancher project called `drover` that no tenant has a role in. All three can list the namespaces of their project, and the owner and the member can create namespaces in it. A token of such a ServiceAccount works through the Rancher proxy, for example with kubectl.

Things to know:

- The ServiceAccounts get the plain Kubernetes roles. They do not get the extra rules of the Rancher role templates, such as monitoring resources or reading nodes. Add what you need with `--bind-cluster-role` and `--bind-role`.
- A role bound with `--bind-cluster-role` must not grant `get` on `namespaces` without `resourceNames`. The api-filter would then refuse every namespace list of every project ServiceAccount.
- The System project, the Default project, and the hidden `drover` project itself get no ServiceAccounts.
- The bindings repair themselves. If a tenant edits or deletes one, the next reconcile run restores it.
- To revoke every token of a ServiceAccount, delete the ServiceAccount; the service creates it again. Do not delete its namespace for this.

## Short-lived tokens

With `--openbao-address`, project-sync configures the Kubernetes secrets engine of [OpenBao](https://openbao.org) for every cluster and project, so that OpenBao can create tokens of the project ServiceAccounts on request.

- Each cluster gets its own mount, `kubernetes/<cluster id>`, with a role and an ACL policy per project and project role. `--openbao-mount-prefix` changes the `kubernetes` part.
- OpenBao reaches a cluster through the Rancher proxy. project-sync gives it a ServiceAccount token for that and replaces the token well before it expires.
- A credential is valid for 15 minutes by default, and a client can ask for up to 2 hours (`--openbao-credential-ttl`, `--openbao-credential-max-ttl`).
- When a project or a cluster is removed from Rancher, the service removes its roles, policies, and mount from OpenBao.

A client gets a token with:

```sh
bao write kubernetes/<cluster id>/creds/<project>-<role> \
  kubernetes_namespace=drover-<cluster id>-<project>
```

## Login roles

A client still has to log in to OpenBao before it can ask for a token. With `--trust-file`, the project owner controls this: they list the identities they trust in the annotation `drover.evil8.io/trust-policy` on their Rancher project.

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

For each valid statement, project-sync writes one login role in OpenBao. A `jwt` statement matches the OIDC token of a CI system on the listed claims. An `aws` statement matches an IAM role. Whoever logs in with that role gets a short OpenBao token that can do one thing: request a credential for the named project role.

The service reports the result per statement in the annotation `managed.drover.evil8.io/trust-policy-status`, with `ready: true` and the login path, or `ready: false` and a reason such as `UnknownIssuer` or `MissingRequiredClaim`.

The platform team sets the boundaries in the file behind `--trust-file`:

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

- `jwtIssuers` lists the JWT issuers that a statement may use. Each one needs a JWT auth mount at `auth/jwt/<issuer>` in OpenBao, and the AWS method needs one at `auth/aws`. project-sync does not create auth mounts.
- `requiredClaims` are claims every statement of that issuer must bind, for example the repository id. `allowedClaims` limits the values of a claim.
- `audience` is the audience a JWT must have.
- `aws` turns the AWS method on and can limit it to a list of accounts.

If you trust the ServiceAccount tokens of a cluster as an issuer, bind the claim `/kubernetes.io/serviceaccount/uid` as well as `sub`. The `sub` claim only holds a namespace and a name, and another tenant could create both.

## Requirements

- A Rancher service user of its own, with an API token in a mounted file (`--token-file`). Do not share the user of the api-filter: project-sync needs far wider rights, and the api-filter sits on the request path of every tenant.
- For project metadata, that user needs `get`, `list`, `watch`, and `patch` on `namespaces` in every cluster, and `list`, `watch`, and `patch` on Rancher projects. The other jobs add rights to create namespaces, ServiceAccounts, roles, and bindings.
- For the short-lived tokens, OpenBao needs a Kubernetes auth mount that project-sync can log in to, with a policy for the mounts, roles, and policies that the service writes.

The [drover chart](https://github.com/evil8io/charts/tree/main/charts/drover) sets all of this up, including OpenBao itself. The exact permission lists and the OpenBao policy are in [.claude/docs/project-sync.md](../.claude/docs/project-sync.md).

## Running it

```sh
drover project-sync --rancher-url https://rancher.cattle-system --token-file /var/run/drover/token \
  --labels example.com/cost-center --name-annotation example.com/project-name
```

`drover project-sync --help` lists every flag with its default. The service serves `/healthz`, writes JSON logs to stderr, and exports OpenTelemetry traces and `drover.sync.*` metrics when `--otlp-endpoint` is set. It never logs a token, a display name, or the content of a trust annotation.
