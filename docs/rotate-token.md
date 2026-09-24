# rotate-token

A command that renews the API token of the Rancher service user in a Kubernetes Secret.

The command runs once: it reads the token from the Secret, and it asks Rancher for the expiry. A token that lasts longer than `--renew-before` is valid, and the run ends with no change. For a new token the command logs in as the service user, and it derives a token from that session. The login is a first step only, because Rancher ignores the TTL of a login token. Last, the command patches the Secret, deletes the old tokens, and ends the session with a logout.

Rancher reduces a TTL above its own maximum without an error, so a different TTL in the answer gives a warning. When the granted TTL is not longer than `--renew-before`, the run completes the rotation and then exits 1, because every later run rotates again.

The run keeps the new token and the token that it read from the Secret. It deletes the other tokens with the same description, except the newest ones up to `--keep`.

With `--password-secret`, a run first writes the password hash that Rancher reads at a local login. Rancher names that Secret after the User object, in the namespace `cattle-local-user-passwords`. A run that finds the current hash leaves the Secret unchanged. The ServiceAccount needs `get` and `patch` on that one Secret. A failure of the password step does not stop the run. The token steps run, and the run then exits 1 with the error of the password step.

A CronJob is the normal caller, because most runs find a valid token and exit 0.

## Configuration

`drover rotate-token [flags]` runs the command once.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--rancher-url` | (required) | URL of Rancher. Use `http://` or `https://`. The path must be empty or `/`. |
| `--rancher-ca-file` | | PEM bundle that verifies an `https` Rancher URL. |
| `--rancher-insecure-skip-verify` | `false` | Skip the certificate verification of an `https` Rancher URL. |
| `--credentials-dir` | (required) | Directory with the files `username` and `password` of the service user. |
| `--token-secret` | (required) | `namespace/name` of the Secret with the API token. |
| `--token-key` | `token` | Key of the token inside the Secret. |
| `--password-secret` | | `namespace/name` of the Secret that Rancher reads for the password of the service user. When set, a run first writes the PBKDF2-SHA3-512 hash of the password into it. |
| `--ttl` | `48h` | Lifetime of a new token. Rancher reduces a value above `auth-token-max-ttl-minutes`, and a reduced value that is not longer than `--renew-before` makes the run exit 1 after the rotation. |
| `--renew-before` | `24h` | Remaining lifetime that starts a rotation. The value must be shorter than `--ttl`. |
| `--keep` | `2` | Number of tokens with the description to keep. The new token and the token that the run read from the Secret count, and the run never deletes them. The value must be 2 or more, because a pod reads a mounted Secret with a delay after the patch. |
| `--description` | `drover rotate-token` | Description of the tokens of this command. It also selects the tokens to delete. |
| `--kube-url` | (in-cluster) | Kubernetes API URL. The default comes from `KUBERNETES_SERVICE_HOST` and `KUBERNETES_SERVICE_PORT`. |
| `--kube-service-account-dir` | `/var/run/secrets/kubernetes.io/serviceaccount` | Directory with the ServiceAccount token and `ca.crt`. |
| `--log-level` | `info` | One of `debug`, `info`, `warn`, or `error`. |
| `--otlp-endpoint` | `$OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC endpoint, `host:port` or a URL. Empty turns telemetry off. |
| `--otlp-traces` | `true` | Send traces to the OTLP endpoint. |
| `--otlp-metrics` | `true` | Send metrics to the OTLP endpoint. |
| `--service-name` | `$OTEL_SERVICE_NAME`, or `drover` | `service.name` resource attribute. |

The exit code is 0 after a valid token and after a rotation, 1 after a failure, and 2 after a flag error.

The command never deletes a token with another description, so a kubeconfig token of the service user stays.

## Permissions

The pod needs `get` and `patch` on the one token Secret:

```yaml
rules:
  - apiGroups: [""]
    resources: [secrets]
    resourceNames: [drover-token]
    verbs: [get, patch]
```

The Secret must exist before the first run. The command reads the ServiceAccount token for every request, because the kubelet replaces the file.

## Logging

The command writes JSON logs to stderr, one line per step. Each line has a `step` field and an `outcome` field. The `token_prune` line has the field `secret_token`: the name of the token that the run read from the Secret and kept, or an empty string. The command never logs a token, a token key, or the password.

## Telemetry

With `--otlp-endpoint` set, one run produces a span named `rotate`, with a child span for each step: `password_sync` (with `--password-secret`), `secret_get`, `token_check`, `login`, `token_create`, `secret_patch`, `token_prune`, and `logout`. The command flushes traces and metrics before it exits, within a 5 s grace period. It exports these metrics:

| Metric | Kind | Unit | Attributes |
| --- | --- | --- | --- |
| `drover.rotate.steps` | Counter | `1` | `step`, `outcome` |
| `drover.rotate.duration` | Histogram | `s` | |
