# rotate-token

This file is the reference of rotate-token: the steps, the flags, the exit codes, and the metrics. The short document for users is `docs/rotate-token.md`.

The `rotate-token` command rotates the API token of the Rancher service user in a Kubernetes Secret.

The command runs once. In a run, the command does these steps:

1. The command reads the token from the Secret.
2. The command asks Rancher for the expiry time of the token. When the time until the token expires is longer than `--renew-before`, the token is valid. The command then ends the run and does not make a change.
3. To get a new token, the command logs in to Rancher as the service user.
4. The command uses the login session to create the new token.
5. The command patches the Secret.
6. The command deletes the old tokens.
7. The command logs out to end the session.

The command accepts only an https URL for `--rancher-url`, because it sends the password in the body of the login request. The command never follows a redirect. After a redirect response, the run fails, except in the `logout` step. In the `logout` step, the command only logs a warning.

Rancher reduces a TTL above its own maximum and does not return an error. For this reason, the command logs a warning when the TTL in the response of Rancher differs from the requested TTL.

The command keeps the new token and the token that it read from the Secret. It deletes the other tokens with the `--description` value, except the newest tokens up to a total of `--keep` tokens. It also deletes the login tokens of earlier runs, which have the description `<description> login`.

When `--password-secret` is set to a Secret, the command first writes the password hash into that Secret in each run. Rancher reads the hash when a local user logs in. Rancher gives that Secret the name of the User object. The Secret is in the namespace `cattle-local-user-passwords`. The ServiceAccount needs the `get` and `patch` permissions on that one Secret.

After a failed password step, the command exits with code 1 and reports the error of that step.

Usually, a CronJob runs the command, because in most runs, the command finds a valid token and exits with code 0.

## Configuration

To run the command once, use `drover rotate-token [flags]`.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--rancher-url` | (required) | The URL of Rancher. Use `https://` only. The path must be empty or `/`. |
| `--rancher-ca-file` | | The PEM bundle that the command uses to verify the certificate of an `https` Rancher URL. |
| `--rancher-insecure-skip-verify` | `false` | When the value is `true`, the command does not verify the certificate of an `https` Rancher URL. The command then sends the password to a server that it does not verify. Together with `--rancher-ca-file`, the command exits with code 2. |
| `--credentials-dir` | (required) | The directory with the files `username` and `password` of the service user. |
| `--token-secret` | (required) | The `namespace/name` of the Secret with the API token. |
| `--token-key` | `token` | The key of the token in the Secret. |
| `--password-secret` | | The `namespace/name` of the Secret that Rancher reads for the password of the service user. When the flag is set, the command first writes the PBKDF2-SHA3-512 hash of the password into that Secret in each run. When the Secret already contains that hash, the command does not write it. The password must have 12 characters or more, which is the minimum password length of Rancher. With a shorter password, the command exits with code 2. |
| `--ttl` | `48h` | The lifetime of a new token. Rancher reduces a value above its setting `auth-token-max-ttl-minutes`. When the reduced value is less than `--renew-before` plus 1 hour, the command exits with code 1 after the rotation. |
| `--renew-before` | `24h` | When the time until the token expires is not longer than this value, the command rotates the token. The value must be shorter than `--ttl`. |
| `--keep` | `2` | The command keeps this number of tokens with the `--description` value. The new token and the token that the command read from the Secret count toward this number. The command never deletes these two tokens. The value must be 2 or more, because a pod reads a mounted Secret with a delay after the patch. For a value below 2, the command exits with code 2. |
| `--description` | `drover rotate-token` | The description of the tokens that this command creates. The command also uses this description to select the tokens to delete. |
| `--kube-url` | (in-cluster) | The URL of the Kubernetes API. Use `https://` only, because the requests contain the ServiceAccount token and the Rancher token. The command takes the default from the environment variables `KUBERNETES_SERVICE_HOST` and `KUBERNETES_SERVICE_PORT`. |
| `--kube-service-account-dir` | `/var/run/secrets/kubernetes.io/serviceaccount` | The directory with the ServiceAccount token and the file `ca.crt`. |
| `--log-level` | `info` | The log level. The value is one of `debug`, `info`, `warn`, or `error`. |
| `--otlp-endpoint` | `$OTEL_EXPORTER_OTLP_ENDPOINT` | The OTLP gRPC endpoint. The value is `host:port` or a URL. When the value is empty, telemetry is off. |
| `--otlp-traces` | `true` | When the value is `true`, the command sends traces to the OTLP endpoint. |
| `--otlp-metrics` | `true` | When the value is `true`, the command sends metrics to the OTLP endpoint. |
| `--service-name` | `$OTEL_SERVICE_NAME`, or `drover` | The value of the `service.name` resource attribute. When the value is empty, as in `--service-name=`, the command does not set a `service.name`, so that a collector can derive it. The OpenTelemetry SDK still takes a `service.name` from `OTEL_SERVICE_NAME` or `OTEL_RESOURCE_ATTRIBUTES`. |

The command exits with one of these codes:

| Exit code | Condition |
| --- | --- |
| 0 | Every step succeeds, or only the `logout` step fails. The command finds a valid token. Or the command completes a rotation, and the run does not fail because of the TTL. |
| 1 | A step other than `logout` fails. The command also exits with code 1 when Rancher reduces the TTL to less than `--renew-before` plus 1 hour. The command also exits with code 1 when the token in the Secret is inside `--renew-before` and younger than 5 minutes. |
| 2 | The command finds an error in the flags, in a file of `--credentials-dir`, in a CA file, or in the telemetry setup. |

The command deletes only tokens with the `--description` value or with the description `<description> login`. A token with another description stays, for example a kubeconfig token of the service user.

## Permissions

The pod needs the `get` and `patch` permissions on the one token Secret:

```yaml
rules:
  - apiGroups: [""]
    resources: [secrets]
    resourceNames: [drover-token]
    verbs: [get, patch]
```

The Secret must exist before the first run. The command reads the ServiceAccount token for each request, because the kubelet replaces the token file.

## Logging

The command writes JSON logs to stderr. For each step that succeeds, the command writes at least one line with a `step` field and an `outcome` field. When the run fails, the last line contains an `error` field.

The `token_prune` line contains the field `secret_token`. The value of this field is the name of the token that the command read from the Secret and kept. When the command did not keep a token from the Secret, the value is an empty string. The command never logs a token, a token key, or the password.

## Telemetry

When `--otlp-endpoint` is set, the command creates one span with the name `rotate` for each run. This span has a child span for each step. The steps are `password_sync`, `secret_get`, `token_check`, `login`, `token_create`, `secret_patch`, `token_prune`, and `logout`. The command does the `password_sync` step only when `--password-secret` is set. Before the command exits, it flushes the traces and metrics within a grace period of 5 s. The command exports these metrics:

| Metric | Kind | Unit | Attributes |
| --- | --- | --- | --- |
| `drover.rotate.steps` | Counter | `1` | `step`, `outcome` |
| `drover.rotate.duration` | Histogram | `s` | |
