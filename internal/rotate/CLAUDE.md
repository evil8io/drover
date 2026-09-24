# rotate-token

Human doc: `docs/rotate-token.md`. Update it with a behaviour change.

Rancher token facts, verified against 2.14.5:

- `POST /v3-public/localProviders/local?action=login` ignores `ttl`. The session length is `auth-user-session-ttl-minutes`. A chosen lifetime needs `POST /v3/tokens` with `ttl` in milliseconds, and Rancher reduces a value above `auth-token-max-ttl-minutes` without an error. Warn when the returned TTL differs from the request. Fail the run after the rotation when the granted TTL is not longer than `--renew-before`, because the new token is inside the renew window at once.
- `responseType: cookie` on the login returns an empty body. Use the token response.
- `expiresAt` is empty right after a create. Derive the expiry from `ttl` and the creation time.
- A delete of the current session token answers 400. End the session with `?action=logout`.
- A login over plain HTTP answers `400 Use HTTPS`.

Rules:

- The description selects the tokens to prune. Never delete a token with another description, because a kubeconfig token of the service user has one.
- Keep `--keep` at 2 or more, because a pod reads a mounted Secret with a delay after the patch, and the old token must work in that window.
- Never delete the token that the run read from the Secret, also when newer tokens with the description exist. A token of an earlier run with a failed patch is newer than the mounted token, and a pod reads the mounted Secret with a delay after the patch.
- The run is check-and-renew, so a CronJob every 10 minutes is cheap and a missed run is harmless. Do not change it to a fixed rotation on a schedule.
- Read the ServiceAccount token per request, because the kubelet replaces the file.
- Password hash, `password.go`: Rancher reads `cattle-local-user-passwords/<User name>` at each local login. The format is PBKDF2-HMAC-SHA3-512 with 210000 iterations, a 32-byte salt, and a 32-byte digest, with the annotation `cattle.io/password-hash: pbkdf2sha3512`. Compare with the stored salt, and leave a current hash alone, because a rewrite changes nothing and churns the Secret. Only this write or the admin `setpassword` action sets a Rancher password; an external secret store cannot. Reject a password shorter than 12 characters when the password step is on, because the hash write skips the Rancher check on `setpassword`. Rancher counts the length with `utf8.RuneCountInString` (verified at 2.14.5).
- Do not end the run on a failed password step. The token steps run first, and the run exits 1 after them, because a permanent failure of the hash write would otherwise leave every component without a Rancher token after the TTL.
- Require `https` for the Rancher URL, and never follow a redirect, because the login body has the password, and a client that follows a 307 or 308 answer resends the body to the target of the redirect.
