# rotate-token

Human doc: `docs/rotate-token.md`. Update it with a behaviour change.

Rancher token facts, verified against 2.14.5:

- `POST /v3-public/localProviders/local?action=login` ignores `ttl`. The session length is `auth-user-session-ttl-minutes`. A chosen lifetime needs `POST /v3/tokens` with `ttl` in milliseconds, and Rancher reduces a value above `auth-token-max-ttl-minutes` without an error, so compare the returned TTL and warn.
- `responseType: cookie` on the login returns an empty body. Use the token response.
- `expiresAt` is empty right after a create. Derive the expiry from `ttl` and the creation time.
- A delete of the current session token answers 400. End the session with `?action=logout`.
- A login over plain HTTP answers `400 Use HTTPS`.

Rules:

- The description selects the tokens to prune. Never delete a token with another description, because a kubeconfig token of the service user has one.
- Keep `--keep` at 2 or more, because a pod reads a mounted Secret with a delay after the patch, and the old token must work in that window.
- The run is check-and-renew, so a CronJob every 10 minutes is cheap and a missed run is harmless. Do not change it to a fixed rotation on a schedule.
- Read the ServiceAccount token per request, because the kubelet replaces the file.
- Password hash, `password.go`: Rancher reads `cattle-local-user-passwords/<User name>` at each local login. The format is PBKDF2-HMAC-SHA3-512 with 210000 iterations, a 32-byte salt, and a 32-byte digest, with the annotation `cattle.io/password-hash: pbkdf2sha3512`. Compare with the stored salt, and leave a current hash alone, because a rewrite changes nothing and churns the Secret. Only this write or the admin `setpassword` action sets a Rancher password; an external secret store cannot.
