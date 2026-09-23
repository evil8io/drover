# Contributing

## Layout

```
cmd/drover/              the binary, with one file per subcommand
internal/filter/         the API filter
internal/projectsync/    the project sync
internal/rotate/         the token rotation
internal/rancherclient/  the HTTP client for Rancher
internal/telemetry/      logs, traces, and metrics
docs/                    one document per subcommand
```

## Checks

drover needs the Go version in `go.mod`. Run these checks before a commit:

1. `go test -race ./...`
2. `go vet ./...`
3. `gofmt -l .` (it must print nothing)

CI runs the same checks, plus golangci-lint, on every pull request.

## Build

Build the binary with `go build ./cmd/drover`.

Build the image with `podman build` or `docker build`. Pass the version with the `VERSION` build argument:

```sh
podman build --build-arg VERSION=0.14.0 -t drover:0.14.0 .
```

## Docs

Each subcommand has a document in `docs/`, with its behaviour, its flags, its permissions, its log fields, and its metrics. A change to one of those updates the document in the same pull request.

## Pull requests

A pull request title follows [Conventional Commits](https://www.conventionalcommits.org/), and a check on the title blocks the merge otherwise. Every pull request is squash-merged, and the title becomes the commit subject on `main`. Add a `!` after the type for a breaking change.

## Releases

[release-please](https://github.com/googleapis/release-please) reads the commit subjects on `main` and opens a release pull request. A merge of that pull request creates a tag, a GitHub release, and the image tags `<version>`, `<major>.<minor>`, and `latest` on `ghcr.io/evil8io/drover`. A push to `main` also pushes the image tags `main` and `sha-<short sha>`. drover has no 1.0 release yet, and a breaking change raises the minor version.
