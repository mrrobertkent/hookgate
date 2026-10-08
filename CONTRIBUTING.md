# Contributing

Thanks for helping. Issues and pull requests are welcome.

## Before you start

- For a bug, open an issue with the configuration (secrets removed), the request that misbehaved and the log line hookgate wrote.
- For a new signature scheme or feature, open an issue first so we can agree on the configuration shape.
- Security problems go through [SECURITY.md](SECURITY.md), not issues.

## Development

Requires Go (the version in `go.mod`).

```sh
go test -race ./...
go test -run='^$' -fuzz=FuzzHMAC -fuzztime=30s ./internal/verify
go vet ./...
gofmt -l .
golangci-lint run   # version pinned in .github/workflows/ci.yml
```

Keep the dependency list short: hookgate depends only on the Go standard library and a YAML parser. Every verification change needs tests for the valid case, the forged case, the missing-header case and the missing-secret case.

## Pull requests

- One logical change per pull request, with tests.
- Describe the user-visible behaviour change in the description; it becomes the release note.
- By contributing you agree that your contribution is licensed under the Apache License 2.0.
