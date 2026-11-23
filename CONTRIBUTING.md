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
go test -run='^$' -fuzz=FuzzHost -fuzztime=30s ./internal/gateway
go test -run='^$' -fuzz='^FuzzStandardWebhooks$' -fuzztime=30s ./internal/verify
go test -run='^$' -fuzz=FuzzStandardWebhooksSecret -fuzztime=30s ./internal/verify
go test -run='^$' -fuzz=FuzzSubjectTemplate -fuzztime=30s ./internal/contract
go test -run='^$' -fuzz=FuzzEventID -fuzztime=30s ./internal/contract
go test -run='^$' -fuzz=FuzzMessageHeaders -fuzztime=30s ./internal/relay
go test -run='^$' -fuzz=FuzzFilterSubject -fuzztime=30s ./internal/relay
go test -run='^$' -fuzz=FuzzReplayIsolation -fuzztime=30s ./internal/relay
go vet ./...
gofmt -l .
golangci-lint run   # version pinned in .github/workflows/ci.yml
```

Keep the dependency list short. hookgate depends on the Go standard library, a YAML parser and the official NATS client (`github.com/nats-io/nats.go`, with its `nkeys` and `nuid` packages). The NATS client is maintained by the NATS project and widely used; without it, publishing to JetStream would need a separate bridge service in every deployment. `golang.org/x/sys/unix`, maintained by the Go project, gives the relay `waitid` on Linux, so it can kill a handler's process group before the group ID can be reused. `github.com/nats-io/nats-server/v2` (embedded servers) and `github.com/nats-io/jwt/v2` (creds files) are used only by tests and are never linked into a binary. Tests also use the Standard Webhooks reference library as an oracle. Any other dependency needs the same justification in its pull request. Every verification change needs tests for the valid case, the forged case, the missing-header case and the missing-secret case.

## Pull requests

- One logical change per pull request, with tests.
- Describe the user-visible behaviour change in the description; it becomes the release note.
- By contributing you agree that your contribution is licensed under the Apache License 2.0.
