<!--
Title: `Area: imperative summary`, for example `Verify: Standard Webhooks signatures`.
Labels: exactly one `type/*` (feature, fix, docs, ci, deps, refactor, test, security); any `area/*` that apply; `breaking-change` if users must change anything.
Delete any section that does not apply, and these comments.
-->

## Summary

<!-- Two or three sentences: what this pull request does and why it matters. -->

## What changes for users

<!-- The release note: behaviour, configuration keys, metrics, images. Write "None" for internal changes. -->

-

> [!IMPORTANT]
> **Breaking change.** <!-- What breaks, who is affected and the exact migration step; delete this block if nothing breaks. -->

## Why

<!-- The problem, with links: issues (`Fixes #123`), docs, upstream specifications or reference implementations, and the files involved. -->

-

## How it works

<!-- Design notes: the approach, alternatives rejected and why, constraints a reviewer should check. A table helps for mappings (status codes, keys, defaults). -->

## Testing

<!-- Commands run and their results. -->

| Command | Result |
| --- | --- |
| `go test -race -count=1 ./...` | |
| `go vet ./...` and `gofmt -l .` | |
| `golangci-lint run` | |

## Docs updated

<!-- Links to the changed docs, or "None". -->

-

## Checklist

- [ ] Exactly one `type/*` label, plus any `area/*` and `breaking-change` labels that apply
- [ ] Tests cover the valid, forged, missing-header and missing-secret cases (for verification changes)
- [ ] New configuration keys have defaults, validation and a [`docs/configuration.md`](https://github.com/mrrobertkent/hookgate/blob/main/docs/configuration.md) entry
- [ ] README or docs updated if behaviour or configuration changed
