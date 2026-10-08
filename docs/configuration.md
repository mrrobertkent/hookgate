# Configuration reference

hookgate reads one YAML file: `-config FILE`, else `$HOOKGATE_CONFIG`, else `/etc/hookgate/hookgate.yaml`. Unknown keys are errors, so a typo stops startup instead of silently changing behaviour. Run `hookgate validate -config FILE` in CI: it checks the file and that every referenced secret is set.

## Server

| Key | Default | Meaning |
|---|---|---|
| `listen` | `:8080` | Webhook listener. |
| `admin_listen` | `:9090` | `/healthz`, `/readyz`, `/metrics`. Keep it off the public network. |
| `max_concurrent` | `64` | Requests verified or forwarded at once; extra requests get `503`. |
| `read_header_timeout` | `5s` | Time allowed for request headers. |
| `read_timeout` | `15s` | Time allowed for the whole request, body included. |
| `write_timeout` | `60s` | Time allowed to write the response; must exceed the longest `forward.timeout`. |
| `idle_timeout` | `120s` | Keep-alive idle time. Keep it above your proxy's idle timeout so the proxy never reuses a closed connection. |
| `drain_delay` | `0s` | After SIGTERM, keep accepting requests this long (with `/readyz` answering `503`) so load balancers stop routing here first. |
| `shutdown_timeout` | `30s` | Time in-flight requests get to finish after the drain. |

## Sources

Each entry of `sources` is one sender.

| Key | Default | Meaning |
|---|---|---|
| `name` | required | Label used in logs and metrics. |
| `path` | required | Exact request path, for example `/github`. Any other path gets `404`. |
| `max_body_bytes` | `1048576` | Larger bodies get `413` and are never forwarded. |
| `allow_query` | `false` | Requests with a query string get `400` unless set: query parameters are not covered by signatures. |
| `reject_status` | `401` | Status returned when a check fails. |
| `checks` | required | One or more checks; **all** must pass. A source with no checks is a configuration error. |
| `forward` | required | Where verified requests go. |

Requests must be `POST` (else `405`) with no `Content-Encoding` other than `identity` (else `415`): signatures cover the bytes as sent.

### `token` check

Compares one request header with a shared secret, in constant time.

```yaml
- token:
    header: X-Webhook-Token
    secrets:
      - env: WEBHOOK_TOKEN
```

### `hmac` check

Verifies a keyed hash of the raw request body.

```yaml
- hmac:
    header: X-Hub-Signature-256
    algorithm: sha256     # sha1 | sha256 | sha512
    encoding: hex         # hex (default) | base64 | base64url
    prefix: "sha256="     # optional; when set, a signature without it is ignored
    separator: ","        # optional; for headers that carry several signatures
    secrets:
      - env: GITHUB_WEBHOOK_SECRET
```

The request passes when any signature in the header matches the HMAC of the body under any secret. Hex is case-insensitive.

### Secrets

Every secret is a reference, never a literal:

```yaml
secrets:
  - env: SIGNING_SECRET            # environment variable
  - file: /run/secrets/signing     # file; one trailing newline is ignored
    optional: true                 # may be unset or missing
```

A non-optional secret that is unset, empty or unreadable makes its source **not ready**: the source answers `503` and forwards nothing, the error is logged, and `hookgate_source_ready{source=...}` is `0`. Other sources keep serving. A check whose secrets are all optional and all unset is also not ready, so a source can never run without a secret.

**Rotation:** add the new secret as a second, optional reference, deploy, switch the sender to the new secret, then remove the old reference.

## `forward`

| Key | Default | Meaning |
|---|---|---|
| `urls` | required | One or more upstream URLs, used exactly as written (path and query included). |
| `resolve_all` | `false` | Resolve each URL's host name to all of its addresses and treat each address as a target, in random order. Use it for a service name that fronts several instances. |
| `pass_headers` | none | Request headers copied upstream. Names are case-insensitive; a trailing `*` matches a prefix (`X-GitHub-*`). Everything else, including the verified secret headers, is dropped. |
| `set_headers` | none | Headers added upstream, each a secret reference (for example the upstream's own credentials). |
| `attempt_timeout` | `5s` | Limit for one attempt, including connecting. |
| `timeout` | `20s` | Limit for all attempts together. Keep it below the sender's own timeout. |
| `max_attempts` | `3` | Attempts across targets. |

The upstream receives the verified body byte for byte, with `POST`. A transport error or `5xx` moves on to the next target; any other status is final and is returned to the sender with the upstream's body (first 64 KiB). If every attempt fails, the sender gets the last `5xx`, or `502` when no upstream answered.

Retries mean an upstream can receive the same request more than once (for example when it stored the request but the response was lost). Make the upstream idempotent or deduplicate downstream.

## Metrics

| Metric | Type | Labels |
|---|---|---|
| `hookgate_requests_total` | counter | `source`, `outcome` = `forwarded`, `rejected`, `upstream_error`, `too_large`, `bad_request`, `not_ready`, `busy` |
| `hookgate_upstream_attempts_total` | counter | `source`, `result` = `2xx`, `3xx`, `4xx`, `5xx`, `error` |
| `hookgate_source_ready` | gauge | `source` |
| `hookgate_in_flight_requests` | gauge | — |
| `hookgate_draining` | gauge | — |

## Logs

One JSON line per request on stderr: `source`, `outcome`, `status`, `reason` (for rejections), `remote`, `forwarded_for`, `bytes`, `duration_ms`. Secrets, signatures and bodies are never logged.
