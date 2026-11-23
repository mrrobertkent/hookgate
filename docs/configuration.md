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
| `name` | required | Label used in logs and metrics. Letters, digits, `-` and `_`, starting with a letter or digit, at most 63 characters (`[A-Za-z0-9][A-Za-z0-9_-]{0,62}`); unique. |
| `hosts` | none | Exact host names or IP addresses this source answers, for example `[hooks.example.com]`. Omitted means any host; an empty or null list, or a null entry, is a configuration error. See [Routing](#routing). |
| `path` | required | Exact request path, for example `/github`. |
| `max_body_bytes` | `1048576` | Larger bodies get `413` and are never forwarded. |
| `allow_query` | `false` | Requests with a query string get `400` unless set: query parameters are not covered by signatures. |
| `reject_status` | `401` | Status returned when a check fails. |
| `checks` | required | One or more checks; **all** must pass. A source with no checks is a configuration error. |
| `forward` | required | Where verified requests go. |

### Routing

A request goes to the source whose `path` equals the decoded request path exactly and that either lists the request's host in `hosts` or has no `hosts`. A source that lists the host beats the path's source without `hosts`. Any other request gets `404`.

- Each (host, path) pair belongs to one source: two sources without `hosts` on one path, or two sources listing the same host on one path, stop startup. A source without `hosts` and sources with `hosts` may share a path.
- The host is the request's effective authority: the authority of an absolute-form request target (`POST http://host/path HTTP/1.1`) when present, else the `Host` header, or `:authority` in HTTP/2. `X-Forwarded-Host` is never read, so a proxy in front must pass the client's `Host` through (Traefik: `passHostHeader`, true by default).
- The host is compared case-insensitively, without its port and without one trailing dot; IP addresses are compared in canonical form, so `[::1]:8080` matches `::1`. A malformed authority (a port that is not up to five digits, unbalanced brackets, brackets around anything but an IPv6 address, an unbracketed IPv6 address) matches no listed host: it reaches the path's source without `hosts` if there is one, else gets `404`.
- In `hosts`, write DNS names as letters, digits and hyphens (internationalised names in punycode, `xn--…`), and IPv6 addresses with or without brackets, quoted in YAML: `hosts: ["::1"]`. Names are lowercased and one trailing dot is dropped. Wildcards, ports and IPv6 zones are configuration errors.

The precedence matches nginx, where a `server_name` that equals the host wins over the port's default server ([nginx: server names](https://nginx.org/en/docs/http/server_names.html)). Traefik instead orders routers by priority, by default the length of the rule ([Traefik: rules and priority](https://doc.traefik.io/traefik/reference/routing-configuration/http/routing/rules-and-priority/#priority-calculation)): give each exact host and path its own router to hookgate rather than a catch-all. hookgate repeats the host match, so a request the proxy misroutes never reaches a source that lists other hosts.

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

Token and HMAC secrets must be at least 16 bytes; use a random value such as `openssl rand -hex 32`. A shorter secret, optional or not, makes its source not ready, and the error names the reference, never the value.

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
