# Configuration reference

hookgate reads one YAML file: `-config FILE`, else `$HOOKGATE_CONFIG`, else `/etc/hookgate/hookgate.yaml`. Unknown keys are errors, so a typo stops startup instead of silently changing behaviour. Run `hookgate validate -config FILE` in CI: it checks the file and that every referenced secret is set.

## Server

| Key | Default | Meaning |
|---|---|---|
| `listen` | `:8080` | Webhook listener. |
| `admin_listen` | `:9090` | `/healthz`, `/readyz`, `/metrics`. Keep it off the public network. |
| `max_concurrent` | `64` | Requests verified or delivered at once; extra requests get `503`. |
| `read_header_timeout` | `5s` | Time allowed for request headers. |
| `read_timeout` | `15s` | Time allowed for the whole request, body included. |
| `write_timeout` | `60s` | Time allowed to write the response; must exceed the longest output `timeout`. |
| `idle_timeout` | `120s` | Keep-alive idle time. Keep it above your proxy's idle timeout so the proxy never reuses a closed connection. |
| `drain_delay` | `0s` | After SIGTERM, keep accepting requests this long (with `/readyz` answering `503`) so load balancers stop routing here first. |
| `shutdown_timeout` | `30s` | Time in-flight requests get to finish after the drain; NATS connections are then drained within what is left of it. |

Every timeout must be greater than zero; `drain_delay` may be `0s` but not negative.

## Sources

Each entry of `sources` is one sender.

| Key | Default | Meaning |
|---|---|---|
| `name` | required | Label used in logs and metrics. Letters, digits, `-` and `_`, starting with a letter or digit, at most 63 characters (`[A-Za-z0-9][A-Za-z0-9_-]{0,62}`); unique. |
| `hosts` | none | Exact host names or IP addresses this source answers, for example `[hooks.example.com]`. Omitted means any host; an empty or null list, or a null entry, is a configuration error. See [Routing](#routing). |
| `path` | required | Exact request path, for example `/github`. |
| `max_body_bytes` | `1048576` | Larger bodies get `413` and are never delivered. The whole body is held in memory while it is verified and delivered, so memory peaks at about `max_concurrent` × the largest `max_body_bytes`. |
| `allow_query` | `false` | Requests with a query string get `400` unless set: query parameters are not covered by signatures. |
| `reject_status` | `401` | Status returned when a check fails. Must not be `2xx` for a JetStream output. |
| `checks` | required | One or more checks; **all** must pass. A source with no checks is a configuration error. |
| `output` | required | Where verified requests go: exactly one of [`http`](#outputhttp) or [`jetstream`](#outputjetstream). |

### Routing

A request goes to the source whose `path` equals the decoded request path exactly and that either lists the request's host in `hosts` or has no `hosts`. A source that lists the host beats the path's source without `hosts`. Any other request gets `404`.

- Each (host, path) pair belongs to one source: two sources without `hosts` on one path, or two sources listing the same host on one path, stop startup. A source without `hosts` and sources with `hosts` may share a path.
- The host is the request's effective authority: the authority of an absolute-form request target (`POST http://host/path HTTP/1.1`) when present, else the `Host` header, or `:authority` in HTTP/2. `X-Forwarded-Host` is never read, so a proxy in front must pass the client's `Host` through (Traefik: `passHostHeader`, true by default).
- The host is compared case-insensitively, without its port and without one trailing dot; IP addresses are compared in canonical form, so `[::1]:8080` matches `::1`. A malformed authority (non-ASCII bytes, a port that is not up to five digits, unbalanced brackets, brackets around anything but an IPv6 address, an unbracketed IPv6 address) matches no listed host: it reaches the path's source without `hosts` if there is one, else gets `404`.
- In `hosts`, write DNS names in ASCII as letters, digits and hyphens (internationalised names in punycode, `xn--…`), and IPv6 addresses with or without brackets, quoted in YAML: `hosts: ["::1"]`. Names are lowercased and one trailing dot is dropped. Wildcards, ports and IPv6 zones are configuration errors.

The precedence matches nginx, where a `server_name` that equals the host wins over the port's default server ([nginx: server names](https://nginx.org/en/docs/http/server_names.html)). Traefik instead picks the router with the highest priority, which by default is the length of its rule ([Traefik: rules and priority](https://doc.traefik.io/traefik/reference/routing-configuration/http/routing/rules-and-priority/#priority-calculation)), so a longer rule elsewhere can take hookgate's requests:

- Give each exact host and path its own router to hookgate rather than a catch-all, with an explicit `priority` when another router could match the same host and path.
- For a source without `hosts`, use a path-only router (``Path(`/events`)``) with an explicit `priority` below every other service's host routers, so it receives only requests no other router claims.

hookgate repeats the host match, so a request the proxy misroutes never reaches a source that lists other hosts.

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

### `standard_webhooks` check

Verifies a [Standard Webhooks](https://github.com/standard-webhooks/standard-webhooks/blob/main/spec/standard-webhooks.md) signature, as sent by Svix and the services built on it.

```yaml
- standard_webhooks:
    headers: svix         # standard (default) | svix
    tolerance: 5m         # default 5m
    secrets:
      - env: WEBHOOK_SECRET
```

| Key | Default | Meaning |
|---|---|---|
| `headers` | `standard` | Header family: `standard` reads `webhook-id`, `webhook-timestamp`, `webhook-signature`; `svix` reads `svix-id`, `svix-timestamp`, `svix-signature`. Only the configured family is read. |
| `tolerance` | `5m` | Largest accepted distance between `*-timestamp` and the gateway's clock, in either direction; must be > 0 and at most `1h`. |
| `secrets` | required | Signing secrets as the sender shows them: standard base64, with or without the `whsec_` prefix. |

The request passes when:

- each of the three headers appears exactly once;
- the timestamp is Unix seconds in canonical decimal form (digits only, no sign, no leading zero) and within `tolerance`;
- any `v1,<base64>` entry of the space-separated signature header equals the HMAC-SHA256, under any secret, of `<id>.<timestamp>.<body>` (the id and timestamp exactly as received, the raw body). Entries of other versions are ignored;
- the id is 1 to 200 bytes of `A-Z a-z 0-9 _ ~ = + -` (no `.`, which separates the signed parts).

The timestamp check limits replays to the tolerance window; it keeps no state, so a request replayed inside the window passes again. A source can have at most one `standard_webhooks` check.

A secret that is not valid standard base64 (padding included) or decodes to fewer than 24 bytes makes its source not ready, like a missing secret.

Give every source its own secret, as the specification requires: a secret shared by two sources lets a request signed for one pass on the other.

### Secrets

Every secret is a reference, never a literal:

```yaml
secrets:
  - env: SIGNING_SECRET            # environment variable
  - file: /run/secrets/signing     # file; one trailing newline is ignored
    optional: true                 # may be unset or missing
```

Token and HMAC secrets must be at least 16 bytes (`standard_webhooks` secrets: 24 decoded bytes); use a random value such as `openssl rand -hex 32`. A shorter secret, optional or not, makes its source not ready, and the error names the reference, never the value.

A non-optional secret that is unset, empty or unreadable makes its source **not ready**: the source answers `503` and delivers nothing, the error is logged, and `hookgate_source_ready{source=...}` is `0`. Other sources keep serving. A check whose secrets are all optional and all unset is also not ready, so a source can never run without a secret.

**Rotation:** add the new secret as a second, optional reference, deploy, switch the sender to the new secret, then remove the old reference.

## Connections

`connections` names the broker connections outputs use. Each is opened once, when a source uses it, and shared by every source that names it.

```yaml
connections:
  events:
    nats:
      urls: ["nats://nats-a:4222", "nats://nats-b:4222", "nats://nats-c:4222"]
      nkey_seed: {file: /run/secrets/hookgate_nkey}
      tls:                                   # optional
        ca: {file: /run/secrets/nats_ca}
```

| Key | Default | Meaning |
|---|---|---|
| name (map key) | required | Referenced by `output.jetstream.connection`. Same rule as source names. |
| `nats.urls` | required | Server URLs: `nats://`, `tls://`, `ws://` or `wss://`, without credentials in the URL. |
| `nats.nkey_seed` | — | Secret reference to a user NKey seed. The public key is fixed when hookgate starts, so switching to a new seed needs a restart. |
| `nats.creds` | — | Secret reference to a creds file (user JWT and seed), for servers in operator mode. A file is read again at every reconnect, so a replaced file takes effect on the next reconnect. |
| `nats.token` | — | Secret reference to a token. |
| `nats.none` | `false` | `true` connects without credentials. Exactly one of `nkey_seed`, `creds`, `token` and `none: true` is required. |
| `nats.tls.ca` | system roots | Secret reference to the PEM CA bundle that signs the servers' certificates. |
| `nats.tls.cert`, `nats.tls.key` | — | Secret references to a PEM client certificate and key, set together. TLS 1.2 is the minimum. |
| `nats.inbox_prefix` | `_INBOX.<name>` | Prefix of the reply subjects; the identity needs subscribe permission on `<prefix>.>` only. Dot-separated tokens without whitespace or wildcards. |
| `nats.connect_timeout` | `5s` | Limit for one connection attempt. |

Authentication secrets may not be `optional`. An unset or unusable one makes every source using the connection not ready, like a missing check secret. An unreachable server is not an error: the client retries forever (with jitter), and the sources answer `503` until it connects. Publishing never waits for a reconnect behind the scenes; it fails at once and the output retries within its `timeout`.

## `output.http`

| Key | Default | Meaning |
|---|---|---|
| `urls` | required | One or more upstream URLs, used exactly as written (path and query included). |
| `resolve_all` | `false` | Resolve each URL's host name to all of its addresses and treat each address as a target, in random order. Use it for a service name that fronts several instances. |
| `pass_headers` | none | Request headers copied upstream. Names are case-insensitive; a trailing `*` matches a prefix (`X-GitHub-*`). Everything else is dropped, and so are the headers a check reads its token or signature from, even when a pattern matches them. |
| `set_headers` | none | Headers added upstream, each a secret reference (for example the upstream's own credentials). |
| `attempt_timeout` | `5s` | Limit for one attempt, including connecting. |
| `timeout` | `20s` | Limit for all attempts together. Keep it below the sender's own timeout. |
| `max_attempts` | `3` | Attempts across targets. |

The upstream receives the verified body byte for byte, with `POST`. A transport error or `5xx` moves on to the next target; any other status is final and is returned to the sender with the upstream's body (first 64 KiB). If every attempt fails, the sender gets the last `5xx`, or `502` when no upstream answered.

Retries mean an upstream can receive the same request more than once (for example when it stored the request but the response was lost). Make the upstream idempotent or deduplicate downstream.

## `output.jetstream`

Publishes each verified request to a NATS JetStream stream and answers `200` only once the stream has acknowledged it. Bodies larger than `inline_max_bytes` go to a JetStream Object Store bucket first and the message carries a reference (a claim check). What consumers receive, and the status the sender gets for each outcome, is the [contract](contract.md).

```yaml
output:
  jetstream:
    connection: events
    stream: WEBHOOK_EVENTS
    pass_headers: [Content-Type, Webhook-*, Svix-*]
    claim_check:
      object_store: {bucket: WEBHOOK_CLAIMS}
```

| Key | Default | Meaning |
|---|---|---|
| `connection` | required | Name of a `connections` entry with `nats`. |
| `subject` | `webhooks.events.{source}` | Subject template; `{source}` is the only placeholder. Tokens may not be empty or contain whitespace, `*` or `>`. |
| `stream` | required | Stream the subject must be stored in. Every publish expects it, so a subject captured by another stream, or by none, fails instead of landing elsewhere. hookgate never creates or changes streams. |
| `pass_headers` | none | Request headers published, under lowercase names (`webhook-id`, `content-type`). Same matching as `output.http.pass_headers`, credential headers excluded. Patterns that could match a reserved name (`Hookgate-*`, `Nats-*`, `Status`, `Description`) are configuration errors. |
| `set_headers` | none | Headers added to every message, names exactly as written, each a non-optional secret reference. Reserved names are configuration errors. |
| `message_id.header` | none | Request header that supplies the event ID when no check authenticates one. Requires `message_id.unauthenticated: true`. Must not be a header any check of the source reads (a token, a signature). |
| `message_id.unauthenticated` | `false` | Acknowledges that the header is not covered by a signature, so anyone who replays a captured body chooses its ID. |
| `claim_check.object_store.bucket` | — | Object Store bucket for bodies over `inline_max_bytes`. Required when `max_body_bytes` exceeds `inline_max_bytes`. |
| `claim_check.reuse_within` | `24h` | A stored body for the same event and content is reused when its upload is younger than this; an older one stays untouched and the body is stored again under a new key. |
| `inline_max_bytes` | `262144` | Largest body published inside the message. |
| `max_header_bytes` | `16384` | Largest encoded size of all published headers, hookgate's own included; a request whose headers would exceed it gets `400`. Between `1024` and `65535`, the largest header block JetStream stores. |
| `attempt_timeout` | `5s` | Limit for one publish attempt (or one claim-check request). |
| `timeout` | `10s` | Limit for the whole output stage: storing the body and every publish attempt. `read_timeout` + `timeout` must not exceed `shutdown_timeout`, so every admitted request can finish its publish during a shutdown. |

**Event ID.** A check that authenticates an event ID supplies it; otherwise `message_id.header`, when configured. An ID that is empty, repeated or not 1–200 bytes of `A-Z a-z 0-9 _ ~ = + -` gets `400`, whichever supplies it. A token or HMAC check may not read the header a check authenticates the ID from (`webhook-id`, `svix-id`), so a secret never becomes an event ID. With an ID, the message carries `Nats-Msg-Id: <source>:<event id>` and the stream drops repeats within its `duplicate_window`; without one, nothing is deduplicated.

**Claim after a reconnect.** When the client reconnects during a claim-checked delivery, the body is stored again (or the stored copy found again) before the next publish attempt, since the new server may not hold it.

**Retries.** A publish attempt that times out, finds no stream responding, finds the connection down, or gets a server-side (`5xx`) JetStream error is retried until `timeout`. A timed-out attempt may still have been stored, so a retry without an event ID can store the event twice: delivery is at least once.

**Inline or claim check.** A body is published inline when it is at most `inline_max_bytes` and, with its headers, fits the smaller of the server's `max_payload` and the stream's `max_msg_size`. Otherwise it is stored under `<source>/<event id>/<sha256>` (or `<source>/<sha256>` without an ID) in chunks of 128 KiB, each acknowledged, and only then is the object's metadata created, on condition that none exists. An existing object is reused when every chunk its metadata counts is still stored, its size and digest match, and its first chunk is no older than `reuse_within`. Otherwise the body goes to `<key>/<n>`, where `n` numbers the `reuse_within`-long time window, so retries in one window share it. hookgate never overwrites, purges or deletes. A body stored for a publish that then fails stays until the bucket TTL removes it.

**Checks.** On every connect and reconnect, every 5 s while one fails and every 30 s while all pass, the source checks its destination. While a check fails the source answers `503`, logs the reason and reports `hookgate_source_ready 0`:

- the stream exists, acknowledges publishes (`no_ack` false), uses `limits` retention (with `interest` or `workqueue` retention a stream can acknowledge an event without storing it) and `discard: new` (with `discard: old` a full stream evicts acknowledged events);
- `inline_max_bytes` + `max_header_bytes` fits the smaller of the server's `max_payload` and the stream's `max_msg_size`;
- with a claim check, the bucket's stream exists and:
  - acknowledges publishes, uses `limits` retention, `discard: new`, no `max_msgs_per_subject` limit (every chunk of an object shares one subject) and direct get (`allow_direct`, the default for buckets);
  - keeps bodies at least as durably as the stream keeps events: as many replicas or more, and file storage when the stream uses file storage;
  - has a `max_bytes`, when limited, of at least `max_body_bytes` + 1 MiB;
  - has a TTL, when set, of at least the stream's `max_age` + `reuse_within` + `timeout` + 1 h, so a body outlives every message that refers to it, a slow upload and a publish after reuse included (a stream without `max_age` needs a bucket without TTL);
  - fits a 128 KiB chunk within the server's `max_payload` and the bucket's `max_msg_size`.

A stream on memory storage or with one replica passes, with a warning in the log: an acknowledgement then does not survive the loss of a server. See [pairing a stream](contract.md#pairing-a-stream).

A disconnect invalidates the last check, and so does any reconnect, counted by the client: while the connection is down, and after a reconnect until the checks pass on the new connection, the source answers `503` and reports `hookgate_source_ready 0`. Readiness is checked again after the body is read, before delivery.

**Shutdown.** On SIGTERM, requests already admitted finish their publish; the NATS connections are drained only after the HTTP server has shut down, within what is left of `shutdown_timeout`.

**Permissions.** The identity needs only the permissions in [contract.md](contract.md#gateway-permissions).

## Metrics

| Metric | Type | Labels |
|---|---|---|
| `hookgate_requests_total` | counter | `source`, `outcome` = `forwarded`, `published`, `duplicate`, `rejected`, `upstream_error`, `output_error`, `too_large`, `bad_request`, `not_ready`, `busy` |
| `hookgate_upstream_attempts_total` | counter | `source`, `result` = `2xx`, `3xx`, `4xx`, `5xx`, `error` |
| `hookgate_publish_attempts_total` | counter | `source`, `result` = `ok`, `duplicate`, `timeout`, `unavailable`, `too_large`, `error` |
| `hookgate_publish_duration_seconds` | histogram | `source`; one observation per publish attempt |
| `hookgate_claim_check_puts_total` | counter | `source`, `result` = `ok`, `error` |
| `hookgate_nats_connected` | gauge | `connection` |
| `hookgate_source_ready` | gauge | `source` |
| `hookgate_in_flight_requests` | gauge | — |
| `hookgate_draining` | gauge | — |

## Logs

One JSON line per request on stderr: `source`, `outcome`, `status`, `reason` (for rejections and failed deliveries), `remote`, `forwarded_for`, `bytes`, `duration_ms`. Secrets, signatures and bodies are never logged.

## Relay

`hookgate-relay` has its own configuration file, documented with the relay: [relay.md](relay.md#configuration).
