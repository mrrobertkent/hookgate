# hookgate-relay

`hookgate-relay` is the consumer half of hookgate's JetStream output. It reads webhook events from a durable JetStream pull consumer, resolves claim-checked bodies, and hands each event to your application's handler: a command it runs, or an HTTP endpoint it posts to. The handler's answer decides whether the event is acknowledged, retried with backoff or written to a dead-letter stream. Nothing is dropped silently.

What a handler receives and how it answers is part of the [contract](contract.md): command inputs and exit codes, HTTP requests and status codes, dead-letter records and the replay marker.

Requires nats-server 2.10.4 or later (multiple consumer filter subjects, and `Term` with a reason). Command handlers are supported on Linux, where the relay kills a handler's remaining process group before reaping the handler, so the group ID cannot have been reused; on other systems (macOS builds, for development) the group is killed just after reaping, and a reused ID could in rare cases hit another handler.

## Install

- **Container:** `ghcr.io/mrrobertkent/hookgate-relay:<version>` (linux/amd64, linux/arm64): distroless static, non-root, binary at `/usr/local/bin/hookgate-relay`, admin port `9091`. It is signed and attested like the gateway image ([verify a release](../README.md#verify-a-release)).
- **Binary:** `hookgate-relay_<version>_<os>_<arch>.tar.gz` from [Releases](https://github.com/mrrobertkent/hookgate/releases).
- **Go:** `go install github.com/mrrobertkent/hookgate/cmd/hookgate-relay@latest`

### Adding a command handler

The relay binary is statically linked, so it runs in any image. Two ways to ship it with your handler:

```dockerfile
# Static handler (Go, Rust with musl, ...): extend the relay image.
FROM ghcr.io/mrrobertkent/hookgate-relay:<version>@sha256:<digest>
COPY --chmod=0555 my-handler /usr/local/bin/my-handler
```

```dockerfile
# Handler that needs glibc or other libraries: copy the relay into a base that has them.
FROM gcr.io/distroless/cc-debian12:nonroot@sha256:<digest>
COPY --from=ghcr.io/mrrobertkent/hookgate-relay:<version>@sha256:<digest> /usr/local/bin/hookgate-relay /usr/local/bin/hookgate-relay
COPY --chmod=0555 my-handler /usr/local/bin/my-handler
USER nonroot:nonroot
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 CMD ["hookgate-relay", "healthcheck"]
ENTRYPOINT ["hookgate-relay"]
CMD ["serve"]
```

The relay runs the handler directly, without a shell; distroless images have none.

## Commands

| Command | Does |
|---|---|
| `hookgate-relay [serve] [-config FILE]` | Runs the relay (default). |
| `hookgate-relay validate [-config FILE]` | Checks the configuration, resolves every secret and checks that the command handler is an executable file. Exit 0 on success. |
| `hookgate-relay healthcheck [-url URL]` | Exit 0 when the admin `/healthz` answers 200; for container probes. Default URL `http://127.0.0.1:9091/healthz`. |
| `hookgate-relay replay -seq N [-seq M ...] [-claim-reference] [-config FILE]` | Republishes dead-letter records ([Replay](#replay)). |
| `hookgate-relay version` | Prints the version. |

The configuration file defaults to `$HOOKGATE_RELAY_CONFIG`, else `/etc/hookgate/relay.yaml`. Unknown keys are errors.

## Configuration

```yaml
admin_listen: ":9091"
connection:
  nats:
    urls: ["nats://nats-a:4222", "nats://nats-b:4222"]
    nkey_seed: {file: /run/secrets/relay_nkey}
stream: WEBHOOK_EVENTS
consumer: app
filter_subjects: [webhooks.events.resend, webhooks.replay.app]
replay_subject: "webhooks.replay.{consumer}"
pass_headers: [content-type, webhook-*, svix-*]
concurrency: 1
max_attempts: 10
backoff: [10s, 1m, 5m, 15m, 1h]
claim_check:
  object_store: {bucket: WEBHOOK_CLAIMS}
dead_letter:
  subject: "webhooks.dead.{consumer}"
  stream: WEBHOOK_DEAD
drain_timeout: 25s
shutdown_nak_delay: 5s
handler:
  command:
    path: /usr/local/bin/my-handler
    args: [ingest]
    env_allow: [TZ]
    set_env: {DATABASE_TOKEN: {file: /run/secrets/db_token}}
    timeout: 60s
```

A complete example with an HTTP handler: [examples/relay.yaml](../examples/relay.yaml). A runnable cluster with the gateway, the relay, a dead letter and a replay: [examples/jetstream](../examples/jetstream/).

| Key | Default | Meaning |
|---|---|---|
| `admin_listen` | `:9091` | Address of `/healthz`, `/readyz` and `/metrics`. |
| `connection.nats` | required | The NATS connection, with the same keys as a gateway connection ([configuration](configuration.md)): `urls`, exactly one of `nkey_seed`, `creds`, `token`, `none`, optional `tls`, `inbox_prefix` (default `_INBOX.relay-<consumer>`), `connect_timeout` (default `5s`). |
| `stream` | required | The event stream. |
| `consumer` | required | The durable pull consumer, created by your deployment ([Consumer](#consumer)). Stream and consumer names (`stream`, `consumer`, `dead_letter.stream`) are 1–255 characters of `A-Z a-z 0-9 _ -`, so the `:`-separated dead-letter and replay message IDs are unambiguous. |
| `filter_subjects` | required | Must equal the consumer's filter subjects; the relay is not ready otherwise. Must list the rendered `replay_subject` literally. Every other filter must not be able to match a subject with the reserved token `replay` after its first token: no literal `replay` token there, and no `*` or `>` after the first token (`>` not at all). List event subjects literally (`webhooks.events.resend`); `webhooks.events.*`, `webhooks.>` and `webhooks.replay.other` are rejected. Must not match the rendered dead-letter subject. |
| `replay_subject` | `webhooks.replay.{consumer}` | Where replays for a consumer are published: `<prefix>.replay.{consumer}`, the only placeholder, with `replay` nowhere in the prefix. Because every relay's replay subject has this shape and no other filter can match it, a replay reaches only the consumer named in its subject, whatever prefixes different relays use. |
| `pass_headers` | none | Application headers given to the handler, matched without regard to case; a trailing `*` matches a prefix. Reserved names (`Hookgate-*`, `Nats-*`, `Status`, `Description`) are rejected: the handler always receives the contract headers. |
| `concurrency` | `1` | Events processed at once by this relay. |
| `max_attempts` | `10` | Delivery attempts before a retryable failure is dead-lettered as `max_attempts` ([Retries](#retries-and-dead-letters)). |
| `backoff` | `[10s, 1m, 5m, 15m, 1h]` | Delay before redelivery after attempt *n*: the *n*-th value, the last one repeating. |
| `claim_check.object_store.bucket` | none | Bucket claim-checked bodies are fetched from. The bucket a message names is ignored. Without it, a claim-checked event is retried and then dead-lettered as `claim_missing`; set it whenever the publisher uses claim checks. Unless the bucket's objects never expire, its TTL must be at least the event stream's `max_age` plus 25 hours (the 24-hour reuse window and an hour's margin), and the stream must have a `max_age`; the relay is not ready otherwise. |
| `dead_letter.subject` | `webhooks.dead.{consumer}` | Subject of dead-letter records. `{consumer}` is the only placeholder. |
| `dead_letter.stream` | required | Stream that captures `dead_letter.subject`, with `limits` retention, `discard: new`, no per-subject message limit, acknowledgements on (no `no_ack`), and a `max_msg_size` no smaller than the event stream's (or none); publishes expect it. Must differ from `stream`. |
| `drain_timeout` | `25s` | On SIGTERM, how long in-flight handlers may finish ([Shutdown](#shutdown)). |
| `shutdown_nak_delay` | `5s` | Redelivery delay of events cancelled by shutdown. |
| `handler` | required | Exactly one of `command`, `http`. |
| `handler.command.path` | required | Absolute path of the program. It runs without a shell; arguments are passed as given. |
| `handler.command.args` | none | Fixed arguments. Event data never appears in arguments. |
| `handler.command.env_allow` | none | Variables copied from the relay's environment when set. Nothing else is inherited, not even `PATH`. Names starting with `HOOKGATE_` are reserved. |
| `handler.command.set_env` | none | Variables set from secret references (`{env: NAME}` or `{file: PATH}`, optionally `optional: true`), resolved once at start. |
| `handler.command.timeout` | `60s` | Limit for one run; the handler's whole process group is then killed and the event retried. |
| `handler.http.url` | required | Absolute `http` or `https` URL, without credentials. Environment proxies are not used. |
| `handler.http.timeout` | `30s` | Limit for one request, including reading the response. |
| `handler.http.set_headers` | none | Headers added to every request, each a secret reference. A configured header replaces a passed header of the same name in any letter case; a configured `Host` sets the request's host. Reserved names and names differing only in case are rejected. |

A secret that cannot be resolved stops the relay at start; `validate` reports it.

## Consumer

The deployment creates the consumer; the relay never creates, changes or deletes it. Requirements, checked at start, every 30 seconds and after every reconnect:

- durable pull consumer (no deliver subject, no `inactive_threshold`), `ack_policy: explicit`;
- `max_deliver: -1`: the relay owns the terminal decision. A finite server limit removes a message from the consumer with only an advisory, so a failing dead-letter publish or repeated crashes would strand events;
- no `backoff`: retry delays come only from the relay. With `backoff` set the server forces `ack_wait` to its first value and shifts every delayed redelivery;
- filter subjects equal to `filter_subjects`, one of them matching the consumer's replay subject;
- not paused on the server;
- `headers_only` off: the relay needs whole messages;
- `ack_wait` of at least 1s;
- pull request limits that admit the relay's requests: `max_batch` 0 or at least `concurrency`, `max_expires` 0 or at least 5s (the relay's pull requests expire after up to 5 seconds and set no byte limit, so `max_bytes` does not matter).

The stream and the claim bucket must be readable, the event and dead-letter streams must store subjects as published (no `subject_transform`, no `republish`: a transform could turn one consumer's replay subject into another's), and the bucket's TTL long enough (see `claim_check`). The dead-letter stream must be readable, capture the dead-letter subject and use `limits` retention: with `interest` or `workqueue` retention the server acknowledges a publish that no consumer is interested in without storing it, and the event would be lost. It must not set `no_ack`: the relay terminates an event only after the dead-letter stream acknowledges its record. It must use `discard: new` and no per-subject message limit, so that a later record can never evict one whose message ID still deduplicates a retried publish. While any check fails, `/readyz` answers `503` with the reason, the relay fetches nothing and the error is logged; events wait in the stream.

`ack_wait` is yours to choose: the relay sends an in-progress acknowledgement every third of it from receipt until the event is settled, so handlers may run longer than `ack_wait`. A short `ack_wait` (30–60 s) makes a crashed relay's events come back sooner.

Example (NATS CLI):

```sh
nats consumer add WEBHOOK_EVENTS app --defaults --pull --ack explicit --wait 60s --max-deliver=-1 \
  --filter webhooks.events.resend --filter webhooks.replay.app --deliver all --max-pending 100
```

## Delivery

Each fetched event has one owner goroutine that alone settles it:

1. **Contract.** A message without `Hookgate-Contract: 1`, `Hookgate-Source`, `Hookgate-Body-Sha256` and `Hookgate-Body-Size`, or with malformed values, is dead-lettered as `invalid_message`.
2. **Body.** An inline body must match the size and SHA-256 headers. A claim-checked body is fetched from the configured bucket by `Hookgate-Claim-Key` and must match too. A mismatch is dead-lettered as `checksum_mismatch` and never reaches the handler. A missing object or a failed fetch is retried; a missing object on the last attempt is dead-lettered as `claim_missing`.
   The fetch has its own deadline (30 seconds or `drain_timeout`, whichever is shorter); at shutdown the relay stops waiting for it at once and naks the event.
3. **Handler.** The command or HTTP endpoint gets the full body and the contract and passed headers ([contract](contract.md#relay-command-handlers)). Its answer is one of: success → acknowledged (`DoubleAck`, confirmed by the server); retryable → naked with the backoff delay; rejected → dead-lettered as `handler_rejected`; configuration fault → [paused](#configuration-faults).

A fetched claim object that does not match the event's size and SHA-256 headers is not trusted: its `checksum_mismatch` record holds no body, only the claim reference.

Delivery is at least once: an event can reach the handler again after a crash, a lost acknowledgement or a replay. Handlers deduplicate as the [contract](contract.md#relay-replays-and-deduplication) describes.

### Retries and dead letters

A retryable failure is naked with `backoff[n-1]` after attempt *n* (JetStream's `NakWithDelay`; a plain nak would redeliver at once). When the failed attempt is attempt `max_attempts` or later, the event is dead-lettered as `max_attempts` instead.

A dead letter is published to the dead-letter subject, expecting the dead-letter stream, with message ID `dead:<stream>:<stream created, unix nanoseconds>:<consumer>:<sequence>` (a stream recreated under the same name gets new IDs), so a publish retried within the dead-letter stream's duplicate window is stored once. Only after the stream acknowledges it is the original terminated. If the publish fails, the event is naked with its backoff delay, never terminated, and `hookgate_relay_dead_letter_publish_failures_total` counts it. Record layout: [contract](contract.md#relay-dead-letter-records).

Dead-letter records carry whole bodies, also of claim-checked events. The server's `max_payload` and the dead-letter stream's `max_msg_size` (if set) must exceed the largest `max_body_bytes` of any source plus the record's headers (allow 16 KiB); the readiness check can only compare the dead-letter stream's limit with the event stream's. A record refused for its size is retried like any failed publish, logged as too large and counted in `hookgate_relay_dead_letter_too_large_total` as well. If the event's own headers, together with the dead-letter metadata, would exceed the broker's 64 KiB header limit, the record leaves them out and carries `Hookgate-Dead-Headers-Dropped: true`; the contract headers and the body stay.

Every delivery runs the handler. When a dead-letter publish fails, the redelivered event is processed again from the start: if the handler now succeeds the event is acknowledged, otherwise it is dead-lettered again (a retryable failure at or past `max_attempts` always is). The cycle repeats with the backoff delay for as long as the publish fails, each failure counted. Handlers must therefore be idempotent, as the [contract](contract.md#relay-replays-and-deduplication) requires anyway.

A relay that stops after the dead-letter publish is acknowledged but before the original is terminated leaves the record in place, and the event is delivered again. If the handler then succeeds, the record describes an event that was processed in the end; replaying it is harmless because handlers process replays idempotently. If it fails again, the dead letter carries the same message ID and is stored once within the duplicate window.

Once the dead-letter stream has acknowledged a record, the original is terminated, even if the relay paused meanwhile; a pause only stops new work (fetching, starting a handler, starting a dead-letter publish). Acknowledgements are confirmed by the server (`DoubleAck`). Naks and terminations are sent without confirmation, as JetStream defines them; `hookgate_relay_settle_failures_total` counts only those that could not be sent. An unsettled event is redelivered after `ack_wait`.

**Sizing `max_attempts`.** Every redelivery counts as an attempt, including the redeliveries after a shutdown cancels a handler, after a pause and after a relay crash: JetStream cannot return a message without counting the delivery. An event delivered after its attempts are used up still runs the handler once more; only a failure then dead-letters it. Size `max_attempts` for the handler failures you want to ride out plus the restarts that can happen meanwhile (rolling updates, node drains). With the default backoff, 10 attempts span about 5 hours 20 minutes.

### Configuration faults

Some failures mean the handler cannot work for any event: the program cannot be started (missing, not executable, not a valid executable, a symlink loop: any start error except a transient resource shortage), it exits `126` or `127`, or the HTTP handler answers `101` or a redirect (`3xx`; redirects are never followed). Dead-lettering every event would be wrong, so the relay **pauses**: the event is naked with its backoff delay, `/readyz` answers `503`, `hookgate_relay_paused` is `1`, the pull request in progress ends at once, no further pulls are made, and every event not yet in a handler is naked without running one (events already delivered, events still being prepared, and events whose dead-letter publish has not started: a paused relay starts no dead-letter publish), and the fault is logged once at error level. Events whose handlers were already running finish normally. The pause lasts until the relay is restarted; fix the handler or its configuration, then restart.

## Shutdown

On SIGTERM or SIGINT the relay stops fetching and `/readyz` answers `503`. In-flight events get `drain_timeout` to finish and are settled normally. When it expires, the remaining handlers are cancelled (a command's whole process group is killed, an HTTP request is aborted), and each of their events is naked with `shutdown_nak_delay`, so another relay, or this one after restart, picks it up. Then the NATS connection is drained.

Set the orchestrator's stop grace period above `drain_timeout` plus 15 seconds (killing, naking and draining the connection). The defaults need a 40-second grace; Docker and Swarm default to 10 seconds (`stop_grace_period`).

## Scaling and ordering

- **Several relays on one consumer** share its events: each event goes to one relay at a time. Use this for throughput and availability; each relay processes up to `concurrency` events.
- **A single relay** is fine when the handler is tied to one host: while it is down, events wait in the stream (up to the stream's `max_age`), and the relay resumes where the consumer left off.
- **Ordering is not guaranteed**, even with one relay and `concurrency: 1`: a retried event is redelivered after its backoff delay, behind events that arrived later. Use timestamps in the payload (or `Hookgate-Received-At`) where order matters.
- Several **applications** each use their own consumer (and replay subject) on the same stream; each gets every event.

## Replay

A dead-lettered event is recovered by replaying its record after fixing the cause:

1. Find the record: `nats stream view WEBHOOK_DEAD` or `nats stream get WEBHOOK_DEAD <seq>`; the headers give the reason, detail, attempts and original subject, sequence and consumer.
2. Replay it: `hookgate-relay replay -seq <seq>` (repeat `-seq` for several), with a configuration whose `connection` has the [replay permissions](#permissions) and whose `dead_letter.stream` and `replay_subject` match the relay's.

The record is republished to the replay subject of the consumer that failed it (`Hookgate-Original-Consumer`), so only that application sees it again; other consumers of the stream are not affected. The body is the record's full body; the contract and application headers are copied from the record and `Hookgate-Replay: <dead-letter stream>:<seq>` is added. A body too large for the event stream (its `max_msg_size` or the server's `max_payload`) is written to the claim bucket as `<source>/replay/<dead-letter stream>-<seq>/<sha256>/<window>`, where `<window>` is the start of the current 24-hour window in Unix seconds, and replaced by a claim reference; `claim_check` must then be configured and pass the same TTL rule as the relay. Replaying the record again within the same window reuses that object, so an object is never reused once 24 hours old. Its message ID is `replay:<dead-letter stream>:<seq>`, so replaying the same record twice within the event stream's duplicate window stores it once (`(already replayed)` is printed).

A record dead-lettered as `claim_missing` or `checksum_mismatch` before its body was resolved holds no body, only the claim-check reference. `replay` refuses it unless `-claim-reference` is given, which republishes the reference: the consumer then fetches the object again, which helps only if the object exists and is intact (for example after `claim_check` was missing from the relay's configuration).

**Why not replay from the sender?** A sender's dashboard replay (for example Resend's) resends the same event ID. Within the event stream's duplicate window the broker drops it as a duplicate of the accepted event; after the window, the handler's deduplication on (source, event ID) drops it. Neither reaches the failed handler again. Replay from the dead-letter stream is the recovery path.

## Permissions

Least-privilege grants, shown for stream `WEBHOOK_EVENTS`, consumer `app`, dead-letter stream `WEBHOOK_DEAD`, bucket `WEBHOOK_CLAIMS` and the default subjects and inbox prefix.

With them the relay can read the stream, consumer, dead-letter stream and bucket descriptions; pull from and acknowledge its own consumer; publish to its own dead-letter subject; read claim objects; and create and delete the temporary consumers the stock Object Store reader uses on the bucket's stream (`OBJ_WEBHOOK_CLAIMS`). The delete grant is a wildcard, so it also allows deleting other readers' temporary consumers on that bucket stream, which interrupts their reads; NATS subjects cannot narrow it further. The relay cannot publish events, use other consumers, purge streams or bucket, or delete messages or other streams' consumers.

Relay:

```
publish:
  $JS.API.STREAM.INFO.WEBHOOK_EVENTS
  $JS.API.CONSUMER.INFO.WEBHOOK_EVENTS.app
  $JS.API.CONSUMER.MSG.NEXT.WEBHOOK_EVENTS.app
  $JS.ACK.WEBHOOK_EVENTS.app.>
  $JS.API.STREAM.INFO.WEBHOOK_DEAD
  webhooks.dead.app
  $JS.API.STREAM.INFO.OBJ_WEBHOOK_CLAIMS                       # claim checks only, from here down
  $JS.API.DIRECT.GET.OBJ_WEBHOOK_CLAIMS.$O.WEBHOOK_CLAIMS.M.>
  $JS.API.CONSUMER.CREATE.OBJ_WEBHOOK_CLAIMS.*.$O.WEBHOOK_CLAIMS.C.*
  $JS.API.CONSUMER.DELETE.OBJ_WEBHOOK_CLAIMS.*
  $JS.FC.OBJ_WEBHOOK_CLAIMS.>
subscribe:
  _INBOX.relay-app.>
  $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.WEBHOOK_EVENTS.app
```

Replay operator (its `inbox_prefix` here is `_INBOX.relay-replay`):

```
publish:
  $JS.API.STREAM.INFO.WEBHOOK_DEAD
  $JS.API.DIRECT.GET.WEBHOOK_DEAD          # dead-letter stream with allow_direct
  $JS.API.STREAM.MSG.GET.WEBHOOK_DEAD      # dead-letter stream without allow_direct
  $JS.API.STREAM.INFO.WEBHOOK_EVENTS
  webhooks.replay.*
  $JS.API.STREAM.INFO.OBJ_WEBHOOK_CLAIMS                       # bodies too large to replay inline, from here down
  $JS.API.DIRECT.GET.OBJ_WEBHOOK_CLAIMS
  $JS.API.DIRECT.GET.OBJ_WEBHOOK_CLAIMS.$O.WEBHOOK_CLAIMS.M.>
  $O.WEBHOOK_CLAIMS.C.>
  $O.WEBHOOK_CLAIMS.M.>
subscribe:
  _INBOX.relay-replay.>
```

The replay operator can read dead-letter records, publish to any consumer's replay subject, and write objects to the claim bucket. `replay` itself only creates objects, but the metadata grant `$O.WEBHOOK_CLAIMS.M.>` covers existing keys too: the identity could publish a deletion marker for any object, after which an event referring to it cannot resolve its body. NATS subjects cannot restrict it to new keys, so keep the replay identity with operators, as you would the gateway's.

The relay's object-store grants are what the stock Object Store reader uses: it binds the bucket, reads object metadata with a direct get on the metadata subject and streams chunks through a short-lived ordered consumer that it deletes afterwards.

## Operations

- **Health:** `GET /healthz` (process up; the image's `HEALTHCHECK`), `GET /readyz` (`200` only while connected, every [consumer check](#consumer) passes, not paused and not draining; otherwise `503` with the reason).
- **Logs:** JSON on stderr, one line per settled delivery: `seq`, `subject`, `attempt`, `source`, `event_id`, `outcome`, and `reason`/`detail` when set. A failing command's last 4 KiB of stderr is logged with it, and only there: never in dead-letter records, `/readyz` or metrics. Handlers must not print secrets or body data to stderr. The relay never logs bodies.
- **Metrics** (`GET /metrics`):

| Metric | Type | Labels |
|---|---|---|
| `hookgate_relay_deliveries_total` | counter | `outcome` = `acked`, `retried`, `dead_lettered`, `dead_letter_failed`, `paused`, `shutdown` |
| `hookgate_relay_handler_duration_seconds` | histogram | `result` = `ack`, `retry`, `reject`, `pause`, `cancelled` |
| `hookgate_relay_dead_letters_total` | counter | `reason` |
| `hookgate_relay_dead_letter_publish_failures_total` | counter | — |
| `hookgate_relay_dead_letter_too_large_total` | counter | — (publishes refused for size; also in the failures above) |
| `hookgate_relay_settle_failures_total` | counter | — (acks not confirmed, naks or terms that could not be sent; the event comes back after `ack_wait`) |
| `hookgate_relay_max_deliveries_advisories_total` | counter | — (server max-deliveries advisories for this consumer; always 0 with `max_deliver: -1`) |
| `hookgate_relay_consumer_pending` | gauge | — (events not yet delivered; refreshed every 30 s) |
| `hookgate_relay_consumer_ack_pending` | gauge | — |
| `hookgate_relay_consumer_redelivered` | gauge | — |
| `hookgate_relay_connected` | gauge | — |
| `hookgate_relay_ready` | gauge | — |
| `hookgate_relay_paused` | gauge | — |
| `hookgate_relay_in_flight` | gauge | — |

Alert on `increase(hookgate_relay_dead_letters_total[1h]) > 0`, on `hookgate_relay_dead_letter_publish_failures_total` or `hookgate_relay_max_deliveries_advisories_total` increasing, on `hookgate_relay_paused == 1`, and on `hookgate_relay_ready == 0` for longer than a few minutes. Pending and ack-pending counts are per consumer, so every relay of a consumer reports the same values.
