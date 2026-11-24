# Event contract

This is what a consumer of hookgate's JetStream output can rely on: subjects, message headers, identifiers, claim checks, delivery guarantees, what the sender is told, and how [`hookgate-relay`](relay.md) hands events to application handlers. Two parts are versioned separately. The message format (subjects, headers, identifiers, claim checks, and the dead-letter and replay records the relay publishes) carries the `Hookgate-Contract` version: a breaking change to it gets a new version, and the gateway and every consumer move to it together. The relay's handler interface (command inputs and exit codes, HTTP status mapping) follows `hookgate-relay`'s release version: a breaking change to it is a new minor release before 1.0 and a new major release after, flagged in the release notes.

## Version

Every message carries `Hookgate-Contract: 1`. A consumer rejects a message whose version it does not know.

## Subjects

Events are published to `webhooks.events.<source>` (the `subject` template, `webhooks.events.{source}` by default). Source names match `[A-Za-z0-9][A-Za-z0-9_-]{0,62}` and so never contain `:`, `/` or `.`.

The relay publishes replays to `webhooks.replay.<consumer>` and dead-letter records to `webhooks.dead.<consumer>` (its `replay_subject` and `dead_letter.subject` defaults), each consumer's to its own subject. Streams carrying these subjects store them as published: no subject transform (of the stream, its sources or its mirror) and no republishing, which could hand one consumer's replay to another. The token `replay` is reserved: every replay subject is `<prefix>.replay.<consumer>`, with `replay` nowhere in the prefix, and no other subject a consumer filters can match a subject whose second-to-last token is `replay`.

## Messages

The message body is the verified request body, byte for byte, or empty when the body is claim-checked.

Header names are case-sensitive on NATS and are published exactly as listed:

| Header | Value |
|---|---|
| `Hookgate-Contract` | `1` |
| `Hookgate-Source` | Source name. |
| `Hookgate-Event-Id` | The event ID; absent when the event has none. |
| `Hookgate-Event-Id-Authenticated` | `true` when a signature check covered the ID, `false` when it came from an unsigned header the operator opted into; absent when there is no ID. |
| `Hookgate-Signed-At` | The signing time the sender's signature covers, RFC 3339 in UTC (`2026-01-02T03:04:05Z`); absent when no check authenticates a timestamp. |
| `Nats-Msg-Id` | `<source>:<event id>`; absent when there is no ID. |
| `Hookgate-Body-Sha256` | Lowercase hex SHA-256 of the full body, inline or claim-checked. |
| `Hookgate-Body-Size` | Size of the full body in bytes, decimal. |
| `Hookgate-Received-At` | When hookgate received the request, RFC 3339 in UTC with milliseconds (`2026-01-02T03:04:05.678Z`). |
| `Hookgate-Claim-Bucket`, `Hookgate-Claim-Key` | Only on claim-checked messages: where the body is stored. |
| Passed request headers | The request headers the source's `pass_headers` selects, under lowercase names (`content-type`, `webhook-id`, `svix-timestamp`), every value kept. |
| `set_headers` | The source's static headers, names exactly as configured. |
| `Hookgate-Replay` | Only on a replay of a dead-letter record: `<dead-letter stream>:<seq>` ([Relay replays](#relay-replays-and-deduplication)). |

Every message carries `Hookgate-Contract`, `Hookgate-Source`, `Hookgate-Body-Sha256` and `Hookgate-Body-Size`; a message without one of them is invalid. The stored message also carries `Nats-Expected-Stream`, which the publish sets; consumers ignore every `Nats-*` header except `Nats-Msg-Id`, and never copy them into a new publish.

No sender header and no configured header can carry a name starting with `Hookgate-` or `Nats-`, or the names `Status` and `Description`: configuration that could produce one is rejected, and request headers with such names are never published. The headers a check reads its token or signature from are never published either.

## Event IDs

An event ID is 1–200 bytes of `A-Z a-z 0-9 _ ~ = + -`. It comes from a check that authenticates it (the signed `webhook-id` of Standard Webhooks), or, only when the operator opts in, from an unsigned request header; `Hookgate-Event-Id-Authenticated` says which. An unsigned ID is whatever the request said: anyone who replays a captured body can choose it.

The message ID `<source>:<event id>` scopes the ID to its source, so two senders whose IDs collide are never merged. Consumers read the event ID from `Hookgate-Event-Id` and the source from `Hookgate-Source`, never by splitting the message ID.

Without an event ID the message has no `Nats-Msg-Id`, the broker deduplicates nothing, and consumers can tell repeats apart only by content (`Hookgate-Body-Sha256`).

## Trust

Only requests that passed every check of their source are published, so consumers trust the gateway's verification and do not verify signatures again. The sender's own ID and timestamp headers (for example `webhook-id` and `webhook-timestamp`) are published when `pass_headers` selects them, so applications can record them; the timestamp a signature covered is also in `Hookgate-Signed-At`.

## Claim checks

A body larger than the source's `inline_max_bytes`, or too large to fit the broker's limits with its headers, is stored in a JetStream Object Store bucket before the message is published. The message body is then empty and:

- `Hookgate-Claim-Bucket` names the bucket;
- `Hookgate-Claim-Key` names the object: `<source>/<event id>/<sha256>`, or `<source>/<sha256>` without an event ID, possibly followed by `/<unix seconds>` when an older copy already held that name.

Objects use the standard Object Store format, so any NATS client reads them (`ObjectStore.Get` in the Go client, `nats object get` on the command line). A consumer:

1. reads the object named by the two headers, from the bucket it is configured for (never one a message names otherwise);
2. checks that the bytes' SHA-256 equals `Hookgate-Body-Sha256`, as it does for inline bodies;
3. treats a missing object as an event it cannot process yet or any more: the bucket TTL removes objects, so the TTL must outlast every message that refers to them (see [Pairing a stream](#pairing-a-stream)).

hookgate stores each object completely before it becomes visible, never overwrites, purges or deletes one, and reuses an identical one stored for an earlier attempt of the same event. A body stored for a publish that then fails is an orphan that the bucket TTL removes.

## What the sender is told

The sender gets `200` only after the stream acknowledged the message, with the body:

```json
{"status":"accepted","duplicate":false}
```

`duplicate` is `true` when the stream already held a message with the same `Nats-Msg-Id`: the event is safe, and the sender gets `200` all the same.

| Outcome | Status | `hookgate_requests_total` outcome |
|---|---|---|
| Stored by the stream (or already stored) | `200`, body above | `published`, `duplicate` |
| Connection down, publish timed out or failed, stream full, wrong stream, claim-check store failed, output checks failing | `503`, body `unavailable` | `output_error`, `not_ready` |
| Message over the server's `max_payload` (prevented by the output's checks; see below) | `413` | `too_large` |
| Event ID empty, repeated or outside its character set (from a signature check or an unsigned header); headers over `max_header_bytes` | `400` | `bad_request` |
| Body over `max_body_bytes` | `413` | `too_large` |
| A check failed | `reject_status` (`401`) | `rejected` |

Broker errors are logged, never returned. The checks and the inline-or-claim-check decision keep every message within the server's `max_payload`, so `413` from the broker stage means the server's limit changed under a running gateway. Many senders retry every status other than `2xx`, `413` included.

An HTTP output answers differently: the upstream's own final status and body, or `502` when no upstream answered; a malformed signed event ID gets `reject_status` there, since the HTTP output does not publish the ID. See [configuration](configuration.md#outputhttp).

## Delivery

- **No ordering.** Requests are handled concurrently and senders retry, so messages can arrive in any order. Order by the payload's own timestamps.
- **At least once.** A publish whose acknowledgement is lost is retried, and senders retry too, so the same event can be published more than once. Within the stream's `duplicate_window` the broker drops repeats that carry the same `Nats-Msg-Id`.
- **Deduplicate on (source, event ID).** Consumers keep `(Hookgate-Source, Hookgate-Event-Id)` as a permanent unique key and treat a repeat as already done. A message carrying `Hookgate-Replay` is a deliberate replay of a failed event: process it again, idempotently, instead of skipping it as a repeat. An event without an event ID cannot be deduplicated; treat each delivery of it as possibly repeated.

## Pairing a stream

hookgate never creates or changes streams or buckets; the deployment does. A stream for webhook events:

| Setting | Value | Why |
|---|---|---|
| `subjects` | `webhooks.events.>` and, for the relay, `webhooks.replay.>` | One subject per source; replays for each relay consumer. |
| `retention` | `limits` (required) | With `interest` or `workqueue` retention the stream can acknowledge an event that no consumer was waiting for without storing it. The output's checks require `limits` and acknowledgements (`no_ack: false`). |
| `storage` | `file` (required for durability) | A memory stream loses everything when its servers restart. hookgate logs a warning. |
| `num_replicas` | `3` (recommended) | A `200` then means a quorum of servers holds the event, so losing one server loses nothing. With one replica hookgate logs a warning. |
| `duplicate_window` | Longer than the slowest sender's retry schedule (48 h covers one that retries for 28 h) | Repeats within the window are dropped by the broker. Each tracked ID costs memory on every replica. |
| `discard` | `new` (required) | A full stream refuses new events, which the sender retries, instead of deleting acknowledged events; with `discard: old` a retry of an evicted event would even be acknowledged as a duplicate. |
| `max_age` | As long as a consumer outage you want to survive (for example 14 days) | |
| `max_msg_size` | At least `inline_max_bytes` + `max_header_bytes` | The output's checks fail otherwise. |

The claim-check bucket needs:

- a TTL of at least the stream's `max_age` + the output's `reuse_within` + its `timeout` + 1 h (16 days covers 14 days + 24 h + 10 s + 1 h): an object is reused only while its first chunk is younger than `reuse_within`, and the publish that follows can take up to `timeout`;
- `max_bytes` of at least `max_body_bytes` + 1 MiB, and at least the stream's replicas and, for a file stream, file storage;
- `limits` retention, `discard: new`, no per-subject message limit and direct get, which every bucket created by a NATS client or the `nats` CLI has. Under other settings an acknowledged chunk can be evicted.

The output's checks enforce all of these. Also turn off rollups on the bucket's stream (`allow_rollup_hdrs: false`; `nats stream edit OBJ_<bucket> --no-allow-rollup`): hookgate's writer never needs them, and with them on, any identity that may write the bucket's metadata can purge stored bodies with one publish. Without rollups a bucket writer can still publish a delete marker that hides an object from readers, so grant bucket write permissions only to hookgate. Stock clients (`nats object put`) cannot write to a bucket without rollups; readers are unaffected.

**Durability on disk.** By default NATS fsyncs JetStream writes every two minutes, so an acknowledged message can sit in the page cache of every replica; a power loss that hits a quorum of servers at once can lose it. Set the server option `jetstream { sync_interval: always }` where an acknowledgement must mean the event is on disk: the server then syncs JetStream writes to disk as it stores them instead of on a timer, at a cost in throughput ([NATS: sync_interval](https://docs.nats.io/reference/config/jetstream/sync_interval)). The option needs a server restart.

The [JetStream example](../examples/jetstream/) creates such a stream and bucket.

## Gateway permissions

The gateway's NATS identity needs exactly these permissions, with `<prefix>` the subject prefix, `<stream>` the event stream, `<bucket>` the claim-check bucket and `<inbox>` the connection's `inbox_prefix`:

```
publish:
  <prefix>.>                              # event subjects, for example webhooks.events.>
  $JS.API.STREAM.INFO.<stream>            # output checks
  $JS.API.STREAM.INFO.OBJ_<bucket>        # claim-check checks
  $JS.API.DIRECT.GET.OBJ_<bucket>         # first chunk of an existing object
  $JS.API.DIRECT.GET.OBJ_<bucket>.>       # metadata of an existing object
  $O.<bucket>.C.>                         # object chunks
  $O.<bucket>.M.>                         # object metadata
subscribe:
  <inbox>.>                               # replies
```

Without a claim check only the first two publish permissions and the subscribe permission are needed. Nothing here allows purging or deleting messages, streams or objects, and the gateway never needs to. The `$JS.API.DIRECT.GET` subjects assume a bucket created by a current NATS client or the `nats` CLI, which enables direct get.

## Relay command handlers

[`hookgate-relay`](relay.md) runs `handler.command.path` once per event, directly (no shell), with `handler.command.args`.

**Input.** The full event body on stdin; claim checks are already resolved, so a handler never sees one. The environment holds only `handler.command.env_allow`, `handler.command.set_env` and:

| Variable | Value |
|---|---|
| `HOOKGATE_CONTRACT` | Contract version, `1`. |
| `HOOKGATE_SOURCE` | Source name. |
| `HOOKGATE_SUBJECT` | Subject the event was consumed from (`webhooks.events.<source>`, or the consumer's replay subject). |
| `HOOKGATE_MESSAGE_ID` | Stored message ID: `<source>:<event id>`, `replay:<dead-letter stream>:<seq>` for a replay, empty when none. |
| `HOOKGATE_EVENT_ID` | `Hookgate-Event-Id`; empty when the event has none. |
| `HOOKGATE_EVENT_ID_AUTHENTICATED` | `true` or `false`; empty when there is no event ID. |
| `HOOKGATE_ATTEMPT` | Delivery attempt, `1` on first delivery. |
| `HOOKGATE_STREAM_SEQ` | Sequence of the message in the event stream. |
| `HOOKGATE_REPLAY` | `Hookgate-Replay` (`<dead-letter stream>:<seq>`) on a replay, else empty. |
| `HOOKGATE_RECEIVED_AT` | `Hookgate-Received-At`. |
| `HOOKGATE_HEADERS` | JSON object, header name → array of values: the contract headers present on the message (`Hookgate-Contract`, `-Source`, `-Event-Id`, `-Event-Id-Authenticated`, `-Signed-At`, `-Body-Sha256`, `-Body-Size`, `-Received-At`, `-Replay`) and the application headers matching `pass_headers`, under their published names. |

Every variable is always set, empty when it has no value.

**Exit status.**

| Exit | Meaning | Relay action |
|---|---|---|
| `0` | Processed. | Acknowledge. |
| `65` (`EX_DATAERR`) | The handler rejects the event as invalid. | Dead-letter as `handler_rejected`. |
| `78` (`EX_CONFIG`), `126`, `127`, or the program cannot be started (missing, not executable, not a valid executable) | Configuration fault. | Nak the event and pause the relay until restart. |
| any other non-zero exit (`75` `EX_TEMPFAIL` included), timeout, killed by a signal (for example the OOM killer) | Failure that may pass. | Retry after the backoff delay. |

A retry on the last allowed attempt dead-letters the event as `max_attempts`. For an exit, the dead letter's detail is the exit status only (`exit status 65`); a timeout, a signal or a start error is named instead. A start error that is a transient resource shortage (`EAGAIN`, `ENOMEM`, `ETXTBSY`, `EMFILE`, `ENFILE`) retries.

Only `65` rejects, because an unknown failure is retried: runtimes exit `1` or `2` on an unhandled exception (Python and Node.js `1`, a Go panic `2`), often for a transient cause such as a lost database connection. Treating those exits as rejections would dead-letter transient failures on their first attempt. A handler that knows an event can never succeed exits `65`.

The fixed arguments and environment (`args`, the `env_allow` values present at start and `set_env`) must leave room for the per-event variables: on Linux the relay refuses to start when one of their strings contains a NUL byte or exceeds the kernel's per-string limit (`MAX_ARG_STRLEN`, 128 KiB on 4 KiB pages), or when, together with what interpreters (`#!` scripts, `binfmt_misc`) may add and the room reserved for the `HOOKGATE_` variables (64 KiB of `HOOKGATE_HEADERS` JSON, which JSON escaping can make larger than the headers on the wire, 4 KiB for the other values and the longest configured subject), they could exceed the total limit (a quarter of the stack size limit, at least 128 KiB and at most 6 MiB, never more than the stack size limit itself; 128 KiB when the stack limit cannot be read). An event whose metadata contains a NUL byte, or whose headers still make the start fail as too large (`E2BIG`), cannot be handed to a program and is dead-lettered as `handler_rejected`. An `E2BIG` for an event within the reserved room is not the event's doing (an interpreter or the stack took more room than the check allows for) and pauses the relay as a configuration fault.

stdout is discarded. The last 4 KiB of stderr appear in the relay's log line for a failed run, nowhere else; a handler must not write secrets or body data to stderr.

On timeout or shutdown the relay kills the handler's whole process group with SIGKILL, whether or not the handler's output is still open. When the handler exits it kills whatever remains of the group (on Linux before reaping the handler, so the group ID cannot have been reused), so processes a handler starts never outlive it. A handler must therefore tolerate being stopped at any point and running again for the same event.

## Relay HTTP handlers

The relay `POST`s the full body to `handler.http.url` with:

- the contract headers present on the message (as for `HOOKGATE_HEADERS`);
- the application headers matching `pass_headers`, under their published names;
- `Hookgate-Attempt`: the delivery attempt, `1` on first delivery;
- `handler.http.set_headers`, replacing a passed header of the same name in any letter case.

| Response | Relay action |
|---|---|
| `2xx` | Acknowledge. |
| `400`, `413`, `415`, `422` | The handler rejects the event: dead-letter as `handler_rejected`, detail `status <code>`. |
| `401`, `403`, `3xx`, `101` | Configuration fault (a refused credential; upgrades and redirects are never followed, `Location` is not read): nak the event and pause the relay until restart. |
| `404`, `408`, `409`, `425`, `429`, `5xx`, any other `4xx` | Retry after the backoff delay, detail `status <code>` (with `Retry-After` when it sets the delay). |
| timeout, connection or transport error, an error reading the response body (whatever the status) | Retry after the backoff delay. The detail names the class only (`timeout`, `connection refused`, `connection reset or closed`, `name resolution failed`, `TLS failure`, `network error`, `malformed response`), never the error text, which can quote what the handler sent. |

An unknown status retries, so only a deliberate answer dead-letters an event. On `429` and `503` a single valid `Retry-After` (delay-seconds or an HTTP-date) sets the redelivery delay: the larger of it and the backoff delay, with `Retry-After` capped at the last `backoff` value. A retry on the last allowed attempt dead-letters the event as `max_attempts`, whatever `Retry-After` says.

At most 64 KiB of the response body is read, within `handler.http.timeout`; it is not otherwise used.

## Relay dead-letter records

A record is published to `webhooks.dead.<consumer>` (default) and captured by the dead-letter stream. It is built from an allowlist, never a copy of the stored message:

| Header | Value |
|---|---|
| `Hookgate-Contract`, `-Source`, `-Event-Id`, `-Event-Id-Authenticated`, `-Signed-At`, `-Received-At`, `-Body-Sha256`, `-Body-Size` | Copied from the event when present. |
| application headers | Every non-reserved header of the event, under its published name. |
| `Hookgate-Dead-Reason` | `handler_rejected`, `max_attempts`, `checksum_mismatch`, `claim_missing` or `invalid_message`. |
| `Hookgate-Dead-Detail` | Short explanation, at most 256 bytes, never body data or handler output. |
| `Hookgate-Dead-Attempts` | Delivery attempts made. |
| `Hookgate-Dead-At` | RFC 3339 UTC, millisecond precision. |
| `Hookgate-Original-Subject`, `-Original-Stream`, `-Original-Seq`, `-Original-Consumer` | Where the event was consumed. |
| `Hookgate-Original-Msg-Id` | The event's message ID, when it had one. |
| `Hookgate-Claim-Bucket`, `Hookgate-Claim-Key` | Only when the record holds no body (below). |
| `Hookgate-Dead-Headers-Dropped` | `true` when the application headers were left out because, with the metadata, they would exceed the broker's 64 KiB header limit; absent otherwise. A replay of such a record carries only the contract headers. |
| `Nats-Msg-Id` | `dead:<stream>:<stream created, unix nanoseconds>:<consumer>:<seq>`: one record per stream incarnation, consumer and event within the dead-letter stream's duplicate window, however often the publish is retried. |

The body is the full resolved body, also for claim-checked events, so a replay never depends on the claim bucket. Exceptions: a `checksum_mismatch` or `claim_missing` record of a claim-checked event holds an empty body plus the claim headers, also when an object was fetched but did not match the event's size and SHA-256; an `invalid_message` record holds the message body as stored.

## Relay replays and deduplication

A replay (`hookgate-relay replay`) republishes a dead-letter record to the failed consumer's replay subject (`webhooks.replay.<consumer>` by default; only that consumer filters it) with the record's body, contract headers and application headers, plus `Hookgate-Replay: <dead-letter stream>:<seq>`, and message ID `replay:<dead-letter stream>:<seq>`. It carries no `Hookgate-Dead-*` or `Hookgate-Original-*` headers. A body too large for the event stream travels as a claim check, like a published event.

Handlers deduplicate and treat `Hookgate-Replay` as [Delivery](#delivery) describes; they process a replay even when they have seen the event before, idempotently (update rather than insert twice).
