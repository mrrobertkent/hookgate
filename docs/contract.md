# Event contract

This is what a consumer of hookgate's JetStream output can rely on: subjects, message headers, identifiers, claim checks, delivery guarantees, and what the sender is told. It is versioned; any change to it is a breaking change for consumers and gets a new version.

## Version

Every message carries `Hookgate-Contract: 1`. A consumer rejects a message whose version it does not know.

## Subjects

Events are published to `webhooks.events.<source>` (the `subject` template, `webhooks.events.{source}` by default). Source names match `[A-Za-z0-9][A-Za-z0-9_-]{0,62}` and so never contain `:`, `/` or `.`.

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

An HTTP output answers differently: the upstream's own final status and body, or `502` when no upstream answered. See [configuration](configuration.md#outputhttp).

## Delivery

- **No ordering.** Requests are handled concurrently and senders retry, so messages can arrive in any order. Order by the payload's own timestamps.
- **At least once.** A publish whose acknowledgement is lost is retried, and senders retry too, so the same event can be published more than once. Within the stream's `duplicate_window` the broker drops repeats that carry the same `Nats-Msg-Id`.
- **Deduplicate on (source, event ID).** Consumers keep `(Hookgate-Source, Hookgate-Event-Id)` as a permanent unique key and treat a repeat as already done. A message carrying `Hookgate-Replay` is a deliberate replay of a failed event: process it again, idempotently, instead of skipping it as a repeat.

## Pairing a stream

hookgate never creates or changes streams or buckets; the deployment does. A stream for webhook events:

| Setting | Value | Why |
|---|---|---|
| `subjects` | `webhooks.events.>` | One subject per source. |
| `retention` | `limits` (required) | With `interest` or `workqueue` retention the stream can acknowledge an event that no consumer was waiting for without storing it. The output's checks require `limits` and acknowledgements (`no_ack: false`). |
| `storage` | `file` (required for durability) | A memory stream loses everything when its servers restart. hookgate logs a warning. |
| `num_replicas` | `3` (recommended) | A `200` then means a quorum of servers holds the event, so losing one server loses nothing. With one replica hookgate logs a warning. |
| `duplicate_window` | Longer than the slowest sender's retry schedule (48 h covers one that retries for 28 h) | Repeats within the window are dropped by the broker. Each tracked ID costs memory on every replica. |
| `discard` | `new` | A full stream refuses new events, which the sender retries, instead of deleting older events no consumer has processed. |
| `max_age` | As long as a consumer outage you want to survive (for example 14 days) | |
| `max_msg_size` | At least `inline_max_bytes` + `max_header_bytes` | The output's checks fail otherwise. |

The claim-check bucket needs:

- a TTL of at least the stream's `max_age` + the output's `reuse_within` + its `timeout` + 1 h (16 days covers 14 days + 24 h + 10 s + 1 h): an object is reused only while its first chunk is younger than `reuse_within`, and the publish that follows can take up to `timeout`;
- `max_bytes` of at least `max_body_bytes` + 1 MiB, and the same replicas and storage as the stream;
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
