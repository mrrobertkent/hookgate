# hookgate with NATS JetStream

A three-node NATS JetStream cluster and hookgate, built from this repository, publishing verified webhooks to a replicated stream. Bodies over 256 KiB go to an Object Store bucket first (claim check). [`hookgate-relay`](../../docs/relay.md), also built from this repository, consumes the stream and posts each event to a small stand-in application; events it rejects land in a dead-letter stream, from which they are replayed. Every NATS user has only least-privilege permissions ([gateway](../../docs/contract.md#gateway-permissions), [relay and replay](../../docs/relay.md#permissions)).

| File | What it is |
|---|---|
| [`compose.yaml`](compose.yaml) | `keys` (generates NKeys), `nats-1`..`nats-3` (the cluster), `setup` (creates the streams, the bucket and the consumer; also the NATS CLI), `hookgate`, `handler` (the stand-in application), `relay`, and `replay` (the operator's replay command, profile `tools`). |
| [`nats.conf`](nats.conf) | Cluster member: JetStream with `sync_interval: always`, `max_payload: 8MB` (dead-letter records carry whole bodies), routes, and the accounts `keys` writes. |
| [`keys.sh`](keys.sh) | Generates admin, gateway, relay and replay NKeys, each into the volume of the one service that uses it, and writes the accounts and permissions. |
| [`setup.sh`](setup.sh) | Creates `WEBHOOK_EVENTS` (events and replays; R3, file, `limits` retention, `discard new` (both required), 48 h duplicate window, 14 days), `WEBHOOK_CLAIMS` (R3, 16-day TTL, rollups off), `WEBHOOK_DEAD` (R3, `limits` retention, a year) and the relay's consumer `app` (explicit acks, `max_deliver -1`, no backoff). |
| [`hookgate.yaml`](hookgate.yaml) | One source, `demo`: a token check, event IDs from `X-Event-Id`, JetStream output with a claim check, passing `Content-Type` and `X-Poison`. |
| [`relay.yaml`](relay.yaml) | The relay of consumer `app`: HTTP handler `http://handler:8080/events`, claim checks, dead letters to `WEBHOOK_DEAD`. |
| [`replay.yaml`](replay.yaml) | The same consumer seen by the replay operator's identity. |
| [`Caddyfile`](Caddyfile) | The stand-in application: answers `200`, except `422` for an event carrying `x-poison` that is not a replay. |

The NKeys are generated into Docker volumes on first start, one per service: the gateway, the relay and the replay command each see only their own seed. Nothing secret is in this directory.

## Run it

```sh
docker compose up -d --build --wait
```

Send an event, the same event again, a large one and a forged one:

```sh
curl -i localhost:8080/events -H 'X-Webhook-Token: demo-token-change-me' -H 'X-Event-Id: evt-1' \
  -H 'Content-Type: application/json' --data-binary '{"hello":"world"}'      # 200 {"status":"accepted","duplicate":false}
curl localhost:8080/events -H 'X-Webhook-Token: demo-token-change-me' -H 'X-Event-Id: evt-1' \
  --data-binary '{"hello":"world"}'                                         # 200 {"status":"accepted","duplicate":true}
head -c 400000 /dev/urandom | base64 > big.txt
curl localhost:8080/events -H 'X-Webhook-Token: demo-token-change-me' -H 'X-Event-Id: evt-2' \
  --data-binary @big.txt                                                    # 200, body stored in WEBHOOK_CLAIMS
curl -i localhost:8080/events -H 'X-Webhook-Token: wrong' --data-binary x   # 401, nothing published
```

The relay delivers each accepted event to the handler and acknowledges it:

```sh
docker compose logs relay | grep delivery          # "outcome":"acked" for evt-1 and evt-2 (claim check resolved)
docker compose logs handler | grep /events         # the handler's request log, with the Hookgate-* headers
```

Send an event the handler rejects, then replay it from the dead-letter stream:

```sh
curl localhost:8080/events -H 'X-Webhook-Token: demo-token-change-me' -H 'X-Event-Id: evt-poison' \
  -H 'X-Poison: yes' --data-binary '{"bad":true}'                          # 200: the gateway stored it
docker compose logs relay | grep evt-poison        # "outcome":"dead_lettered","reason":"handler_rejected","detail":"status 422"
docker compose run --rm --no-deps setup nats stream get WEBHOOK_DEAD 1     # the record: whole body, Hookgate-Dead-* headers
docker compose run --rm replay -seq 1
                                                   # WEBHOOK_DEAD:1 -> webhooks.replay.app, WEBHOOK_EVENTS:<seq>
docker compose logs relay | grep webhooks.replay   # acked: the handler accepts the replay (Hookgate-Replay: WEBHOOK_DEAD:1)
docker compose run --rm replay -seq 1           # "(already replayed)": stored once
```

In a real deployment you fix the handler before replaying; this stand-in accepts replays to show the flow. A replay goes only to the consumer that failed the event (`webhooks.replay.app`).

Look at what was stored:

```sh
docker compose run --rm --no-deps setup nats stream get WEBHOOK_EVENTS 1   # inline body and Hookgate-* headers
docker compose run --rm --no-deps setup nats stream get WEBHOOK_EVENTS 2   # empty body, Hookgate-Claim-* headers
docker compose run --rm --no-deps setup nats object ls WEBHOOK_CLAIMS
curl -s localhost:9090/metrics | grep -E 'hookgate_(publish|claim|nats|source_ready)'
curl -s localhost:9091/metrics | grep -E 'hookgate_relay_(deliveries|dead_letters)'
```

Lose a node while sending: the stream keeps a quorum and hookgate reconnects to another server.

```sh
docker compose kill nats-1
curl localhost:8080/events -H 'X-Webhook-Token: demo-token-change-me' -H 'X-Event-Id: evt-3' --data-binary '{}'   # 200
docker compose start nats-1
```

Clean up:

```sh
docker compose down -v
rm -f big.txt
```

## Adapting it

- Set `DEMO_WEBHOOK_TOKEN` to a real secret, or replace the source with your sender's checks ([recipes](../../README.md#recipes)).
- Point `relay.yaml` at your application (`handler.http.url`, or a `command` handler in an image built `FROM` the relay image; see [the relay docs](../../docs/relay.md#adding-a-command-handler)). Keep the relay's `stop_grace_period` above `drain_timeout` + 15 s.
- The servers stop with `SIGUSR2` (lame duck mode) so leaderships and clients move before a server exits; keep `stop_grace_period` above `lame_duck_duration` (default 2 minutes).
- In production, give the cluster routes credentials or TLS, keep the NKey seeds in your secret store, and size `max_bytes`, `max_age` and the bucket TTL as [the contract](../../docs/contract.md#pairing-a-stream) describes.
