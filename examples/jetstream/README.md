# hookgate with NATS JetStream

A three-node NATS JetStream cluster and hookgate, built from this repository, publishing verified webhooks to a replicated stream. Bodies over 256 KiB go to an Object Store bucket first (claim check). The gateway's NATS user has only the [least-privilege permissions](../../docs/contract.md#gateway-permissions).

| File | What it is |
|---|---|
| [`compose.yaml`](compose.yaml) | `keys` (generates NKeys), `nats-1`..`nats-3` (the cluster), `setup` (creates the stream and the bucket; also the NATS CLI), `hookgate`. |
| [`nats.conf`](nats.conf) | Cluster member: JetStream with `sync_interval: always`, routes, and the accounts `keys` writes. |
| [`keys.sh`](keys.sh) | Generates an admin and a gateway NKey and writes the accounts and permissions. |
| [`setup.sh`](setup.sh) | Creates `WEBHOOK_EVENTS` (R3, `discard new`, 48 h duplicate window, 14 days) and `WEBHOOK_CLAIMS` (R3, 16-day TTL). |
| [`hookgate.yaml`](hookgate.yaml) | One source, `demo`: a token check, event IDs from `X-Event-Id`, JetStream output with a claim check. |

The NKeys are generated into a Docker volume on first start; nothing secret is in this directory.

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

Look at what was stored:

```sh
docker compose run --rm --no-deps setup nats stream get WEBHOOK_EVENTS 1   # inline body and Hookgate-* headers
docker compose run --rm --no-deps setup nats stream get WEBHOOK_EVENTS 2   # empty body, Hookgate-Claim-* headers
docker compose run --rm --no-deps setup nats object ls WEBHOOK_CLAIMS
curl -s localhost:9090/metrics | grep -E 'hookgate_(publish|claim|nats|source_ready)'
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
- The servers stop with `SIGUSR2` (lame duck mode) so leaderships and clients move before a server exits; keep `stop_grace_period` above `lame_duck_duration` (default 2 minutes).
- In production, give the cluster routes credentials or TLS, keep the NKey seeds in your secret store, and size `max_bytes`, `max_age` and the bucket TTL as [the contract](../../docs/contract.md#pairing-a-stream) describes.
