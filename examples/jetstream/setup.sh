#!/bin/sh
# Creates the event stream, the claim-check bucket, the dead-letter stream and the relay's consumer; neither hookgate
# nor the relay creates them.
set -eu
# An existing stream from before the relay was added gains the replay subjects.
if nats stream info WEBHOOK_EVENTS >/dev/null 2>&1; then
  nats stream edit WEBHOOK_EVENTS --subjects 'webhooks.events.>' --subjects 'webhooks.replay.>' --force
else
  nats stream add WEBHOOK_EVENTS \
    --subjects 'webhooks.events.>' --subjects 'webhooks.replay.>' --storage file --replicas 3 --retention limits \
    --discard new --max-age 14d --dupe-window 48h --max-bytes 2GiB --defaults
fi
# TTL covers the stream's max_age (14d) + the gateway's reuse_within (24h) + timeout (10s) + 1h.
nats object info WEBHOOK_CLAIMS >/dev/null 2>&1 || nats object add WEBHOOK_CLAIMS \
  --storage file --replicas 3 --ttl 16d --max-bucket-size 4GiB
# hookgate never needs rollups; with them on, any writer of the bucket could purge stored bodies.
nats stream edit OBJ_WEBHOOK_CLAIMS --no-allow-rollup --force
# Dead letters keep whole bodies for a year; limits retention so every acknowledged record is stored.
nats stream info WEBHOOK_DEAD >/dev/null 2>&1 || nats stream add WEBHOOK_DEAD \
  --subjects 'webhooks.dead.>' --storage file --replicas 3 --retention limits --discard new \
  --max-age 365d --dupe-window 48h --max-bytes 2GiB --allow-direct --defaults
# The relay owns retries and the terminal decision: explicit acks, no server delivery limit, no backoff.
nats consumer info WEBHOOK_EVENTS app >/dev/null 2>&1 || nats consumer add WEBHOOK_EVENTS app --pull \
  --ack explicit --wait 60s --max-deliver=-1 --deliver all --max-pending 100 \
  --filter webhooks.events.demo --filter webhooks.replay.app --defaults
