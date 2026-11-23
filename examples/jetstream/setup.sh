#!/bin/sh
# Creates the event stream and the claim-check bucket the gateway expects; hookgate never creates them.
set -eu
nats stream info WEBHOOK_EVENTS >/dev/null 2>&1 || nats stream add WEBHOOK_EVENTS \
  --subjects 'webhooks.events.>' --storage file --replicas 3 --retention limits --discard new \
  --max-age 14d --dupe-window 48h --max-bytes 2GiB --defaults
# TTL covers the stream's max_age (14d) + the gateway's reuse_within (24h) + timeout (10s) + 1h.
nats object info WEBHOOK_CLAIMS >/dev/null 2>&1 || nats object add WEBHOOK_CLAIMS \
  --storage file --replicas 3 --ttl 16d --max-bucket-size 4GiB
# hookgate never needs rollups; with them on, any writer of the bucket could purge stored bodies.
nats stream edit OBJ_WEBHOOK_CLAIMS --no-allow-rollup --force
