#!/bin/sh
# Creates the event stream and the claim-check bucket the gateway expects; hookgate never creates them.
set -eu
nats stream info WEBHOOK_EVENTS >/dev/null 2>&1 || nats stream add WEBHOOK_EVENTS \
  --subjects 'webhooks.events.>' --storage file --replicas 3 --retention limits --discard new \
  --max-age 14d --dupe-window 48h --max-bytes 2GiB --defaults
# TTL covers the stream's max_age (14d) plus the gateway's reuse_within (24h), with a day's margin.
nats object info WEBHOOK_CLAIMS >/dev/null 2>&1 || nats object add WEBHOOK_CLAIMS \
  --storage file --replicas 3 --ttl 16d --max-bucket-size 4GiB
