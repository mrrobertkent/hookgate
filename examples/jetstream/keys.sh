#!/bin/sh
# Generates the demo NKeys once, each into the volume of the one service that uses it, and writes the accounts the
# NATS servers include.
set -eu
for id in admin hookgate relay replay; do
  [ -s "/keys/$id/$id.nk" ] || nk -gen user > "/keys/$id/$id.nk"
done
pub() {
  local user="$1"
  nk -inkey "/keys/$user/$user.nk" -pubout
}
admin=$(pub admin)
hookgate=$(pub hookgate)
relay=$(pub relay)
replay=$(pub replay)
cat > /keys/nats/auth.conf <<CONF
accounts {
  WEBHOOKS {
    jetstream: enabled
    users: [
      # Creates the stream and the bucket, and inspects them.
      {nkey: $admin}
      # The gateway: publish events and claim-checked bodies, nothing else.
      {nkey: $hookgate, permissions: {
        publish: {allow: [
          "webhooks.events.>",
          "\$JS.API.STREAM.INFO.WEBHOOK_EVENTS",
          "\$JS.API.STREAM.INFO.OBJ_WEBHOOK_CLAIMS",
          "\$JS.API.DIRECT.GET.OBJ_WEBHOOK_CLAIMS",
          "\$JS.API.DIRECT.GET.OBJ_WEBHOOK_CLAIMS.>",
          "\$O.WEBHOOK_CLAIMS.C.>",
          "\$O.WEBHOOK_CLAIMS.M.>"
        ]}
        subscribe: {allow: ["_INBOX.hookgate.>"]}
      }}
      # The relay of consumer app: its own consumer, its own dead-letter subject, claim-check reads.
      {nkey: $relay, permissions: {
        publish: {allow: [
          "\$JS.API.STREAM.INFO.WEBHOOK_EVENTS",
          "\$JS.API.CONSUMER.INFO.WEBHOOK_EVENTS.app",
          "\$JS.API.CONSUMER.MSG.NEXT.WEBHOOK_EVENTS.app",
          "\$JS.ACK.WEBHOOK_EVENTS.app.>",
          "\$JS.API.STREAM.INFO.WEBHOOK_DEAD",
          "webhooks.dead.app",
          "\$JS.API.STREAM.INFO.OBJ_WEBHOOK_CLAIMS",
          "\$JS.API.DIRECT.GET.OBJ_WEBHOOK_CLAIMS.\$O.WEBHOOK_CLAIMS.M.>",
          "\$JS.API.CONSUMER.CREATE.OBJ_WEBHOOK_CLAIMS.*.\$O.WEBHOOK_CLAIMS.C.*",
          "\$JS.API.CONSUMER.DELETE.OBJ_WEBHOOK_CLAIMS.*",
          "\$JS.FC.OBJ_WEBHOOK_CLAIMS.>"
        ]}
        subscribe: {allow: ["_INBOX.relay-app.>", "\$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.WEBHOOK_EVENTS.app"]}
      }}
      # The replay operator: read dead letters, republish to replay subjects, store bodies too large to inline.
      {nkey: $replay, permissions: {
        publish: {allow: [
          "\$JS.API.STREAM.INFO.WEBHOOK_DEAD",
          "\$JS.API.DIRECT.GET.WEBHOOK_DEAD",
          "\$JS.API.STREAM.MSG.GET.WEBHOOK_DEAD",
          "\$JS.API.STREAM.INFO.WEBHOOK_EVENTS",
          "webhooks.replay.*",
          "\$JS.API.STREAM.INFO.OBJ_WEBHOOK_CLAIMS",
          "\$JS.API.DIRECT.GET.OBJ_WEBHOOK_CLAIMS",
          "\$JS.API.DIRECT.GET.OBJ_WEBHOOK_CLAIMS.\$O.WEBHOOK_CLAIMS.M.>",
          "\$O.WEBHOOK_CLAIMS.C.>",
          "\$O.WEBHOOK_CLAIMS.M.>"
        ]}
        subscribe: {allow: ["_INBOX.relay-replay.>"]}
      }}
    ]
  }
}
CONF
# hookgate, the relay and replay run as the distroless nonroot user (65532); setup runs as root.
chown 65532 /keys/hookgate/hookgate.nk /keys/relay/relay.nk /keys/replay/replay.nk
chmod 0400 /keys/*/*.nk
