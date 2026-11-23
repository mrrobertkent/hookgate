#!/bin/sh
# Generates the demo NKeys once and writes the accounts the NATS servers include.
set -eu
cd /keys
[ -s hookgate.nk ] || nk -gen user > hookgate.nk
[ -s admin.nk ] || nk -gen user > admin.nk
hookgate=$(nk -inkey hookgate.nk -pubout)
admin=$(nk -inkey admin.nk -pubout)
cat > auth.conf <<CONF
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
    ]
  }
}
CONF
# hookgate runs as the distroless nonroot user (65532).
chown 65532 hookgate.nk
chmod 0400 hookgate.nk admin.nk
