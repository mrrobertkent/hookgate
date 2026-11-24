# High availability

hookgate runs as one instance or as several. Instances are stateless and active-active: each is identical, there is no leader, and they share no state and never talk to each other. Any instance can take any request, so a balancer needs no sticky sessions. Losing an instance loses only the answers to the requests it was handling; whether those are sent again depends on the sender (see [Duplicates and retries](#duplicates-and-retries)).

Every instance must load the same configuration and the same secrets. Instances that differ answer the same request differently.

## One instance

One instance needs nothing beyond the [configuration](configuration.md). While it restarts, senders get connection errors or `502`/`503` from the proxy in front. Senders that retry deliver those events later; a sender that does not retry automatically needs its failed deliveries redelivered by hand or by a script (GitHub, for example, does not redeliver failed webhook deliveries on its own: [Handling failed webhook deliveries](https://docs.github.com/en/webhooks/using-webhooks/handling-failed-webhook-deliveries)). `drain_delay` can stay at `0s` when nothing balances traffic to the instance.

## Several instances

Put the instances behind whatever already spreads traffic: a reverse proxy, a load balancer, or Docker Swarm with a proxy in front. hookgate needs no balancer tier of its own.

The balancer:

- sends webhook traffic to each instance's `listen` port (`:8080`), and terminates TLS;
- health-checks each instance with `GET /readyz` on its `admin_listen` port (`:9090`) and stops routing to an instance that answers anything but `200`;
- keeps its upstream idle timeout below hookgate's `idle_timeout`, so it never reuses a connection hookgate has closed.

Keep the admin port off the public network: the balancer and the metrics scraper reach it on an internal network.

### Behind a reverse proxy or load balancer

Any proxy with active health checks works: HAProxy, NGINX, Envoy, Traefik, Caddy, or a cloud load balancer. Configure one pool with every instance's webhook address and an active health check of `/readyz` on the admin port, then size `drain_delay` to the time the proxy takes to stop routing to an instance ([Drain and stop grace](#drain-and-stop-grace)).

### Docker Swarm

Run hookgate as a service with one task per node (`mode: global`) or with a fixed number of replicas, and put a proxy that routes to each task in front. `global` only decides placement: it starts one task on every eligible node. It does not route traffic, and Swarm's virtual IP and routing mesh never call `/readyz`, so readiness takes effect only through a proxy that health-checks each task.

A complete stack file with Traefik, deployed separately, as the proxy:

```yaml
services:
  hookgate:
    image: ghcr.io/mrrobertkent/hookgate:<version>@sha256:<digest>
    configs:
      - source: hookgate
        target: /etc/hookgate/hookgate.yaml
    secrets:
      - github_webhook_secret            # mounted at /run/secrets/github_webhook_secret
      - upstream_authorization           # "Bearer <token>" for the collector
    networks: [proxy]
    stop_grace_period: 60s               # > drain_delay + shutdown_timeout
    deploy:
      mode: global
      update_config:
        order: start-first
        parallelism: 1
        delay: 30s                       # >= the proxy's discovery interval + health-check interval
        failure_action: rollback
      labels:
        traefik.enable: "true"
        traefik.swarm.network: proxy
        traefik.http.routers.hookgate.rule: Host(`hooks.example.com`)
        traefik.http.services.hookgate.loadbalancer.server.port: "8080"
        traefik.http.services.hookgate.loadbalancer.healthcheck.path: /readyz
        traefik.http.services.hookgate.loadbalancer.healthcheck.port: "9090"
        traefik.http.services.hookgate.loadbalancer.healthcheck.interval: 2s
        traefik.http.services.hookgate.loadbalancer.healthcheck.timeout: 1s

configs:
  hookgate:
    file: ./hookgate.yaml

secrets:
  github_webhook_secret:
    external: true                       # docker secret create github_webhook_secret -
  upstream_authorization:
    external: true

networks:
  proxy:
    external: true                       # the overlay network Traefik is attached to
```

with, in `hookgate.yaml`:

```yaml
drain_delay: 10s
shutdown_timeout: 30s
sources:
  - name: github
    path: /github
    checks:
      - hmac:
          header: X-Hub-Signature-256
          algorithm: sha256
          prefix: "sha256="
          secrets: [{file: /run/secrets/github_webhook_secret}]
    output:
      http:
        urls: ["http://tasks.otel-collector:8088/github"]
        resolve_all: true
        pass_headers: [Content-Type, X-GitHub-*]
        set_headers:
          Authorization: {file: /run/secrets/upstream_authorization}
```

The upstream is an OpenTelemetry Collector service, `otel-collector`, in the same stack and network, whose `webhook_event` receiver takes the raw webhook on `:8088/github` and checks the bearer token in `upstream_authorization`, as in [examples/otel-collector](../examples/otel-collector/).

- **Routing to tasks.** Traefik's Swarm provider routes to each task's own address by default, so it health-checks each task; `traefik.swarm.lbswarm=true` would hand balancing to the virtual IP and hide the tasks' health ([Traefik Swarm routing](https://doc.traefik.io/traefik/reference/routing-configuration/other-providers/swarm/)).
- **Rollout overlap.** With `start-first`, Swarm stops the old task once the new one is running and its container health check (`/healthz`) passes. The proxy only learns about the new task at its next discovery poll (Traefik's Swarm provider polls every 15 s by default, `providers.swarm.refreshSeconds`: [Traefik Swarm provider](https://doc.traefik.io/traefik/reference/install-configuration/providers/swarm/)); until then the old task, already draining, is the node's only instance. Traefik adds a newly discovered task as available and can route to it before its first `/readyz` probe. That is acceptable for hookgate: an instance answers requests as soon as it listens, and a source that is not ready yet answers `503` on its own, which a sender that retries delivers again. A sender that does not retry loses that delivery unless it is redelivered by hand or by a script; GitHub is one ([Handling failed webhook deliveries](https://docs.github.com/en/webhooks/using-webhooks/handling-failed-webhook-deliveries)). Swarm spaces updates by time only, so `update_config.delay` cannot guarantee the replacement is in rotation before the next task is replaced; set it to at least the discovery interval plus one health-check interval so that, normally, it is. Run instances on at least two nodes so the others carry the traffic during each replacement. With a single eligible node, expect a gap of up to one discovery interval on every update, with the same consequence for senders that do not retry.
- **Upstreams in the same Swarm.** An HTTP output reaching a replicated upstream service names `tasks.<service>` with `resolve_all: true`. `tasks.<service>` resolves to every task's address, and `resolve_all` makes each address a target, so a transport error or `5xx` retries on another task. A plain `<service>` name resolves to one virtual IP and leaves the retry nothing else to try.

### NATS connections

List every NATS server in `nats.urls`. After a disconnect the client tries the listed servers in turn: each attempt at an unreachable server can take up to `nats.connect_timeout` (an address that drops packets uses all of it), and in each round the client waits its reconnect delay (2 s, with jitter) before trying the last server. With the [readiness opt-in](#readiness), set `connect_timeout` well below `disconnect_grace` and keep the grace above one such round, or a single unreachable server can fail readiness while the others are up.

## Drain and stop grace

On SIGTERM an instance:

1. answers `503` on `/readyz` and keeps accepting requests for `drain_delay`, while the balancer notices and stops routing to it;
2. stops accepting, and gives in-flight requests `shutdown_timeout` to finish;
3. drains its NATS connections within what is left of `shutdown_timeout`, and exits.

`drain_delay` defaults to `0s`. Behind a balancer, set it to at least the time the balancer takes to stop routing to an instance that fails `/readyz`: its health-check interval plus timeout, times the failed checks it needs before removing a target. Otherwise requests routed to an instance that has already stopped accepting fail. Discovery of new instances is a separate delay, covered by the rollout overlap above, and `drain_delay` cannot hide it: a draining instance is out of rotation once its check fails.

The orchestrator's stop grace (`stop_grace_period`, `terminationGracePeriodSeconds`) must exceed `drain_delay` + `shutdown_timeout`, or the process is killed mid-request.

## What is per instance

Each instance reports and limits only itself.

- **Metrics.** `/metrics` counts the requests this instance answered. Scrape every instance (in Swarm, for example, Prometheus `dockerswarm_sd_configs` with the `tasks` role, or `dns_sd_configs` on `tasks.<service>`) and aggregate across instances: `sum by (source, outcome) (rate(hookgate_requests_total[5m]))` is the fleet's request rate. `sum(hookgate_ready) or vector(0)` is the number of instances taking traffic; `sum` alone returns nothing, rather than `0`, when no instance can be scraped. Alert on `absent(hookgate_ready)` for the whole fleet being down, and when `hookgate_source_ready`, `hookgate_nats_connected` or `hookgate_nats_credentials_rejected` shows a problem on any instance.
- **`max_concurrent`.** The limit applies to each instance: the fleet admits up to instances × `max_concurrent` requests at once. An instance at its limit answers `503`.
- **Logs.** Each instance logs the requests it answered; collect them all.

## Secret rotation

### Verification secrets

A check accepts any of its secrets, so rotating a sender's signing secret or token never needs instances to agree at a single instant. Optional slots exist only for these inbound verification secrets:

```yaml
checks:
  - hmac:
      header: X-Hub-Signature-256
      algorithm: sha256
      prefix: "sha256="
      secrets:
        - env: GITHUB_WEBHOOK_SECRET
        - env: GITHUB_WEBHOOK_SECRET_NEXT
          optional: true
```

1. Keep an optional `*_NEXT` reference beside the secret.
2. Set the new value in the `*_NEXT` slot and roll every instance. Each now accepts both secrets.
3. Once every instance runs with it, switch the sender to the new secret.
4. Keep the old secret until the sender's retry span has passed: a delivery signed before the switch can still be retried with its old signature.
5. Move the new value into the main slot, empty the `*_NEXT` slot, and roll again.

Never switch the sender before the roll in step 2 has finished: an instance still without the new secret rejects every request signed with it.

### Output and connection credentials

NATS credentials (`nkey_seed`, `creds`, `token`, TLS files) and `set_headers` values are not optional and take one value each. Rotate them by redeploying: make the destination accept both the old and the new credential (add the new NATS user, or the new upstream token), roll every instance onto the new value, then remove the old one from the destination.

## Duplicates and retries

Delivery is at least once for senders that retry until they get a `2xx`: hookgate answers `2xx` only after the output has the event. A sender that does not retry automatically can lose an event that failed before it was acknowledged (an instance lost mid-request, a `503`), unless it is redelivered by hand or by a script. hookgate keeps no state and does not deduplicate. An event arrives twice when a sender retries after an answer was lost (an instance died after delivering but before answering), or when hookgate retries an attempt whose outcome it did not learn. Instances add no other source of duplicates.

- **HTTP output.** Every repeat reaches the upstream. The upstream must be idempotent or deduplicate, on the sender's event ID where it has one.
- **JetStream output.** The broker drops a repeat only when the event has a stable ID and the repeat arrives within the stream's `duplicate_window`. With an ID, each message carries `Nats-Msg-Id: <source>:<event id>`, so the repeat is dropped whichever instance published it, and the sender gets `200` with `"duplicate":true`. A repeat after the window reaches the stream again; consumers drop it by keeping (`Hookgate-Source`, `Hookgate-Event-Id`) as a unique key. A message without `Hookgate-Event-Id` is never deduplicated by the broker, and consumers must not use a missing ID as a key, which would merge unrelated events; see [Event IDs](contract.md#event-ids) and [Delivery](contract.md#delivery).

## Readiness

By default `/readyz` answers `503` only while draining. A configuration fault fails its own source, never readiness: every instance shares it, so failing readiness would take every instance, and every other source, out of the balancer. Configuration faults include a missing or malformed secret, a missing stream or bucket, and NATS credentials that are rejected (an authorization violation, expired or revoked authentication, a refused client certificate, or a server certificate the configured CA does not sign), which `hookgate_nats_credentials_rejected` reports. A rejection seen during an outage keeps the whole outage a configuration fault, whatever errors follow (a server reports an expiry and then closes the socket), until the connection succeeds. A seed or creds file that cannot be read or parsed when the client signs in again counts as rejected credentials.

A lost output connection is different: it is local to one instance (a network partition, a broker node that went away), and another instance may still reach the broker. With [`readiness.require_output_connections: true`](configuration.md#readiness), `/readyz` answers `503` while any connection a source's output uses has been down for longer than `disconnect_grace`, and the balancer sends that instance's traffic to the others. A reconnect within the grace (a broker restart, a leader change) leaves the instance in rotation.

Turn it on when:

- several instances run behind a balancer that health-checks `/readyz`, and
- their sources publish to JetStream.

Leave it off for a single instance: there it takes every source out of service, including sources whose output needs no connection, instead of letting those keep serving. When every instance loses the broker at once, all of them fail readiness, and the balancer has no target either way.

Size `disconnect_grace` as described under [NATS connections](#nats-connections). `hookgate_ready` reports what `/readyz` answers.

## Relays

[hookgate-relay](relay.md) scales the same way on the consumer side: run several relays with the same configuration on one durable consumer. They pull from it, so no balancer sits in front of them, and the server hands each event to one relay at a time ([Scaling and ordering](relay.md#scaling-and-ordering)).

- **Placement.** Run relays on several nodes (a `global` or replicated service). A relay's `/readyz` on its admin port tells the orchestrator whether it fetches; it routes no traffic.
- **Losing a relay.** Its unsettled events come back to another relay after the consumer's `ack_wait`; a short `ack_wait` brings them back sooner ([Consumer](relay.md#consumer)).
- **Stop grace.** Each relay needs a stop grace above `drain_timeout` plus 15 seconds ([Shutdown](relay.md#shutdown)).
- **Per relay.** `concurrency` and the metrics are per relay: the fleet runs up to relays × `concurrency` handlers at once; sum the relay metrics across instances as for the gateway.
- **Duplicates.** A redelivered event runs its handler again, on whichever relay gets it, so handlers deduplicate on (`Hookgate-Source`, `Hookgate-Event-Id`) as the [contract](contract.md#relay-replays-and-deduplication) requires.
