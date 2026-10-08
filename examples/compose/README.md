# hookgate with Docker Compose

A single hookgate container with a pinned image, a health check, a restart policy, a read-only filesystem and no capabilities. The webhook port is published; the admin port (`/healthz`, `/readyz`, `/metrics`) is bound to loopback only.

| File | Purpose |
|---|---|
| [`compose.yaml`](compose.yaml) | The hookgate service, plus a stand-in `backend` in the `demo` profile. |
| [`hookgate.yaml`](hookgate.yaml) | One GitHub source: hex HMAC-SHA256 with a `sha256=` prefix, forwarded to `http://backend:80/`. |
| [`.env.example`](.env.example) | The secrets Compose reads into the container's environment. |

## Run it

Needs Docker with Compose v2, `curl` and `openssl`.

```sh
sed "s/^GITHUB_WEBHOOK_SECRET=.*/GITHUB_WEBHOOK_SECRET=$(openssl rand -hex 32)/" .env.example > .env
docker compose --profile demo up -d --wait
```

Send a signed request and a forged one:

```sh
. ./.env
BODY='{"zen":"Keep it logically awesome."}'
SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$GITHUB_WEBHOOK_SECRET" -hex | awk '{print $NF}')

curl -i localhost:8080/github -H "X-Hub-Signature-256: sha256=$SIG" --data-binary "$BODY"     # 200, echoed by the backend
curl -i localhost:8080/github -H "X-Hub-Signature-256: sha256=$SIG" --data-binary "${BODY}x"  # 401, never forwarded
curl -s localhost:9090/metrics | grep hookgate_requests_total
```

## Use it

1. Point `forward.urls` in `hookgate.yaml` at your upstream and add a source per sender ([recipes](../../README.md#recipes), [configuration reference](../../docs/configuration.md)).
2. Put every secret in `.env` and reference it under `environment:` in `compose.yaml`. Keep `.env` out of version control.
3. Check the file before deploying: `docker compose run --rm hookgate validate`.
4. Start without the stand-in backend: `docker compose up -d --wait`.
5. Terminate TLS in your reverse proxy and route the webhook paths to port `8080`. Scrape `/metrics` from the host or a sidecar on the same network.

Upgrade by changing the image tag and digest, then `docker compose up -d --wait`. On stop, hookgate keeps serving for `drain_delay` and then gives in-flight requests up to `shutdown_timeout` (30 s by default); `stop_grace_period` stays above their sum.

## Clean up

```sh
docker compose --profile demo down
```
