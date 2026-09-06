# Deployment templates

Kubernetes, Render, and Railway run the complete Go service, including background head coordination and WebSocket subscriptions. Vercel provides an optional HTTP forwarding endpoint for an already-running backend.

## Container configuration

The Docker image includes [deploy/config.yaml](../deploy/config.yaml) at `/etc/rpc-proxy/config.yaml`. It starts with two Ethereum mainnet providers, HTTP polling, a 64 MiB process-local cache, and Datadog disabled. Supply these variables:

| Variable | Value |
| --- | --- |
| `REDIS_URL` | Redis or Valkey connection URL; use `rediss://` when TLS is required. |
| `ETHEREUM_PROVIDER_A_HTTP_URL` | First Ethereum mainnet provider's HTTP RPC URL. |
| `ETHEREUM_PROVIDER_B_HTTP_URL` | Second Ethereum mainnet provider's HTTP RPC URL. |
| `PORT` | Set to `8080` on Render and Railway so platform routing matches the configured listener. The Go server reads its port from YAML. |

Use providers that support the EVM capability probes described in the main README. Missing environment values fail startup. Edit the deployment config to add chains, header-based credentials through `header_envs`, or optional `websocket_url_env` values, then rebuild the image. You can also mount a complete config at `/etc/rpc-proxy/config.yaml` without rebuilding.

All replicas must use the same Redis state and `key_prefix`. Use a persistent store with `noeviction`; head and reorg records must not be evicted as cache entries. Persistence alone does not guarantee that a failover or restore preserves every acknowledged write. Pause traffic if Redis state is lost or rolled back and assess recovery before resuming reads.

Only port `8080` belongs on the client ingress. The admin listener on `8081` exposes readiness and redacted status to private probes. Set ingress authentication, rate limits, request-body limits, and upload timeouts before sharing a public endpoint. Enable WebSocket upgrades and suitable connection timeouts for `/ws/{chain}`.

## Kubernetes

[deploy/kubernetes.yaml](../deploy/kubernetes.yaml) creates two proxy replicas and a ClusterIP Service. It expects an existing Redis or Valkey service, reachable from the cluster, and a Secret named `rpc-proxy-secrets` in the deployment namespace.

Build and push an image to your registry, replacing this example image name and tag with your own:

```sh
docker build -t ghcr.io/your-org/rpc-proxy:your-tag .
docker push ghcr.io/your-org/rpc-proxy:your-tag
```

Replace `image` in the manifest with that exact image reference. Add `imagePullSecrets` to the Pod spec if your registry requires authentication. Build for your cluster's CPU architecture when it differs from your local machine.

Create an ignored `.env` file containing the three required credential variables, one `KEY=value` per line. Then create the Secret and apply the manifest:

```sh
kubectl create secret generic rpc-proxy-secrets --from-env-file=.env
kubectl apply -f deploy/kubernetes.yaml
kubectl rollout status deployment/rpc-proxy
kubectl port-forward service/rpc-proxy 8080:8080
```

Use the same namespace for both commands, adding `--namespace` if needed. Route your existing ingress to Service `rpc-proxy`, port `8080`. Startup/liveness probes check `/health/live`; readiness checks `/health/ready` on the separate admin container port. After updating the Secret, restart the deployment with `kubectl rollout restart deployment/rpc-proxy`.

## Render

[render.yaml](../render.yaml) creates an always-on Docker web service and a private Render Key Value instance. The template selects paid compute plans and disables automatic deploys from upstream pushes.

Click **Deploy to Render** in the README, review the services, and enter both provider URLs when prompted. `REDIS_URL` is linked to the managed store and `PORT` is set to `8080`. Use `/rpc/ethereum` for HTTP and `/ws/ethereum` for WebSockets on the service hostname.

Render's [default TCP health check](https://render.com/docs/health-checks) checks the public listener. It does not verify Redis, head freshness, or reorg readiness. The private admin endpoint remains available on port `8081` for monitoring within the private network; do not set `healthCheckPath: /health/ready` on the public service because that route exists only on the admin listener.

The [Blueprint specification](https://render.com/docs/blueprint-spec) describes plan, scaling, and environment-variable overrides.

## Railway

[railway.json](../railway.json) configures the Docker build, one replica, and restart-on-failure behavior. The README button opens Railway's project creation flow; it is not a published multi-service template link.

1. Fork this repository, then choose **Deploy from GitHub repo** in Railway and select your fork.
2. Add a Redis service in the same project, or use an existing Redis/Valkey store. Configure persistent storage and `noeviction` for the shared head state.
3. Set `REDIS_URL` on the proxy to `${{Redis.REDIS_URL}}` when the database service is named `Redis`, or supply your existing store's URL.
4. Set both provider URL variables and `PORT=8080` on the proxy.
5. Deploy and generate a domain targeting port `8080`. Use `/rpc/ethereum` and `/ws/ethereum` on that hostname.

The template leaves Railway's public HTTP health check unset because readiness is served only on private port `8081`. Deployment activation therefore does not guarantee RPC readiness; monitor the private admin endpoint. Do not enable app sleeping for the persistent coordinator.

For a reusable stack button, [create a Railway template](https://docs.railway.com/templates/create) containing the proxy, Redis, and these variables, then replace the README button's `https://railway.com/new` URL with Railway's generated template URL. `railway.json` configures a service; it does not create the database, variables, domain, or a Railway template ID.

## Vercel

The **Deploy with Vercel** button deploys [deploy/vercel](../deploy/vercel/README.md). It requires `RPC_PROXY_ORIGIN`, the HTTPS origin of a backend already running on Render, Railway, or Kubernetes.

Vercel's request-scoped functions are unsuitable for this service's persistent coordinator and WebSocket server. The template instead emits an [external HTTP rewrite](https://vercel.com/docs/rewrites) for `POST /rpc/{chain}` with caching disabled. Use the backend directly for WebSocket subscriptions. Authentication and rate limiting remain ingress responsibilities.

When forking or relocating these templates, update the repository URLs in the Render and Vercel buttons. The linked repository must contain these files before the buttons can deploy them.
