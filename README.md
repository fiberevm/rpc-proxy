![RPC Proxy — by Fiber](docs/assets/rpc-proxy-banner.png)

# RPC Proxy

Made by [Fiber](https://github.com/fiberevm).

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https%3A%2F%2Fgithub.com%2Ffiberevm%2Frpc-proxy)
[![Deploy on Railway](https://railway.com/button.svg)](https://railway.com/new)

Render deploys the full service with Redis. Railway opens project setup. See the [deployment guide](docs/deployment.md), including the [Kubernetes template](deploy/kubernetes.yaml).

**Exact-block EVM reads across RPC providers.**

RPC providers can lag behind each other. RPC Proxy picks a verified block for each request. Supported EVM state reads use that exact block hash.

If a provider cannot serve it, the proxy tries another or waits for it to catch up. It returns an error if none can serve it. It never silently uses an older block.

Supports EVM and Solana, with many chains and providers. Redis shares the accepted head across proxy instances.

[Quick start](#run-locally) · [Deploy](docs/deployment.md) · [Consistency](#consistency-contract) · [Reorgs](#reorg-protection) · [Operations](#operational-notes) · [Tests](#verification)

## Run locally

You need Go 1.27.1 and Redis or Valkey.

```sh
git clone https://github.com/fiberevm/rpc-proxy.git
cd rpc-proxy
cp config.example.yaml rpc-proxy.yaml
```

Edit `rpc-proxy.yaml`:

- Keep the chains you need.
- Set `redis.url`. The example uses `redis://127.0.0.1:6379/0`.
- Replace the example `http_url` and `websocket_url` values with your provider URLs.
- Put any auth headers directly in `headers`.
- Set `datadog.enabled: false` if you do not have a Datadog Agent.

No environment variables are needed. Keep secrets in your local config. It is ignored by Git and Docker. Never put real keys in the example file.

Start Redis, then run:

```sh
make run
```

Check readiness:

```sh
curl --fail http://127.0.0.1:8081/health/ready
```

Read a balance:

```sh
curl http://127.0.0.1:8080/rpc/ethereum \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x0000000000000000000000000000000000000000","latest"]}'
```

See [config.example.yaml](config.example.yaml) for all settings. Restart after config changes.

## QuickNode multichain

Use [config.quicknode.example.yaml](config.quicknode.example.yaml) to configure several chains with one QuickNode RPC URL. Enable Multichain on that endpoint in the QuickNode dashboard first, following [QuickNode's multichain guide](https://www.quicknode.com/guides/quicknode-products/how-to-use-multichain-endpoint).

```yaml
quicknode:
  http_url: "https://your-endpoint.base-mainnet.quiknode.pro/YOUR_TOKEN/"

chains:
  - name: ethereum
    family: evm
    chain_id: "0x1"
    quicknode_network: mainnet
  - name: base
    family: evm
    chain_id: "0x2105"
    quicknode_network: base-mainnet
```

Keep the usual Redis, timing, and telemetry settings. The shared URL can come from any standard QuickNode network endpoint. Each `quicknode_network` is the exact network slug from your dashboard; Ethereum mainnet uses `mainnet`, Solana uses `solana-mainnet`, and BNB Smart Chain uses `bsc`. Chains still need their configured EVM chain ID or Solana genesis hash, which startup checks against the provider.

The proxy generates one upstream named `quicknode` per opted-in chain, reusing the endpoint name, token, and query parameters. It also generates EVM WebSocket URLs. Avalanche `avalanche-mainnet` and `avalanche-testnet` use `/ext/bc/C/rpc` for HTTP and [`/ext/bc/C/ws` for WebSocket](https://www.quicknode.com/docs/avalanche/eth_subscribe). Other custom domains or network-specific URL paths should use explicit `upstreams`.

Set `quicknode.http_url_env` instead of `http_url` to load the shared URL from an environment variable. Optional `id`, `max_concurrency` (per chain, default 256), and `websocket_disabled` settings apply to the generated upstreams. Explicit upstreams remain available alongside QuickNode and must have distinct IDs. Chains without `quicknode_network` continue using their explicit upstreams.

## Client method tracking

Declare client labels in your config:

```yaml
clients: [wallet, indexer]
```

Send the label as a header or URL query parameter:

```sh
curl http://127.0.0.1:8080/rpc/ethereum \
  -H 'Content-Type: application/json' \
  -H 'X-RPC-Client: wallet' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}'
# URL-only clients: http://127.0.0.1:8080/rpc/ethereum?client=wallet
# Browser WebSockets: ws://127.0.0.1:8080/ws/ethereum?client=wallet
```

A nonempty `X-RPC-Client` header takes precedence over `?client=`. Labels must match configuration exactly and use 1–64 lowercase letters, digits, underscores, or hyphens, starting with a letter. Missing labels use `anonymous`; unregistered labels use `unknown`. Both names are reserved. Labels identify applications for telemetry; they do not authenticate callers.

Datadog's `rpc_proxy.client.method` counter has `client`, `chain`, `family`, `method`, and `transport` (`http` or `websocket`) tags. To compare usage, query:

```text
sum:rpc_proxy.client.method{*} by {client,chain,method}.as_count()
```

The bundled [Datadog dashboard](deploy/datadog-dashboard.json) includes a client filter and RPC method usage chart.

Each parsed RPC item on a configured chain counts once, including batch items, notifications, static replies, cache hits, rejected calls, and calls blocked by Redis failure. Upstream retries and WebSocket head notifications do not count as extra client requests. These are attempted-method counts, not success counts. Malformed envelopes and unknown chains are excluded. Supported methods retain their names; rejected writes, unknown methods, and invalid requests use the bounded `write`, `unknown`, and `invalid` method tags.

Structured `rpc method requested` logs carry the same fields, including when Datadog is disabled. HTTP request metrics and request traces also carry the client tag, as do WebSocket connection metrics. Client labels never become cache keys or get forwarded to upstream providers.

## Consistency contract

- **EVM:** supported state reads use a block hash with `requireCanonical: true`.
- **Solana:** supported reads use `minContextSlot`. This sets a minimum slot, not an exact snapshot.
- **Batches:** all EVM `latest` reads in a batch use one snapshot. Later requests may use newer heads. Solana items share a slot floor, but can return different slots and forks.
- **Latest:** the newest valid head seen by your trusted providers. Not a promise about the whole network.
- **Read-only:** writes, unknown methods, and EVM `pending`, `safe`, and `finalized` reads are rejected.

EVM state methods include `eth_getBalance`, `eth_getStorageAt`, `eth_getTransactionCount`, `eth_getCode`, `eth_call`, and `eth_getProof`. Their numeric selectors and block-number lookup methods follow parent hashes from the accepted snapshot, bounded by `reorg_depth` and `head_request_timeout`. Numbers above the snapshot or outside that ancestry window fail closed; use an explicit block hash for older state history. A number equal to the accepted height uses its accepted hash directly. See the [method registry](internal/gateway/transform.go) for the full list.

`eth_getTransactionReceipt` takes one transaction hash and uses the request's pinned head as a height ceiling. A receipt above that height is treated as not yet included; the proxy tries other providers and returns `null` if all eligible replies are null or above the ceiling. A lookup error is not treated as a missing receipt. The cutoff also applies to cache hits and stays fixed if the head advances during the request. Receipt responses report `X-RPC-Consistency: pinned-head-ceiling` and the cutoff head in `X-RPC-Head-Number`/`X-RPC-Head-Hash`. This height check does not prove canonical inclusion or finality.

`eth_getLogs` rewrites `latest`, including omitted bounds, to the request's pinned head. Multi-block queries remain range queries, so old `fromBlock` values do not require an ancestry walk. Numeric bounds above the pin are rejected, and returned logs must fall within the requested range. The proxy checks the pinned head's hash before and after the query. Single-block queries within the ancestry window use a block hash; older single-block numeric queries use the range path. Range reads and fee history report `X-RPC-Consistency: boundary-checked`; they do not promise atomic fork consistency during the query. Solana `getBlock`, `getBlockTime`, and `getBlockCommitment` report `unverified`; batches combining them with slot-floor reads report `mixed-targets`.

The exact-block guarantee assumes trusted providers honor the hash selector; startup probes and response identity checks cannot prove arbitrary returned state cryptographically. The proxy does not make every supported method an exact snapshot read. See the [consistency audit](docs/consistency-audit.md) for the guarantees, fixes, and limits.

## Request caching

`eth_chainId` and Solana `getGenesisHash` return the configured identity without calling Redis or an upstream. Startup still checks provider identities. `eth_blockNumber` already uses the accepted head; it keeps the freshness and reorg checks.

Successful EVM state reads, block-hash lookups, and single-block logs use a bounded cache in each proxy process. Keys include the chain, method, transformed parameters, exact block hash, and reorg epoch. Repeated `latest` reads at one head share results; a new head selects different entries. Concurrent identical misses share one upstream operation, with independent client deadlines.

Non-null transaction receipts are cached after their inclusion block is verified through the accepted head's parent links. This walk is bounded by `reorg_depth` and `head_request_timeout`; ancestor headers share the same bounded cache. Receipt keys use the chain, transaction hash and reorg epoch, so entries survive normal head advancement and are invalidated by recorded reorgs. Receipts whose inclusion cannot be verified retain the existing uncached lookup behavior. No finality wait is required.

Cache hits retain the normal shared-store admission and final reorg checks, including the batch fence. All receipt responses, including nulls and uncached lookups, join that fence. Errors and `null` are never retained. Receipt loads share work only when their pinned heads match; verified positive entries remain reusable across head advancement with the caller's height ceiling reapplied. Log ranges, fee history, and Solana state reads stay uncached. Historical state/block-number lookups require verified parent traversal before consulting the result cache; the accepted height needs no upstream number lookup.

The defaults are 4,096 entries, 64 MiB of retained keys/results/upstream IDs, a 1 MiB entry limit, and five-minute retention. Entry metadata and in-flight responses use additional memory. Set `cache.disabled: true` to disable caching and request coalescing; local responses remain enabled. See [config.example.yaml](config.example.yaml).

`X-RPC-Cache` reports `static`, `local`, `hit`, `miss`, `shared`, or `bypass`; batches with different reported outcomes use `mixed`. `X-RPC-Upstream` on a hit identifies the original provider. Datadog's `rpc_proxy.cache.request` counter is tagged by chain, method, and outcome.

See the [method-by-method cache policy](docs/request-caching.md) for the reasoning and next candidates.

## Reorg protection

A reorg replaces part of the chain. The proxy handles it like this:

1. Record the reorg in Redis. Block new block-based reads on every proxy instance.
2. Check the new branch and find a shared ancestor. The default limit is 64 blocks, set by `reorg_depth`.
3. Publish the verified head and allow new reads. Clients retry against that head.

At equal height, reads stay blocked while providers disagree. A longer valid branch can win. Recovery can also finish if providers agree on the existing head. Missing chain history keeps reads blocked. Recovery beyond the depth limit needs operator help.

Reads already in progress get a final Redis check. If a reorg was recorded after the request chose its head, the results are discarded. The check covers the whole batch, including receipt results and nulls, even if some items finished earlier. Static identity reads do not depend on the head.

For example, a read starts at block **100 / hash A**. The proxy records a switch to B before the final check. The old result is discarded, even if a provider still accepts A. The client gets `-32070` with `retryable: true`. A new read after recovery uses B.

Normal new blocks do not cancel reads. A switch from **A → B → A** still counts as a reorg.

This means **consistent data or an error**, not guaranteed availability or finality. The check cannot undo replies already checked or protect against later reorgs. All instances must use the same Redis state. Stale or rolled-back Redis state breaks this guarantee.

Upgrade all proxy instances and coordinators before relying on reorg protection. Do not clear Redis head keys to bypass a recovery failure.

## API

- `POST /rpc/{chain}`: JSON-RPC reads, batches, and notifications.
- `GET /ws/{chain}`: EVM `newHeads` subscriptions only.
- `GET /health/live`: process health.
- `GET /health/ready`: ready to serve reads.
- `GET /status`: heads and provider status, with secrets hidden.

Health and status use the separate admin port. Readiness fails while the head is stale, Redis is unavailable, or a reorg is pending.

Responses include `X-RPC-Upstream` and `X-RPC-Consistency` where relevant. Successful block-based reads also include a head hash, number, or slot. Mixed-target batches do not claim one shared block.

JSON-RPC errors use HTTP 200:

- `-32070`: consistent state unavailable.
- `-32071`: unsupported read semantics.
- `-32072`: writes disabled.
- `-32073`: chain unavailable.

Notifications return no JSON-RPC body.

## Operational notes

- Use at least two providers per production chain.
- Set `websocket_url` to follow EVM `newHeads`. Only one coordinator per chain opens upstream subscriptions.
- `poll_interval` defaults to `2s`. EVM polls only when a subscription is missing, disconnected, or idle. Solana polls its slots.
- Keep `websocket_idle_timeout` above normal block spacing and below `max_head_age`.
- Provider checks run once at startup. Failed checks need a restart. There are no periodic capability checks.
- WebSocket clients receive accepted heads. The proxy fills gaps and skips duplicate hashes. Unsafe gaps, pending reorgs, and slow clients close the connection. Reconnect with backoff.
- Keep the proxy private and add auth at your ingress. It has no built-in public auth or rate limits.

Metrics and traces go to Datadog. Profiling is optional. The proxy writes JSON logs to stdout. DogStatsD uses `datadog.statsd_address`; APM uses `datadog.agent_address`.

See the [dashboard](deploy/datadog-dashboard.json) and [monitor guide](deploy/datadog-monitors.md). They cover stale heads, provider lag, Redis errors, reorgs, and rejected reads. Import them and set alerts for your chains.

## Verification

```sh
make test
make test-race
make vet
docker build -t rpc-proxy:local .
```

Most tests need no external services. The [Redis tests](internal/head/redis_integration_test.go) are opt-in.
