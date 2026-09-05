![RPC Proxy — by Fiber](docs/assets/rpc-proxy-banner.png)

# RPC Proxy

Made by [Fiber](https://github.com/fiberevm).

**Guaranteed read consistency across RPC providers.**

RPC providers can lag behind each other. RPC Proxy picks a verified block for each request. Supported EVM state reads use that exact block hash.

If a provider cannot serve it, the proxy tries another or waits for it to catch up. It returns an error if none can serve it. It never silently uses an older block.

Supports EVM and Solana, with many chains and providers. Redis shares the accepted head across proxy instances.

[Quick start](#run-locally) · [Consistency](#consistency-contract) · [Reorgs](#reorg-protection) · [Operations](#operational-notes) · [Tests](#verification)

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

## Consistency contract

- **EVM:** supported state reads use a block hash with `requireCanonical: true`.
- **Solana:** supported reads use `minContextSlot`. This sets a minimum slot, not an exact snapshot.
- **Batches:** all `latest` reads in a batch use one snapshot. Later requests may use newer heads.
- **Latest:** the newest valid head seen by your trusted providers. Not a promise about the whole network.
- **Read-only:** writes, unknown methods, and EVM `pending`, `safe`, and `finalized` reads are rejected.

EVM state methods include `eth_getBalance`, `eth_getStorageAt`, `eth_getTransactionCount`, `eth_getCode`, `eth_call`, and `eth_getProof`. Explicit past block numbers and hashes are also supported. See the [method registry](internal/gateway/transform.go) for the full list.

`eth_getTransactionReceipt` takes one transaction hash. The proxy tries other providers on `null`. A lookup error is not treated as a missing receipt. Receipt reads do not promise the same snapshot as state reads or prove that a block is final.

Log ranges and fee history check boundary hashes. They cannot promise one snapshot if a provider changes forks during the read.

## Reorg protection

A reorg replaces part of the chain. The proxy handles it like this:

1. Record the reorg in Redis. Block new block-based reads on every proxy instance.
2. Check the new branch and find a shared ancestor. The default limit is 64 blocks, set by `reorg_depth`.
3. Publish the verified head and allow new reads. Clients retry against that head.

At equal height, reads stay blocked while providers disagree. A longer valid branch can win. Recovery can also finish if providers agree on the existing head. Missing chain history keeps reads blocked. Recovery beyond the depth limit needs operator help.

Reads already in progress get a final Redis check. If a reorg was recorded after the request chose its head, the results are discarded. The check covers the whole batch, even if some items finished earlier. Static reads and receipt lookups keep their separate rules.

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
