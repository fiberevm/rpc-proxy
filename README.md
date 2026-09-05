# Fiber RPC Proxy

**Guaranteed read consistency across RPC providers.**

For supported EVM state reads, Fiber RPC Proxy returns data from the exact block hash selected for the request—or a consistency error. A lagging provider cannot silently answer from an older block.

RPC providers do not always agree on the latest block. When one provider sees a transaction's block before another, switching providers can make a balance or contract read appear to go backwards. Fiber RPC Proxy is a read-only, multichain JSON-RPC gateway that tracks the freshest verified head observed from your configured providers and pins supported EVM state reads to that hash. Switching providers does not change the state selected for the request.

- **Exact-state reads:** supported EVM balance, storage, code, transaction-count, contract-call, and proof reads are pinned by block hash, not a provider's interpretation of `latest`.
- **Failover without stale fallback:** try another provider or wait for catch-up; if the target remains unavailable, return a consistency error instead of older state.
- **Consistency across replicas and batches:** fenced Redis coordination shares the accepted head across replicas; every batch captures one snapshot.
- **Subscription-first, latest-only EVM heads:** `newHeads` drives updates, with HTTP polling only as fallback. No `safe`/`finalized` polling or periodic capability revalidation.
- **Solana slot floors:** supported reads receive an accepted `minContextSlot`, not an exact historical-state promise.
- **Observability:** Datadog traces, DogStatsD metrics, structured redacted logs, and private health/status endpoints.

[Quick start](#run-locally) · [Consistency contract](#consistency-contract) · [Operations](#operational-notes) · [Verification](#verification)

This is an initial implementation. The [remaining production acceptance work](#remaining-production-acceptance-work) includes real-provider failover drills and the 1,000 RPS load target; those are not claimed as verified production results.

## Consistency contract

For example, once the proxy accepts block **N** with hash **H**, a `latest` balance or contract read using that snapshot is pinned to **H**. A provider still at **N−1** is not allowed to answer from its older state. The proxy finds a provider that can serve **H**, waits within the request deadline, or returns an error.

- EVM state reads are rewritten to an exact EIP-1898 block hash with `requireCanonical: true`.
- EVM tracks `latest` only. The `safe` and `finalized` tags are rejected with unsupported-consistency errors; explicit historical numbers/hashes remain supported.
- Solana state reads receive a `minContextSlot` equal to or greater than the globally accepted slot.
- A Redis-backed coordinator supplies one accepted snapshot to every proxy replica.
- A request never falls back to a block or slot older than its selected snapshot.
- A JSON-RPC batch uses one snapshot. Separate requests may advance to newer snapshots.
- "Latest" is the freshest valid head observed from configured trusted upstreams. It is not a Byzantine or network-global guarantee.

The service rejects writes, EVM pending-state reads, and unknown methods rather than serving them without the stated guarantee.

The exact-block guarantee applies to supported pinned EVM state reads against trusted upstreams, not every RPC response. Transaction-receipt lookup has no block selector and does not promise snapshot consistency or finality. Solana guarantees a slot floor rather than an exact state snapshot. Chain reorgs can replace an accepted head; consistency does not mean balances can never decrease or that a block is final.

## Request path

1. Startup validates provider identity/capabilities once. The chain coordinator observes all configured providers, verifies EVM linkage, and publishes EVM `latest` (or Solana commitment slots) through a fenced Redis leader.
2. The HTTP handler reads one Redis snapshot for the whole request or batch and transforms every supported state-dependent method against it.
3. Routing probes provider availability for that exact hash or slot floor, prefers the observing provider, retries other eligible providers once, waits for catch-up when necessary, and fails closed at the request deadline.
4. EVM `newHeads` subscribers consume the accepted Redis Stream. Missing headers are backfilled by hash, duplicate hashes are suppressed, and unrecoverable gaps close the connection.

The strict registries live in [internal/gateway/transform.go](internal/gateway/transform.go). Adding a method requires defining its exact consistency transformation and tests; there is intentionally no vendor-method passthrough switch.

EVM state capabilities are checked once at startup, per method, with both a known hash and a nonexistent hash. A successful balance probe does not authorize contract calls or proofs. Providers that ignore the hash selector are excluded from that method; `/status` lists `pinned_state_methods` for each provider. There is no periodic capability revalidation. A provider or method that fails startup validation remains ineligible until the proxy is restarted; ordinary request failures still drive circuit breaking and failover.

`eth_getTransactionReceipt` is supported as a transaction-hash lookup with exactly one 32-byte transaction hash parameter. Receipt objects, including failed-transaction receipts and chain-specific fields, are preserved. The proxy tries other healthy, validated providers on `null` and returns `null` only when every attempted provider supporting the method reports no receipt; a transport or malformed-response failure is not treated as absence. Pending and unknown transactions can therefore return the standard `null` result. The method has no block selector ([Ethereum JSON-RPC specification](https://ethereum.org/developers/docs/apis/json-rpc/#eth_gettransactionreceipt)), so it does not require EIP-1898 or claim an accepted-head snapshot/finality guarantee. Receipt-only responses carry `X-RPC-Consistency: transaction-hash-lookup`; mixed receipt/state batches carry `mixed-targets` without a single head header.

Solana state methods supported in v1 are `getAccountInfo`, `getBalance`, `getMultipleAccounts`, `getProgramAccounts`, `getTokenAccountsByOwner`, `getTokenAccountsByDelegate`, `getLatestBlockhash`, `getFeeForMessage`, `isBlockhashValid`, `simulateTransaction`, `getStakeMinimumDelegation`, `getSlot`, `getSlotLeader`, `getEpochInfo`, `getBlockHeight`, `getTransactionCount`, and `getSignaturesForAddress`. Methods without a documented slot-floor parameter, including `getTokenAccountBalance`, `getTokenSupply`, `getTokenLargestAccounts`, and `getSupply`, are rejected.

The effective Solana floor is the maximum of the accepted slot and the caller's `minContextSlot`, without floating-point conversion. Context-bearing responses must include a sufficient `context.slot`. `getProgramAccounts` requests context internally and removes the wrapper only when the caller requested the default array shape. Context-free methods rely on the upstream honoring the documented floor; `getSlot` and `getEpochInfo` also have their returned slots verified. See the official [program accounts](https://solana.com/docs/rpc/http/getprogramaccounts), [token balance](https://solana.com/docs/rpc/http/gettokenaccountbalance), and [slot](https://solana.com/docs/rpc/http/getslot) interfaces.

Diagnostic head headers describe successful requests' actual targets, including historical EVM blocks and caller-supplied Solana slot floors. A batch with different targets receives `X-RPC-Consistency: mixed-targets` and no misleading single head number/hash. Static methods have no head headers; explicit hash reads omit a number when it is unknown.

JSON-RPC server errors use `-32070` for consistency unavailable, `-32071` for unsupported consistency semantics, `-32072` for writes/signing disabled, and `-32073` for an unknown chain. Valid JSON-RPC calls return HTTP 200; notification-only requests return no JSON-RPC body.

## Run locally

Requirements are Go 1.27.1 and Redis/Valkey.

Clone the repository and copy the [example configuration](config.example.yaml):

```sh
git clone https://github.com/fiberevm/rpc-proxy.git
cd rpc-proxy
cp config.example.yaml rpc-proxy.yaml
```

Edit `rpc-proxy.yaml` for the chains you need. The example includes Ethereum and Solana; remove the Solana entry for an EVM-only deployment. Set `datadog.enabled: false` when running without a Datadog Agent.

Set `redis.url` directly in the config (the example uses `redis://127.0.0.1:6379/0`). Replace each upstream's `http_url` and, for EVM, `websocket_url` with your actual provider endpoints. The `*.example.invalid` URLs are placeholders. If a provider requires authentication headers, put their values directly under that upstream's `headers` mapping.

All example settings are supplied through the config file; no environment-variable setup is required. Start Redis/Valkey, then run:

```sh
make run
```

Keep real credentials only in your local `rpc-proxy.yaml`, which is excluded from Git and Docker build contexts. Do not put them in the committed example file, and restrict access to credential-bearing configs. There is no hot reload; restart the process after configuration changes. Providers that fail startup validation require a restart before they can be retried.

HTTP JSON-RPC is available at `POST /rpc/{chain}`. EVM `newHeads` is available at `GET /ws/{chain}`. Health and redacted status endpoints listen on the separately configured admin address.

Check readiness before sending traffic:

```sh
curl --fail http://127.0.0.1:8081/health/ready
```

```sh
curl http://127.0.0.1:8080/rpc/ethereum \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x0000000000000000000000000000000000000000","latest"]}'
```

To poll a transaction receipt, replace the example hash with the transaction's hash:

```sh
curl http://127.0.0.1:8080/rpc/ethereum \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"]}'
```

## Operational notes

- Production chains should configure at least two upstreams.
- EVM head tracking is subscription-first: configure `websocket_url` on every upstream to subscribe to `newHeads`. Only the elected coordinator opens these upstream subscriptions. A confirmed subscription bootstraps its current head once over HTTP; subsequent notifications are verified by hash before acceptance. Hash verification, ancestry backfill, request-time availability checks, and normal client reads still use HTTP.
- `poll_interval` (default `2s`) is the per-upstream EVM `latest` fallback interval, not an additional poll alongside healthy subscriptions. Providers without WebSockets, with rejected/disconnected subscriptions, or without a new verified head within `websocket_idle_timeout` (default 75% of `max_head_age`) are polled. Disconnected streams reconnect with backoff; duplicate headers and unrelated frames do not extend the idle deadline. Configure the idle timeout above normal block spacing and below `max_head_age`, leaving room for the fallback interval and request latency.
- EVM never polls `safe` or `finalized`. Readiness requires only a fresh `latest` head and an eligible provider. Legacy safe/finalized Redis entries are ignored by EVM reads, readiness, status, and head-age metrics; no Redis flush is needed. Solana continues to poll all three commitments at `poll_interval`.
- `head_request_timeout` (default `2s`) bounds each coordinator head query and WebSocket setup independently of poll frequency. Startup identity/capability validation runs once per replica, with no periodic revalidation.
- When upgrading an older configuration, remove `commitment_poll_interval` and `validation_interval`; these settings no longer exist and strict YAML validation rejects them. Restart the proxy to apply the new configuration.
- EVM upstreams that fail the per-method EIP-1898 probes remain unavailable for those state reads.
- DogStatsD metrics are sent over UDP to `datadog.statsd_address`.
- `coordinator.subscription_active` reports active upstream subscriptions; `coordinator.latest_poll` counts fallback latest probes, and `coordinator.subscription_error` distinguishes acknowledgment and idle failures. These use bounded chain/upstream/failure-class tags.
- Datadog APM and optional continuous profiling use `datadog.agent_address`.
- Upstream URLs and headers are never included in status output or metric tags.
- Set custom headers directly in each upstream's `headers` mapping in your local config. Never commit real endpoint credentials or authentication headers.
- YAML field names are checked strictly; unknown fields and multiple documents fail startup.
- Availability checks are coalesced and independently cancelable. Negative checks expire after 75 ms, and each chain's cache is bounded to 4,096 entries.
- Redis publishes every accepted fork transition, including a return to an earlier hash. Duplicate suppression belongs to each live WebSocket connection, not the shared stream.
- Subscriptions capture an atomic cursor/header checkpoint before acknowledgment. Trimmed checkpoints and unverifiable backfill close the connection; clients should reconnect with backoff. Each connection is limited to 128 subscriptions and 65,536 distinct delivered hashes, after which it must reconnect to preserve bounded memory and connection-scoped deduplication.
- Leadership renewal runs independently of slow ancestry validation. An unreachable provider is not treated as evidence authorizing a rollback. Solana floors never decrease because another provider is lagging.
- Upstream redirects are rejected so credential-bearing custom headers cannot be forwarded to another endpoint. Logs redact full endpoint URLs, including API keys embedded in paths; status exposes a local `metric_submission_errors` counter.

See the [Datadog dashboard](deploy/datadog-dashboard.json) and [monitor guide](deploy/datadog-monitors.md) for the initial dashboards and alerts. Run behind private networking and ingress authentication; the proxy does not provide public-endpoint authentication or rate limiting.

## Verification

```sh
make test
make test-race
make vet
go test ./internal/gateway -run '^$' -bench BenchmarkGatewayAcceptedBlockNumber -benchmem
docker build -t rpc-proxy:local .
```

The normal suite has no external Redis dependency. The [Redis integration test](internal/head/redis_integration_test.go) is opt-in; its source documents the separate test-only setup for exercising a real Redis instance.

Short fuzz checks:

```sh
go test ./internal/jsonrpc -run '^$' -fuzz FuzzParseEnvelope -fuzztime=10s
go test ./internal/gateway -run '^$' -fuzz FuzzTransformEVM -fuzztime=10s
go test ./internal/gateway -run '^$' -fuzz FuzzTransformSolana -fuzztime=10s
```

## Remaining production acceptance work

- Run a representative read mix at 1,000 RPS with colocated Redis, real upstreams, and Datadog enabled. The local `eth_blockNumber` microbenchmark is not evidence for the 5 ms p99 overhead target.
- Verify provider compatibility per chain and commitment, and exercise real upstream WebSocket disconnects and multi-replica coordinator failover in staging.
- EVM ancestry recovery is bounded by `reorg_depth` and requires upstream access to parent headers. A gap beyond that window fails closed and needs operator recovery; automatic long-outage resynchronization is not implemented.
- Range logs and fee history verify boundary hashes before and after the read. This is not an atomic backend snapshot if one upstream endpoint itself switches forks between those calls; exact per-block log fan-out is not implemented.
- Import the dashboard, configure distribution percentiles and chain-specific monitor thresholds, and verify alert delivery. These artifacts do not provision Datadog automatically.
