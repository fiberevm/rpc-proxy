# Request caching policy

The cache reduces upstream work while preserving the proxy's read consistency contract. It stores RPC result bytes, never request IDs or whole batch responses. Each batch item selects its target from the shared request snapshot and receives its own response envelope.

## Implemented

| Requests | Policy | Reason |
| --- | --- | --- |
| `eth_chainId` | Serve configured `chain_id` locally | Identity is supplied by configuration and checked against upstreams at startup. No request-time Redis or provider dependency. |
| Solana `getGenesisHash` | Serve configured `genesis_hash` locally | Identifies the configured cluster; providers are checked at startup. |
| `eth_blockNumber` | Serve the accepted head number locally | Already implemented. Requires a fresh snapshot and the final reorg fence. |
| `eth_getBalance`, `eth_getStorageAt`, `eth_getTransactionCount`, `eth_getCode`, `eth_call`, `eth_getProof` | Cache successful results after pinning to an exact block hash | These supported state reads use EIP-1898 with `requireCanonical: true`. Complete transformed parameters distinguish addresses, storage slots, calls, and overrides. |
| `eth_getBlockByHash`, `eth_getBlockTransactionCountByHash`, `eth_getTransactionByBlockHashAndIndex`, `eth_getUncleByBlockHashAndIndex`, `eth_getUncleCountByBlockHash` | Cache successful, non-null results by hash and parameters | Full-transaction flags and indices are part of the key. Supported number-based variants first become hash-based requests. |
| `eth_getLogs` with `blockHash`, or a range reduced to one exact block | Cache successful results, including empty arrays | The block hash fixes the queried block; the full filter remains part of the key. |
| `eth_getTransactionReceipt` | Cache non-null receipts whose inclusion block is verified on the accepted branch | Follow exact parent hashes from the accepted head, bounded by `reorg_depth` and `head_request_timeout`. This includes receipts with failed execution status. Errors and null lookups are never retained. |

Hash-pinned state reads follow [EIP-1898](https://eips.ethereum.org/EIPS/eip-1898); block-hash log filters follow [EIP-234](https://eips.ethereum.org/EIPS/eip-234). Static identity methods are described in the [Ethereum JSON-RPC API](https://ethereum.org/developers/docs/apis/json-rpc/#eth_chainid) and [Solana getGenesisHash reference](https://solana.com/docs/rpc/http/getgenesishash).

## Invalidation and capacity

RPC result keys include the chain name, transformed method and parameters, and accepted head's reorg epoch. Hash-pinned reads also include their target hash. Receipt requests instead use the transaction hash in their parameters, with the verified inclusion block retained in the receipt. A normal new head changes `latest` keys but preserves verified receipts. A recorded reorg changes the epoch, including a return to a previously seen hash. Historical entries may be reused within that epoch, but numeric selectors must first select their hash through the accepted snapshot's parent links. This walk is bounded by `reorg_depth` and `head_request_timeout`; older history requires an explicit hash. A number equal to the accepted height uses the snapshot directly.

A receipt at the accepted head or its direct parent needs no additional provider read for inclusion verification. Older receipt blocks require a bounded walk through exact parent hashes. The first proof may fetch up to `reorg_depth - 1` ancestor headers; these immutable headers share the same capacity budget and coalesce concurrent lookups across receipts. Knowing a block hash or querying its height alone does not prove membership in the accepted branch. Cached ancestor headers are reusable across epochs, but their links to each request's accepted head must still be checked.

Every receipt request requires a fresh, non-pending accepted head. An upstream receipt above that pinned height is treated as not yet visible, with provider failover before returning null. Receipts at or below the ceiling remain uncached if they are beyond the ancestry limit, on another branch, or their ancestry is unavailable. Once verified, an entry may remain cached beyond the admission depth as the chain advances, until expiry or a reorg. A cached receipt above a caller's older snapshot becomes null for that caller without discarding the positive entry. The height ceiling applies with caching disabled too; it does not prove canonical inclusion or finality.

The gateway checks admission before consulting the cache and performs the shared Redis fence before writing the entire response or batch. All receipt responses participate, including nulls, unverified uncached results, and cache hits. A cache hit cannot bypass pending recovery, a stale head, an unavailable store, or a reorg during the request. Results loaded during a subsequently recorded reorg remain under the old epoch and cannot satisfy a new-epoch request. This depends on the coordinator recording reorgs and on intact shared Redis state; it does not prove finality or detect an unobserved provider fork.

The cache is local to each process, uses least-recently-used eviction, and has both entry-count and byte limits. TTL bounds retention; it is not permission to serve an older `latest` result. Expired entries are removed when read or evicted under capacity pressure. Errors, absent/malformed result bytes, null results, and oversized responses are not retained. Empty log arrays, `"0x"`, and `"0x0"` are valid cacheable results.

Identical misses share one upstream routing operation. Receipt flights additionally include the pinned head hash: the same transaction can be null at height 100 and visible at 101, so those uncached answers must not share a flight. Verified positive receipt cache keys remain independent of ordinary head advancement. Each waiter uses its own context; shared work has a separate deadline bounded by `server.request_timeout`. Canceling one client cannot abort another client's request. Each waiter still performs its own final shared-store fence. Disabling the cache also disables this coalescing.

## Deferred candidates

| Requests | Current behavior | What would make caching appropriate |
| --- | --- | --- |
| `net_version` | Forward | Add a separately configured or startup-verified network ID. Do not infer it from the signing chain ID. |
| `web3_clientVersion`, Solana `getVersion` | Forward | Decide whether to expose a proxy identity or explicitly cache provider metadata with a short TTL. The current results describe an upstream. |
| Multi-block `eth_getLogs` and `eth_feeHistory` | Forward with boundary checks | Cache only after a separate range/finality policy accounts for the current boundary-check limitations and provider-dependent fee-history availability. |
| Solana state and simulation methods | Forward with the existing slot floor | `minContextSlot` is a lower bound, not a fixed bank. A cache needs an explicit allowed-age/commitment policy and context-slot validation. |
| Solana `getBlock`, `getBlockTime` | Forward | Finalized-only, validated non-null historical results are candidates for a later method-specific cache. |
| Solana `getEpochSchedule` | Forward | A startup-verified cluster constant is a candidate for local serving. |
| Solana `getHealth`, `getBlockCommitment` | Forward | Provider health and confirmation progress are live observations. |
| Gas-price suggestions and other unregistered methods | Preserve strict-registry rejection | Supporting them requires an explicit freshness contract and a separate registry change. |
| Writes, filters, subscriptions, pending reads | Preserve existing handling | These are stateful, live, or rejected; they must not enter the result cache. |

The distinction between network ID and chain ID is documented by [EIP-695](https://eips.ethereum.org/EIPS/eip-695). Solana exposes context and `minContextSlot` in methods such as [getBalance](https://solana.com/docs/rpc/http/getbalance).

## Measuring savings

Use `rpc_proxy.cache.request` by `chain`, `method`, and `outcome` together with `rpc_proxy.upstream.request`. `hit` requests avoid the routed upstream read and its availability probe. `shared` identifies requests participating in a coalesced load, including its first caller. `miss` also includes successful responses that are deliberately not retained, such as null or oversized results. Static calls avoid request-time Redis and provider work entirely.

Receipt hits avoid both the transaction lookup and ancestry reads. `rpc_proxy.cache.receipt_unverified` counts non-null receipts whose inclusion could not be established during cache admission. Such loads still return their upstream result without retaining it.

Start with popular balance, code, and contract reads at `latest`. A response-cache hit on an older numeric block still pays for verified parent traversal; caching that mapping safely is a separate optimization. Multiple proxy instances warm their own caches. Shared response caching in Redis may be useful later, but adds serialization, network work, and another capacity budget to the head-store dependency.
