# Consistency audit — 2026-09-06

The proxy can provide one exact EVM block state for supported hash-pinned reads across trusted providers. It does **not** provide one exact state across every supported RPC method or across separate moving-head requests. Solana's minimum-slot contract, transaction receipt lookups, and multi-block range methods have weaker semantics. Following the audit, receipt visibility was capped at the request's pinned height, and log ranges were explicitly kept as range queries with `latest` fixed to that same pinned height.

## Defects corrected

| Finding | Failure scenario | Correction |
| --- | --- | --- |
| Numeric EVM selectors were chosen independently from providers | A batch reads `latest` at accepted block 100/A while a numeric `0x64` lookup selects 100/B from another fork. Both reads could succeed without a recorded reorg. | Select the accepted height directly from the snapshot. For past numbers, follow exact parent hashes from that snapshot and validate height continuity. Reject future numbers, unavailable ancestry, and distances beyond `reorg_depth`. Bound traversal by `head_request_timeout`. |
| Availability cache omitted validation constraints | A successful explicit-hash probe populated the same key as a coordinator probe for that hash with a different height or parent. The cached result bypassed the stricter header check. | Include commitment, height, and parent in the cache/coalescing key, and use that same key for invalidation. |
| Routed EVM responses could contradict the pin | The availability probe succeeds, but the actual read returns `null` state, a different block/transaction inclusion hash, or logs from another block. Successful malformed results could be cached. | Reject null pinned state/block/count/log results and contradictory block, transaction, and single-block log identities. Retry eligible providers; fail closed if none succeeds. Preserve legitimate null indexed transaction lookups. |
| Range responses overstated their guarantee | `eth_getLogs` ranges and `eth_feeHistory` returned `exact-block-hash` although only their boundaries were checked. | Report `boundary-checked`; retain the existing pre/post checks and reorg fence. |
| Unpinned Solana reads inherited a batch's slot guarantee | A batch combines `getAccountInfo` with `getBlock`, `getBlockTime`, or `getBlockCommitment`. Only the first item enforces a slot floor. | Report `unverified` for the unpinned lookup alone and `mixed-targets` for combined batches. |

Historical numeric state/block selection is deliberately more restrictive. Previously, returning some provider's block at that height was treated as sufficient. The proxy now returns an error when it cannot establish membership in the selected branch. Callers needing older state history can supply the desired block hash explicitly. Log ranges have a different contract: they preserve numeric ranges, replace `latest` with the pinned height, check the pinned head before/after the call, and reject returned logs outside the requested bounds. They do not require a full ancestry walk for old range bounds.

## Invariants inspected

- A single shared-store snapshot is selected before a batch executes. Provider retries retain the transformed hash or minimum slot.
- Supported EVM state methods send an EIP-1898 hash object with `requireCanonical: true`; they never retry using `latest` or a lower height. Startup probes reject methods that successfully answer an unknown block hash.
- One fenced coordinator publishes each chain's accepted head. Leadership tokens prevent expired leaders from publishing or beginning recovery. Reorg epochs are recorded before recovery and survive leader changes.
- The final shared-store check covers all pinned batch items, including items that finished early, cache hits, and all receipt responses, including nulls. A pending or changed reorg epoch invalidates their responses. Ordinary head advancement does not invalidate an earlier snapshot.
- Response cache keys include the chain, complete transformed arguments, block hash where applicable, and reorg epoch. Caller IDs are outside cached payloads. Errors and null results are not retained; coalesced callers retain independent deadlines and final fences.
- Receipt cache inclusion is proven by accepted-head ancestry. All receipt results obey the request's pinned height ceiling. Unproven receipts at or below it retain weaker uncached lookup semantics. Flights at different pinned heads are separated so a future-to-null decision cannot leak into a newer request.
- Solana accepted slot floors do not decrease. Context-bearing responses below the requested floor are rejected. Slot equality is not fork identity.
- WebSocket delivery reads the shared accepted-head stream, detects trimmed checkpoints, and checks parent continuity when filling gaps. It is a head event stream, not a transactional state snapshot or a finality signal.

## Limits that remain part of the contract

1. **Separate requests may select newer heads.** Use one EVM batch for related `latest` reads, or reuse an explicit hash for a workflow spanning requests. A reorg can still cause an error.
2. **Solana is not an exact snapshot API here.** Different providers can return different banks above `minContextSlot`, even in one batch. The response header describes a minimum, not the returned slot or a shared fork.
3. **Receipts are height-capped transaction lookups.** No returned receipt can exceed the request's pinned height. An uncached receipt can still belong to another branch, and a null result is not a proof of absence from the pinned branch. Only cache retention requires ancestry proof; every receipt response receives the final reorg fence. No finality is implied.
4. **Boundary checks are not atomic range execution.** A provider can change forks during a range read and return to the original branch before the final boundary check. Range and fee-history results must not be used as an exact snapshot guarantee.
5. **Providers are trusted.** Hash selection and identity checks do not cryptographically verify balances, execution results, proofs, or complete log coverage. A provider that lies consistently can pass probes. `requireCanonical` describes that provider's canonical view, not network finality.
6. **Redis and coordinator observations define the fence.** All replicas must use the same intact Redis keys and compatible chain configuration. Redis rollback or split state, a reorg not yet recorded by the coordinator, or a reorg after the final check falls outside the guarantee. A reply already checked cannot be recalled.

For a consumer requiring exact state across every successful read, restrict usage to the exact-block EVM methods. The other method classes require separate consistency policies or rejection; standard Solana slot floors cannot supply that stronger contract.

## Verification

Regression coverage includes conflicting number/hash answers behind one provider URL, parent traversal and missing/discontinuous ancestry, reused availability probes with different constraints, invalid pinned payloads followed by valid cached replies, range headers, and mixed Solana batches. Existing tests cover failover, stale-head admission, reorg races across a batch, cache fencing, receipt inclusion, leadership takeover, and Redis stream retention.

Passed `go test -race -count=1 ./...` with `RPC_PROXY_TEST_REDIS_URL` pointing to an isolated local Redis instance, so the optional Redis tests ran. Also passed `go vet ./...`, Go formatting checks, and `git diff --check`. These are local automated checks, not a live-provider soak test or a formal proof.

Protocol references: [EIP-1898](https://eips.ethereum.org/EIPS/eip-1898) defines hash selection and provider-relative canonicality and describes the limitations of pre/post number checks. The [Solana getAccountInfo reference](https://solana.com/docs/rpc/http/getaccountinfo) defines context-bearing account reads and `minContextSlot`.
