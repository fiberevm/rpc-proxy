# Datadog monitors

Create these monitors with the deployment's normal notification routing:

EVM freshness and lag monitors should cover `commitment:latest` only. Remove legacy EVM safe/finalized monitor groups when upgrading; those heads are no longer tracked. Keep all three commitment groups for Solana.

- **Consistency unavailable:** `sum(last_5m):sum:rpc_proxy.consistency.failure{*}.as_count() > 0`.
- **No eligible upstream:** `sum(last_5m):sum:rpc_proxy.consistency.failure{failure_class:no_eligible_upstream} by {chain}.as_count() > 0`.
- **Accepted head stale:** `max(last_5m):max:rpc_proxy.head.age{*} by {chain,commitment} > 30`, with chain-and-commitment-specific thresholds, including Solana. Enable missing-data alerts so a dead coordinator cannot leave the last healthy gauge unnoticed.
- **Accepted head stuck:** alert on `rpc_proxy.head.stuck_age` above a chain-and-commitment-specific multiple of the expected advancement interval.
- **Provider lag:** `max(last_5m):max:rpc_proxy.provider.head_lag{*} by {chain,upstream,commitment} > 3`, with chain-specific thresholds.
- **Upstream error ratio:** alert when the `transport_error`, `http_error`, and `invalid_response` request rate exceeds 5% by `{chain,upstream}` for five minutes. Monitor `rpc_error` separately; this includes expected negative capability probes and caller execution failures.
- **Redis failures:** `sum(last_5m):sum:rpc_proxy.redis.failure{*} by {operation}.as_count() > 0`.
- **Coordinator churn:** `sum(last_10m):sum:rpc_proxy.coordinator.leadership_change{state:lost} by {chain}.as_count() > 2`.
- **Reorg recovery stuck:** `min(last_1m):max:rpc_proxy.head.reorg_pending{*} by {chain} > 0`, with a chain-specific recovery window and missing-data alerts. A pending marker deliberately blocks pinned reads; investigate provider disagreement and missing ancestry rather than deleting Redis state.
- **Reorg-invalidated reads:** monitor `rpc_proxy.reorg.read_rejected` by `{chain,failure_class}` and correlate with `rpc_proxy.head.reorg_detected`. Brief rejection spikes during recovery are expected fail-closed behavior; sustained errors consume the availability error budget. `reorg_changed` means the request crossed a reorg, `reorg_pending` means recovery is unresolved, and `reorg_check_unavailable` means the final shared check could not be completed.
- **WebSocket recovery:** alert on any `rpc_proxy.ws.recovery_failure`, and on sustained `rpc_proxy.ws.upstream_failover` without subsequent `rpc_proxy.ws.head` traffic.
- **Subscription fallback:** for EVM upstreams configured with WebSockets, alert on sustained `rpc_proxy.coordinator.latest_poll` traffic alongside `rpc_proxy.coordinator.subscription_active < 1`. Correlate `rpc_proxy.coordinator.subscription_error` by `{chain,upstream,failure_class}` to distinguish rejected acknowledgments from idle streams. HTTP-only providers legitimately poll and should be excluded from this alert.
- **Latency SLO:** p99 `rpc_proxy.request.duration` above the chain-specific upstream budget for ten minutes.
- **Error SLO:** consistency and upstream failures exceed the service's configured error-budget burn-rate thresholds.

Do not add block hashes, transaction hashes, addresses, request IDs, or free-form client identifiers as tags. The proxy's `client` tag uses only configured application labels plus the bounded `anonymous` and `unknown` buckets.

The dashboard's client filter applies to inbound request metrics and the RPC method usage widget. Provider and coordinator metrics describe shared work and remain independent of that filter. `rpc_proxy.client.method` counts attempts per parsed item, including notifications and cached or rejected calls; group by `{client,chain,method,transport}` to compare applications. It does not measure provider billing or successful calls.

Enable percentile aggregation for the DogStatsD distribution metrics before using p95/p99 queries. `upstream.request.duration` includes reading and validating the response body. The `/status` field `metric_submission_errors` counts locally rejected metric submissions; UDP transport does not confirm Agent receipt.
