package gateway

import (
	"context"
	"strings"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
)

type evmResponseFence struct {
	runtime   *chain.Runtime
	snapshot  head.Snapshot
	responses []processedResponse
}

func (g *Gateway) fenceEVMResponses(ctx context.Context, fence evmResponseFence) {
	if fence.runtime.Config.Family != "evm" {
		return
	}
	needsFence := false
	for _, response := range fence.responses {
		if response.target != nil {
			needsFence = true
			break
		}
	}
	if !needsFence {
		return
	}
	// One final shared-store read fences every result, including batch items that finished early.
	redisCtx, finish := g.telemetry.Span(ctx, "redis.reorg_fence", "chain:"+fence.runtime.Config.Name, "operation:reorg_fence")
	current, err := g.store.Snapshot(redisCtx, fence.runtime.Config.Name)
	finish(err)
	initial, initialExists := fence.snapshot.Get(head.Latest)
	latest, latestExists := current.Get(head.Latest)
	failureClass := ""
	switch {
	case err != nil:
		g.telemetry.Count("redis.failure", 1, "chain:"+fence.runtime.Config.Name, "operation:reorg_fence")
		failureClass = "reorg_check_unavailable"
	case ctx.Err() != nil:
		failureClass = "deadline"
	case !initialExists || !latestExists || initial.IsZero() || latest.IsZero():
		failureClass = "reorg_check_unavailable"
	case initial.ReorgPending || latest.ReorgPending:
		failureClass = "reorg_pending"
	case initial.ReorgEpoch != latest.ReorgEpoch || latest.Generation < initial.Generation:
		failureClass = "reorg_changed"
	case latest.Number <= initial.Number && !strings.EqualFold(latest.Hash, initial.Hash):
		failureClass = "reorg_changed"
	case fence.runtime.Config.MaxHeadAge.Value() > 0 && (latest.ObservedAt.IsZero() || time.Since(latest.ObservedAt) > fence.runtime.Config.MaxHeadAge.Value()):
		failureClass = "stale_head"
	}
	if failureClass == "" {
		return
	}
	g.telemetry.Count("consistency.failure", 1, "chain:"+fence.runtime.Config.Name, "failure_class:"+failureClass)
	for index, response := range fence.responses {
		if response.target == nil {
			continue
		}
		rpcErr := jsonrpc.ConsistencyUnavailable(map[string]any{"chain": fence.runtime.Config.Name, "reason": failureClass, "retryable": true})
		// Discard both the result and its diagnostic head/upstream; never leak invalidated data.
		fence.responses[index] = processedResponse{response: jsonrpc.Failure(response.response.ID, rpcErr)}
		g.telemetry.Count("reorg.read_rejected", 1, "chain:"+fence.runtime.Config.Name, "failure_class:"+failureClass)
	}
}
