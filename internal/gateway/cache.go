package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
	"github.com/fiberevm/rpc-proxy/internal/services/receipts"
	"github.com/fiberevm/rpc-proxy/internal/services/requestcache"
)

type responseRequest struct {
	runtime  *chain.Runtime
	snapshot head.Snapshot
	prepared preparedRequest
}

func (g *Gateway) getResponse(ctx context.Context, request responseRequest) (requestcache.Lookup, *jsonrpc.Error) {
	cacheable := g.cache != nil && g.cache.Enabled() && g.isCacheable(request)
	accepted, _ := request.snapshot.Get(head.Latest)
	load := func(loadCtx context.Context) (requestcache.Response, error) {
		payload, upstreamID, rpcErr := g.route(loadCtx, request.runtime, request.prepared)
		response := requestcache.Response{Payload: payload, Upstream: upstreamID, SkipCache: request.prepared.receiptHash != ""}
		if rpcErr != nil {
			return response, rpcErr
		}
		if cacheable && request.prepared.receiptHash != "" && !bytes.Equal(bytes.TrimSpace(payload), []byte("null")) {
			block, err := g.getEVMReceiptBlock(payload, request.prepared.receiptHash)
			if err != nil {
				return response, fmt.Errorf("read receipt inclusion: %w", err)
			}
			verificationCtx, cancel := context.WithTimeout(loadCtx, request.runtime.Config.HeadRequestTimeout.Value())
			verified, verificationErr := g.receipts[request.runtime.Config.Name].ContainsBlock(verificationCtx, receipts.BlockRequest{Block: block, Accepted: accepted})
			cancel()
			// Height-capped lookups remain usable without ancestry proof. Only verified
			// inclusion is retained across normal head advancement in the response cache.
			response.SkipCache = verificationErr != nil || !verified
			if response.SkipCache {
				g.telemetry.Count("cache.receipt_unverified", 1, "chain:"+request.runtime.Config.Name)
			}
		}
		return response, nil
	}
	lookup := requestcache.Lookup{Status: "bypass"}
	var err error
	if cacheable {
		// Hash the transformed parameters, including call overrides, full-transaction flags,
		// log filters and indices. IDs and the caller's moving "latest" tag are excluded.
		// Epoch separation prevents old-fork entries from returning after A -> B -> A.
		targetHash := ""
		if request.prepared.target != nil && request.prepared.receiptHash == "" {
			targetHash = request.prepared.target.Hash
		}
		// Receipt requests use their transaction hash and epoch. Their verified inclusion
		// remains valid across ordinary head advancement, without another provider call.
		key := fmt.Sprintf("%q:%d:%q:%s:%x", request.runtime.Config.Name, accepted.ReorgEpoch,
			request.prepared.method, targetHash, sha256.Sum256(request.prepared.parameters))
		flightKey := key
		if request.prepared.receiptHash != "" {
			// A future receipt is null at one pin and visible at the next.
			// Positive cache entries survive advancement; in-flight answers do not.
			flightKey += ":" + accepted.Hash
		}
		lookup, err = g.cache.GetOrLoad(ctx, requestcache.Request{Key: key, FlightKey: flightKey, Load: load})
		if err == nil && request.prepared.receiptHash != "" && string(lookup.Response.Payload) != "null" {
			block, blockErr := g.getEVMReceiptBlock(lookup.Response.Payload, request.prepared.receiptHash)
			if blockErr != nil {
				err = fmt.Errorf("read cached receipt inclusion: %w", blockErr)
			} else if block.Number > accepted.Number {
				// Apply the caller's ceiling even when another request warmed the cache
				// at a newer head. Do not discard the reusable positive cache entry.
				lookup.Response.Payload = json.RawMessage("null")
			}
		}
	} else {
		lookup.Response, err = load(ctx)
	}
	if err != nil {
		var rpcErr *jsonrpc.Error
		if errors.As(err, &rpcErr) {
			return lookup, rpcErr
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return lookup, jsonrpc.ConsistencyUnavailable(map[string]any{"chain": request.runtime.Config.Name, "reason": "response unavailable before deadline", "retryable": true})
		}
		g.logger.ErrorContext(ctx, "read response cache", "chain", request.runtime.Config.Name, "error", err)
		return lookup, jsonrpc.InternalError("read response cache")
	}
	return lookup, nil
}

func (g *Gateway) isCacheable(request responseRequest) bool {
	prepared := request.prepared
	if request.runtime.Config.Family != "evm" {
		return false
	}
	if prepared.receiptHash != "" {
		accepted, exists := request.snapshot.Get(head.Latest)
		return g.receipts[request.runtime.Config.Name] != nil && exists && !accepted.IsZero() && !accepted.ReorgPending && !accepted.ObservedAt.IsZero() && time.Since(accepted.ObservedAt) <= request.runtime.Config.MaxHeadAge.Value()
	}
	if prepared.target == nil || prepared.target.Hash == "" || len(prepared.verifyBoundaries) != 0 {
		return false
	}
	// Multi-block ranges, fee history and Solana minimum-slot reads stay uncached.
	if _, supported := g.getEVMStateBlockIndex(prepared.method); supported {
		return prepared.requireEIP1898
	}
	return g.isEVMExplicitHashMethod(prepared.method) || prepared.method == "eth_getLogs"
}

func (g *Gateway) isStaticMethod(method, family string) bool {
	return family == "evm" && method == "eth_chainId" || family == "solana" && method == "getGenesisHash"
}

func (g *Gateway) prepareStaticRequest(runtime *chain.Runtime, request jsonrpc.Request) (preparedRequest, *jsonrpc.Error) {
	parameters, rpcErr := jsonrpc.ParseParams(request.Params)
	if rpcErr != nil {
		return preparedRequest{}, rpcErr
	}
	if len(parameters) != 0 {
		return preparedRequest{}, jsonrpc.InvalidParams(request.Method + " does not accept parameters")
	}
	identity := runtime.Config.ChainID
	if runtime.Config.Family == "solana" {
		identity = runtime.Config.GenesisHash
	}
	payload, err := json.Marshal(identity)
	if err != nil {
		return preparedRequest{}, jsonrpc.InternalError("encode configured chain identity")
	}
	return preparedRequest{method: request.Method, localResponse: payload}, nil
}
