package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
	"github.com/fiberevm/rpc-proxy/internal/services/receipts"
	"github.com/fiberevm/rpc-proxy/internal/services/requestcache"
	"github.com/fiberevm/rpc-proxy/internal/telemetry"
	"github.com/fiberevm/rpc-proxy/internal/upstream"
	"github.com/fiberevm/rpc-proxy/internal/utils"
)

type Gateway struct {
	config    *config.Config
	store     head.Store
	runtimes  map[string]*chain.Runtime
	logger    *slog.Logger
	telemetry *telemetry.Telemetry
	cache     *requestcache.Service
	receipts  map[string]*receipts.Service
}

// Options contains the validated dependencies required to serve RPC traffic.
type Options struct {
	Config    *config.Config
	Store     head.Store
	Runtimes  map[string]*chain.Runtime
	Logger    *slog.Logger
	Telemetry *telemetry.Telemetry
	Cache     *requestcache.Service
}

type processedResponse struct {
	response        jsonrpc.Response
	upstream        string
	target          *head.Head
	cacheStatus     string
	receiptLookup   bool
	boundaryChecked bool
	unverifiedRead  bool
}

// NewGateway builds an HTTP and WebSocket gateway from validated configuration and injected services.
func NewGateway(options Options) *Gateway {
	receiptVerifiers := make(map[string]*receipts.Service)
	for chainName, runtime := range options.Runtimes {
		if runtime.Config.Family == "evm" {
			receiptVerifiers[chainName] = receipts.NewService(receipts.Options{Blocks: runtime, Cache: options.Cache, MaxDepth: runtime.Config.ReorgDepth})
		}
	}
	return &Gateway{
		config:    options.Config,
		store:     options.Store,
		runtimes:  options.Runtimes,
		logger:    options.Logger,
		telemetry: options.Telemetry,
		cache:     options.Cache,
		receipts:  receiptVerifiers,
	}
}

// RPCHandler returns the chain-aware JSON-RPC HTTP handler mounted at /rpc/{chain}.
func (g *Gateway) RPCHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		chainName := request.PathValue("chain")
		if chainName == "" {
			chainName = strings.TrimPrefix(request.URL.Path, "/rpc/")
		}
		g.ServeRPC(w, request, chainName)
	})
}

// ServeRPC validates, pins, routes, and writes one JSON-RPC request or batch for chainName.
func (g *Gateway) ServeRPC(w http.ResponseWriter, request *http.Request, chainName string) {
	started := time.Now()
	g.telemetry.HTTPStarted()
	defer g.telemetry.HTTPFinished()
	w.Header().Set("Content-Type", "application/json")
	if request.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), g.config.Server.RequestTimeout.Value())
	defer cancel()
	body, err := io.ReadAll(http.MaxBytesReader(w, request.Body, g.config.Server.MaxBodyBytes))
	if err != nil {
		g.writeRPCError(w, nil, jsonrpc.InvalidRequest("request body exceeds configured limit"))
		return
	}
	requests, batch, parseErr := jsonrpc.ParseEnvelope(body, g.config.Server.MaxBatchSize)
	if parseErr != nil {
		g.writeRPCError(w, nil, parseErr)
		return
	}
	runtime, ok := g.runtimes[chainName]
	if !ok {
		g.writeEnvelope(w, requests, batch, func(request jsonrpc.Request) jsonrpc.Response {
			return jsonrpc.Failure(request.ID, jsonrpc.ChainUnavailable(chainName))
		})
		return
	}
	client := g.getClient(request)
	ctx, finish := g.telemetry.Span(ctx, "rpc.request", "chain:"+chainName, "family:"+runtime.Config.Family, "client:"+client)
	for _, rpcRequest := range requests {
		g.recordClientMethod(ctx, clientMethod{client: client, transport: "http", runtime: runtime, request: rpcRequest})
	}
	var requestErr error
	outcome := "error"
	defer func() {
		finish(requestErr)
		g.telemetry.Count("request", 1, "chain:"+chainName, "family:"+runtime.Config.Family, "outcome:"+outcome, "client:"+client)
		g.telemetry.Distribution("request.duration", float64(time.Since(started).Microseconds())/1000, "chain:"+chainName, "family:"+runtime.Config.Family, "client:"+client)
	}()
	var snapshot head.Snapshot
	var snapshotErr error
	for _, rpcRequest := range requests {
		if g.isStaticMethod(rpcRequest.Method, runtime.Config.Family) {
			continue
		}
		redisCtx, finishRedis := g.telemetry.Span(ctx, "redis.snapshot", "chain:"+chainName, "operation:snapshot")
		snapshot, snapshotErr = g.store.Snapshot(redisCtx, chainName)
		finishRedis(snapshotErr)
		if snapshotErr != nil {
			g.telemetry.Count("redis.failure", 1, "chain:"+chainName, "operation:snapshot")
			g.telemetry.Count("consistency.failure", 1, "chain:"+chainName, "failure_class:redis")
		}
		break
	}
	responses := make([]processedResponse, len(requests))
	include := make([]bool, len(requests))
	var wg sync.WaitGroup
	for index, rpcRequest := range requests {
		index, rpcRequest := index, rpcRequest
		if rpcRequest.IsNotification() && rpcRequest.Validate() == nil {
			include[index] = false
		} else {
			include[index] = true
		}
		// Static items remain available even when another batch item needs an unavailable store.
		if snapshotErr != nil && !g.isStaticMethod(rpcRequest.Method, runtime.Config.Family) {
			responses[index] = processedResponse{response: jsonrpc.Failure(rpcRequest.ID, jsonrpc.ConsistencyUnavailable(map[string]any{"chain": chainName, "reason": "head store unavailable"}))}
			continue
		}
		wg.Add(1)
		go func() { defer wg.Done(); responses[index] = g.process(ctx, runtime, snapshot, rpcRequest) }()
	}
	wg.Wait()
	g.fenceEVMResponses(ctx, evmResponseFence{runtime: runtime, snapshot: snapshot, responses: responses})
	filtered := make([]jsonrpc.Response, 0, len(responses))
	selectedUpstream := ""
	cacheStatus := ""
	for i, response := range responses {
		if include[i] {
			filtered = append(filtered, response.response)
			if response.cacheStatus != "" {
				if cacheStatus == "" {
					cacheStatus = response.cacheStatus
				} else if cacheStatus != response.cacheStatus {
					cacheStatus = "mixed"
				}
			}
			if response.upstream != "" {
				if selectedUpstream == "" {
					selectedUpstream = response.upstream
				} else if selectedUpstream != response.upstream {
					selectedUpstream = "multiple"
				}
			}
		}
	}
	if selectedUpstream != "" {
		w.Header().Set("X-RPC-Upstream", selectedUpstream)
	}
	if cacheStatus != "" {
		w.Header().Set("X-RPC-Cache", cacheStatus)
	}
	g.setResponseHeadHeaders(w.Header(), runtime.Config.Family, responses)
	if len(filtered) == 0 {
		outcome = "notification"
		w.WriteHeader(http.StatusNoContent)
		return
	}
	var writeErr error
	if batch {
		writeErr = json.NewEncoder(w).Encode(filtered)
	} else {
		writeErr = json.NewEncoder(w).Encode(filtered[0])
	}
	if writeErr != nil {
		g.logger.ErrorContext(ctx, "write JSON-RPC response", "error", writeErr)
	}
	outcome = "success"
	for _, response := range filtered {
		if response.Error != nil {
			outcome = "error"
			requestErr = response.Error
			break
		}
	}
}

func (g *Gateway) process(ctx context.Context, runtime *chain.Runtime, snapshot head.Snapshot, request jsonrpc.Request) processedResponse {
	if rpcErr := request.Validate(); rpcErr != nil {
		g.telemetry.Count("request.error", 1, "chain:"+runtime.Config.Name, "method:invalid", "failure_class:invalid_request")
		return processedResponse{response: jsonrpc.Failure(request.ID, rpcErr)}
	}
	headContext, finishHeadLookup := g.telemetry.Span(ctx, "head.get", "chain:"+runtime.Config.Name, "method:"+g.metricMethod(request.Method, runtime.Config.Family))
	prepared, rpcErr := g.transformRequest(headContext, runtime, snapshot, request)
	if rpcErr != nil {
		finishHeadLookup(rpcErr)
	} else {
		finishHeadLookup(nil)
	}
	if rpcErr != nil {
		g.telemetry.Count("request.error", 1, "chain:"+runtime.Config.Name, "method:"+g.metricMethod(request.Method, runtime.Config.Family), "failure_class:transformation")
		attributes := []any{"chain", runtime.Config.Name, "method", request.Method, "rpc_error_code", rpcErr.Code}
		attributes = append(attributes, g.telemetry.TraceAttrs(ctx)...)
		g.logger.WarnContext(ctx, "rpc request rejected", attributes...)
		return processedResponse{response: jsonrpc.Failure(request.ID, rpcErr)}
	}
	if runtime.Config.Family == "evm" && prepared.target != nil {
		accepted, exists := snapshot.Get(head.Latest)
		if !exists || accepted.IsZero() || accepted.ReorgPending {
			g.telemetry.Count("consistency.failure", 1, "chain:"+runtime.Config.Name, "failure_class:reorg_pending")
			return processedResponse{response: jsonrpc.Failure(request.ID, jsonrpc.ConsistencyUnavailable(map[string]any{"chain": runtime.Config.Name, "reason": "accepted head unavailable or reorg pending", "retryable": true}))}
		}
	}
	if prepared.target != nil && prepared.target.Commitment != "explicit" && runtime.Config.MaxHeadAge.Value() > 0 && (prepared.target.ObservedAt.IsZero() || time.Since(prepared.target.ObservedAt) > runtime.Config.MaxHeadAge.Value()) {
		g.telemetry.Count("consistency.failure", 1, "chain:"+runtime.Config.Name, "failure_class:stale_head")
		g.telemetry.Count("request.error", 1, "chain:"+runtime.Config.Name, "method:"+g.metricMethod(request.Method, runtime.Config.Family), "failure_class:stale_head")
		return processedResponse{response: jsonrpc.Failure(request.ID, jsonrpc.ConsistencyUnavailable(map[string]any{"chain": runtime.Config.Name, "reason": "accepted head is stale", "commitment": prepared.target.Commitment}))}
	}
	if prepared.localResponse != nil {
		cacheStatus := "local"
		if prepared.target == nil {
			cacheStatus = "static"
		}
		g.telemetry.Count("cache.request", 1, "chain:"+runtime.Config.Name, "method:"+request.Method, "outcome:"+cacheStatus)
		return processedResponse{response: jsonrpc.Success(request.ID, prepared.localResponse), upstream: "proxy", target: prepared.target, cacheStatus: cacheStatus}
	}
	routeCtx, finishRoute := g.telemetry.Span(ctx, "rpc.route", "chain:"+runtime.Config.Name, "method:"+g.metricMethod(request.Method, runtime.Config.Family))
	lookup, upstreamErr := g.getResponse(routeCtx, responseRequest{runtime: runtime, snapshot: snapshot, prepared: prepared})
	upstreamResponse, upstreamID := lookup.Response.Payload, lookup.Response.Upstream
	g.telemetry.Count("cache.request", 1, "chain:"+runtime.Config.Name, "method:"+g.metricMethod(request.Method, runtime.Config.Family), "outcome:"+lookup.Status)
	if upstreamErr != nil {
		finishRoute(upstreamErr, "upstream:"+upstreamID)
	} else {
		finishRoute(nil, "upstream:"+upstreamID)
	}
	if upstreamErr != nil {
		g.telemetry.Count("request.error", 1, "chain:"+runtime.Config.Name, "method:"+g.metricMethod(request.Method, runtime.Config.Family), "failure_class:upstream")
		attributes := []any{"chain", runtime.Config.Name, "method", request.Method, "rpc_error_code", upstreamErr.Code}
		attributes = append(attributes, g.telemetry.TraceAttrs(ctx)...)
		g.logger.WarnContext(ctx, "rpc request failed", attributes...)
		return processedResponse{response: jsonrpc.Failure(request.ID, upstreamErr), upstream: upstreamID, target: prepared.target, cacheStatus: lookup.Status}
	}
	return processedResponse{
		response: jsonrpc.Success(request.ID, upstreamResponse), upstream: upstreamID, target: prepared.target,
		cacheStatus: lookup.Status, receiptLookup: prepared.receiptHash != "", boundaryChecked: len(prepared.verifyBoundaries) > 0,
		unverifiedRead: runtime.Config.Family == "solana" && g.isSolanaUnpinnedRead(request.Method),
	}
}

func (g *Gateway) route(ctx context.Context, runtime *chain.Runtime, prepared preparedRequest) (json.RawMessage, string, *jsonrpc.Error) {
	started := time.Now()
	attempted := map[string]bool{}
	for {
		candidates := runtime.Candidates(prepared.requireEIP1898)
		if prepared.target != nil && prepared.target.Origin != "" {
			sort.SliceStable(candidates, func(i, j int) bool {
				return candidates[i].ID == prepared.target.Origin && candidates[j].ID != prepared.target.Origin
			})
		}
		eligible := 0
		nullReceipts := 0
		nullReceiptUpstream := ""
		for _, candidate := range candidates {
			if attempted[candidate.ID] || !candidate.SupportsMethod(prepared.method) {
				continue
			}
			if prepared.requireEIP1898 && !candidate.SupportsPinnedMethod(prepared.method) {
				continue
			}
			if prepared.target != nil && prepared.receiptHash == "" {
				probeCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
				available := false
				var availabilityErr error
				if runtime.Config.Family == "evm" {
					available, availabilityErr = runtime.HasEVMBlock(probeCtx, candidate, *prepared.target)
				}
				if runtime.Config.Family == "solana" {
					available, availabilityErr = runtime.HasSolanaSlot(probeCtx, candidate, *prepared.target)
				}
				cancel()
				if availabilityErr != nil {
					g.telemetry.Count("routing.availability_error", 1, "chain:"+runtime.Config.Name, "upstream:"+candidate.ID)
					continue
				}
				if !available {
					continue
				}
			}
			eligible++
			attempted[candidate.ID] = true
			if len(attempted) > 1 {
				g.telemetry.Count("routing.retry", 1, "chain:"+runtime.Config.Name, "method:"+prepared.method, "upstream:"+candidate.ID)
			}
			if len(prepared.verifyBoundaries) > 0 && !g.verifyEVMBoundaries(ctx, runtime, candidate, prepared.verifyBoundaries) {
				continue
			}
			upstreamCall, err := candidate.Call(ctx, prepared.method, prepared.parameters)
			if err != nil {
				continue
			}
			if upstreamCall.Error != nil && upstreamCall.Error.Code == jsonrpc.CodeMethodNotFound {
				candidate.MarkMethodUnsupported(prepared.method)
				if prepared.receiptHash != "" {
					eligible--
				}
				continue
			}
			if chain.IsUnavailableRPCError(upstreamCall.Error) {
				if prepared.target != nil {
					runtime.InvalidateAvailability(candidate.ID, *prepared.target)
				}
				continue
			}
			if upstreamCall.Error != nil {
				return nil, candidate.ID, upstreamCall.Error
			}
			if runtime.Config.Family == "evm" && prepared.target != nil {
				if err := g.verifyEVMResponse(evmResponseVerification{runtime: runtime, prepared: prepared, payload: upstreamCall.Result}); err != nil {
					runtime.InvalidateAvailability(candidate.ID, *prepared.target)
					g.telemetry.Count("upstream.response_invalid", 1, "chain:"+runtime.Config.Name, "upstream:"+candidate.ID, "method:"+prepared.method)
					continue
				}
			}
			if prepared.receiptHash != "" {
				if bytes.Equal(bytes.TrimSpace(upstreamCall.Result), []byte("null")) {
					// Try other providers in case this transaction index is behind.
					nullReceipts++
					nullReceiptUpstream = candidate.ID
					continue
				}
				block, err := g.getEVMReceiptBlock(upstreamCall.Result, prepared.receiptHash)
				if err != nil {
					g.telemetry.Count("upstream.receipt_invalid", 1, "chain:"+runtime.Config.Name, "upstream:"+candidate.ID, "method:"+prepared.method)
					continue
				}
				if block.Number > prepared.target.Number {
					// Providers may be ahead of the proxy. Future inclusion is not yet
					// visible to this request, even if the shared head advances meanwhile.
					nullReceipts++
					nullReceiptUpstream = candidate.ID
					continue
				}
			}
			if prepared.verifyContextSlot && prepared.target != nil {
				if err := g.verifySolanaContext(upstreamCall.Result, prepared.target.Number); err != nil {
					runtime.InvalidateAvailability(candidate.ID, *prepared.target)
					continue
				}
			}
			if runtime.Config.Family == "solana" && prepared.target != nil {
				var slot *uint64
				if prepared.method == "getSlot" {
					if json.Unmarshal(upstreamCall.Result, &slot) != nil || slot == nil || *slot < prepared.target.Number {
						continue
					}
				}
				if prepared.method == "getEpochInfo" {
					var epoch struct {
						AbsoluteSlot *uint64 `json:"absoluteSlot"`
					}
					if json.Unmarshal(upstreamCall.Result, &epoch) != nil || epoch.AbsoluteSlot == nil || *epoch.AbsoluteSlot < prepared.target.Number {
						continue
					}
				}
			}
			if prepared.unwrapContext {
				var envelope struct {
					Value json.RawMessage `json:"value"`
				}
				if json.Unmarshal(upstreamCall.Result, &envelope) != nil || len(envelope.Value) == 0 {
					continue
				}
				upstreamCall.Result = envelope.Value
			}
			if len(prepared.verifyBoundaries) > 0 {
				if !g.verifyEVMBoundaries(ctx, runtime, candidate, prepared.verifyBoundaries) {
					runtime.InvalidateAvailability(candidate.ID, *prepared.target)
					continue
				}
			}
			if len(attempted) > 1 || (prepared.target != nil && prepared.target.Origin != "" && candidate.ID != prepared.target.Origin) {
				g.telemetry.Count("routing.failover", 1, "chain:"+runtime.Config.Name, "method:"+prepared.method, "upstream:"+candidate.ID)
			}
			g.telemetry.Distribution("pin.wait", float64(time.Since(started).Microseconds())/1000, "chain:"+runtime.Config.Name, "method:"+prepared.method, "upstream:"+candidate.ID)
			return upstreamCall.Result, candidate.ID, nil
		}
		if prepared.receiptHash != "" && len(attempted) > 0 {
			if nullReceipts > 0 && nullReceipts == eligible {
				return json.RawMessage("null"), nullReceiptUpstream, nil
			}
			// Transport/protocol failures are not evidence of a missing receipt.
			break
		}
		if eligible == 0 && len(attempted) >= len(candidates) && len(candidates) > 0 {
			break
		}
		select {
		case <-ctx.Done():
			g.telemetry.Count("consistency.failure", 1, "chain:"+runtime.Config.Name, "failure_class:deadline")
			return nil, "", jsonrpc.ConsistencyUnavailable(g.getTargetErrorData(runtime.Config.Name, prepared.target, attempted))
		case <-time.After(75 * time.Millisecond):
		}
	}
	g.telemetry.Count("consistency.failure", 1, "chain:"+runtime.Config.Name, "failure_class:no_eligible_upstream")
	return nil, "", jsonrpc.ConsistencyUnavailable(g.getTargetErrorData(runtime.Config.Name, prepared.target, attempted))
}

func (g *Gateway) getEVMReceiptBlock(receipt json.RawMessage, transactionHash string) (head.Head, error) {
	var inclusion struct {
		TransactionHash string `json:"transactionHash"`
		BlockHash       string `json:"blockHash"`
		BlockNumber     string `json:"blockNumber"`
	}
	if err := json.Unmarshal(receipt, &inclusion); err != nil {
		return head.Head{}, fmt.Errorf("decode transaction receipt: %w", err)
	}
	if !utils.IsEVMHash(inclusion.TransactionHash) || !strings.EqualFold(inclusion.TransactionHash, transactionHash) || !utils.IsEVMHash(inclusion.BlockHash) {
		return head.Head{}, errors.New("receipt has an invalid transaction or block hash")
	}
	blockNumber, err := utils.ParseEVMQuantity(inclusion.BlockNumber)
	if err != nil {
		return head.Head{}, fmt.Errorf("invalid receipt block number: %w", err)
	}
	return head.Head{Family: "evm", Number: blockNumber, Hash: inclusion.BlockHash}, nil
}

func (g *Gateway) verifyEVMBoundaries(ctx context.Context, runtime *chain.Runtime, candidate *upstream.Client, targets []head.Head) bool {
	for _, target := range targets {
		parameters, err := jsonrpc.MarshalCallParams(utils.FormatEVMQuantity(target.Number), false)
		if err != nil {
			return false
		}
		blockCall, err := candidate.Call(ctx, "eth_getBlockByNumber", parameters)
		if err != nil || blockCall.Error != nil {
			return false
		}
		block, err := runtime.ParseEVMHead(chain.EVMHeadParseRequest{Commitment: target.Commitment, Origin: candidate.ID, Header: blockCall.Result})
		if err != nil || block.Number != target.Number || !strings.EqualFold(block.Hash, target.Hash) {
			return false
		}
	}
	return true
}

func (g *Gateway) getTargetErrorData(chainName string, target *head.Head, attempted map[string]bool) map[string]any {
	ids := make([]string, 0, len(attempted))
	for id := range attempted {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	errorDetails := map[string]any{"chain": chainName, "attempted_upstreams": ids}
	if target != nil {
		errorDetails["target_number"] = target.Number
		if target.Hash != "" {
			errorDetails["target_hash"] = target.Hash
		}
		errorDetails["commitment"] = target.Commitment
	}
	return errorDetails
}

func (g *Gateway) setResponseHeadHeaders(headers http.Header, family string, responses []processedResponse) {
	var acceptedHead *head.Head
	hasReceiptLookup := false
	hasOtherTarget := false
	boundaryChecked := false
	unverifiedRead := false
	for _, response := range responses {
		unverifiedRead = unverifiedRead || response.unverifiedRead
		if response.target == nil || response.response.Error != nil {
			continue
		}
		hasReceiptLookup = hasReceiptLookup || response.receiptLookup
		hasOtherTarget = hasOtherTarget || !response.receiptLookup
		if acceptedHead != nil {
			sameTarget := acceptedHead.Identity() == response.target.Identity() && acceptedHead.Commitment == response.target.Commitment
			if family == "evm" {
				// A numeric selector and latest can identify the very same EVM state.
				sameTarget = strings.EqualFold(acceptedHead.Hash, response.target.Hash)
			}
			if !sameTarget {
				headers.Set("X-RPC-Consistency", "mixed-targets")
				return
			}
		}
		boundaryChecked = boundaryChecked || response.boundaryChecked
		acceptedHead = response.target
	}
	if hasReceiptLookup && hasOtherTarget {
		headers.Set("X-RPC-Consistency", "mixed-targets")
		return
	}
	if unverifiedRead {
		if acceptedHead != nil {
			headers.Set("X-RPC-Consistency", "mixed-targets")
		} else {
			headers.Set("X-RPC-Consistency", "unverified")
		}
		return
	}
	if acceptedHead == nil {
		return
	}
	if family == "evm" {
		if acceptedHead.Commitment != "explicit" || acceptedHead.Number != 0 || acceptedHead.Origin != "" {
			headers.Set("X-RPC-Head-Number", utils.FormatEVMQuantity(acceptedHead.Number))
		}
		headers.Set("X-RPC-Head-Hash", acceptedHead.Hash)
		headers.Set("X-RPC-Consistency", "exact-block-hash")
		if hasReceiptLookup {
			// These headers identify the receipt visibility cutoff, not its inclusion block.
			headers.Set("X-RPC-Consistency", "pinned-head-ceiling")
		}
		if boundaryChecked {
			// The hash identifies the range boundary, not an atomic execution snapshot.
			headers.Set("X-RPC-Consistency", "boundary-checked")
		}
	} else {
		headers.Set("X-RPC-Head-Slot", fmt.Sprint(acceptedHead.Number))
		headers.Set("X-RPC-Consistency", "minimum-context-slot")
	}
}

func (g *Gateway) writeRPCError(w http.ResponseWriter, id json.RawMessage, rpcErr *jsonrpc.Error) {
	if err := json.NewEncoder(w).Encode(jsonrpc.Failure(id, rpcErr)); err != nil {
		g.logger.Error("write JSON-RPC error", "error", err)
	}
}

func (g *Gateway) writeEnvelope(w http.ResponseWriter, requests []jsonrpc.Request, batch bool, fn func(jsonrpc.Request) jsonrpc.Response) {
	responses := make([]jsonrpc.Response, 0, len(requests))
	for _, request := range requests {
		if !request.IsNotification() || request.Validate() != nil {
			if rpcErr := request.Validate(); rpcErr != nil {
				responses = append(responses, jsonrpc.Failure(request.ID, rpcErr))
			} else {
				responses = append(responses, fn(request))
			}
		}
	}
	if len(responses) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if batch {
		if err := json.NewEncoder(w).Encode(responses); err != nil {
			g.logger.Error("write JSON-RPC batch", "error", err)
		}
	} else {
		if err := json.NewEncoder(w).Encode(responses[0]); err != nil {
			g.logger.Error("write JSON-RPC response", "error", err)
		}
	}
}

func (g *Gateway) metricMethod(method string, family string) string {
	if family == "evm" {
		if method == "eth_subscribe" || method == "eth_unsubscribe" {
			return method
		}
		if _, ok := g.getEVMStateBlockIndex(method); ok {
			return method
		}
		if _, ok := g.getEVMBlockMethod(method); ok {
			return method
		}
		if g.isEVMHeadNeutralMethod(method) || g.isEVMExplicitHashMethod(method) || method == "eth_blockNumber" || method == "eth_getLogs" || method == "eth_feeHistory" || method == "eth_getTransactionReceipt" {
			return method
		}
	} else if _, ok := g.getSolanaConfigIndex(method); ok || g.isSolanaHeadNeutralMethod(method) {
		return method
	}
	if g.isWriteMethod(method, family) {
		return "write"
	}
	return "unknown"
}
