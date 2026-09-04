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
}

// Options contains the validated dependencies required to serve RPC traffic.
type Options struct {
	Config    *config.Config
	Store     head.Store
	Runtimes  map[string]*chain.Runtime
	Logger    *slog.Logger
	Telemetry *telemetry.Telemetry
}

type processedResponse struct {
	response      jsonrpc.Response
	upstream      string
	target        *head.Head
	receiptLookup bool
}

// NewGateway builds an HTTP and WebSocket gateway from validated configuration and injected services.
func NewGateway(options Options) *Gateway {
	return &Gateway{
		config:    options.Config,
		store:     options.Store,
		runtimes:  options.Runtimes,
		logger:    options.Logger,
		telemetry: options.Telemetry,
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
	ctx, finish := g.telemetry.Span(ctx, "rpc.request", "chain:"+chainName, "family:"+runtime.Config.Family)
	var requestErr error
	outcome := "error"
	defer func() {
		finish(requestErr)
		g.telemetry.Count("request", 1, "chain:"+chainName, "family:"+runtime.Config.Family, "outcome:"+outcome)
		g.telemetry.Distribution("request.duration", float64(time.Since(started).Microseconds())/1000, "chain:"+chainName, "family:"+runtime.Config.Family)
	}()
	redisCtx, finishRedis := g.telemetry.Span(ctx, "redis.snapshot", "chain:"+chainName, "operation:snapshot")
	snapshot, err := g.store.Snapshot(redisCtx, chainName)
	finishRedis(err)
	if err != nil {
		requestErr = err
		g.telemetry.Count("redis.failure", 1, "chain:"+chainName, "operation:snapshot")
		g.telemetry.Count("consistency.failure", 1, "chain:"+chainName, "failure_class:redis")
		g.writeEnvelope(w, requests, batch, func(request jsonrpc.Request) jsonrpc.Response {
			return jsonrpc.Failure(request.ID, jsonrpc.ConsistencyUnavailable(map[string]any{"chain": chainName, "reason": "head store unavailable"}))
		})
		return
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
		wg.Add(1)
		go func() { defer wg.Done(); responses[index] = g.process(ctx, runtime, snapshot, rpcRequest) }()
	}
	wg.Wait()
	filtered := make([]jsonrpc.Response, 0, len(responses))
	selectedUpstream := ""
	for i, response := range responses {
		if include[i] {
			filtered = append(filtered, response.response)
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
	if prepared.target != nil && prepared.target.Commitment != "explicit" && runtime.Config.MaxHeadAge.Value() > 0 && (prepared.target.ObservedAt.IsZero() || time.Since(prepared.target.ObservedAt) > runtime.Config.MaxHeadAge.Value()) {
		g.telemetry.Count("consistency.failure", 1, "chain:"+runtime.Config.Name, "failure_class:stale_head")
		g.telemetry.Count("request.error", 1, "chain:"+runtime.Config.Name, "method:"+g.metricMethod(request.Method, runtime.Config.Family), "failure_class:stale_head")
		return processedResponse{response: jsonrpc.Failure(request.ID, jsonrpc.ConsistencyUnavailable(map[string]any{"chain": runtime.Config.Name, "reason": "accepted head is stale", "commitment": prepared.target.Commitment}))}
	}
	if prepared.localResponse != nil {
		return processedResponse{response: jsonrpc.Success(request.ID, prepared.localResponse), upstream: "proxy", target: prepared.target}
	}
	routeCtx, finishRoute := g.telemetry.Span(ctx, "rpc.route", "chain:"+runtime.Config.Name, "method:"+g.metricMethod(request.Method, runtime.Config.Family))
	upstreamResponse, upstreamID, upstreamErr := g.route(routeCtx, runtime, prepared)
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
		return processedResponse{response: jsonrpc.Failure(request.ID, upstreamErr), upstream: upstreamID}
	}
	return processedResponse{response: jsonrpc.Success(request.ID, upstreamResponse), upstream: upstreamID, target: prepared.target, receiptLookup: prepared.receiptHash != ""}
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
			if prepared.target != nil {
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
			if prepared.receiptHash != "" {
				if bytes.Equal(bytes.TrimSpace(upstreamCall.Result), []byte("null")) {
					// A lagging transaction index must not hide another provider's receipt.
					nullReceipts++
					nullReceiptUpstream = candidate.ID
					continue
				}
				if err := g.verifyEVMReceipt(upstreamCall.Result, prepared.receiptHash); err != nil {
					g.telemetry.Count("upstream.receipt_invalid", 1, "chain:"+runtime.Config.Name, "upstream:"+candidate.ID, "method:"+prepared.method)
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
			// A failed or malformed lookup is not evidence that the receipt is absent.
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

func (g *Gateway) verifyEVMReceipt(receipt json.RawMessage, transactionHash string) error {
	var inclusion struct {
		TransactionHash string `json:"transactionHash"`
		BlockHash       string `json:"blockHash"`
		BlockNumber     string `json:"blockNumber"`
	}
	if err := json.Unmarshal(receipt, &inclusion); err != nil {
		return fmt.Errorf("decode transaction receipt: %w", err)
	}
	if !utils.IsEVMHash(inclusion.TransactionHash) || !strings.EqualFold(inclusion.TransactionHash, transactionHash) || !utils.IsEVMHash(inclusion.BlockHash) {
		return errors.New("receipt has an invalid transaction or block hash")
	}
	if _, err := utils.ParseEVMQuantity(inclusion.BlockNumber); err != nil {
		return fmt.Errorf("invalid receipt block number: %w", err)
	}
	return nil
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
	for _, response := range responses {
		hasReceiptLookup = hasReceiptLookup || response.receiptLookup
		if response.target == nil {
			continue
		}
		if acceptedHead != nil && (acceptedHead.Identity() != response.target.Identity() || acceptedHead.Commitment != response.target.Commitment) {
			headers.Set("X-RPC-Consistency", "mixed-targets")
			return
		}
		acceptedHead = response.target
	}
	if hasReceiptLookup {
		if acceptedHead != nil {
			headers.Set("X-RPC-Consistency", "mixed-targets")
		} else {
			headers.Set("X-RPC-Consistency", "transaction-hash-lookup")
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
