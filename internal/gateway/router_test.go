package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
	"github.com/fiberevm/rpc-proxy/internal/telemetry"
	"github.com/fiberevm/rpc-proxy/internal/utils"
)

type fakeEVM struct {
	t              *testing.T
	number         uint64
	hash           string
	parent         string
	stateCalls     int
	lastStateArgs  []json.RawMessage
	failState      bool
	stateReplyHook func()
	stateError     *jsonrpc.Error
	receipt        receiptReply
	mu             sync.Mutex
	server         *httptest.Server
}

func newFakeEVM(t *testing.T, number uint64, hashValue, parent string) *fakeEVM {
	t.Helper()
	fake := &fakeEVM{t: t, number: number, hash: hashValue, parent: parent}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeEVM) serveHTTP(w http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		f.t.Errorf("read fake request: %v", err)
		return
	}
	var call struct {
		JSONRPC string            `json:"jsonrpc"`
		ID      json.RawMessage   `json:"id"`
		Method  string            `json:"method"`
		Params  []json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(body, &call); err != nil {
		f.t.Errorf("decode fake request: %v", err)
		return
	}
	var result any
	number, hashValue, parent, failState := f.snapshot()
	switch call.Method {
	case "eth_chainId":
		result = "0x1"
	case "eth_getTransactionReceipt":
		if len(call.Params) != 1 {
			f.t.Errorf("receipt parameters were rewritten: %s", call.Params)
			return
		}
		f.mu.Lock()
		reply := f.receipt
		f.mu.Unlock()
		if reply.httpStatus != 0 {
			w.WriteHeader(reply.httpStatus)
			return
		}
		writeFake(f.t, w, call.ID, reply.receipt, reply.rpcError)
		return
	case "eth_getBlockByNumber":
		var blockRef string
		if err := json.Unmarshal(call.Params[0], &blockRef); err != nil {
			f.t.Errorf("decode block reference: %v", err)
			return
		}
		if blockRef == "latest" || blockRef == utils.FormatEVMQuantity(number) {
			result = evmBlock(number, hashValue, parent)
		} else {
			result = nil
		}
	case "eth_getBlockByHash":
		var hashRef string
		if err := json.Unmarshal(call.Params[0], &hashRef); err != nil {
			f.t.Errorf("decode block hash: %v", err)
			return
		}
		if strings.EqualFold(hashRef, hashValue) {
			result = evmBlock(number, hashValue, parent)
		} else if strings.EqualFold(hashRef, parent) {
			result = map[string]any{"number": utils.FormatEVMQuantity(number - 1), "hash": parent, "parentHash": testHash('0')}
		} else {
			result = nil
		}
	case "eth_getBalance":
		if len(call.Params) == 2 && strings.Contains(string(call.Params[1]), testHash('0')) {
			writeFake(f.t, w, call.ID, nil, &jsonrpc.Error{Code: -32001, Message: "block not found"})
			return
		}
		f.mu.Lock()
		f.stateCalls++
		f.lastStateArgs = append([]json.RawMessage(nil), call.Params...)
		stateReplyHook := f.stateReplyHook
		stateError := f.stateError
		f.mu.Unlock()
		if failState {
			writeFake(f.t, w, call.ID, nil, &jsonrpc.Error{Code: -32001, Message: "block not found"})
			return
		}
		var blockRef struct {
			BlockHash        string `json:"blockHash"`
			RequireCanonical bool   `json:"requireCanonical"`
		}
		if len(call.Params) != 2 || json.Unmarshal(call.Params[1], &blockRef) != nil || !strings.EqualFold(blockRef.BlockHash, hashValue) || !blockRef.RequireCanonical {
			writeFake(f.t, w, call.ID, nil, &jsonrpc.Error{Code: -32001, Message: "block not found"})
			return
		}
		result = "0x64"
		if stateReplyHook != nil {
			stateReplyHook()
		}
		if stateError != nil {
			writeFake(f.t, w, call.ID, nil, stateError)
			return
		}
	default:
		writeFake(f.t, w, call.ID, nil, &jsonrpc.Error{Code: -32601, Message: "method not found"})
		return
	}
	writeFake(f.t, w, call.ID, result, nil)
}

func (f *fakeEVM) snapshot() (uint64, string, string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.number, f.hash, f.parent, f.failState
}
func (f *fakeEVM) setFailState(value bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failState = value
}
func (f *fakeEVM) set(number uint64, hashValue, parent string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.number, f.hash, f.parent = number, hashValue, parent
}
func (f *fakeEVM) calls() int { f.mu.Lock(); defer f.mu.Unlock(); return f.stateCalls }

func evmBlock(number uint64, hashValue, parent string) map[string]any {
	return map[string]any{"number": utils.FormatEVMQuantity(number), "hash": hashValue, "parentHash": parent, "transactions": []any{}}
}

func writeFake(t *testing.T, w http.ResponseWriter, id json.RawMessage, response any, rpcErr *jsonrpc.Error) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if rpcErr != nil {
		if err := json.NewEncoder(w).Encode(jsonrpc.Failure(id, rpcErr)); err != nil {
			t.Errorf("encode fake RPC error: %v", err)
		}
		return
	}
	encodedResponse, err := json.Marshal(response)
	if err != nil {
		t.Errorf("encode fake RPC result: %v", err)
		return
	}
	if err := json.NewEncoder(w).Encode(jsonrpc.Success(id, encodedResponse)); err != nil {
		t.Errorf("write fake RPC result: %v", err)
	}
}

func TestGatewayRoutesLatestReadOnlyToProviderWithAcceptedHead(t *testing.T) {
	headN := testHash('a')
	parent := testHash('9')
	providerA := newFakeEVM(t, 100, headN, parent)
	providerB := newFakeEVM(t, 99, parent, testHash('8'))
	proxy, store := newTestGateway(t, 500*time.Millisecond, []config.UpstreamConfig{{ID: "a", HTTPURL: providerA.server.URL, MaxConcurrency: 8}, {ID: "b", HTTPURL: providerB.server.URL, MaxConcurrency: 8}})
	store.Set(head.Head{Chain: "test", Family: "evm", Commitment: head.Latest, Number: 100, Hash: headN, ParentHash: parent, Origin: "a", ObservedAt: time.Now()})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/rpc/test", bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x0000000000000000000000000000000000000000","latest"]}`))
	proxy.ServeRPC(recorder, request, "test")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
	}
	var response jsonrpc.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error != nil || string(response.Result) != `"0x64"` {
		t.Fatalf("unexpected response: %s", recorder.Body.String())
	}
	if providerA.calls() != 2 {
		t.Fatalf("expected capability probe plus one state call on A, got %d", providerA.calls())
	}
	if providerB.calls() != 1 {
		t.Fatalf("expected only capability probe on stale B, got %d state calls", providerB.calls())
	}
	if recorder.Header().Get("X-RPC-Head-Hash") != headN || recorder.Header().Get("X-RPC-Upstream") != "a" {
		t.Fatalf("missing diagnostic headers: %#v", recorder.Header())
	}
}

func TestTransformEVMExplicitNumberGetsAndPinsHash(t *testing.T) {
	provider := newFakeEVM(t, 100, testHash('a'), testHash('9'))
	proxy, _ := newTestGateway(t, time.Second, []config.UpstreamConfig{{ID: "provider", HTTPURL: provider.server.URL, MaxConcurrency: 8}})
	snapshot := head.Snapshot{Chain: "test", Heads: map[string]head.Head{head.Latest: {Chain: "test", Family: "evm", Commitment: head.Latest, Number: 100, Hash: testHash('a'), ObservedAt: time.Now()}}}
	prepared, rpcErr := proxy.transformRequest(context.Background(), proxy.runtimes["test"], snapshot, jsonrpc.Request{JSONRPC: "2.0", Method: "eth_getBalance", Params: json.RawMessage(`["address","0x64"]`)})
	if rpcErr != nil || prepared.target == nil || prepared.target.Number != 100 || prepared.target.Hash != testHash('a') || !strings.Contains(string(prepared.parameters), `"requireCanonical":true`) {
		t.Fatalf("explicit number was not hash-pinned: prepared=%+v err=%v", prepared, rpcErr)
	}
}

func TestTransformEVMRejectsReversedHistoricalLogRange(t *testing.T) {
	provider := newFakeEVM(t, 99, testHash('9'), testHash('8'))
	proxy, _ := newTestGateway(t, time.Second, []config.UpstreamConfig{{ID: "provider", HTTPURL: provider.server.URL, MaxConcurrency: 8}})
	_, rpcErr := proxy.transformRequest(context.Background(), proxy.runtimes["test"], evmSnapshot(), jsonrpc.Request{JSONRPC: "2.0", Method: "eth_getLogs", Params: json.RawMessage(`[{"fromBlock":"latest","toBlock":"0x63"}]`)})
	if rpcErr == nil || rpcErr.Code != jsonrpc.CodeInvalidParams {
		t.Fatalf("reversed log range was not rejected: %v", rpcErr)
	}
}

func TestGatewayFailsClosedWhenNoProviderHasAcceptedHead(t *testing.T) {
	provider := newFakeEVM(t, 99, testHash('9'), testHash('8'))
	proxy, store := newTestGateway(t, 150*time.Millisecond, []config.UpstreamConfig{{ID: "stale", HTTPURL: provider.server.URL, MaxConcurrency: 8}})
	store.Set(head.Head{Chain: "test", Family: "evm", Commitment: head.Latest, Number: 100, Hash: testHash('a'), ParentHash: testHash('9'), Origin: "missing", ObservedAt: time.Now()})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/rpc/test", bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x0000000000000000000000000000000000000000","latest"]}`))
	proxy.ServeRPC(recorder, request, "test")
	var response jsonrpc.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Code != jsonrpc.CodeConsistencyUnavailable {
		t.Fatalf("expected consistency error, got %s", recorder.Body.String())
	}
	if provider.calls() != 1 {
		t.Fatalf("stale provider received state read; calls=%d", provider.calls())
	}
}

func TestGatewayWaitsForProviderToCatchUp(t *testing.T) {
	provider := newFakeEVM(t, 99, testHash('9'), testHash('8'))
	proxy, store := newTestGateway(t, 900*time.Millisecond, []config.UpstreamConfig{{ID: "catching-up", HTTPURL: provider.server.URL, MaxConcurrency: 8}})
	store.Set(head.Head{Chain: "test", Family: "evm", Commitment: head.Latest, Number: 100, Hash: testHash('a'), ParentHash: testHash('9'), Origin: "catching-up", ObservedAt: time.Now()})
	go func() {
		time.Sleep(100 * time.Millisecond)
		provider.set(100, testHash('a'), testHash('9'))
	}()
	started := time.Now()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/rpc/test", bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x0000000000000000000000000000000000000000","latest"]}`))
	proxy.ServeRPC(recorder, request, "test")
	var response jsonrpc.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error != nil || string(response.Result) != `"0x64"` {
		t.Fatalf("expected catch-up success, got %s", recorder.Body.String())
	}
	if elapsed := time.Since(started); elapsed < 100*time.Millisecond || elapsed >= 900*time.Millisecond {
		t.Fatalf("unexpected wait duration: %s", elapsed)
	}
}

func TestGatewayFailsOverAfterOriginFailsAndPeerCatchesUp(t *testing.T) {
	origin := newFakeEVM(t, 100, testHash('a'), testHash('9'))
	peer := newFakeEVM(t, 99, testHash('9'), testHash('8'))
	proxy, store := newTestGateway(t, 900*time.Millisecond, []config.UpstreamConfig{
		{ID: "origin", HTTPURL: origin.server.URL, MaxConcurrency: 8},
		{ID: "peer", HTTPURL: peer.server.URL, MaxConcurrency: 8},
	})
	origin.setFailState(true)
	store.Set(head.Head{Chain: "test", Family: "evm", Commitment: head.Latest, Number: 100, Hash: testHash('a'), ParentHash: testHash('9'), Origin: "origin", ObservedAt: time.Now()})
	go func() {
		time.Sleep(100 * time.Millisecond)
		peer.set(100, testHash('a'), testHash('9'))
	}()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/rpc/test", bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x0000000000000000000000000000000000000000","latest"]}`))
	proxy.ServeRPC(recorder, request, "test")
	var response jsonrpc.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error != nil || string(response.Result) != `"0x64"` || recorder.Header().Get("X-RPC-Upstream") != "peer" {
		t.Fatalf("expected successful peer failover, got headers=%v response=%s", recorder.Header(), recorder.Body.String())
	}
	if origin.calls() != 2 || peer.calls() != 2 {
		t.Fatalf("unexpected state attempts: origin=%d peer=%d", origin.calls(), peer.calls())
	}
}

func TestGatewayRejectsWritesAndPreservesBatchSnapshot(t *testing.T) {
	provider := newFakeEVM(t, 100, testHash('a'), testHash('9'))
	proxy, store := newTestGateway(t, time.Second, []config.UpstreamConfig{{ID: "a", HTTPURL: provider.server.URL, MaxConcurrency: 8}})
	store.Set(head.Head{Chain: "test", Family: "evm", Commitment: head.Latest, Number: 100, Hash: testHash('a'), Origin: "a", ObservedAt: time.Now()})
	body := `[{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"},{"jsonrpc":"2.0","id":2,"method":"eth_sendRawTransaction","params":["0x00"]}]`
	recorder := httptest.NewRecorder()
	proxy.ServeRPC(recorder, httptest.NewRequest(http.MethodPost, "/rpc/test", bytes.NewBufferString(body)), "test")
	var responses []jsonrpc.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &responses); err != nil {
		t.Fatal(err)
	}
	if len(responses) != 2 || string(responses[0].Result) != `"0x64"` || responses[1].Error == nil || responses[1].Error.Code != jsonrpc.CodeWritesDisabled {
		t.Fatalf("unexpected batch: %s", recorder.Body.String())
	}
}

func TestGatewayReturnsErrorForInvalidBatchMember(t *testing.T) {
	provider := newFakeEVM(t, 100, testHash('a'), testHash('9'))
	proxy, store := newTestGateway(t, time.Second, []config.UpstreamConfig{{ID: "a", HTTPURL: provider.server.URL, MaxConcurrency: 8}})
	store.Set(head.Head{Chain: "test", Family: "evm", Commitment: head.Latest, Number: 100, Hash: testHash('a'), Origin: "a", ObservedAt: time.Now()})
	recorder := httptest.NewRecorder()
	proxy.ServeRPC(recorder, httptest.NewRequest(http.MethodPost, "/rpc/test", bytes.NewBufferString(`[1,{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber"}]`)), "test")
	var responses []jsonrpc.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &responses); err != nil {
		t.Fatal(err)
	}
	if len(responses) != 2 || responses[0].Error == nil || responses[0].Error.Code != jsonrpc.CodeInvalidRequest || string(responses[1].Result) != `"0x64"` {
		t.Fatalf("unexpected batch response: %s", recorder.Body.String())
	}
}

func TestGatewayNotificationHasNoResponseBody(t *testing.T) {
	provider := newFakeEVM(t, 100, testHash('a'), testHash('9'))
	proxy, store := newTestGateway(t, time.Second, []config.UpstreamConfig{{ID: "a", HTTPURL: provider.server.URL, MaxConcurrency: 8}})
	store.Set(head.Head{Chain: "test", Family: "evm", Commitment: head.Latest, Number: 100, Hash: testHash('a'), Origin: "a", ObservedAt: time.Now()})
	recorder := httptest.NewRecorder()
	proxy.ServeRPC(recorder, httptest.NewRequest(http.MethodPost, "/rpc/test", bytes.NewBufferString(`{"jsonrpc":"2.0","method":"eth_blockNumber"}`)), "test")
	if recorder.Code != http.StatusNoContent || recorder.Body.Len() != 0 {
		t.Fatalf("notification produced a response: status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestGatewayLocalBlockNumberFailsClosedForStaleHead(t *testing.T) {
	proxy, store := newTestGateway(t, time.Second, nil)
	for _, observedAt := range []time.Time{time.Now().Add(-time.Minute), {}} {
		store.Set(head.Head{Chain: "test", Family: "evm", Commitment: head.Latest, Number: 100, Hash: testHash('a'), ObservedAt: observedAt})
		recorder := httptest.NewRecorder()
		proxy.ServeRPC(recorder, httptest.NewRequest(http.MethodPost, "/rpc/test", strings.NewReader(`{"jsonrpc":"2.0","id":"stale","method":"eth_blockNumber"}`)), "test")
		var response jsonrpc.Response
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if response.Error == nil || response.Error.Code != jsonrpc.CodeConsistencyUnavailable || string(response.ID) != `"stale"` {
			t.Fatalf("stale local response: %s", recorder.Body.String())
		}
	}
}

func TestUnknownChainPreservesBatchIDsAndNotifications(t *testing.T) {
	proxy, _ := newTestGateway(t, time.Second, nil)
	recorder := httptest.NewRecorder()
	proxy.ServeRPC(recorder, httptest.NewRequest(http.MethodPost, "/rpc/missing", strings.NewReader(`[{"jsonrpc":"2.0","id":"call","method":"eth_blockNumber"},{"jsonrpc":"2.0","method":"eth_blockNumber"},false]`)), "missing")
	var responses []jsonrpc.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &responses); err != nil {
		t.Fatal(err)
	}
	if len(responses) != 2 || string(responses[0].ID) != `"call"` || responses[0].Error.Code != jsonrpc.CodeChainUnavailable || responses[1].Error.Code != jsonrpc.CodeInvalidRequest {
		t.Fatalf("unexpected responses: %s", recorder.Body.String())
	}
}

func TestDiagnosticHeadersDescribeActualTargets(t *testing.T) {
	proxy, _ := newTestGateway(t, time.Second, nil)
	historical := head.Head{Family: "evm", Commitment: "explicit", Number: 99, Hash: testHash('b')}
	latest := head.Head{Family: "evm", Commitment: head.Latest, Number: 100, Hash: testHash('a')}
	headers := http.Header{}
	proxy.setResponseHeadHeaders(headers, "evm", []processedResponse{{target: &historical}})
	if headers.Get("X-RPC-Head-Hash") != historical.Hash || headers.Get("X-RPC-Head-Number") != "0x63" {
		t.Fatalf("wrong historical headers: %v", headers)
	}
	headers = http.Header{}
	proxy.setResponseHeadHeaders(headers, "evm", []processedResponse{{target: &historical}, {target: &latest}})
	if headers.Get("X-RPC-Consistency") != "mixed-targets" || headers.Get("X-RPC-Head-Hash") != "" {
		t.Fatalf("misleading mixed-batch headers: %v", headers)
	}
}

func newTestGateway(t *testing.T, timeout time.Duration, upstreams []config.UpstreamConfig) (*Gateway, *head.MemoryStore) {
	t.Helper()
	tel, err := telemetry.NewTelemetry(config.DatadogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Server: config.ServerConfig{RequestTimeout: config.Duration(timeout), MaxBodyBytes: 1 << 20, MaxBatchSize: 100, WebsocketQueueSize: 16}}
	chainConfig := config.ChainConfig{Name: "test", Family: "evm", ChainID: "0x1", PollInterval: config.Duration(20 * time.Millisecond), MaxHeadAge: config.Duration(5 * time.Second), ReorgDepth: 16, Upstreams: upstreams}
	runtime := chain.NewRuntime(chainConfig, tel)
	validationCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	if err := runtime.Validate(validationCtx); err != nil {
		t.Fatal(err)
	}
	cancel()
	store := head.NewMemoryStore()
	gateway := NewGateway(Options{Config: cfg, Store: store, Runtimes: map[string]*chain.Runtime{"test": runtime}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Telemetry: tel})
	return gateway, store
}
