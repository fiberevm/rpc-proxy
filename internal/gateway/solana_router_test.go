package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
	"github.com/fiberevm/rpc-proxy/internal/telemetry"
)

type fakeSolana struct {
	t            *testing.T
	mu           sync.Mutex
	slot         uint64
	stateCalls   int
	lastConfig   map[string]json.RawMessage
	server       *httptest.Server
	responseSlot *uint64
	omitContext  bool
}

func newFakeSolana(t *testing.T, slot uint64) *fakeSolana {
	fake := &fakeSolana{t: t, slot: slot}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeSolana) serveHTTP(w http.ResponseWriter, request *http.Request) {
	var call struct {
		ID     json.RawMessage   `json:"id"`
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	if err := json.NewDecoder(request.Body).Decode(&call); err != nil {
		f.t.Errorf("decode fake request: %v", err)
		return
	}
	f.mu.Lock()
	slot := f.slot
	responseSlot := slot
	if f.responseSlot != nil {
		responseSlot = *f.responseSlot
	}
	omitContext := f.omitContext
	f.mu.Unlock()
	switch call.Method {
	case "getGenesisHash":
		writeFake(f.t, w, call.ID, "test-genesis", nil)
	case "getSlot":
		minimum := solanaMinimum(f.t, call.Params, 0)
		if slot < minimum {
			writeFake(f.t, w, call.ID, nil, &jsonrpc.Error{Code: -32016, Message: "Minimum context slot has not been reached"})
			return
		}
		writeFake(f.t, w, call.ID, slot, nil)
	case "getAccountInfo", "getProgramAccounts":
		var account string
		if len(call.Params) > 0 {
			if err := json.Unmarshal(call.Params[0], &account); err != nil {
				f.t.Errorf("decode fake Solana account: %v", err)
				return
			}
		}
		var cfg map[string]json.RawMessage
		if len(call.Params) > 1 {
			if err := json.Unmarshal(call.Params[1], &cfg); err != nil {
				f.t.Errorf("decode fake Solana configuration: %v", err)
				return
			}
		}
		if account == "account" {
			f.mu.Lock()
			f.stateCalls++
			f.lastConfig = cfg
			f.mu.Unlock()
		}
		minimum := solanaMinimum(f.t, call.Params, 1)
		if slot < minimum {
			writeFake(f.t, w, call.ID, nil, &jsonrpc.Error{Code: -32016, Message: "Minimum context slot has not been reached"})
			return
		}
		var accountState any
		if call.Method == "getProgramAccounts" {
			accountState = []any{}
		}
		if omitContext {
			writeFake(f.t, w, call.ID, accountState, nil)
		} else {
			writeFake(f.t, w, call.ID, map[string]any{"context": map[string]any{"slot": responseSlot}, "value": accountState}, nil)
		}
	default:
		writeFake(f.t, w, call.ID, nil, &jsonrpc.Error{Code: -32601, Message: "method not found"})
	}
}

func solanaMinimum(t *testing.T, parameters []json.RawMessage, index int) uint64 {
	t.Helper()
	if len(parameters) <= index {
		return 0
	}
	var cfg struct {
		MinContextSlot uint64 `json:"minContextSlot"`
	}
	if err := json.Unmarshal(parameters[index], &cfg); err != nil {
		t.Errorf("decode minimum Solana context: %v", err)
		return 0
	}
	return cfg.MinContextSlot
}

func (f *fakeSolana) callsAndMinimum() (int, uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var minimum uint64
	if f.lastConfig != nil && json.Unmarshal(f.lastConfig["minContextSlot"], &minimum) != nil {
		f.t.Errorf("invalid recorded minContextSlot: %#v", f.lastConfig["minContextSlot"])
	}
	return f.stateCalls, minimum
}

func TestGatewaySolanaInjectsSlotFloorAndSkipsStaleProvider(t *testing.T) {
	fresh := newFakeSolana(t, 100)
	stale := newFakeSolana(t, 99)
	proxy, store := newSolanaTestGateway(t, []config.UpstreamConfig{
		{ID: "fresh", HTTPURL: fresh.server.URL, MaxConcurrency: 8},
		{ID: "stale", HTTPURL: stale.server.URL, MaxConcurrency: 8},
	})
	store.Set(head.Head{Chain: "solana", Family: "solana", Commitment: head.Finalized, Number: 100, Origin: "fresh", ObservedAt: time.Now()})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/rpc/solana", bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":["account",{"commitment":"finalized","minContextSlot":90}]}`))
	proxy.ServeRPC(recorder, request, "solana")
	var response jsonrpc.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error != nil {
		t.Fatalf("unexpected response: %s", recorder.Body.String())
	}
	if calls, minimum := fresh.callsAndMinimum(); calls != 1 || minimum != 100 {
		t.Fatalf("fresh provider calls=%d minimum=%d", calls, minimum)
	}
	if calls, minimum := stale.callsAndMinimum(); calls != 0 {
		t.Fatalf("stale provider answered state request: calls=%d minimum=%d", calls, minimum)
	}
	if recorder.Header().Get("X-RPC-Head-Slot") != "100" || recorder.Header().Get("X-RPC-Consistency") != "minimum-context-slot" {
		t.Fatalf("unexpected headers: %#v", recorder.Header())
	}
}

func newSolanaTestGateway(t *testing.T, upstreams []config.UpstreamConfig) (*Gateway, *head.MemoryStore) {
	t.Helper()
	tel, err := telemetry.NewTelemetry(config.DatadogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Server: config.ServerConfig{RequestTimeout: config.Duration(time.Second), MaxBodyBytes: 1 << 20, MaxBatchSize: 100, WebsocketQueueSize: 16}}
	chainConfig := config.ChainConfig{Name: "solana", Family: "solana", GenesisHash: "test-genesis", PollInterval: config.Duration(20 * time.Millisecond), MaxHeadAge: config.Duration(5 * time.Second), ReorgDepth: 16, Upstreams: upstreams}
	runtime := chain.NewRuntime(chainConfig, tel)
	validationCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	if err := runtime.Validate(validationCtx); err != nil {
		t.Fatal(err)
	}
	cancel()
	store := head.NewMemoryStore()
	proxy := NewGateway(Options{Config: cfg, Store: store, Runtimes: map[string]*chain.Runtime{"solana": runtime}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Telemetry: tel})
	return proxy, store
}

func TestSolanaRejectsStateBelowCallerFloorAndMissingContext(t *testing.T) {
	for _, omitContext := range []bool{false, true} {
		provider := newFakeSolana(t, 120)
		proxy, store := newSolanaTestGateway(t, []config.UpstreamConfig{{ID: "provider", HTTPURL: provider.server.URL, MaxConcurrency: 8}})
		provider.mu.Lock()
		staleSlot := uint64(100)
		provider.responseSlot = &staleSlot
		provider.omitContext = omitContext
		provider.mu.Unlock()
		store.Set(head.Head{Chain: "solana", Family: "solana", Commitment: head.Finalized, Number: 100, ObservedAt: time.Now()})
		recorder := httptest.NewRecorder()
		proxy.ServeRPC(recorder, httptest.NewRequest(http.MethodPost, "/rpc/solana", bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":["account",{"minContextSlot":120}]}`)), "solana")
		var response jsonrpc.Response
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if response.Error == nil || response.Error.Code != jsonrpc.CodeConsistencyUnavailable {
			t.Fatalf("accepted unverifiable state: %s", recorder.Body.String())
		}
	}
}

func TestProgramAccountsPreservesResponseShape(t *testing.T) {
	provider := newFakeSolana(t, 120)
	proxy, store := newSolanaTestGateway(t, []config.UpstreamConfig{{ID: "provider", HTTPURL: provider.server.URL, MaxConcurrency: 8}})
	store.Set(head.Head{Chain: "solana", Family: "solana", Commitment: head.Finalized, Number: 100, ObservedAt: time.Now()})
	for _, testCase := range []struct{ options, want string }{
		{`{}`, `[]`},
		{`{"withContext":false}`, `[]`},
		{`{"withContext":true}`, `{"context":{"slot":120},"value":[]}`},
	} {
		recorder := httptest.NewRecorder()
		proxy.ServeRPC(recorder, httptest.NewRequest(http.MethodPost, "/rpc/solana", bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"getProgramAccounts","params":["account",`+testCase.options+`]}`)), "solana")
		var response jsonrpc.Response
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if response.Error != nil || string(response.Result) != testCase.want {
			t.Fatalf("unexpected response shape: %s", recorder.Body.String())
		}
	}
}
