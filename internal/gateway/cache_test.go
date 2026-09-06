package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
)

func TestGatewayStaticIdentityNeedsNoStoreOrProvider(t *testing.T) {
	for _, test := range []struct{ family, method, identity string }{
		{"evm", "eth_chainId", "0x1"},
		{"solana", "getGenesisHash", "test-genesis"},
	} {
		t.Run(test.method, func(t *testing.T) {
			proxy, _ := newTestGateway(t, time.Second, nil)
			proxy.runtimes["test"].Config.Family = test.family
			proxy.runtimes["test"].Config.GenesisHash = test.identity
			proxy.store = nil // Any store access fails this test, even if its error were ignored.
			for _, parameters := range []string{"", `,"params":[]`, `,"params":null`} {
				recorder := httptest.NewRecorder()
				body := `{"jsonrpc":"2.0","id":"identity","method":"` + test.method + `"` + parameters + `}`
				proxy.ServeRPC(recorder, httptest.NewRequest(http.MethodPost, "/rpc/test", bytes.NewBufferString(body)), "test")
				response := decodeCacheResponse(t, recorder)
				if response.Error != nil || string(response.Result) != `"`+test.identity+`"` || string(response.ID) != `"identity"` || recorder.Header().Get("X-RPC-Upstream") != "proxy" || recorder.Header().Get("X-RPC-Cache") != "static" || recorder.Header().Get("X-RPC-Head-Hash") != "" {
					t.Fatalf("static identity response: %s headers=%v", recorder.Body.String(), recorder.Header())
				}
			}
			for _, invalid := range []struct {
				body string
				code int
			}{
				{`{"jsonrpc":"1.0","id":1,"method":"` + test.method + `"}`, jsonrpc.CodeInvalidRequest},
				{`{"jsonrpc":"2.0","id":1,"method":"` + test.method + `","params":[1]}`, jsonrpc.CodeInvalidParams},
				{`{"jsonrpc":"2.0","id":1,"method":"` + test.method + `","params":{}}`, jsonrpc.CodeInvalidParams},
			} {
				recorder := httptest.NewRecorder()
				proxy.ServeRPC(recorder, httptest.NewRequest(http.MethodPost, "/rpc/test", bytes.NewBufferString(invalid.body)), "test")
				response := decodeCacheResponse(t, recorder)
				if response.Error == nil || response.Error.Code != invalid.code {
					t.Fatalf("invalid static request accepted: %s", recorder.Body.String())
				}
			}
			recorder := httptest.NewRecorder()
			body := `[{"jsonrpc":"2.0","method":"` + test.method + `"}]`
			proxy.ServeRPC(recorder, httptest.NewRequest(http.MethodPost, "/rpc/test", bytes.NewBufferString(body)), "test")
			if recorder.Code != http.StatusNoContent || recorder.Body.Len() != 0 {
				t.Fatalf("static notification returned a body: %s", recorder.Body.String())
			}
		})
	}
}

func TestGatewayStaticBatchItemSurvivesStoreFailure(t *testing.T) {
	fixture := newReorgGatewayFixture(t)
	faultStore := &unavailableSnapshotStore{Store: fixture.store}
	faultStore.unavailable.Store(true)
	fixture.proxy.store = faultStore
	recorder := fixture.request(`[{"jsonrpc":"2.0","id":1,"method":"eth_chainId"},{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber"}]`)
	var responses []jsonrpc.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &responses); err != nil {
		t.Fatal(err)
	}
	if len(responses) != 2 || responses[0].Error != nil || string(responses[0].Result) != `"0x1"` || responses[1].Error == nil || responses[1].Error.Code != jsonrpc.CodeConsistencyUnavailable {
		t.Fatalf("store failure broke static batch item: %s", recorder.Body.String())
	}
}

func TestGatewayCacheSharesBatchResultsAndPreservesIDs(t *testing.T) {
	fixture := newReorgGatewayFixture(t)
	before := fixture.provider.calls()
	recorder := fixture.request(`[
		{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["address","latest"]},
		{"jsonrpc":"2.0","id":"two","method":"eth_getBalance","params":["address","latest"]},
		{"jsonrpc":"2.0","method":"eth_getBalance","params":["address","latest"]}
	]`)
	var responses []jsonrpc.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &responses); err != nil {
		t.Fatal(err)
	}
	if len(responses) != 2 || string(responses[0].ID) != "1" || string(responses[1].ID) != `"two"` || fixture.provider.calls() != before+1 {
		t.Fatalf("batch did not share one load with independent IDs: %s calls=%d", recorder.Body.String(), fixture.provider.calls()-before)
	}
	for _, response := range responses {
		if response.Error != nil || string(response.Result) != `"0x64"` {
			t.Fatalf("batch load failed: %s", recorder.Body.String())
		}
	}
	recorder = fixture.request(`{"jsonrpc":"2.0","id":null,"method":"eth_getBalance","params":["address","latest"]}`)
	response := decodeCacheResponse(t, recorder)
	if response.Error != nil || string(response.ID) != "null" || recorder.Header().Get("X-RPC-Cache") != "hit" || fixture.provider.calls() != before+1 || recorder.Header().Get("X-RPC-Head-Hash") != fixture.current.Hash {
		t.Fatalf("cached response lost ID or head: %s headers=%v", recorder.Body.String(), recorder.Header())
	}
	recorder = fixture.request(`{"jsonrpc":"2.0","id":3,"method":"eth_getBalance","params":["different-address","latest"]}`)
	if response := decodeCacheResponse(t, recorder); response.Error != nil || fixture.provider.calls() != before+2 || recorder.Header().Get("X-RPC-Cache") != "miss" {
		t.Fatalf("different parameters reused the cache: %s", recorder.Body.String())
	}
}

func TestGatewayCacheSeparatesHeadsChainsAndReorgEpochs(t *testing.T) {
	fixture := newReorgGatewayFixture(t)
	const body = `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["address","latest"]}`
	if response := decodeCacheResponse(t, fixture.request(body)); response.Error != nil {
		t.Fatal(response.Error)
	}
	before := fixture.provider.calls()
	otherConfig := fixture.proxy.runtimes["test"].Config
	otherConfig.Name = "other"
	otherRuntime := chain.NewRuntime(otherConfig, fixture.proxy.telemetry)
	if err := otherRuntime.Validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	fixture.proxy.runtimes["other"] = otherRuntime
	otherHead := fixture.current
	otherHead.Chain = "other"
	fixture.store.Set(otherHead)
	before = fixture.provider.calls()
	otherRecorder := httptest.NewRecorder()
	fixture.proxy.ServeRPC(otherRecorder, httptest.NewRequest(http.MethodPost, "/rpc/other", bytes.NewBufferString(body)), "other")
	if response := decodeCacheResponse(t, otherRecorder); response.Error != nil || fixture.provider.calls() != before+1 || otherRecorder.Header().Get("X-RPC-Cache") != "miss" {
		t.Fatalf("another chain reused the cache: %s", otherRecorder.Body.String())
	}
	for _, transition := range []string{"new head", "return to original fork"} {
		before = fixture.provider.calls()
		if transition == "new head" {
			next := fixture.current
			next.Number++
			next.ParentHash, next.Hash = next.Hash, testHash('b')
			fixture.publish(next)
			fixture.provider.set(next.Number, next.Hash, next.ParentHash)
		} else {
			fixture.beginReorg()
			fixture.recover(testHash('a'))
			fixture.provider.set(fixture.current.Number, fixture.current.Hash, fixture.current.ParentHash)
		}
		recorder := fixture.request(body)
		if response := decodeCacheResponse(t, recorder); response.Error != nil || fixture.provider.calls() != before+1 || recorder.Header().Get("X-RPC-Cache") != "miss" {
			t.Fatalf("%s reused old state: %s headers=%v", transition, recorder.Body.String(), recorder.Header())
		}
	}
}

type cacheFenceStore struct {
	head.Store
	calls          atomic.Int32
	beforeSnapshot func(int32)
}

// Snapshot runs the test's transition before reading the requested shared head.
func (s *cacheFenceStore) Snapshot(ctx context.Context, chainName string) (head.Snapshot, error) {
	s.beforeSnapshot(s.calls.Add(1))
	return s.Store.Snapshot(ctx, chainName)
}

func TestGatewayCacheStillRequiresFreshHeadAndFinalReorgFence(t *testing.T) {
	for _, failure := range []string{"pending reorg", "stale head", "redis unavailable", "reorg during cache hit"} {
		t.Run(failure, func(t *testing.T) {
			fixture := newReorgGatewayFixture(t)
			const body = `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["address","latest"]}`
			if response := decodeCacheResponse(t, fixture.request(body)); response.Error != nil {
				t.Fatal(response.Error)
			}
			before := fixture.provider.calls()
			switch failure {
			case "pending reorg":
				fixture.beginReorg()
			case "stale head":
				stale := fixture.current
				stale.ObservedAt = time.Now().Add(-time.Minute)
				fixture.publish(stale)
			case "redis unavailable":
				faultStore := &unavailableSnapshotStore{Store: fixture.store}
				faultStore.unavailable.Store(true)
				fixture.proxy.store = faultStore
			case "reorg during cache hit":
				fixture.proxy.store = &cacheFenceStore{Store: fixture.store, beforeSnapshot: func(calls int32) {
					if calls == 2 {
						fixture.beginReorg()
					}
				}}
			}
			recorder := fixture.request(body)
			response := decodeCacheResponse(t, recorder)
			if response.Error == nil || response.Error.Code != jsonrpc.CodeConsistencyUnavailable || len(response.Result) != 0 || fixture.provider.calls() != before || recorder.Header().Get("X-RPC-Head-Hash") != "" {
				t.Fatalf("cache escaped %s: %s headers=%v", failure, recorder.Body.String(), recorder.Header())
			}
		})
	}
}

func TestGatewayCacheDoesNotStoreUpstreamErrors(t *testing.T) {
	fixture := newReorgGatewayFixture(t)
	fixture.provider.mu.Lock()
	fixture.provider.stateError = &jsonrpc.Error{Code: -32000, Message: "execution reverted"}
	fixture.provider.mu.Unlock()
	const body = `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["address","latest"]}`
	if response := decodeCacheResponse(t, fixture.request(body)); response.Error == nil || response.Error.Code != -32000 {
		t.Fatalf("lost upstream error: %+v", response)
	}
	fixture.provider.mu.Lock()
	fixture.provider.stateError = nil
	fixture.provider.mu.Unlock()
	before := fixture.provider.calls()
	recorder := fixture.request(body)
	if response := decodeCacheResponse(t, recorder); response.Error != nil || fixture.provider.calls() != before+1 {
		t.Fatalf("error cached: %s", recorder.Body.String())
	}
}

func TestGatewayCachesBlockHashRequestsWithDistinctFlags(t *testing.T) {
	fixture := newReorgGatewayFixture(t)
	for _, fullTransactions := range []bool{false, true} {
		body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["latest",%t]}`, fullTransactions)
		for _, expectedStatus := range []string{"miss", "hit"} {
			recorder := fixture.request(body)
			if response := decodeCacheResponse(t, recorder); response.Error != nil || recorder.Header().Get("X-RPC-Cache") != expectedStatus {
				t.Fatalf("block flags collided: %s headers=%v", recorder.Body.String(), recorder.Header())
			}
		}
	}
}

func TestGatewayCacheDistinguishesSingleBlockLogsFromRanges(t *testing.T) {
	for _, test := range []struct {
		name, method, parameters, payload string
		cacheable                         bool
	}{
		{"block hash", "eth_getLogs", `[{"blockHash":"` + testHash('a') + `"}]`, `[]`, true},
		{"single block", "eth_getLogs", `[{"fromBlock":"latest","toBlock":"latest","topics":[]}]`, `[]`, true},
		{"range", "eth_getLogs", `[{"fromBlock":"0x63","toBlock":"latest"}]`, `[]`, false},
		{"fee history", "eth_feeHistory", `[2,"latest",[]]`, `{"oldestBlock":"0x63","baseFeePerGas":["0x1","0x1","0x1"],"gasUsedRatio":[0.5,0.5]}`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := newFakeEVM(t, 100, testHash('a'), testHash('9'))
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				body, err := io.ReadAll(request.Body)
				if err != nil {
					t.Errorf("read cache test request: %v", err)
					return
				}
				var call jsonrpc.Request
				if err := json.Unmarshal(body, &call); err != nil {
					t.Errorf("decode cache test request: %v", err)
					return
				}
				if call.Method == test.method {
					calls.Add(1)
					writeFake(t, w, call.ID, json.RawMessage(test.payload), nil)
					return
				}
				if call.Method == "eth_getBlockByNumber" && bytes.Equal(call.Params, []byte(`["0x63",false]`)) {
					writeFake(t, w, call.ID, evmBlock(99, testHash('9'), testHash('8')), nil)
					return
				}
				request.Body = io.NopCloser(bytes.NewReader(body))
				provider.serveHTTP(w, request)
			}))
			t.Cleanup(server.Close)
			proxy, store := newTestGateway(t, time.Second, []config.UpstreamConfig{{ID: "provider", HTTPURL: server.URL, MaxConcurrency: 8}})
			store.Set(head.Head{Chain: "test", Family: "evm", Commitment: head.Latest, Number: 100, Hash: testHash('a'), ParentHash: testHash('9'), Origin: "provider", ObservedAt: time.Now()})
			for attempt := range 2 {
				recorder := httptest.NewRecorder()
				body := fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":%q,"params":%s}`, attempt, test.method, test.parameters)
				proxy.ServeRPC(recorder, httptest.NewRequest(http.MethodPost, "/rpc/test", bytes.NewBufferString(body)), "test")
				response := decodeCacheResponse(t, recorder)
				if response.Error != nil || string(response.Result) != test.payload {
					t.Fatalf("read failed: %s", recorder.Body.String())
				}
				consistency := "exact-block-hash"
				if !test.cacheable {
					consistency = "boundary-checked"
				}
				if recorder.Header().Get("X-RPC-Consistency") != consistency {
					t.Fatalf("overstated consistency: %v", recorder.Header())
				}
				if attempt == 1 {
					if test.cacheable && (calls.Load() != 1 || recorder.Header().Get("X-RPC-Cache") != "hit") || !test.cacheable && (calls.Load() != 2 || recorder.Header().Get("X-RPC-Cache") != "bypass") {
						t.Fatalf("incorrect cache policy: calls=%d headers=%v", calls.Load(), recorder.Header())
					}
				}
			}
		})
	}
}

func TestGatewayCacheRetainsOnlyVerifiedReceiptInclusion(t *testing.T) {
	fixture := newReorgGatewayFixture(t)
	transactionHash := testHash('c')
	receipt := `{"transactionHash":"` + transactionHash + `","blockHash":"` + fixture.current.Hash + `","blockNumber":"0x64","status":"0x1","logs":[]}`
	for _, test := range []struct{ upstream, expected, status string }{
		{"null", "null", "miss"},
		{receipt, receipt, "miss"},
		{"null", receipt, "hit"},
	} {
		fixture.provider.setReceipt(receiptReply{receipt: json.RawMessage(test.upstream)})
		recorder := fixture.request(`{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["` + transactionHash + `"]}`)
		if response := decodeCacheResponse(t, recorder); response.Error != nil || string(response.Result) != test.expected || recorder.Header().Get("X-RPC-Cache") != test.status {
			t.Fatalf("cached receipt absence or inclusion: %s headers=%v", recorder.Body.String(), recorder.Header())
		}
	}
	if calls := fixture.provider.receiptCallCount(); calls != 2 {
		t.Fatalf("expected null lookup and one verified receipt load, got %d upstream calls", calls)
	}
}

func TestGatewayCacheDoesNotFreezeSolanaContext(t *testing.T) {
	provider := newFakeSolana(t, 100)
	proxy, store := newSolanaTestGateway(t, []config.UpstreamConfig{{ID: "provider", HTTPURL: provider.server.URL, MaxConcurrency: 8}})
	store.Set(head.Head{Chain: "solana", Family: "solana", Commitment: head.Finalized, Number: 100, Origin: "provider", ObservedAt: time.Now()})
	for _, slot := range []uint64{100, 101} {
		provider.mu.Lock()
		provider.slot = slot
		provider.mu.Unlock()
		recorder := httptest.NewRecorder()
		body := `{"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":["account"]}`
		proxy.ServeRPC(recorder, httptest.NewRequest(http.MethodPost, "/rpc/solana", bytes.NewBufferString(body)), "solana")
		response := decodeCacheResponse(t, recorder)
		var account struct {
			Context struct {
				Slot uint64 `json:"slot"`
			} `json:"context"`
		}
		if err := json.Unmarshal(response.Result, &account); err != nil {
			t.Fatal(err)
		}
		if response.Error != nil || account.Context.Slot != slot || recorder.Header().Get("X-RPC-Cache") != "bypass" {
			t.Fatalf("Solana context was cached: %s", recorder.Body.String())
		}
	}
	if calls, _ := provider.callsAndMinimum(); calls != 2 {
		t.Fatalf("Solana state was cached: calls=%d", calls)
	}
}

func decodeCacheResponse(t *testing.T, recorder *httptest.ResponseRecorder) jsonrpc.Response {
	t.Helper()
	var response jsonrpc.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response %s: %v", recorder.Body.String(), err)
	}
	return response
}
