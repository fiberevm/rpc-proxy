package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
)

func TestBatchNumberedReadsCannotSelectProviderFork(t *testing.T) {
	provider := newFakeEVM(t, 100, testHash('a'), testHash('9'))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var call jsonrpc.Request
		if err := json.Unmarshal(body, &call); err != nil {
			t.Error(err)
			return
		}
		// One provider URL may balance numbered reads and hash reads across forks.
		if call.Method == "eth_getBlockByNumber" && strings.Contains(string(call.Params), `"0x64"`) || call.Method == "eth_getBlockByHash" && strings.Contains(string(call.Params), testHash('b')) {
			writeFake(t, w, call.ID, evmBlock(100, testHash('b'), testHash('9')), nil)
			return
		}
		if call.Method == "eth_getBalance" && strings.Contains(string(call.Params), testHash('b')) {
			writeFake(t, w, call.ID, "0x2", nil)
			return
		}
		request.Body = io.NopCloser(bytes.NewReader(body))
		provider.serveHTTP(w, request)
	}))
	t.Cleanup(server.Close)
	proxy, store := newTestGateway(t, time.Second, []config.UpstreamConfig{{ID: "provider", HTTPURL: server.URL, MaxConcurrency: 8}})
	store.Set(head.Head{Chain: "test", Family: "evm", Commitment: head.Latest, Number: 100, Hash: testHash('a'), ParentHash: testHash('9'), Origin: "provider", ObservedAt: time.Now()})
	for range 2 {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/rpc/test", strings.NewReader(`[
			{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["address","latest"]},
			{"jsonrpc":"2.0","id":2,"method":"eth_getBalance","params":["address","0x64"]},
			{"jsonrpc":"2.0","id":3,"method":"eth_getBalance","params":["address",{"blockNumber":"0x64"}]}
		]`))
		proxy.ServeRPC(recorder, request, "test")
		var responses []jsonrpc.Response
		if err := json.Unmarshal(recorder.Body.Bytes(), &responses); err != nil {
			t.Fatal(err)
		}
		if len(responses) != 3 {
			t.Fatalf("unexpected batch: %s", recorder.Body.String())
		}
		for _, response := range responses {
			if response.Error != nil || string(response.Result) != `"0x64"` {
				t.Fatalf("provider fork escaped accepted snapshot: %s", recorder.Body.String())
			}
		}
		if recorder.Header().Get("X-RPC-Consistency") != "exact-block-hash" || recorder.Header().Get("X-RPC-Head-Hash") != testHash('a') {
			t.Fatalf("same-state batch lost its block identity: %v", recorder.Header())
		}
	}
}

func TestInvalidPinnedResponsesFailClosedWithoutPoisoningCache(t *testing.T) {
	for _, test := range []struct {
		name, method, parameters, invalid, valid string
	}{
		{"null state", "eth_getBalance", `["address","latest"]`, `null`, `"0x64"`},
		{"wrong block", "eth_getBlockByHash", `["` + testHash('a') + `",true]`, `{"number":"0x64","hash":"` + testHash('b') + `","parentHash":"` + testHash('9') + `"}`, `{"number":"0x64","hash":"` + testHash('a') + `","parentHash":"` + testHash('9') + `"}`},
		{"null block", "eth_getBlockByHash", `["` + testHash('a') + `",true]`, `null`, `{"number":"0x64","hash":"` + testHash('a') + `","parentHash":"` + testHash('9') + `"}`},
		{"wrong logs", "eth_getLogs", `[{"blockHash":"` + testHash('a') + `"}]`, `[{"blockHash":"` + testHash('b') + `"}]`, `[]`},
		{"removed logs", "eth_getLogs", `[{"blockHash":"` + testHash('a') + `"}]`, `[{"blockHash":"` + testHash('a') + `","removed":true}]`, `[]`},
		{"null logs", "eth_getLogs", `[{"blockHash":"` + testHash('a') + `"}]`, `null`, `[]`},
		{"null count", "eth_getBlockTransactionCountByHash", `["` + testHash('a') + `"]`, `null`, `"0x0"`},
		{"wrong transaction", "eth_getTransactionByBlockHashAndIndex", `["` + testHash('a') + `","0x0"]`, `{"blockHash":"` + testHash('b') + `"}`, `{"blockHash":"` + testHash('a') + `"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := newFakeEVM(t, 100, testHash('a'), testHash('9'))
			var active, invalid atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				body, err := io.ReadAll(request.Body)
				if err != nil {
					t.Error(err)
					return
				}
				var call jsonrpc.Request
				if err := json.Unmarshal(body, &call); err != nil {
					t.Error(err)
					return
				}
				// Availability and startup probes remain valid; only the routed result fails.
				if active.Load() && call.Method == test.method && (test.method != "eth_getBlockByHash" || strings.Contains(string(call.Params), "true")) {
					payload := test.valid
					if invalid.Load() {
						payload = test.invalid
					}
					writeFake(t, w, call.ID, json.RawMessage(payload), nil)
					return
				}
				request.Body = io.NopCloser(bytes.NewReader(body))
				provider.serveHTTP(w, request)
			}))
			t.Cleanup(server.Close)
			proxy, store := newTestGateway(t, time.Second, []config.UpstreamConfig{{ID: "provider", HTTPURL: server.URL, MaxConcurrency: 8}})
			store.Set(head.Head{Chain: "test", Family: "evm", Commitment: head.Latest, Number: 100, Hash: testHash('a'), ParentHash: testHash('9'), Origin: "provider", ObservedAt: time.Now()})
			active.Store(true)
			for _, bad := range []bool{true, false, false} {
				invalid.Store(bad)
				recorder := httptest.NewRecorder()
				request := httptest.NewRequest(http.MethodPost, "/rpc/test", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"`+test.method+`","params":`+test.parameters+`}`))
				proxy.ServeRPC(recorder, request, "test")
				response := decodeCacheResponse(t, recorder)
				if bad {
					if response.Error == nil || response.Error.Code != jsonrpc.CodeConsistencyUnavailable || len(response.Result) != 0 {
						t.Fatalf("invalid upstream result escaped: %s", recorder.Body.String())
					}
				} else if response.Error != nil || string(response.Result) != test.valid {
					t.Fatalf("valid response lost or cache poisoned: %s", recorder.Body.String())
				}
			}
		})
	}
}

func TestSolanaUnpinnedReadsDoNotClaimBatchSlotFloor(t *testing.T) {
	provider := newFakeSolana(t, 100)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var call jsonrpc.Request
		if err := json.Unmarshal(body, &call); err != nil {
			t.Error(err)
			return
		}
		switch call.Method {
		case "getBlock", "getBlockTime", "getBlockCommitment":
			writeFake(t, w, call.ID, nil, nil)
			return
		}
		request.Body = io.NopCloser(bytes.NewReader(body))
		provider.serveHTTP(w, request)
	}))
	t.Cleanup(server.Close)
	proxy, store := newSolanaTestGateway(t, []config.UpstreamConfig{{ID: "provider", HTTPURL: server.URL, MaxConcurrency: 8}})
	store.Set(head.Head{Chain: "solana", Family: "solana", Commitment: head.Finalized, Number: 100, ObservedAt: time.Now()})
	for _, method := range []string{"getBlock", "getBlockTime", "getBlockCommitment"} {
		for _, batch := range []bool{false, true} {
			body := `{"jsonrpc":"2.0","id":1,"method":"` + method + `","params":[99]}`
			consistency := "unverified"
			if batch {
				body = `[` + body + `,{"jsonrpc":"2.0","id":2,"method":"getAccountInfo","params":["account"]}]`
				consistency = "mixed-targets"
			}
			recorder := httptest.NewRecorder()
			proxy.ServeRPC(recorder, httptest.NewRequest(http.MethodPost, "/rpc/solana", strings.NewReader(body)), "solana")
			if strings.Contains(recorder.Body.String(), `"error"`) || recorder.Header().Get("X-RPC-Consistency") != consistency || recorder.Header().Get("X-RPC-Head-Slot") != "" {
				t.Fatalf("unpinned Solana read claimed slot coverage: %s, %v", recorder.Body.String(), recorder.Header())
			}
		}
	}
}
