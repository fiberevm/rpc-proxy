package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
)

func TestReceiptVisibilityUsesPinnedHeightWithAndWithoutCache(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(map[bool]string{false: "uncached", true: "cached"}[cached], func(t *testing.T) {
			fixture := newReorgGatewayFixture(t)
			if !cached {
				fixture.proxy.cache = nil
			}
			next := fixture.current
			next.Number++
			next.ParentHash, next.Hash = fixture.current.Hash, testHash('d')
			receipt := newTestReceipt(t, next)
			fixture.provider.setReceipt(receiptReply{receipt: receipt})
			for range 2 {
				recorder := fixture.requestReceipt()
				response := decodeCacheResponse(t, recorder)
				if response.Error != nil || string(response.Result) != "null" || recorder.Header().Get("X-RPC-Head-Number") != "0x64" {
					t.Fatalf("receipt exceeded pinned height: %s, %v", recorder.Body.String(), recorder.Header())
				}
			}
			fixture.publish(next)
			recorder := fixture.requestReceipt()
			response := decodeCacheResponse(t, recorder)
			if response.Error != nil || string(response.Result) != string(receipt) || fixture.provider.receiptCallCount() != 3 || recorder.Header().Get("X-RPC-Head-Number") != "0x65" {
				t.Fatalf("receipt did not become visible at its inclusion height: %s, %v", recorder.Body.String(), recorder.Header())
			}
		})
	}
}

func TestConcurrentReceiptLookupsKeepIndependentPinnedHeights(t *testing.T) {
	fixture := newReorgGatewayFixture(t)
	next := fixture.current
	next.Number++
	next.ParentHash, next.Hash = fixture.current.Hash, testHash('d')
	receipt := newTestReceipt(t, next)
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	fixture.provider.setReceipt(receiptReply{receipt: receipt, beforeResponse: func() { close(started); <-release }})
	olderDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { olderDone <- fixture.requestReceipt() }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("older request never reached the upstream")
	}
	fixture.publish(next)
	fixture.provider.setReceipt(receiptReply{receipt: receipt})
	newerDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { newerDone <- fixture.requestReceipt() }()
	select {
	case recorder := <-newerDone:
		response := decodeCacheResponse(t, recorder)
		if response.Error != nil || string(response.Result) != string(receipt) {
			t.Fatalf("newer request used the older request's cutoff: %s", recorder.Body.String())
		}
	case <-time.After(time.Second):
		unblock()
		t.Fatal("requests at different pins shared the same in-flight receipt load")
	}
	unblock()
	select {
	case recorder := <-olderDone:
		response := decodeCacheResponse(t, recorder)
		if response.Error != nil || string(response.Result) != "null" || recorder.Header().Get("X-RPC-Head-Number") != "0x64" {
			t.Fatalf("head advancement changed the older request's cutoff: %s, %v", recorder.Body.String(), recorder.Header())
		}
	case <-time.After(time.Second):
		t.Fatal("older receipt request did not finish")
	}
}

func TestLogRangesUsePinnedLatestDespiteNewerUpstream(t *testing.T) {
	for _, test := range []struct {
		name, filter, from, payload string
		advance, wantError, batch   bool
	}{
		{name: "long history", filter: `{"fromBlock":"0x1","toBlock":"latest","topics":[]}`, from: `"0x1"`, payload: `[]`},
		{name: "earliest through latest", filter: `{"fromBlock":"earliest","toBlock":"latest"}`, from: `"0x0"`, payload: `[]`},
		{name: "omitted latest", filter: `{"fromBlock":"0x1"}`, from: `"0x1"`, payload: `[]`},
		{name: "logs at pinned height", filter: `{"fromBlock":"0x1","toBlock":"latest"}`, from: `"0x1"`, payload: `[{"blockNumber":"0x64"}]`},
		{name: "head advances during read", filter: `{"fromBlock":"0x1","toBlock":"latest"}`, from: `"0x1"`, payload: `[{"blockNumber":"0x64"}]`, advance: true},
		{name: "batch shares head across logs and receipts", filter: `{"fromBlock":"0x1","toBlock":"latest"}`, from: `"0x1"`, payload: `[]`, advance: true, batch: true},
		{name: "future logs rejected", filter: `{"fromBlock":"0x1","toBlock":"latest"}`, from: `"0x1"`, payload: `[{"blockNumber":"0x65"}]`, wantError: true},
		{name: "logs below requested start", filter: `{"fromBlock":"0x1","toBlock":"latest"}`, from: `"0x1"`, payload: `[{"blockNumber":"0x0"}]`, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := newFakeEVM(t, 200, testHash('e'), testHash('d'))
			store := head.NewMemoryStore()
			pinned := head.Head{Chain: "test", Family: "evm", Commitment: head.Latest, Number: 100, Hash: testHash('a'), ParentHash: testHash('9'), ObservedAt: time.Now()}
			store.Set(pinned)
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
				if call.Method == "eth_getLogs" {
					var filters []map[string]json.RawMessage
					if err := json.Unmarshal(call.Params, &filters); err != nil || len(filters) != 1 {
						t.Errorf("invalid log parameters: %s, %v", call.Params, err)
						return
					}
					if string(filters[0]["fromBlock"]) != test.from || string(filters[0]["toBlock"]) != `"0x64"` || filters[0]["blockHash"] != nil {
						t.Errorf("log range did not preserve pinned latest: %s", call.Params)
					}
					if test.advance {
						newer := pinned
						newer.Number++
						newer.ParentHash, newer.Hash = pinned.Hash, testHash('b')
						store.Set(newer)
					}
					writeFake(t, w, call.ID, json.RawMessage(test.payload), nil)
					return
				}
				if call.Method == "eth_getBlockByHash" && strings.Contains(string(call.Params), pinned.Hash) || call.Method == "eth_getBlockByNumber" && strings.Contains(string(call.Params), `"0x64"`) {
					writeFake(t, w, call.ID, evmBlock(pinned.Number, pinned.Hash, pinned.ParentHash), nil)
					return
				}
				request.Body = io.NopCloser(bytes.NewReader(body))
				provider.serveHTTP(w, request)
			}))
			t.Cleanup(server.Close)
			proxy, _ := newTestGateway(t, time.Second, []config.UpstreamConfig{{ID: "provider", HTTPURL: server.URL, MaxConcurrency: 8}})
			proxy.store = store
			recorder := httptest.NewRecorder()
			body := `{"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[` + test.filter + `]}`
			if test.batch {
				provider.setReceipt(receiptReply{receipt: newTestReceipt(t, head.Head{Number: 101, Hash: testHash('b')})})
				body = `[` + body + `,{"jsonrpc":"2.0","id":2,"method":"eth_getTransactionReceipt","params":["` + testHash('c') + `"]},{"jsonrpc":"2.0","id":3,"method":"eth_blockNumber"}]`
			}
			proxy.ServeRPC(recorder, httptest.NewRequest(http.MethodPost, "/rpc/test", strings.NewReader(body)), "test")
			if test.batch {
				var responses []jsonrpc.Response
				if err := json.Unmarshal(recorder.Body.Bytes(), &responses); err != nil {
					t.Fatal(err)
				}
				if len(responses) != 3 || responses[0].Error != nil || responses[1].Error != nil || responses[2].Error != nil || string(responses[0].Result) != test.payload || string(responses[1].Result) != "null" || string(responses[2].Result) != `"0x64"` {
					t.Fatalf("batch did not retain one pinned latest: %s", recorder.Body.String())
				}
				return
			}
			response := decodeCacheResponse(t, recorder)
			if test.wantError {
				if response.Error == nil || response.Error.Code != jsonrpc.CodeConsistencyUnavailable || len(response.Result) != 0 {
					t.Fatalf("out-of-range logs escaped: %s", recorder.Body.String())
				}
			} else if response.Error != nil || string(response.Result) != test.payload || recorder.Header().Get("X-RPC-Head-Number") != "0x64" {
				t.Fatalf("pinned log range failed: %s, %v", recorder.Body.String(), recorder.Header())
			}
		})
	}
}
