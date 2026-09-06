package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
	"github.com/fiberevm/rpc-proxy/internal/utils"
)

func newTestReceipt(t *testing.T, block head.Head) json.RawMessage {
	t.Helper()
	receipt, err := json.Marshal(map[string]any{
		"transactionHash": testHash('c'), "transactionIndex": "0x0", "blockHash": block.Hash,
		"blockNumber": utils.FormatEVMQuantity(block.Number), "from": "0x0000000000000000000000000000000000000001",
		"to": "0x0000000000000000000000000000000000000002", "contractAddress": nil,
		"cumulativeGasUsed": "0x5208", "gasUsed": "0x5208", "effectiveGasPrice": "0x1", "status": "0x1",
		"type": "0x2", "logs": []any{}, "logsBloom": "0x" + strings.Repeat("0", 512),
	})
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func (f *reorgGatewayFixture) requestReceipt() *httptest.ResponseRecorder {
	f.t.Helper()
	return f.request(`{"jsonrpc":"2.0","id":"receipt","method":"eth_getTransactionReceipt","params":["` + testHash('c') + `"]}`)
}

func TestReceiptCacheSurvivesAdvancementAndInvalidatesOnReorg(t *testing.T) {
	fixture := newReorgGatewayFixture(t)
	original := newTestReceipt(t, fixture.current)
	fixture.provider.setReceipt(receiptReply{receipt: original})
	if response := decodeCacheResponse(t, fixture.requestReceipt()); response.Error != nil {
		t.Fatal(response.Error)
	}
	next := fixture.current
	next.Number++
	next.ParentHash, next.Hash = next.Hash, testHash('b')
	fixture.publish(next)
	fixture.provider.setReceipt(receiptReply{})
	recorder := fixture.requestReceipt()
	if response := decodeCacheResponse(t, recorder); response.Error != nil || string(response.Result) != string(original) || recorder.Header().Get("X-RPC-Cache") != "hit" || fixture.provider.receiptCallCount() != 1 {
		t.Fatalf("ordinary advancement lost cached receipt: %s headers=%v", recorder.Body.String(), recorder.Header())
	}
	if recorder.Header().Get("X-RPC-Consistency") != "pinned-head-ceiling" || recorder.Header().Get("X-RPC-Head-Hash") != next.Hash {
		t.Fatalf("receipt cache lost its head ceiling: %v", recorder.Header())
	}
	fixture.beginReorg()
	fixture.recover(testHash('d'))
	// Re-inclusion can have different execution contents, including a failed status.
	reincluded := newTestReceipt(t, fixture.current)
	reincluded = json.RawMessage(strings.Replace(string(reincluded), `"status":"0x1"`, `"status":"0x0"`, 1))
	fixture.provider.setReceipt(receiptReply{receipt: reincluded})
	recorder = fixture.requestReceipt()
	if response := decodeCacheResponse(t, recorder); response.Error != nil || string(response.Result) != string(reincluded) || recorder.Header().Get("X-RPC-Cache") != "miss" || fixture.provider.receiptCallCount() != 2 {
		t.Fatalf("old inclusion survived reorg: %s headers=%v", recorder.Body.String(), recorder.Header())
	}
	fixture.provider.setReceipt(receiptReply{})
	recorder = fixture.requestReceipt()
	if response := decodeCacheResponse(t, recorder); response.Error != nil || string(response.Result) != string(reincluded) || recorder.Header().Get("X-RPC-Cache") != "hit" {
		t.Fatalf("failed execution receipt was not cached: %s", recorder.Body.String())
	}
}

func TestReceiptCacheLeavesUnverifiedLookupsUncached(t *testing.T) {
	for _, test := range []string{"orphan", "future", "old", "unavailable ancestor", "pending reorg", "stale head", "missing head", "disabled"} {
		t.Run(test, func(t *testing.T) {
			fixture := newReorgGatewayFixture(t)
			block := fixture.current
			expectedStatus := "miss"
			wantError := false
			switch test {
			case "orphan":
				block.Hash = testHash('e')
			case "future":
				block.Number++
				block.Hash = testHash('e')
			case "old":
				block.Number -= 17 // The test runtime permits an ancestry distance of 16.
				block.Hash = testHash('e')
			case "unavailable ancestor":
				block.Number -= 3
				block.Hash = testHash('e')
			case "pending reorg":
				fixture.beginReorg()
				wantError = true
			case "stale head":
				stale := fixture.current
				stale.ObservedAt = time.Now().Add(-time.Minute)
				fixture.publish(stale)
				wantError = true
			case "missing head":
				fixture.proxy.store = head.NewMemoryStore()
				wantError = true
			case "disabled":
				fixture.proxy.cache = nil
				expectedStatus = "bypass"
			}
			receipt := newTestReceipt(t, block)
			fixture.provider.setReceipt(receiptReply{receipt: receipt})
			expected := receipt
			if test == "future" {
				expected = json.RawMessage("null")
			}
			for range 2 {
				recorder := fixture.requestReceipt()
				response := decodeCacheResponse(t, recorder)
				if wantError {
					if response.Error == nil || response.Error.Code != jsonrpc.CodeConsistencyUnavailable || len(response.Result) != 0 {
						t.Fatalf("receipt served without a fresh pinned head: %s", recorder.Body.String())
					}
					continue
				}
				if response.Error != nil || string(response.Result) != string(expected) || recorder.Header().Get("X-RPC-Cache") != expectedStatus {
					t.Fatalf("uncached lookup behavior changed: %s headers=%v", recorder.Body.String(), recorder.Header())
				}
			}
			expectedCalls := 2
			if wantError {
				expectedCalls = 0
			}
			if fixture.provider.receiptCallCount() != expectedCalls {
				t.Fatalf("unverified receipt was retained: %d calls", fixture.provider.receiptCallCount())
			}
		})
	}
}

func TestReceiptCacheMissesAndHitsRequireFinalFence(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, failure := range []string{"pending", "recovered", "same fork again", "stale", "redis unavailable"} {
			t.Run(failure+map[bool]string{false: "/miss", true: "/hit"}[cached], func(t *testing.T) {
				fixture := newReorgGatewayFixture(t)
				fixture.provider.setReceipt(receiptReply{receipt: newTestReceipt(t, fixture.current)})
				if cached {
					if response := decodeCacheResponse(t, fixture.requestReceipt()); response.Error != nil {
						t.Fatal(response.Error)
					}
				}
				faultStore := &unavailableSnapshotStore{Store: fixture.store}
				fixture.proxy.store = &cacheFenceStore{Store: faultStore, beforeSnapshot: func(calls int32) {
					if calls != 2 {
						return
					}
					switch failure {
					case "pending", "recovered", "same fork again":
						fixture.beginReorg()
						if failure != "pending" {
							fixture.recover(testHash('d'))
						}
						if failure == "same fork again" {
							fixture.beginReorg()
							fixture.recover(testHash('a'))
						}
					case "stale":
						stale := fixture.current
						stale.ObservedAt = time.Now().Add(-time.Minute)
						fixture.publish(stale)
					case "redis unavailable":
						faultStore.unavailable.Store(true)
					}
				}}
				recorder := fixture.requestReceipt()
				response := decodeCacheResponse(t, recorder)
				if response.Error == nil || response.Error.Code != jsonrpc.CodeConsistencyUnavailable || len(response.Result) != 0 || recorder.Header().Get("X-RPC-Upstream") != "" || recorder.Header().Get("X-RPC-Cache") != "" {
					t.Fatalf("verified receipt escaped final fence: %s headers=%v", recorder.Body.String(), recorder.Header())
				}
				if fixture.provider.receiptCallCount() != 1 {
					t.Fatalf("cache hit unexpectedly contacted upstream: %d calls", fixture.provider.receiptCallCount())
				}
				if failure == "same fork again" {
					// The rejected old-epoch load must not be reused, even after A -> B -> A.
					fixture.proxy.store = fixture.store
					fixture.provider.setReceipt(receiptReply{})
					recorder = fixture.requestReceipt()
					if response := decodeCacheResponse(t, recorder); response.Error != nil || string(response.Result) != "null" || fixture.provider.receiptCallCount() != 2 {
						t.Fatalf("invalidated receipt escaped through cache: %s", recorder.Body.String())
					}
				}
			})
		}
	}
}

func TestReceiptCacheSharesBatchLoadAndFencesWholeBatch(t *testing.T) {
	fixture := newReorgGatewayFixture(t)
	receipt := newTestReceipt(t, fixture.current)
	fixture.provider.setReceipt(receiptReply{receipt: receipt})
	batch := `[
		{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["` + testHash('c') + `"]},
		{"jsonrpc":"2.0","id":"second","method":"eth_getTransactionReceipt","params":["` + testHash('c') + `"]},
		{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["` + testHash('c') + `"]},
		{"jsonrpc":"2.0","id":"static","method":"eth_chainId"}
	]`
	recorder := fixture.request(batch)
	var responses []jsonrpc.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &responses); err != nil {
		t.Fatal(err)
	}
	if len(responses) != 3 || string(responses[0].ID) != "1" || string(responses[1].ID) != `"second"` || fixture.provider.receiptCallCount() != 1 {
		t.Fatalf("receipt batch sharing or IDs changed: %s, calls=%d", recorder.Body.String(), fixture.provider.receiptCallCount())
	}
	for _, response := range responses[:2] {
		if response.Error != nil || string(response.Result) != string(receipt) {
			t.Fatalf("receipt batch lost result: %+v", response)
		}
	}
	fixture.proxy.store = &cacheFenceStore{Store: fixture.store, beforeSnapshot: func(calls int32) {
		if calls == 2 {
			fixture.beginReorg()
		}
	}}
	recorder = fixture.request(batch)
	responses = nil
	if err := json.Unmarshal(recorder.Body.Bytes(), &responses); err != nil {
		t.Fatal(err)
	}
	for _, response := range responses[:2] {
		if response.Error == nil || response.Error.Code != jsonrpc.CodeConsistencyUnavailable || len(response.Result) != 0 {
			t.Fatalf("receipt batch escaped fence: %s", recorder.Body.String())
		}
	}
	if responses[2].Error != nil || string(responses[2].Result) != `"0x1"` || recorder.Header().Get("X-RPC-Head-Hash") != "" {
		t.Fatalf("static response or diagnostics changed: %s headers=%v", recorder.Body.String(), recorder.Header())
	}
}

func TestReceiptCacheRejectsLateOldForkLoad(t *testing.T) {
	fixture := newReorgGatewayFixture(t)
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	fixture.provider.setReceipt(receiptReply{receipt: newTestReceipt(t, fixture.current), beforeResponse: func() { close(started); <-release }})
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- fixture.requestReceipt() }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("receipt load never started")
	}
	fixture.beginReorg()
	fixture.recover(testHash('d'))
	unblock()
	select {
	case recorder := <-done:
		if response := decodeCacheResponse(t, recorder); response.Error == nil || response.Error.Code != jsonrpc.CodeConsistencyUnavailable {
			t.Fatalf("late receipt escaped fence: %s", recorder.Body.String())
		}
	case <-time.After(4 * time.Second):
		t.Fatal("receipt load did not finish")
	}
	fixture.provider.setReceipt(receiptReply{})
	recorder := fixture.requestReceipt()
	if response := decodeCacheResponse(t, recorder); response.Error != nil || string(response.Result) != "null" || fixture.provider.receiptCallCount() != 2 {
		t.Fatalf("late old-fork receipt poisoned cache: %s", recorder.Body.String())
	}
}

func TestReceiptCacheCannotUseInclusionBeyondCallerSnapshot(t *testing.T) {
	fixture := newReorgGatewayFixture(t)
	older := fixture.current
	newer := older
	newer.Number++
	newer.ParentHash, newer.Hash = older.Hash, testHash('d')
	fixture.publish(newer)
	fixture.provider.setReceipt(receiptReply{receipt: newTestReceipt(t, newer)})
	if response := decodeCacheResponse(t, fixture.requestReceipt()); response.Error != nil {
		t.Fatal(response.Error)
	}
	fixture.provider.setReceipt(receiptReply{})
	// Simulate a request that selected its snapshot before another request warmed the cache.
	request := jsonrpc.Request{JSONRPC: "2.0", ID: json.RawMessage("1"), Method: "eth_getTransactionReceipt", Params: json.RawMessage(`["` + testHash('c') + `"]`)}
	snapshot := head.Snapshot{Chain: "test", Heads: map[string]head.Head{head.Latest: older}}
	response := fixture.proxy.process(t.Context(), fixture.proxy.runtimes["test"], snapshot, request)
	if response.response.Error != nil || string(response.response.Result) != "null" || response.cacheStatus != "hit" || response.target == nil || response.target.Number != older.Number || fixture.provider.receiptCallCount() != 1 {
		t.Fatalf("newer cache entry leaked into old snapshot: %+v", response)
	}
}

// Keep the single-receipt HTTP contract explicit alongside the batch coverage.
func TestReceiptCachePreservesExplicitNullID(t *testing.T) {
	fixture := newReorgGatewayFixture(t)
	fixture.provider.setReceipt(receiptReply{receipt: newTestReceipt(t, fixture.current)})
	fixture.requestReceipt()
	recorder := fixture.request(`{"jsonrpc":"2.0","id":null,"method":"eth_getTransactionReceipt","params":["` + testHash('c') + `"]}`)
	if response := decodeCacheResponse(t, recorder); recorder.Code != http.StatusOK || response.Error != nil || string(response.ID) != "null" || recorder.Header().Get("X-RPC-Cache") != "hit" {
		t.Fatalf("cached receipt lost explicit null ID: %s", recorder.Body.String())
	}
}
