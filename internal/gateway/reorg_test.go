package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
)

type reorgGatewayFixture struct {
	t        *testing.T
	proxy    *Gateway
	store    *head.MemoryStore
	provider *fakeEVM
	token    string
	current  head.Head
}

func newReorgGatewayFixture(t *testing.T) *reorgGatewayFixture {
	t.Helper()
	provider := newFakeEVM(t, 100, testHash('a'), testHash('9'))
	proxy, store := newTestGateway(t, 3*time.Second, []config.UpstreamConfig{{ID: "provider", HTTPURL: provider.server.URL, MaxConcurrency: 8}})
	token, acquired, err := store.Acquire(context.Background(), "test", "leader", 30*time.Second)
	if err != nil || !acquired {
		t.Fatalf("acquire leader: acquired=%v err=%v", acquired, err)
	}
	fixture := &reorgGatewayFixture{t: t, proxy: proxy, store: store, provider: provider, token: token}
	fixture.publish(head.Head{Chain: "test", Family: "evm", Commitment: head.Latest, Number: 100, Hash: testHash('a'), ParentHash: testHash('9'), Origin: "provider", ObservedAt: time.Now().UTC()})
	return fixture
}

func (f *reorgGatewayFixture) publish(accepted head.Head) {
	f.t.Helper()
	published, err := f.store.Publish(context.Background(), f.token, accepted)
	if err != nil {
		f.t.Fatal(err)
	}
	f.current = published
}

func (f *reorgGatewayFixture) beginReorg() {
	f.t.Helper()
	guarded, err := f.store.BeginReorg(context.Background(), "test", f.token)
	if err != nil {
		f.t.Fatal(err)
	}
	f.current = guarded
}

func (f *reorgGatewayFixture) recover(hash string) {
	f.t.Helper()
	replacement := f.current
	replacement.Hash = hash
	replacement.ReorgPending = false
	replacement.ObservedAt = time.Now().UTC()
	f.publish(replacement)
}

func (f *reorgGatewayFixture) request(body string) *httptest.ResponseRecorder {
	f.t.Helper()
	recorder := httptest.NewRecorder()
	f.proxy.ServeRPC(recorder, httptest.NewRequest(http.MethodPost, "/rpc/test", bytes.NewBufferString(body)), "test")
	return recorder
}

func (f *reorgGatewayFixture) blockStateReply() (<-chan struct{}, func()) {
	f.t.Helper()
	started, release := make(chan struct{}), make(chan struct{})
	var startOnce, releaseOnce sync.Once
	f.provider.mu.Lock()
	f.provider.stateReplyHook = func() {
		startOnce.Do(func() { close(started) })
		<-release
	}
	f.provider.mu.Unlock()
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	f.t.Cleanup(unblock)
	return started, unblock
}

type unavailableSnapshotStore struct {
	head.Store
	unavailable atomic.Bool
}

// Snapshot simulates Redis becoming unavailable between request admission and response fencing.
func (s *unavailableSnapshotStore) Snapshot(ctx context.Context, chain string) (head.Snapshot, error) {
	if s.unavailable.Load() {
		return head.Snapshot{}, errors.New("test head store unavailable")
	}
	return s.Store.Snapshot(ctx, chain)
}

func TestGatewayFencesInflightReadAcrossReorg(t *testing.T) {
	for _, test := range []struct {
		name         string
		failureClass string
	}{
		{name: "unchanged"},
		{name: "ordinary advancement"},
		{name: "leadership change"},
		{name: "pending recovery", failureClass: "reorg_pending"},
		{name: "completed recovery", failureClass: "reorg_changed"},
		{name: "orphan execution error", failureClass: "reorg_changed"},
		{name: "return to original fork", failureClass: "reorg_changed"},
		{name: "redis unavailable", failureClass: "reorg_check_unavailable"},
		{name: "stale head", failureClass: "stale_head"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newReorgGatewayFixture(t)
			faultStore := &unavailableSnapshotStore{Store: fixture.store}
			fixture.proxy.store = faultStore
			if test.name == "orphan execution error" {
				fixture.provider.mu.Lock()
				fixture.provider.stateError = &jsonrpc.Error{Code: -32000, Message: "execution reverted", Data: "orphaned-state-error"}
				fixture.provider.mu.Unlock()
			}
			started, release := fixture.blockStateReply()
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				done <- fixture.request(`{"jsonrpc":"2.0","id":"read-1","method":"eth_getBalance","params":["address","latest"]}`)
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("state read never reached old-fork provider")
			}

			switch test.name {
			case "pending recovery":
				fixture.beginReorg()
			case "completed recovery", "return to original fork", "orphan execution error":
				fixture.beginReorg()
				fixture.recover(testHash('b'))
				if test.name == "return to original fork" {
					fixture.beginReorg()
					fixture.recover(testHash('a'))
				}
			case "ordinary advancement":
				next := fixture.current
				next.Number++
				next.ParentHash = next.Hash
				next.Hash = testHash('b')
				fixture.publish(next)
			case "leadership change":
				if err := fixture.store.Release(context.Background(), "test", fixture.token); err != nil {
					t.Fatal(err)
				}
				if _, acquired, err := fixture.store.Acquire(context.Background(), "test", "new-leader", time.Second); err != nil || !acquired {
					t.Fatalf("takeover: acquired=%v err=%v", acquired, err)
				}
			case "redis unavailable":
				faultStore.unavailable.Store(true)
			case "stale head":
				stale := fixture.current
				stale.ObservedAt = time.Now().Add(-time.Minute)
				fixture.publish(stale)
			}
			// This provider still believes A is canonical and successfully serves its state.
			// The final shared fence, not an upstream error, must reject the orphaned response.
			release()
			var recorder *httptest.ResponseRecorder
			select {
			case recorder = <-done:
			case <-time.After(4 * time.Second):
				t.Fatal("request did not finish")
			}
			var response jsonrpc.Response
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if recorder.Code != http.StatusOK || string(response.ID) != `"read-1"` {
				t.Fatalf("JSON-RPC envelope changed: status=%d body=%s", recorder.Code, recorder.Body.String())
			}
			if test.failureClass == "" {
				if response.Error != nil || string(response.Result) != `"0x64"` || recorder.Header().Get("X-RPC-Head-Hash") != testHash('a') {
					t.Fatalf("valid snapshot read rejected: %s headers=%v", recorder.Body.String(), recorder.Header())
				}
				return
			}
			if response.Error == nil || response.Error.Code != jsonrpc.CodeConsistencyUnavailable || len(response.Result) != 0 || !strings.Contains(recorder.Body.String(), test.failureClass) || !strings.Contains(recorder.Body.String(), `"retryable":true`) {
				t.Fatalf("invalidated read escaped fence: %s", recorder.Body.String())
			}
			if recorder.Header().Get("X-RPC-Head-Hash") != "" || recorder.Header().Get("X-RPC-Head-Number") != "" || recorder.Header().Get("X-RPC-Upstream") != "" {
				t.Fatalf("invalidated diagnostics escaped fence: %v", recorder.Header())
			}
		})
	}
}

func TestGatewayReorgAdmissionAndRecovery(t *testing.T) {
	fixture := newReorgGatewayFixture(t)
	fixture.beginReorg()
	beforeCalls := fixture.provider.calls()
	recorder := fixture.request(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["address","latest"]}`)
	var response jsonrpc.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Code != jsonrpc.CodeConsistencyUnavailable || fixture.provider.calls() != beforeCalls {
		t.Fatalf("pending reorg admitted a state read: %s", recorder.Body.String())
	}
	ready := httptest.NewRecorder()
	fixture.proxy.AdminHandler().ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if ready.Code != http.StatusServiceUnavailable {
		t.Fatalf("pending reorg passed readiness: %d", ready.Code)
	}
	fixture.recover(testHash('b'))
	fixture.provider.set(100, testHash('b'), testHash('9'))
	recorder = fixture.request(`{"jsonrpc":"2.0","id":2,"method":"eth_getBalance","params":["address","latest"]}`)
	response = jsonrpc.Response{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error != nil || string(response.Result) != `"0x64"` || recorder.Header().Get("X-RPC-Head-Hash") != testHash('b') {
		t.Fatalf("recovered read did not use new fork: %s headers=%v", recorder.Body.String(), recorder.Header())
	}
	ready = httptest.NewRecorder()
	fixture.proxy.AdminHandler().ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("recovered head did not restore readiness: %d", ready.Code)
	}
}

func TestGatewayReorgFencesEntireBatch(t *testing.T) {
	fixture := newReorgGatewayFixture(t)
	started, release := fixture.blockStateReply()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- fixture.request(`[
			{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["address","latest"]},
			{"jsonrpc":"2.0","id":"height","method":"eth_blockNumber","params":[]},
			{"jsonrpc":"2.0","id":3,"method":"eth_chainId","params":[]},
			{"jsonrpc":"2.0","id":4,"method":"eth_getTransactionReceipt","params":["` + testHash('c') + `"]},
			{"jsonrpc":"2.0","method":"eth_getBalance","params":["address","latest"]},
			null
		]`)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("batch never reached upstream")
	}
	fixture.beginReorg()
	fixture.recover(testHash('b'))
	release()
	var recorder *httptest.ResponseRecorder
	select {
	case recorder = <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("batch did not finish")
	}
	var responses []jsonrpc.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &responses); err != nil {
		t.Fatal(err)
	}
	if recorder.Code != http.StatusOK || len(responses) != 5 {
		t.Fatalf("batch envelope or notifications changed: %s", recorder.Body.String())
	}
	for index, id := range []string{"1", `"height"`} {
		if string(responses[index].ID) != id || responses[index].Error == nil || responses[index].Error.Code != jsonrpc.CodeConsistencyUnavailable || len(responses[index].Result) != 0 {
			t.Fatalf("batch item escaped fence: %+v", responses[index])
		}
	}
	if responses[2].Error != nil || string(responses[2].Result) != `"0x1"` || responses[3].Error != nil || string(responses[3].Result) != "null" || responses[4].Error == nil || responses[4].Error.Code != jsonrpc.CodeInvalidRequest {
		t.Fatalf("non-pinned batch semantics changed: %s", recorder.Body.String())
	}
	if recorder.Header().Get("X-RPC-Head-Hash") != "" || recorder.Header().Get("X-RPC-Head-Number") != "" {
		t.Fatalf("batch leaked orphan head diagnostics: %v", recorder.Header())
	}
}
