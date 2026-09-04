package coordinator

import (
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

	"github.com/coder/websocket"
	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/telemetry"
	"github.com/fiberevm/rpc-proxy/internal/utils"
)

type subscriptionProvider struct {
	mu         sync.Mutex
	server     *httptest.Server
	latest     head.Head
	blocks     map[string]head.Head
	canonical  map[uint64]head.Head
	counts     map[string]int
	frames     chan []byte
	disconnect chan struct{}
	reject     bool
}

func newSubscriptionProvider(t *testing.T, reject bool) *subscriptionProvider {
	t.Helper()
	genesis := coordinatorBlock(0, 'a', '0')
	provider := &subscriptionProvider{
		latest: genesis, blocks: map[string]head.Head{genesis.Hash: genesis}, canonical: map[uint64]head.Head{0: genesis},
		counts: make(map[string]int), frames: make(chan []byte, 16), disconnect: make(chan struct{}), reject: reject,
	}
	provider.server = httptest.NewServer(http.HandlerFunc(provider.serveHTTP))
	t.Cleanup(provider.server.Close)
	return provider
}

func (p *subscriptionProvider) serveHTTP(w http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/ws" {
		p.serveWebsocket(w, request)
		return
	}
	var call struct {
		ID     json.RawMessage   `json:"id"`
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	if json.NewDecoder(request.Body).Decode(&call) != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	p.mu.Lock()
	var response any = "0x0"
	switch call.Method {
	case "eth_chainId":
		response = "0x1"
	case "eth_getBlockByNumber", "eth_getBlockByHash":
		response = nil
		var selector string
		if len(call.Params) > 0 && json.Unmarshal(call.Params[0], &selector) == nil {
			p.counts[selector]++
			if call.Method == "eth_getBlockByHash" {
				if block, exists := p.blocks[selector]; exists {
					response = blockResult(block)
				}
			} else if selector == head.Latest {
				response = blockResult(p.latest)
			} else if selector == head.Safe || selector == head.Finalized {
				response = blockResult(p.canonical[0])
			} else if number, err := utils.ParseEVMQuantity(selector); err == nil {
				if block, exists := p.canonical[number]; exists {
					response = blockResult(block)
				}
			}
		}
	}
	p.mu.Unlock()
	if err := json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": call.ID, "result": response}); err != nil {
		return
	}
}

func (p *subscriptionProvider) serveWebsocket(w http.ResponseWriter, request *http.Request) {
	conn, err := websocket.Accept(w, request, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	_, payload, err := conn.Read(request.Context())
	if err != nil {
		return
	}
	var call struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params []string        `json:"params"`
	}
	if json.Unmarshal(payload, &call) != nil || call.Method != "eth_subscribe" || len(call.Params) != 1 || call.Params[0] != "newHeads" {
		return
	}
	acknowledgment := map[string]any{"jsonrpc": "2.0", "id": call.ID, "result": "test-subscription"}
	if p.reject {
		acknowledgment = map[string]any{"jsonrpc": "2.0", "id": call.ID, "error": map[string]any{"code": -32601, "message": "subscriptions disabled"}}
	}
	encodedAck, err := json.Marshal(acknowledgment)
	if err != nil || conn.Write(request.Context(), websocket.MessageText, encodedAck) != nil {
		return
	}
	ctx := conn.CloseRead(request.Context())
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.disconnect:
			return
		case frame := <-p.frames:
			if conn.Write(ctx, websocket.MessageText, frame) != nil {
				return
			}
		}
	}
}

func (p *subscriptionProvider) count(selector string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.counts[selector]
}

func (p *subscriptionProvider) setHead(block head.Head) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.latest = block
	p.blocks[block.Hash] = block
	p.canonical[block.Number] = block
}

func (p *subscriptionProvider) notify(t *testing.T, subscriptionID string, block head.Head) {
	t.Helper()
	frame, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "eth_subscription", "params": map[string]any{"subscription": subscriptionID, "result": blockResult(block)}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case p.frames <- frame:
	case <-time.After(time.Second):
		t.Fatal("upstream notification queue stalled")
	}
}

func startSubscriptionCoordinator(t *testing.T, providers []*subscriptionProvider, idleTimeout time.Duration) *Coordinator {
	t.Helper()
	tel, err := telemetry.NewTelemetry(config.DatadogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	chainConfig := config.ChainConfig{
		Name: "test", Family: "evm", ChainID: "0x1", ReorgDepth: 8,
		PollInterval:       config.Duration(10 * time.Millisecond),
		HeadRequestTimeout: config.Duration(time.Second), WebsocketIdleTimeout: config.Duration(idleTimeout), MaxHeadAge: config.Duration(5 * time.Second),
	}
	for index, provider := range providers {
		upstream := config.UpstreamConfig{ID: string(rune('a' + index)), HTTPURL: provider.server.URL, MaxConcurrency: 16}
		if index == 0 {
			upstream.WebsocketURL = strings.Replace(provider.server.URL, "http://", "ws://", 1) + "/ws"
		}
		chainConfig.Upstreams = append(chainConfig.Upstreams, upstream)
	}
	runtime := chain.NewRuntime(chainConfig, tel)
	if err := runtime.Validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	coordinator := NewCoordinator(Options{
		Runtime: runtime, Store: head.NewMemoryStore(), Owner: "test", LeaderTTL: time.Second,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Telemetry: tel,
	})
	token, acquired, err := coordinator.store.Acquire(context.Background(), "test", "test", time.Second)
	if err != nil || !acquired {
		t.Fatalf("acquire coordinator leadership: acquired=%v err=%v", acquired, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); coordinator.runLeader(ctx, token) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("coordinator did not stop its workers")
		}
	})
	return coordinator
}

func waitForSubscriptionCondition(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", description)
		case <-ticker.C:
		}
	}
}

func (c *Coordinator) waitForTestHead(t *testing.T, expected head.Head) {
	t.Helper()
	waitForSubscriptionCondition(t, "accepted head "+expected.Hash, func() bool {
		snapshot, err := c.store.Snapshot(context.Background(), "test")
		accepted := snapshot.Heads[head.Latest]
		return err == nil && accepted.Hash == expected.Hash && accepted.Number == expected.Number
	})
}

func (c *Coordinator) waitForTestPolls(t *testing.T) {
	t.Helper()
	waitForSubscriptionCondition(t, "completed startup polls", func() bool {
		_, running := c.polling.Load(head.Latest)
		return !running
	})
}

func TestHealthySubscriptionSuppressesLatestPollingAndTracksReorgs(t *testing.T) {
	provider := newSubscriptionProvider(t, false)
	coordinator := startSubscriptionCoordinator(t, []*subscriptionProvider{provider}, 2*time.Second)
	waitForSubscriptionCondition(t, "active subscription", func() bool { return coordinator.hasFreshSubscription("a") })
	// Let any startup poll that raced the subscription finish before counting steady-state traffic.
	coordinator.waitForTestPolls(t)
	initialLatest := provider.count(head.Latest)
	initialHashChecks := provider.count(coordinatorHash('a'))
	provider.notify(t, "test-subscription", coordinatorBlock(0, 'a', '0'))
	time.Sleep(50 * time.Millisecond)
	if provider.count(head.Safe) != 0 || provider.count(head.Finalized) != 0 || provider.count(coordinatorHash('a')) != initialHashChecks {
		t.Fatal("fallback tick or duplicate header triggered unnecessary commitment/hash queries")
	}
	newHead := coordinatorBlock(1, 'b', 'a')
	provider.setHead(newHead)
	provider.notify(t, "test-subscription", newHead)
	coordinator.waitForTestHead(t, newHead)
	if provider.count(newHead.Hash) == 0 {
		t.Fatal("accepted notification without verifying its hash over HTTP")
	}
	replacement := coordinatorBlock(1, 'c', 'a')
	provider.setHead(replacement)
	provider.notify(t, "test-subscription", replacement)
	coordinator.waitForTestHead(t, replacement)
	// Observe beyond the former commitment timer as well as multiple fallback ticks.
	time.Sleep(250 * time.Millisecond)
	if provider.count(head.Safe) != 0 || provider.count(head.Finalized) != 0 {
		t.Fatal("latest-only tracking queried safe or finalized")
	}
	snapshot, err := coordinator.store.Snapshot(context.Background(), "test")
	if err != nil || len(snapshot.Heads) != 1 || snapshot.Heads[head.Latest].Hash != replacement.Hash {
		t.Fatalf("expected only the latest accepted head: snapshot=%+v err=%v", snapshot, err)
	}
	if latest := provider.count(head.Latest); latest != initialLatest {
		t.Fatalf("healthy subscription caused latest HTTP polling: before=%d after=%d", initialLatest, latest)
	}
}

func TestSubscriptionFailureRestoresLatestPolling(t *testing.T) {
	for _, failure := range []string{"disconnect", "silent", "duplicates", "wrong-subscription", "unverifiable-head", "rejected"} {
		t.Run(failure, func(t *testing.T) {
			provider := newSubscriptionProvider(t, failure == "rejected")
			coordinator := startSubscriptionCoordinator(t, []*subscriptionProvider{provider}, 120*time.Millisecond)
			if failure != "rejected" {
				waitForSubscriptionCondition(t, "active subscription", func() bool { return coordinator.hasFreshSubscription("a") })
			}
			coordinator.waitForTestPolls(t)
			initialLatest := provider.count(head.Latest)
			newHead := coordinatorBlock(1, 'b', 'a')
			provider.setHead(newHead)
			switch failure {
			case "disconnect":
				provider.disconnect <- struct{}{}
			case "duplicates", "wrong-subscription":
				for attempt := 0; attempt < 8; attempt++ {
					if failure == "duplicates" {
						provider.notify(t, "test-subscription", coordinatorBlock(0, 'a', '0'))
					} else {
						provider.notify(t, "unrelated-subscription", newHead)
					}
					time.Sleep(20 * time.Millisecond)
				}
			case "unverifiable-head":
				provider.notify(t, "test-subscription", coordinatorBlock(1, 'f', 'a'))
			}
			coordinator.waitForTestHead(t, newHead)
			if provider.count(head.Latest) <= initialLatest {
				t.Fatal("failed subscription did not restore HTTP latest polling")
			}
			if coordinator.hasFreshSubscription("a") {
				t.Fatal("failed subscription is still suppressing fallback")
			}
			if provider.count(head.Safe) != 0 || provider.count(head.Finalized) != 0 {
				t.Fatal("subscription failure triggered safe/finalized polling")
			}
		})
	}
}

func TestHTTPOnlyPeerStillPolledWithHealthySubscription(t *testing.T) {
	streaming := newSubscriptionProvider(t, false)
	httpOnly := newSubscriptionProvider(t, false)
	coordinator := startSubscriptionCoordinator(t, []*subscriptionProvider{streaming, httpOnly}, 2*time.Second)
	waitForSubscriptionCondition(t, "active subscription", func() bool { return coordinator.hasFreshSubscription("a") })
	coordinator.waitForTestPolls(t)
	initialStreaming := streaming.count(head.Latest)
	initialHTTP := httpOnly.count(head.Latest)
	newHead := coordinatorBlock(1, 'b', 'a')
	httpOnly.setHead(newHead)
	coordinator.waitForTestHead(t, newHead)
	if streaming.count(head.Latest) != initialStreaming || httpOnly.count(head.Latest) <= initialHTTP {
		t.Fatal("latest fallback was not scoped to the HTTP-only upstream")
	}
	if httpOnly.count(head.Safe) != 0 || httpOnly.count(head.Finalized) != 0 {
		t.Fatal("HTTP-only provider was asked for safe/finalized")
	}
}

func TestSubscriptionReconnectBootstrapsMissedHeads(t *testing.T) {
	provider := newSubscriptionProvider(t, false)
	coordinator := startSubscriptionCoordinator(t, []*subscriptionProvider{provider}, 3*time.Second)
	waitForSubscriptionCondition(t, "active subscription", func() bool { return coordinator.hasFreshSubscription("a") })
	provider.disconnect <- struct{}{}
	waitForSubscriptionCondition(t, "disconnected subscription", func() bool { return !coordinator.hasFreshSubscription("a") })
	provider.setHead(coordinatorBlock(1, 'b', 'a'))
	provider.setHead(coordinatorBlock(2, 'c', 'b'))
	missedHead := coordinatorBlock(3, 'd', 'c')
	provider.setHead(missedHead)
	coordinator.waitForTestHead(t, missedHead)
	waitForSubscriptionCondition(t, "reconnected subscription", func() bool { return coordinator.hasFreshSubscription("a") })
	coordinator.waitForTestPolls(t)
	initialLatest := provider.count(head.Latest)
	nextHead := coordinatorBlock(4, 'e', 'd')
	provider.setHead(nextHead)
	provider.notify(t, "test-subscription", nextHead)
	coordinator.waitForTestHead(t, nextHead)
	time.Sleep(50 * time.Millisecond)
	if provider.count(head.Latest) != initialLatest {
		t.Fatal("reconnected stream did not suppress latest HTTP polling")
	}
}
