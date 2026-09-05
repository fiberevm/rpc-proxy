package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
	"github.com/fiberevm/rpc-proxy/internal/telemetry"
	"github.com/fiberevm/rpc-proxy/internal/utils"
)

func TestWebsocketNewHeadsSuppressesDuplicateHashButEmitsReorgReplacement(t *testing.T) {
	tel, err := telemetry.NewTelemetry(config.DatadogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Server: config.ServerConfig{WebsocketQueueSize: 16}}
	runtime := chain.NewRuntime(config.ChainConfig{Name: "test", Family: "evm", ReorgDepth: 8}, tel)
	store := head.NewMemoryStore()
	gateway := NewGateway(Options{Config: cfg, Store: store, Runtimes: map[string]*chain.Runtime{"test": runtime}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Telemetry: tel})
	server := httptest.NewServer(gateway.WebsocketHandler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/ws/test", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	request := map[string]any{"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": []any{"newHeads"}}
	if err := writeTestWS(ctx, conn, request); err != nil {
		t.Fatal(err)
	}
	_, payload, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var subscribed jsonrpc.Response
	if err := json.Unmarshal(payload, &subscribed); err != nil || subscribed.Error != nil {
		t.Fatalf("subscribe response: %s err=%v", payload, err)
	}
	parent := testHash('0')
	first := wsHead(t, 10, testHash('a'), parent)
	store.Set(first)
	if got := readHeadHash(t, ctx, conn); got != first.Hash {
		t.Fatalf("first hash %q", got)
	}
	store.Set(first)
	replacement := wsHead(t, 10, testHash('b'), parent)
	store.Set(replacement)
	if got := readHeadHash(t, ctx, conn); got != replacement.Hash {
		t.Fatalf("duplicate was not suppressed or replacement lost: %q", got)
	}
}

type publishingCheckpointStore struct {
	head.Store
	next head.Head
}

// StreamStart publishes immediately after capturing the baseline to expose acknowledgment races.
func (s publishingCheckpointStore) StreamStart(ctx context.Context, chainName string) (head.Event, error) {
	checkpoint, err := s.Store.StreamStart(ctx, chainName)
	if err != nil {
		return head.Event{}, err
	}
	s.Store.(*head.MemoryStore).Set(s.next)
	return checkpoint, nil
}

func TestWebsocketDoesNotSkipHeadPublishedDuringSubscriptionSetup(t *testing.T) {
	proxy, store := newTestGateway(t, time.Second, nil)
	first := wsHead(t, 10, testHash('a'), testHash('9'))
	next := wsHead(t, 11, testHash('b'), first.Hash)
	store.Set(first)
	proxy.store = publishingCheckpointStore{Store: store, next: next}
	server := httptest.NewServer(proxy.WebsocketHandler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/ws/test", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	if err := writeTestWS(ctx, conn, jsonrpc.Request{JSONRPC: "2.0", ID: json.RawMessage(`1`), Method: "eth_subscribe", Params: json.RawMessage(`["newHeads"]`)}); err != nil {
		t.Fatal(err)
	}
	_, ack, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var response jsonrpc.Response
	if err := json.Unmarshal(ack, &response); err != nil || response.Error != nil || string(response.ID) != "1" {
		t.Fatalf("acknowledgment missing: %s %v", ack, err)
	}
	if got := readHeadHash(t, ctx, conn); got != next.Hash {
		t.Fatalf("lost concurrent head: %s", got)
	}
}

func TestWebsocketRejectsInvalidParentHeights(t *testing.T) {
	proxy, _ := newTestGateway(t, time.Second, nil)
	session := &wsSession{gateway: proxy, runtime: proxy.runtimes["test"], ctx: context.Background()}
	last := wsHead(t, 10, testHash('a'), testHash('9'))
	next := wsHead(t, 20, testHash('b'), last.Hash)
	if _, err := session.expandPath(last, next, map[string]head.Head{last.Hash: last}); err == nil {
		t.Fatal("accepted a parent link that skips heights")
	}
}

func TestWebsocketReorgPending(t *testing.T) {
	for _, pendingAtSubscribe := range []bool{true, false} {
		t.Run(fmt.Sprintf("pending_at_subscribe_%v", pendingAtSubscribe), func(t *testing.T) {
			fixture := newReorgGatewayFixture(t)
			if pendingAtSubscribe {
				fixture.beginReorg()
			}
			server := httptest.NewServer(fixture.proxy.WebsocketHandler())
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/ws/test", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.CloseNow()
			if err := writeTestWS(ctx, conn, jsonrpc.Request{JSONRPC: "2.0", ID: json.RawMessage(`1`), Method: "eth_subscribe", Params: json.RawMessage(`["newHeads"]`)}); err != nil {
				t.Fatal(err)
			}
			_, payload, err := conn.Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var response jsonrpc.Response
			if err := json.Unmarshal(payload, &response); err != nil {
				t.Fatal(err)
			}
			if pendingAtSubscribe {
				if response.Error == nil || response.Error.Code != jsonrpc.CodeConsistencyUnavailable {
					t.Fatalf("subscription admitted during recovery: %s", payload)
				}
				return
			}
			if response.Error != nil {
				t.Fatalf("initial subscription failed: %s", payload)
			}
			fixture.beginReorg()
			if _, _, err := conn.Read(ctx); websocket.CloseStatus(err) != websocket.StatusTryAgainLater {
				t.Fatalf("live pending recovery did not close retryably: %v", err)
			}
		})
	}
}

func TestWebsocketBackfillsMissingHeaders(t *testing.T) {
	last := wsHead(t, 10, testHash('0'), testHash('9'))
	provider := newFakeEVM(t, 12, testHash('c'), testHash('b'))
	proxy, _ := newTestGateway(t, time.Second, []config.UpstreamConfig{{ID: "provider", HTTPURL: provider.server.URL, MaxConcurrency: 8}})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	session := &wsSession{gateway: proxy, runtime: proxy.runtimes["test"], ctx: ctx}
	next := wsHead(t, 12, testHash('c'), testHash('b'))
	next.Origin = "provider"
	path, err := session.expandPath(last, next, map[string]head.Head{last.Hash: last})
	if err != nil {
		t.Fatal(err)
	}
	if len(path) != 2 || path[0].Number != 11 || path[0].Hash != testHash('b') || path[1].Hash != next.Hash {
		t.Fatalf("unexpected backfill path: %+v", path)
	}
}

func wsHead(t *testing.T, number uint64, hashValue, parent string) head.Head {
	t.Helper()
	header, err := json.Marshal(map[string]any{"number": utils.FormatEVMQuantity(number), "hash": hashValue, "parentHash": parent})
	if err != nil {
		t.Fatal(err)
	}
	return head.Head{Chain: "test", Family: "evm", Commitment: head.Latest, Number: number, Hash: hashValue, ParentHash: parent, Header: header, ObservedAt: time.Now()}
}

func writeTestWS(ctx context.Context, conn *websocket.Conn, message any) error {
	encodedMessage, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("encode test websocket message: %w", err)
	}
	return conn.Write(ctx, websocket.MessageText, encodedMessage)
}

func readHeadHash(t *testing.T, ctx context.Context, conn *websocket.Conn) string {
	t.Helper()
	_, payload, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var message struct {
		Params struct {
			Result struct {
				Hash string `json:"hash"`
			} `json:"result"`
		} `json:"params"`
	}
	if err := json.Unmarshal(payload, &message); err != nil {
		t.Fatal(err)
	}
	return message.Params.Result.Hash
}
