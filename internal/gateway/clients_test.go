package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
)

func TestHTTPClientAttribution(t *testing.T) {
	for _, test := range []struct{ name, header, query, client string }{
		{"header", "wallet", "", "wallet"},
		{"query", "", "?client=indexer", "indexer"},
		{"header precedence", "wallet", "?client=indexer", "wallet"},
		{"anonymous", "", "", "anonymous"},
		{"unknown", "private-unregistered-client", "", "unknown"},
		{"unknown header precedence", "private-unregistered-client", "?client=wallet", "unknown"},
		{"no normalization", "Wallet", "", "unknown"},
		{"tag injection", "wallet,method:injected", "", "unknown"},
	} {
		t.Run(test.name, func(t *testing.T) {
			proxy, _ := newTestGateway(t, time.Second, nil)
			proxy.config.Clients = []string{"wallet", "indexer"}
			proxy.store = nil // Static requests must still avoid all store/provider work.
			var logs bytes.Buffer
			proxy.logger = slog.New(slog.NewJSONHandler(&logs, nil))
			request := httptest.NewRequest(http.MethodPost, "/rpc/test"+test.query, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}`))
			request.Header.Set("X-RPC-Client", test.header)
			recorder := httptest.NewRecorder()
			proxy.RPCHandler().ServeHTTP(recorder, request)
			response := decodeCacheResponse(t, recorder)
			if response.Error != nil || string(response.Result) != `"0x1"` {
				t.Fatalf("tagging changed RPC behavior: %s", recorder.Body.String())
			}
			var entry map[string]any
			if err := json.Unmarshal(logs.Bytes(), &entry); err != nil {
				t.Fatal(err)
			}
			if entry["client"] != test.client || entry["method"] != "eth_chainId" || entry["chain"] != "test" || entry["transport"] != "http" {
				t.Fatalf("incorrect attribution: %s", logs.String())
			}
			if strings.Contains(logs.String(), "private-unregistered-client") || strings.Contains(logs.String(), "injected") {
				t.Fatalf("unregistered client leaked into logs: %s", logs.String())
			}
		})
	}
}

func TestClientUsageCountsBatchItemsAndCacheHits(t *testing.T) {
	fixture := newReorgGatewayFixture(t)
	fixture.proxy.config.Clients = []string{"wallet", "indexer"}
	var logs bytes.Buffer
	fixture.proxy.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	before := fixture.provider.calls()
	batch := `[
		{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["address","latest"]},
		{"jsonrpc":"2.0","method":"eth_getBalance","params":["address","latest"]},
		{"jsonrpc":"2.0","id":2,"method":"eth_chainId"},
		{"jsonrpc":"2.0","id":3,"method":"eth_sendRawTransaction","params":["0x0"]},
		{"jsonrpc":"2.0","id":4,"method":"private-custom-method"},
		42
	]`
	for _, client := range fixture.proxy.config.Clients {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/rpc/test?client="+client, strings.NewReader(batch))
		fixture.proxy.RPCHandler().ServeHTTP(recorder, request)
		var responses []jsonrpc.Response
		if err := json.Unmarshal(recorder.Body.Bytes(), &responses); err != nil {
			t.Fatal(err)
		}
		if len(responses) != 5 || responses[0].Error != nil || responses[1].Error != nil || responses[2].Error == nil || responses[3].Error == nil || responses[4].Error == nil {
			t.Fatalf("unexpected batch responses: %s", recorder.Body.String())
		}
	}
	if fixture.provider.calls() != before+1 {
		t.Fatal("client labels prevented sharing the same cached RPC result")
	}
	counts := make(map[string]int)
	decoder := json.NewDecoder(&logs)
	for decoder.More() {
		var entry struct {
			Message string `json:"msg"`
			Client  string `json:"client"`
			Method  string `json:"method"`
		}
		if err := decoder.Decode(&entry); err != nil {
			t.Fatal(err)
		}
		if entry.Message == "rpc method requested" {
			counts[entry.Client+"/"+entry.Method]++
		}
	}
	for _, client := range fixture.proxy.config.Clients {
		for method, want := range map[string]int{"eth_getBalance": 2, "eth_chainId": 1, "write": 1, "unknown": 1, "invalid": 1} {
			if counts[client+"/"+method] != want {
				t.Fatalf("incorrect usage count for %s/%s: %v", client, method, counts)
			}
		}
	}
}

func TestClientUsageSurvivesStoreFailure(t *testing.T) {
	fixture := newReorgGatewayFixture(t)
	faultStore := &unavailableSnapshotStore{Store: fixture.store}
	faultStore.unavailable.Store(true)
	fixture.proxy.store = faultStore
	var logs bytes.Buffer
	fixture.proxy.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	recorder := fixture.request(`{"jsonrpc":"2.0","method":"eth_blockNumber"}`)
	if recorder.Code != http.StatusNoContent || !strings.Contains(logs.String(), `"method":"eth_blockNumber"`) || !strings.Contains(logs.String(), `"client":"anonymous"`) {
		t.Fatalf("notification usage lost during Redis failure: %s %s", recorder.Body.String(), logs.String())
	}
}

func TestWebsocketClientAttribution(t *testing.T) {
	for _, test := range []struct{ name, header, query, client string }{
		{"query", "", "?client=wallet", "wallet"},
		{"header precedence", "indexer", "?client=wallet", "indexer"},
		{"unknown", "", "?client=private-unregistered-client", "unknown"},
	} {
		t.Run(test.name, func(t *testing.T) {
			proxy, _ := newTestGateway(t, time.Second, nil)
			proxy.config.Clients = []string{"wallet", "indexer"}
			// The HTTP server writes logs concurrently; a channel writer
			// gives the test complete records without reading a live bytes.Buffer.
			usageLogs := make(chan []byte, 8)
			proxy.logger = slog.New(slog.NewJSONHandler(clientLogWriter{records: usageLogs}, nil))
			server := httptest.NewServer(proxy.WebsocketHandler())
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/ws/test"+test.query, &websocket.DialOptions{HTTPHeader: http.Header{"X-Rpc-Client": []string{test.header}}})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.CloseNow()
			for i, method := range []string{"eth_subscribe", "eth_unsubscribe"} {
				request := jsonrpc.Request{JSONRPC: "2.0", ID: json.RawMessage(`1`), Method: method, Params: json.RawMessage(`[]`)}
				if i == 1 {
					request.ID = nil // Notifications must be attributed, too.
				}
				if err := writeTestWS(ctx, conn, request); err != nil {
					t.Fatal(err)
				}
				select {
				case record := <-usageLogs:
					var entry map[string]any
					if err := json.Unmarshal(record, &entry); err != nil {
						t.Fatal(err)
					}
					if entry["client"] != test.client || entry["method"] != method || entry["transport"] != "websocket" {
						t.Fatalf("incorrect websocket attribution: %s", record)
					}
				case <-ctx.Done():
					t.Fatal("missing websocket method usage")
				}
			}
		})
	}
}

type clientLogWriter struct {
	records chan []byte
}

// Write captures each complete slog record in a channel for concurrent gateway tests.
func (w clientLogWriter) Write(record []byte) (int, error) {
	w.records <- bytes.Clone(record)
	return len(record), nil
}
