package upstream

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
	"github.com/fiberevm/rpc-proxy/internal/telemetry"
)

func TestClientRejectsInvalidProtocolEnvelopes(t *testing.T) {
	for _, encoded := range []string{
		`{"jsonrpc":"2.0","id":1}`,
		`{"jsonrpc":"2.0","id":1,"result":"0x1","error":{"code":-32000,"message":"failed"}}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32000}}`,
		`{"jsonrpc":"2.0","id":2,"result":"0x1"}`,
		`{"jsonrpc":"1.0","id":1,"result":"0x1"}`,
	} {
		t.Run(encoded, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if _, err := w.Write([]byte(encoded)); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			client := testClient(t, server.URL)
			if _, err := client.Call(context.Background(), "eth_getBalance", nil); err == nil {
				t.Fatal("accepted malformed upstream response")
			}
		})
	}
}

func TestClientRejectsRedirectWithoutForwardingCredentials(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { redirected.Add(1); w.WriteHeader(http.StatusOK) }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	client := testClient(t, source.URL)
	client.Headers = map[string]string{"X-API-Key": "private-key"}
	if _, err := client.Call(context.Background(), "eth_chainId", nil); err == nil {
		t.Fatal("accepted redirect")
	}
	if redirected.Load() != 0 {
		t.Fatal("forwarded a credential-bearing request")
	}
}

func TestTransportErrorsDoNotExposeEndpointCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.Close()
	client := testClient(t, server.URL+"/v2/private-key?token=query-secret")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := client.Call(ctx, "eth_chainId", nil)
	if err == nil {
		t.Fatal("expected connection failure")
	}
	for _, message := range []string{err.Error(), client.Status().LastError} {
		if strings.Contains(message, "private-key") || strings.Contains(message, "query-secret") {
			t.Fatalf("credential leak: %s", message)
		}
	}
}

func TestClientPreservesNullResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var call jsonrpc.Request
		if err := json.NewDecoder(r.Body).Decode(&call); err != nil {
			t.Error(err)
			return
		}
		if err := json.NewEncoder(w).Encode(jsonrpc.Success(call.ID, json.RawMessage("null"))); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	response, err := testClient(t, server.URL).Call(context.Background(), "eth_getBlockByHash", nil)
	if err != nil || response.Error != nil || string(response.Result) != "null" {
		t.Fatalf("response=%+v err=%v", response, err)
	}
}

func testClient(t *testing.T, endpoint string) *Client {
	t.Helper()
	tel, err := telemetry.NewTelemetry(config.DatadogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return NewClient(Options{Config: config.UpstreamConfig{ID: "test", HTTPURL: endpoint, MaxConcurrency: 4}, Telemetry: tel})
}
