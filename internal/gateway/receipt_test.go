package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
)

type receiptReply struct {
	receipt    json.RawMessage
	rpcError   *jsonrpc.Error
	httpStatus int
}

func (f *fakeEVM) setReceipt(reply receiptReply) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.receipt = reply
}

func TestTransformTransactionReceipt(t *testing.T) {
	runtime := testRuntime(t, "evm")
	transactionHash := testHash('c')
	for _, testCase := range []struct {
		name       string
		parameters string
		wantError  bool
	}{
		{"transaction hash", `["` + transactionHash + `"]`, false},
		{"missing hash", `[]`, true},
		{"null hash", `[null]`, true},
		{"block selector", `["` + transactionHash + `","latest"]`, true},
		{"short hash", `["0x1234"]`, true},
		{"invalid hex", `["` + testHash('z') + `"]`, true},
		{"object hash", `[{"blockHash":"` + transactionHash + `"}]`, true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			prepared, rpcErr := (&Gateway{}).transformRequest(context.Background(), runtime, head.Snapshot{}, jsonrpc.Request{
				JSONRPC: "2.0", Method: "eth_getTransactionReceipt", Params: json.RawMessage(testCase.parameters),
			})
			if testCase.wantError {
				if rpcErr == nil || rpcErr.Code != jsonrpc.CodeInvalidParams {
					t.Fatalf("expected invalid parameters, got %v", rpcErr)
				}
				return
			}
			if rpcErr != nil {
				t.Fatal(rpcErr)
			}
			if prepared.method != "eth_getTransactionReceipt" || string(prepared.parameters) != testCase.parameters || prepared.receiptHash != transactionHash || prepared.target != nil || prepared.requireEIP1898 {
				t.Fatalf("unexpected receipt transformation: %+v", prepared)
			}
		})
	}
	if method := (&Gateway{}).metricMethod("eth_getTransactionReceipt", "evm"); method != "eth_getTransactionReceipt" {
		t.Fatalf("receipt metrics tagged as %q", method)
	}
}

func TestGatewayReceiptLookupAndFailover(t *testing.T) {
	transactionHash := testHash('c')
	receipt := json.RawMessage(`{"transactionHash":"` + transactionHash + `","transactionIndex":"0x0","blockHash":"` + testHash('a') + `","blockNumber":"0x64","from":"0x0000000000000000000000000000000000000001","to":"0x0000000000000000000000000000000000000002","contractAddress":null,"cumulativeGasUsed":"0x5208","gasUsed":"0x5208","effectiveGasPrice":"0x1","status":"0x1","type":"0x2","logs":[],"logsBloom":"0x` + strings.Repeat("0", 512) + `","l1Fee":"0x123"}`)
	for _, testCase := range []struct {
		name          string
		first, second receiptReply
		want          json.RawMessage
		wantError     bool
	}{
		{"mined receipt", receiptReply{receipt: receipt}, receiptReply{}, receipt, false},
		{"lagging index returns null", receiptReply{}, receiptReply{receipt: receipt}, receipt, false},
		{"HTTP failover", receiptReply{httpStatus: 503}, receiptReply{receipt: receipt}, receipt, false},
		{"unsupported provider", receiptReply{rpcError: jsonrpc.MethodNotFound("eth_getTransactionReceipt")}, receiptReply{receipt: receipt}, receipt, false},
		{"pending or unknown transaction", receiptReply{}, receiptReply{}, json.RawMessage("null"), false},
		{"null and unsupported provider", receiptReply{}, receiptReply{rpcError: jsonrpc.MethodNotFound("eth_getTransactionReceipt")}, json.RawMessage("null"), false},
		{"null and failed provider", receiptReply{}, receiptReply{httpStatus: 503}, nil, true},
		{"wrong transaction receipt", receiptReply{receipt: json.RawMessage(strings.Replace(string(receipt), transactionHash, testHash('d'), 1))}, receiptReply{receipt: receipt}, receipt, false},
		{"malformed receipt and null", receiptReply{receipt: json.RawMessage(`{"blockNumber":"0x64"}`)}, receiptReply{}, nil, true},
		{"failed transaction is still a receipt", receiptReply{receipt: json.RawMessage(strings.Replace(string(receipt), `"status":"0x1"`, `"status":"0x0"`, 1))}, receiptReply{}, json.RawMessage(strings.Replace(string(receipt), `"status":"0x1"`, `"status":"0x0"`, 1)), false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			providers := map[string]*fakeEVM{
				"a": newFakeEVM(t, 100, testHash('a'), testHash('9')),
				"b": newFakeEVM(t, 99, testHash('9'), testHash('8')),
			}
			proxy, _ := newTestGateway(t, time.Second, []config.UpstreamConfig{
				{ID: "a", HTTPURL: providers["a"].server.URL, MaxConcurrency: 8},
				{ID: "b", HTTPURL: providers["b"].server.URL, MaxConcurrency: 8},
			})
			// Set the first and second replies after validation establishes latency ranking.
			candidates := proxy.runtimes["test"].Candidates(false)
			providers[candidates[0].ID].setReceipt(testCase.first)
			providers[candidates[1].ID].setReceipt(testCase.second)
			for _, candidate := range candidates {
				candidate.SetEIP1898(false)
			}
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/rpc/test", strings.NewReader(`{"jsonrpc":"2.0","id":"receipt-id","method":"eth_getTransactionReceipt","params":["`+transactionHash+`"]}`))
			proxy.ServeRPC(recorder, request, "test")
			var response jsonrpc.Response
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if recorder.Code != http.StatusOK || string(response.ID) != `"receipt-id"` {
				t.Fatalf("lost protocol identity: %d %s", recorder.Code, recorder.Body.String())
			}
			if testCase.wantError {
				if response.Error == nil || response.Error.Code != jsonrpc.CodeConsistencyUnavailable {
					t.Fatalf("expected closed failure, got %s", recorder.Body.String())
				}
				return
			}
			if response.Error != nil || string(response.Result) != string(testCase.want) {
				t.Fatalf("unexpected receipt response: %s", recorder.Body.String())
			}
			if recorder.Header().Get("X-RPC-Consistency") != "transaction-hash-lookup" || recorder.Header().Get("X-RPC-Head-Hash") != "" || recorder.Header().Get("X-RPC-Upstream") == "" {
				t.Fatalf("misleading receipt headers: %v", recorder.Header())
			}
		})
	}
}

func TestReceiptBatchAndNotificationSemantics(t *testing.T) {
	provider := newFakeEVM(t, 100, testHash('a'), testHash('9'))
	proxy, store := newTestGateway(t, time.Second, []config.UpstreamConfig{{ID: "provider", HTTPURL: provider.server.URL, MaxConcurrency: 8}})
	store.Set(head.Head{Chain: "test", Family: "evm", Commitment: head.Latest, Number: 100, Hash: testHash('a'), ObservedAt: time.Now()})
	for _, testCase := range []struct {
		body   string
		status int
	}{
		{`{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["` + testHash('c') + `"]}`, http.StatusNoContent},
		{`[{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["` + testHash('c') + `"]},{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber"},{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["` + testHash('c') + `"]}]`, http.StatusOK},
	} {
		recorder := httptest.NewRecorder()
		proxy.ServeRPC(recorder, httptest.NewRequest(http.MethodPost, "/rpc/test", strings.NewReader(testCase.body)), "test")
		if recorder.Code != testCase.status {
			t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
		}
		if testCase.status == http.StatusNoContent {
			if recorder.Body.Len() != 0 {
				t.Fatal("notification produced a body")
			}
			continue
		}
		var responses []jsonrpc.Response
		if err := json.Unmarshal(recorder.Body.Bytes(), &responses); err != nil {
			t.Fatal(err)
		}
		if len(responses) != 2 || responses[0].Error != nil || string(responses[0].ID) != "1" || string(responses[0].Result) != "null" || responses[1].Error != nil || string(responses[1].ID) != "2" || string(responses[1].Result) != `"0x64"` {
			t.Fatalf("unexpected batch: %s", recorder.Body.String())
		}
		if recorder.Header().Get("X-RPC-Consistency") != "mixed-targets" || recorder.Header().Get("X-RPC-Head-Hash") != "" {
			t.Fatalf("receipt batch claims exact pinning: %v", recorder.Header())
		}
	}
}
