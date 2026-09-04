package gateway

import (
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

func TestEVMReadinessRequiresOnlyFreshLatest(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		latestAge  time.Duration
		omitLatest bool
		wantStatus int
	}{
		{name: "fresh latest without other commitments", wantStatus: http.StatusOK},
		{name: "stale latest", latestAge: time.Minute, wantStatus: http.StatusServiceUnavailable},
		{name: "missing latest", omitLatest: true, wantStatus: http.StatusServiceUnavailable},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			provider := newFakeEVM(t, 100, testHash('a'), testHash('9'))
			proxy, store := newTestGateway(t, time.Second, []config.UpstreamConfig{{ID: "a", HTTPURL: provider.server.URL, MaxConcurrency: 8}})
			if !testCase.omitLatest {
				store.Set(head.Head{Chain: "test", Family: "evm", Commitment: head.Latest, Number: 100, Hash: testHash('a'), ObservedAt: time.Now().Add(-testCase.latestAge)})
			}
			for _, includeLegacyHeads := range []bool{false, true} {
				if includeLegacyHeads {
					for _, commitment := range []string{head.Safe, head.Finalized} {
						store.Set(head.Head{Chain: "test", Family: "evm", Commitment: commitment, Number: 90, Hash: testHash('b'), ObservedAt: time.Now().Add(-time.Hour)})
					}
				}
				recorder := httptest.NewRecorder()
				proxy.AdminHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
				if recorder.Code != testCase.wantStatus {
					t.Fatalf("legacy=%v status=%d body=%s", includeLegacyHeads, recorder.Code, recorder.Body.String())
				}
			}
		})
	}
}

func TestEVMStatusHidesUntrackedStoredHeads(t *testing.T) {
	proxy, store := newTestGateway(t, time.Second, nil)
	for _, commitment := range []string{head.Latest, head.Safe, head.Finalized} {
		store.Set(head.Head{Chain: "test", Family: "evm", Commitment: commitment, Number: 100, Hash: testHash('a'), ObservedAt: time.Now(), Header: json.RawMessage(`{"number":"0x64"}`)})
	}
	recorder := httptest.NewRecorder()
	proxy.AdminHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/status", nil))
	var response struct {
		Chains map[string]struct {
			Heads map[string]head.Head `json:"heads"`
		} `json:"chains"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	heads := response.Chains["test"].Heads
	if recorder.Code != http.StatusOK || len(heads) != 1 || heads[head.Latest].Hash != testHash('a') || len(heads[head.Latest].Header) != 0 {
		t.Fatalf("unexpected latest-only status: %s", recorder.Body.String())
	}
}

func TestLatestOnlyBatchPreservesErrorsIDsAndNotifications(t *testing.T) {
	proxy, store := newTestGateway(t, time.Second, nil)
	for _, commitment := range []string{head.Latest, head.Safe, head.Finalized} {
		store.Set(head.Head{Chain: "test", Family: "evm", Commitment: commitment, Number: 100, Hash: testHash('a'), ObservedAt: time.Now()})
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/rpc/test", strings.NewReader(`[
		{"jsonrpc":"2.0","id":"safe","method":"eth_getBalance","params":["address","safe"]},
		{"jsonrpc":"2.0","id":"latest","method":"eth_blockNumber"},
		{"jsonrpc":"2.0","method":"eth_getBalance","params":["address","finalized"]}
	]`))
	proxy.ServeRPC(recorder, request, "test")
	var responses []jsonrpc.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &responses); err != nil {
		t.Fatal(err)
	}
	if recorder.Code != http.StatusOK || len(responses) != 2 {
		t.Fatalf("unexpected batch response: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	for _, response := range responses {
		switch string(response.ID) {
		case `"safe"`:
			if response.Error == nil || response.Error.Code != jsonrpc.CodeUnsupportedConsistency {
				t.Fatalf("safe request was not rejected: %+v", response)
			}
		case `"latest"`:
			if response.Error != nil || string(response.Result) != `"0x64"` {
				t.Fatalf("latest request failed: %+v", response)
			}
		default:
			t.Fatalf("unexpected response ID: %s", response.ID)
		}
	}
}
