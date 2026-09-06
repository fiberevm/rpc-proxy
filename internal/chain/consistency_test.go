package chain

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
	"github.com/fiberevm/rpc-proxy/internal/telemetry"
)

func TestNumberedTargetsFollowAcceptedAncestry(t *testing.T) {
	acceptedHash := "0x" + strings.Repeat("a", 64)
	parentHash := "0x" + strings.Repeat("b", 64)
	ancestorHash := "0x" + strings.Repeat("c", 64)
	for _, test := range []struct {
		name, selector, expectedHash, parentNumber string
		depth                                      int
		missingHead, pending, missingParent        bool
	}{
		{name: "same height ignores provider fork", selector: `"0x64"`, expectedHash: acceptedHash, depth: 2},
		{name: "number object ignores provider fork", selector: `{"blockNumber":"0x64"}`, expectedHash: acceptedHash, depth: 2},
		{name: "parent on accepted fork", selector: `"0x63"`, expectedHash: parentHash, parentNumber: "0x63", depth: 2},
		{name: "ancestor on accepted fork", selector: `"0x62"`, expectedHash: ancestorHash, parentNumber: "0x63", depth: 2},
		{name: "future height", selector: `"0x65"`, depth: 2},
		{name: "missing snapshot", selector: `"0x64"`, missingHead: true, depth: 2},
		{name: "pending recovery", selector: `"0x64"`, pending: true, depth: 2},
		{name: "beyond window", selector: `"0x62"`, depth: 1},
		{name: "missing ancestry", selector: `"0x63"`, missingParent: true, depth: 2},
		{name: "discontinuous ancestry", selector: `"0x63"`, parentNumber: "0x62", depth: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				var call jsonrpc.Request
				if err := json.NewDecoder(request.Body).Decode(&call); err != nil {
					t.Error(err)
					return
				}
				// Number reads report an unrelated fork, as a provider load balancer can.
				payload := json.RawMessage(`{"number":"0x64","hash":"0x` + strings.Repeat("d", 64) + `","parentHash":"` + parentHash + `"}`)
				if call.Method == "eth_getBlockByHash" {
					payload = json.RawMessage(`null`)
					if strings.Contains(string(call.Params), parentHash) && !test.missingParent {
						payload = json.RawMessage(`{"number":"` + test.parentNumber + `","hash":"` + parentHash + `","parentHash":"` + ancestorHash + `"}`)
					} else if strings.Contains(string(call.Params), ancestorHash) {
						payload = json.RawMessage(`{"number":"0x62","hash":"` + ancestorHash + `","parentHash":"` + ancestorHash + `"}`)
					}
				}
				if err := json.NewEncoder(w).Encode(jsonrpc.Success(call.ID, payload)); err != nil {
					t.Error(err)
				}
			}))
			t.Cleanup(server.Close)
			tel, err := telemetry.NewTelemetry(config.DatadogConfig{})
			if err != nil {
				t.Fatal(err)
			}
			runtime := NewRuntime(config.ChainConfig{Name: "test", Family: "evm", ReorgDepth: test.depth, HeadRequestTimeout: config.Duration(time.Second), Upstreams: []config.UpstreamConfig{{ID: "provider", HTTPURL: server.URL, MaxConcurrency: 8}}}, tel)
			runtime.valid["provider"] = true
			snapshot := head.Snapshot{Heads: map[string]head.Head{}}
			if !test.missingHead {
				snapshot.Heads[head.Latest] = head.Head{Chain: "test", Family: "evm", Commitment: head.Latest, Number: 100, Hash: acceptedHash, ParentHash: parentHash, ReorgPending: test.pending}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			target, err := runtime.GetEVMBlock(ctx, json.RawMessage(test.selector), snapshot)
			if test.expectedHash == "" {
				if !errors.Is(err, ErrEVMBlockUnavailable) {
					t.Fatalf("unproven numbered target accepted: %+v, %v", target, err)
				}
			} else if err != nil || target.Hash != test.expectedHash {
				t.Fatalf("wrong branch selected: %+v, %v", target, err)
			}
		})
	}
}

func TestHashAvailabilityCannotBypassHeaderValidation(t *testing.T) {
	blockHash := "0x" + strings.Repeat("a", 64)
	parentHash := "0x" + strings.Repeat("b", 64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var call jsonrpc.Request
		if err := json.NewDecoder(request.Body).Decode(&call); err != nil {
			t.Error(err)
			return
		}
		payload := json.RawMessage(`{"number":"0x64","hash":"` + blockHash + `","parentHash":"` + parentHash + `"}`)
		if err := json.NewEncoder(w).Encode(jsonrpc.Success(call.ID, payload)); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)
	tel, err := telemetry.NewTelemetry(config.DatadogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(config.ChainConfig{Name: "test", Family: "evm", Upstreams: []config.UpstreamConfig{{ID: "provider", HTTPURL: server.URL, MaxConcurrency: 8}}}, tel)
	for _, test := range []struct {
		name      string
		target    head.Head
		available bool
	}{
		{"hash only", head.Head{Hash: blockHash, Commitment: "explicit"}, true},
		{"wrong height", head.Head{Hash: blockHash, Commitment: head.Latest, Number: 101, ParentHash: parentHash}, false},
		{"wrong parent", head.Head{Hash: blockHash, Commitment: head.Latest, Number: 100, ParentHash: blockHash}, false},
		{"valid header", head.Head{Hash: blockHash, Commitment: head.Latest, Number: 100, ParentHash: parentHash}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			available, err := runtime.HasEVMBlock(context.Background(), runtime.Upstreams[0], test.target)
			if err != nil || available != test.available {
				t.Fatalf("availability=%v, error=%v", available, err)
			}
		})
	}
}
