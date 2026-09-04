package chain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
	"github.com/fiberevm/rpc-proxy/internal/telemetry"
)

func TestParseEVMHeadRejectsMalformedIdentity(t *testing.T) {
	runtime := &Runtime{Config: config.ChainConfig{Name: "test"}}
	for _, header := range []string{
		`null`,
		`{"number":"0x1","hash":"0xabc","parentHash":"0xabc"}`,
		`{"number":"0x1","hash":"0x` + strings.Repeat("a", 64) + `"}`,
		`{"number":"0x01","hash":"0x` + strings.Repeat("a", 64) + `","parentHash":"0x` + strings.Repeat("b", 64) + `"}`,
	} {
		t.Run(header, func(t *testing.T) {
			if _, err := runtime.ParseEVMHead(EVMHeadParseRequest{Header: json.RawMessage(header)}); err == nil {
				t.Fatal("accepted invalid header")
			}
		})
	}
}

func TestNumberedBlockLookupRejectsWrongHeight(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var call jsonrpc.Request
		if err := json.NewDecoder(request.Body).Decode(&call); err != nil {
			t.Error(err)
			return
		}
		header := json.RawMessage(`{"number":"0x63","hash":"0x` + strings.Repeat("a", 64) + `","parentHash":"0x` + strings.Repeat("b", 64) + `"}`)
		if err := json.NewEncoder(w).Encode(jsonrpc.Success(call.ID, header)); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	tel, err := telemetry.NewTelemetry(config.DatadogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(config.ChainConfig{Name: "test", Family: "evm", Upstreams: []config.UpstreamConfig{{ID: "wrong", HTTPURL: server.URL, MaxConcurrency: 8}}}, tel)
	runtime.valid["wrong"] = true
	if _, err := runtime.GetEVMBlock(context.Background(), json.RawMessage(`"0x64"`), head.Snapshot{}); !errors.Is(err, ErrEVMBlockUnavailable) {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := runtime.EVMBlockByNumberFrom(context.Background(), EVMBlockByNumberRequest{Number: 100, UpstreamID: "wrong"}); err == nil {
		t.Fatal("accepted wrong height")
	}
}

func TestAvailabilityProbeCoalescesAndWaitersCancelIndependently(t *testing.T) {
	runtime := &Runtime{availability: map[string]availabilityRecord{}}
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	check := func(ctx context.Context) (bool, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
			return true, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { _, err := runtime.probe(ctx, "head", check); first <- err }()
	<-started
	cancel()
	select {
	case err := <-first:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled waiter remained blocked")
	}
	close(release)
	available, err := runtime.probe(context.Background(), "head", check)
	if err != nil || !available || calls.Load() != 1 {
		t.Fatalf("available=%v err=%v calls=%d", available, err, calls.Load())
	}
}

func TestAvailabilityCacheIsBounded(t *testing.T) {
	runtime := &Runtime{availability: map[string]availabilityRecord{}}
	for index := 0; index < 4200; index++ {
		if _, err := runtime.probe(context.Background(), fmt.Sprint(index), func(context.Context) (bool, error) { return true, nil }); err != nil {
			t.Fatal(err)
		}
	}
	if len(runtime.availability) > 4096 {
		t.Fatalf("cache grew to %d", len(runtime.availability))
	}
}

func TestStateCapabilityProbesRejectMethodsThatIgnoreHash(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var call jsonrpc.Request
		if err := json.NewDecoder(r.Body).Decode(&call); err != nil {
			t.Error(err)
			return
		}
		response := jsonrpc.Success(call.ID, json.RawMessage(`"0x0"`))
		// Only balance honors the block hash; the other methods silently ignore it.
		if call.Method == "eth_getBalance" && strings.Contains(string(call.Params), "0x"+strings.Repeat("0", 64)) {
			response = jsonrpc.Failure(call.ID, &jsonrpc.Error{Code: -32001, Message: "block not found"})
		}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	tel, err := telemetry.NewTelemetry(config.DatadogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(config.ChainConfig{Name: "test", Family: "evm", Upstreams: []config.UpstreamConfig{{ID: "provider", HTTPURL: server.URL, MaxConcurrency: 8}}}, tel)
	candidate := runtime.Upstreams[0]
	runtime.probeEVMStateMethods(context.Background(), candidate, "0x"+strings.Repeat("a", 64))
	if !candidate.SupportsPinnedMethod("eth_getBalance") {
		t.Fatal("rejected hash-aware method")
	}
	for _, method := range []string{"eth_getCode", "eth_getStorageAt", "eth_call", "eth_getProof", "eth_getTransactionCount"} {
		if candidate.SupportsPinnedMethod(method) {
			t.Fatalf("accepted method ignoring hash: %s", method)
		}
	}
}
