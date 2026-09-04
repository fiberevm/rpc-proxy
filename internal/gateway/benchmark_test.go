package gateway

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/telemetry"
)

func BenchmarkGatewayAcceptedBlockNumber(b *testing.B) {
	tel, err := telemetry.NewTelemetry(config.DatadogConfig{})
	if err != nil {
		b.Fatal(err)
	}
	cfg := &config.Config{Server: config.ServerConfig{RequestTimeout: config.Duration(time.Second), MaxBodyBytes: 1 << 20, MaxBatchSize: 100}}
	runtime := chain.NewRuntime(config.ChainConfig{Name: "test", Family: "evm", MaxHeadAge: config.Duration(time.Minute)}, tel)
	store := head.NewMemoryStore()
	store.Set(head.Head{Chain: "test", Family: "evm", Commitment: head.Latest, Number: 100, Hash: testHash('a'), ObservedAt: time.Now()})
	proxy := NewGateway(Options{Config: cfg, Store: store, Runtimes: map[string]*chain.Runtime{"test": runtime}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Telemetry: tel})
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/rpc/test", bytes.NewReader(body))
		proxy.ServeRPC(recorder, request, "test")
		if recorder.Code != http.StatusOK {
			b.Fatalf("unexpected status: %d", recorder.Code)
		}
	}
}
