package coordinator

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/telemetry"
	"github.com/fiberevm/rpc-proxy/internal/utils"
)

func TestSelectFreshestKeepsCurrentOnEqualHeightDisagreement(t *testing.T) {
	current := head.Head{Number: 10, Hash: "current"}
	selected := (&Coordinator{}).selectFreshest(current, []head.Head{{Number: 10, Hash: "fork"}, {Number: 10, Hash: "current", Origin: "b"}})
	if selected.Hash != "current" {
		t.Fatalf("selected %q", selected.Hash)
	}
}

func TestSelectFreshestAdvancesToHighest(t *testing.T) {
	selected := (&Coordinator{}).selectFreshest(head.Head{Number: 10, Hash: "old"}, []head.Head{{Number: 11, Hash: "a"}, {Number: 12, Hash: "b"}})
	if selected.Number != 12 || selected.Hash != "b" {
		t.Fatalf("unexpected selection: %+v", selected)
	}
}

func TestValidateEVMTransitionAcceptsContinuousJump(t *testing.T) {
	b10 := coordinatorBlock(10, 'a', '9')
	b11 := coordinatorBlock(11, 'b', 'a')
	b12 := coordinatorBlock(12, 'c', 'b')
	coordinator := testCoordinator(t, 4, []head.Head{b10, b11, b12}, map[uint64]head.Head{10: b10, 11: b11, 12: b12}, b12)
	reorg, err := coordinator.validateEVMTransition(context.Background(), evmTransitionRequest{current: b10, candidate: b12})
	if err != nil || reorg {
		t.Fatalf("continuous jump rejected: reorg=%v err=%v", reorg, err)
	}
}

func TestValidateEVMTransitionAcceptsBoundedReorg(t *testing.T) {
	common := coordinatorBlock(9, '9', '8')
	current := coordinatorBlock(10, 'a', '9')
	fork10 := coordinatorBlock(10, 'd', '9')
	fork11 := coordinatorBlock(11, 'e', 'd')
	fork12 := coordinatorBlock(12, 'f', 'e')
	coordinator := testCoordinator(t, 4, []head.Head{common, current, fork10, fork11, fork12}, map[uint64]head.Head{9: common, 10: fork10, 11: fork11, 12: fork12}, fork12)
	reorg, err := coordinator.validateEVMTransition(context.Background(), evmTransitionRequest{current: current, candidate: fork12})
	if err != nil || !reorg {
		t.Fatalf("bounded reorg rejected: reorg=%v err=%v", reorg, err)
	}
}

func TestValidateEVMTransitionRejectsDisconnectedFork(t *testing.T) {
	currentParent := coordinatorBlock(9, '9', '8')
	current := coordinatorBlock(10, 'a', '9')
	forkParent := coordinatorBlock(9, '6', '5')
	fork10 := coordinatorBlock(10, 'd', '6')
	fork11 := coordinatorBlock(11, 'e', 'd')
	fork12 := coordinatorBlock(12, 'f', 'e')
	coordinator := testCoordinator(t, 1, []head.Head{currentParent, current, forkParent, fork10, fork11, fork12}, map[uint64]head.Head{9: forkParent, 10: fork10, 11: fork11, 12: fork12}, fork12)
	if _, err := coordinator.validateEVMTransition(context.Background(), evmTransitionRequest{current: current, candidate: fork12}); err == nil {
		t.Fatal("expected disconnected fork rejection")
	}
}

func TestTransitionDoesNotTrustUnlinkedNumberLookup(t *testing.T) {
	current := coordinatorBlock(10, 'a', '9')
	fork10 := coordinatorBlock(10, 'd', '6')
	fork11 := coordinatorBlock(11, 'e', 'd')
	fork12 := coordinatorBlock(12, 'f', 'e')
	// A load balancer may answer numbered reads from the old fork and hash reads from a new fork.
	coordinator := testCoordinator(t, 4, []head.Head{current, fork10, fork11, fork12}, map[uint64]head.Head{10: current}, fork12)
	if _, err := coordinator.validateEVMTransition(context.Background(), evmTransitionRequest{current: current, candidate: fork12}); err == nil {
		t.Fatal("accepted disconnected chain using unrelated numbered response")
	}
}

func TestSolanaObservationCannotLowerAcceptedFloor(t *testing.T) {
	coordinator := testCoordinator(t, 4, nil, nil, coordinatorBlock(10, 'a', '9'))
	store := head.NewMemoryStore()
	coordinator.store = store
	current := store.Set(head.Head{Chain: "test", Family: "solana", Commitment: head.Confirmed, Number: 100, ObservedAt: time.Now()})
	if err := coordinator.consider(context.Background(), "unused", head.Head{Chain: "test", Family: "solana", Commitment: head.Confirmed, Number: 99, ObservedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(context.Background(), "test")
	if err != nil || snapshot.Heads[head.Confirmed].Number != 100 || snapshot.Heads[head.Confirmed].Generation != current.Generation {
		t.Fatalf("floor changed: %+v %v", snapshot, err)
	}
}

func TestMissingProviderEvidenceDoesNotAuthorizeRollback(t *testing.T) {
	current := coordinatorBlock(10, 'a', '9')
	coordinator := testCoordinator(t, 4, []head.Head{current}, nil, current)
	if evidence, err := coordinator.getCanonicalEvidence(context.Background(), current); evidence.observed || err == nil {
		t.Fatalf("missing response authorized rollback: observed=%v err=%v", evidence.observed, err)
	}
}

func testCoordinator(t *testing.T, reorgDepth int, blocks []head.Head, canonical map[uint64]head.Head, latest head.Head) *Coordinator {
	t.Helper()
	byHash := map[string]head.Head{}
	for _, block := range blocks {
		byHash[block.Hash] = block
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var call struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(request.Body).Decode(&call); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		var response any
		switch call.Method {
		case "eth_chainId":
			response = "0x1"
		case "eth_getBalance":
			response = "0x0"
		case "eth_getBlockByHash":
			var hashValue string
			if err := json.Unmarshal(call.Params[0], &hashValue); err != nil {
				t.Errorf("decode block hash: %v", err)
				return
			}
			if block, ok := byHash[hashValue]; ok {
				response = blockResult(block)
			}
		case "eth_getBlockByNumber":
			var number string
			if err := json.Unmarshal(call.Params[0], &number); err != nil {
				t.Errorf("decode block number: %v", err)
				return
			}
			if number == "latest" {
				response = blockResult(latest)
			} else if parsed, err := utils.ParseEVMQuantity(number); err == nil {
				if block, ok := canonical[parsed]; ok {
					response = blockResult(block)
				}
			}
		}
		encodedResponse, err := json.Marshal(response)
		if err != nil {
			t.Errorf("encode fake response: %v", err)
			return
		}
		if err := json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(call.ID), "result": json.RawMessage(encodedResponse)}); err != nil {
			t.Errorf("write fake response: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	tel, err := telemetry.NewTelemetry(config.DatadogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	runtime := chain.NewRuntime(config.ChainConfig{Name: "test", Family: "evm", ChainID: "0x1", PollInterval: config.Duration(time.Second), ReorgDepth: reorgDepth, Upstreams: []config.UpstreamConfig{{ID: "provider", HTTPURL: server.URL, MaxConcurrency: 8}}}, tel)
	if err := runtime.Validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !runtime.IsValid("provider") {
		t.Fatal("test provider failed validation")
	}
	return NewCoordinator(Options{
		Runtime: runtime, Store: head.NewMemoryStore(), Owner: "test", LeaderTTL: time.Second,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Telemetry: tel,
	})
}

func coordinatorBlock(number uint64, hashByte, parentByte byte) head.Head {
	return head.Head{Chain: "test", Family: "evm", Commitment: head.Latest, Number: number, Hash: coordinatorHash(hashByte), ParentHash: coordinatorHash(parentByte), Origin: "provider", ObservedAt: time.Now()}
}

func coordinatorHash(value byte) string { return "0x" + strings.Repeat(string(value), 64) }

func blockResult(block head.Head) map[string]any {
	return map[string]any{"number": utils.FormatEVMQuantity(block.Number), "hash": block.Hash, "parentHash": block.ParentHash, "transactions": []any{}}
}
