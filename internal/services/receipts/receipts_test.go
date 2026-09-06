package receipts

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/services/requestcache"
)

type ancestorReader struct {
	headers map[string]head.Head
	calls   atomic.Int32
}

// EVMBlockByHash supplies complete headers from an in-memory branch for inclusion tests.
func (r *ancestorReader) EVMBlockByHash(_ context.Context, request chain.EVMBlockByHashRequest) (head.Head, error) {
	r.calls.Add(1)
	block, exists := r.headers[request.Hash]
	if !exists {
		return head.Head{}, errors.New("ancestor unavailable")
	}
	return block, nil
}

func TestReceiptInclusionRequiresContinuousAcceptedAncestry(t *testing.T) {
	for _, test := range []struct {
		name       string
		number     uint64
		hashNumber uint64
		missing    bool
		broken     bool
		pending    bool
		want       bool
		wantError  bool
	}{
		{name: "accepted block", number: 100, hashNumber: 100, want: true},
		{name: "direct parent", number: 99, hashNumber: 99, want: true},
		{name: "older ancestor", number: 98, hashNumber: 98, want: true},
		{name: "depth boundary", number: 97, hashNumber: 97, want: true},
		{name: "future receipt", number: 101, hashNumber: 101},
		{name: "beyond depth", number: 96, hashNumber: 96},
		{name: "orphan at head", number: 100, hashNumber: 200},
		{name: "orphan parent", number: 99, hashNumber: 200},
		{name: "orphan ancestor", number: 98, hashNumber: 200},
		{name: "missing header", number: 98, hashNumber: 98, missing: true, wantError: true},
		{name: "discontinuous header", number: 98, hashNumber: 98, broken: true, wantError: true},
		{name: "pending reorg", number: 100, hashNumber: 100, pending: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := &ancestorReader{headers: make(map[string]head.Head)}
			for number := uint64(96); number <= 100; number++ {
				block := head.Head{Chain: "test", Family: "evm", Number: number, Hash: fmt.Sprintf("0x%064x", number), ParentHash: fmt.Sprintf("0x%064x", number-1), Origin: "provider", ObservedAt: time.Now()}
				reader.headers[block.Hash] = block
			}
			accepted := reader.headers[fmt.Sprintf("0x%064x", 100)]
			accepted.ReorgPending = test.pending
			parentHash := accepted.ParentHash
			if test.missing {
				delete(reader.headers, parentHash)
			}
			if test.broken {
				parent := reader.headers[parentHash]
				parent.Number = 95
				reader.headers[parentHash] = parent
			}
			verifier := NewService(Options{Blocks: reader, MaxDepth: 3})
			verified, err := verifier.ContainsBlock(context.Background(), BlockRequest{Accepted: accepted, Block: head.Head{Number: test.number, Hash: fmt.Sprintf("0x%064x", test.hashNumber)}})
			if verified != test.want || (err != nil) != test.wantError {
				t.Fatalf("inclusion: verified=%v error=%v", verified, err)
			}
			if (test.number >= 99 || test.number < 97 || test.pending) && reader.calls.Load() != 0 {
				t.Fatalf("unnecessary ancestry lookup: %d calls", reader.calls.Load())
			}
		})
	}
}

func TestReceiptAncestryHeadersAreSharedWithoutTrustingOldCanonicality(t *testing.T) {
	parent := head.Head{Chain: "test", Family: "evm", Number: 99, Hash: fmt.Sprintf("0x%064x", 99), ParentHash: fmt.Sprintf("0x%064x", 98)}
	accepted := head.Head{Chain: "test", Family: "evm", Number: 100, Hash: fmt.Sprintf("0x%064x", 100), ParentHash: parent.Hash}
	reader := &ancestorReader{headers: map[string]head.Head{parent.Hash: parent}}
	cache := requestcache.NewService(requestcache.Options{Config: config.CacheConfig{MaxEntries: 8, MaxBytes: 8192, MaxEntryBytes: 4096, TTL: config.Duration(time.Minute)}, LoadTimeout: time.Second})
	verifier := NewService(Options{Blocks: reader, Cache: cache, MaxDepth: 3})
	for range 2 {
		verified, err := verifier.ContainsBlock(context.Background(), BlockRequest{Accepted: accepted, Block: head.Head{Number: 98, Hash: parent.ParentHash}})
		if err != nil || !verified {
			t.Fatalf("ancestor was not verified: %v, %v", verified, err)
		}
	}
	if reader.calls.Load() != 1 {
		t.Fatalf("ancestry headers were not shared: %d upstream calls", reader.calls.Load())
	}
	// The old header remains known after a reorg, but cannot prove inclusion on another branch.
	accepted.ParentHash = fmt.Sprintf("0x%064x", 199)
	accepted.ReorgEpoch++
	verified, err := verifier.ContainsBlock(context.Background(), BlockRequest{Accepted: accepted, Block: head.Head{Number: 98, Hash: parent.ParentHash}})
	if verified || err == nil {
		t.Fatalf("cached orphan header proved canonicality: %v, %v", verified, err)
	}
}
