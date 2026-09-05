package head

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestRedisReorgFenceAcrossClients(t *testing.T) {
	rawURL := os.Getenv("RPC_PROXY_TEST_REDIS_URL")
	if rawURL == "" {
		t.Skip("RPC_PROXY_TEST_REDIS_URL is not set")
	}
	options := RedisStoreOptions{URL: rawURL, KeyPrefix: fmt.Sprintf("rpc-proxy-reorg-test-%d", time.Now().UnixNano()), StreamMaxLength: 32}
	writer, err := NewRedisStore(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := writer.Close(); err != nil {
			t.Error(err)
		}
	})
	reader, err := NewRedisStore(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	})
	testReorgFence(t, writer, reader)
}

func TestRedisStoreFencingGenerationAndStream(t *testing.T) {
	rawURL := os.Getenv("RPC_PROXY_TEST_REDIS_URL")
	if rawURL == "" {
		t.Skip("RPC_PROXY_TEST_REDIS_URL is not set")
	}
	options := RedisStoreOptions{URL: rawURL, KeyPrefix: fmt.Sprintf("rpc-proxy-test-%d", time.Now().UnixNano()), StreamMaxLength: 32}
	store, err := NewRedisStore(options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	firstToken, acquired, err := store.Acquire(ctx, "ethereum", "first", 150*time.Millisecond)
	if err != nil || !acquired {
		t.Fatalf("first acquire: acquired=%v err=%v", acquired, err)
	}
	first, err := store.Publish(ctx, firstToken, Head{Chain: "ethereum", Family: "evm", Commitment: Latest, Number: 10, Hash: hash(10), ObservedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if _, acquired, err := store.Acquire(ctx, "ethereum", "second", time.Second); err != nil || acquired {
		t.Fatalf("concurrent leader acquired: acquired=%v err=%v", acquired, err)
	}
	time.Sleep(175 * time.Millisecond)
	secondToken, acquired, err := store.Acquire(ctx, "ethereum", "second", time.Second)
	if err != nil || !acquired {
		t.Fatalf("takeover acquire: acquired=%v err=%v", acquired, err)
	}
	if _, err := store.Publish(ctx, firstToken, Head{Chain: "ethereum", Family: "evm", Commitment: Latest, Number: 11, Hash: hash(11)}); !errors.Is(err, ErrLeadershipLost) {
		t.Fatalf("expired leader was not fenced: %v", err)
	}
	if _, err := store.BeginReorg(ctx, "ethereum", firstToken); !errors.Is(err, ErrLeadershipLost) {
		t.Fatalf("expired leader changed reorg state: %v", err)
	}
	second, err := store.Publish(ctx, secondToken, Head{Chain: "ethereum", Family: "evm", Commitment: Latest, Number: 11, Hash: hash(11), ParentHash: first.Hash, ObservedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if second.Generation <= first.Generation {
		t.Fatalf("generation did not advance: first=%d second=%d", first.Generation, second.Generation)
	}
	snapshot, err := store.Snapshot(ctx, "ethereum")
	if err != nil || snapshot.Heads[Latest].Hash != second.Hash {
		t.Fatalf("unexpected snapshot: %+v err=%v", snapshot, err)
	}
	events, err := store.ReadEvents(ctx, "ethereum", "0-0", 100*time.Millisecond, 10)
	if err != nil || len(events) != 2 {
		t.Fatalf("unexpected stream: len=%d err=%v", len(events), err)
	}
	returned, err := store.Publish(ctx, secondToken, first)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := store.StreamStart(ctx, "ethereum")
	if err != nil || checkpoint.Head.Generation != returned.Generation || checkpoint.Head.Hash != first.Hash {
		t.Fatalf("returning fork transition missing: %+v %v", checkpoint, err)
	}
	// Exact trimming must make an old live cursor fail closed, not resume after a gap.
	if err := store.client.XTrimMaxLen(ctx, store.key("ethereum", "events"), 1).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadEvents(ctx, "ethereum", events[0].ID, time.Millisecond, 10); !errors.Is(err, ErrStreamGap) {
		t.Fatalf("expected retention error, got %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.StreamCursor(ctx, "ethereum"); err == nil {
		t.Fatal("closed Redis was mistaken for an empty stream")
	}
}
