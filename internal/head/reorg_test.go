package head

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryReorgFence(t *testing.T) {
	store := NewMemoryStore()
	testReorgFence(t, store, store)
}

func testReorgFence(t *testing.T, writer, reader Store) {
	t.Helper()
	ctx := context.Background()
	token, acquired, err := writer.Acquire(ctx, "test", "first", 10*time.Second)
	if err != nil || !acquired {
		t.Fatalf("acquire: acquired=%v err=%v", acquired, err)
	}
	first, err := writer.Publish(ctx, token, Head{Chain: "test", Family: "evm", Commitment: Latest, Number: 10, Hash: hash(1), ParentHash: hash(0), ObservedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	guarded, err := writer.BeginReorg(ctx, "test", token)
	if err != nil || !guarded.ReorgPending || guarded.ReorgEpoch != 1 || guarded.Generation != first.Generation || guarded.Hash != first.Hash || !guarded.ObservedAt.Equal(first.ObservedAt) {
		t.Fatalf("invalid reorg barrier: %+v err=%v", guarded, err)
	}
	duplicate, err := writer.BeginReorg(ctx, "test", token)
	if err != nil || duplicate.ReorgEpoch != guarded.ReorgEpoch {
		t.Fatalf("duplicate detection changed epoch: %+v err=%v", duplicate, err)
	}
	snapshot, err := reader.Snapshot(ctx, "test")
	if err != nil || !snapshot.Heads[Latest].ReorgPending || snapshot.Heads[Latest].ReorgEpoch != 1 {
		t.Fatalf("reader did not observe barrier: %+v err=%v", snapshot, err)
	}
	if _, err := writer.Publish(ctx, token, first); !errors.Is(err, ErrReorgFence) {
		t.Fatalf("stale observation cleared barrier: %v", err)
	}
	if err := writer.Release(ctx, "test", token); err != nil {
		t.Fatal(err)
	}
	nextToken, acquired, err := writer.Acquire(ctx, "test", "second", 10*time.Second)
	if err != nil || !acquired {
		t.Fatalf("takeover: acquired=%v err=%v", acquired, err)
	}
	if _, err := writer.BeginReorg(ctx, "test", token); !errors.Is(err, ErrLeadershipLost) {
		t.Fatalf("former leader changed barrier: %v", err)
	}
	guarded, err = writer.BeginReorg(ctx, "test", nextToken)
	if err != nil || guarded.ReorgEpoch != 1 || !guarded.ReorgPending {
		t.Fatalf("takeover lost barrier: %+v err=%v", guarded, err)
	}
	replacement := Head{Chain: "test", Family: "evm", Commitment: Latest, Number: 10, Hash: hash(2), ParentHash: first.ParentHash, ObservedAt: time.Now().UTC(), ReorgEpoch: guarded.ReorgEpoch}
	if _, err := writer.Publish(ctx, token, replacement); !errors.Is(err, ErrLeadershipLost) {
		t.Fatalf("former leader published recovery: %v", err)
	}
	replacement, err = writer.Publish(ctx, nextToken, replacement)
	if err != nil || replacement.ReorgPending || replacement.ReorgEpoch != 1 || replacement.Generation <= first.Generation {
		t.Fatalf("recovery publication failed: %+v err=%v", replacement, err)
	}
	next := replacement
	next.Number++
	next.ParentHash = replacement.Hash
	next.Hash = hash(3)
	next, err = writer.Publish(ctx, nextToken, next)
	if err != nil || next.ReorgEpoch != replacement.ReorgEpoch {
		t.Fatalf("normal advance changed reorg epoch: %+v err=%v", next, err)
	}
	guarded, err = writer.BeginReorg(ctx, "test", nextToken)
	if err != nil {
		t.Fatal(err)
	}
	first.ReorgEpoch = guarded.ReorgEpoch
	returned, err := writer.Publish(ctx, nextToken, first)
	if err != nil || returned.ReorgEpoch != 2 || returned.ReorgPending || returned.Generation <= next.Generation {
		t.Fatalf("returning fork lost epoch: %+v err=%v", returned, err)
	}
	if _, err := writer.Publish(ctx, nextToken, next); !errors.Is(err, ErrReorgFence) {
		t.Fatalf("old epoch overwrote recovered head: %v", err)
	}
	snapshot, err = reader.Snapshot(ctx, "test")
	if err != nil || snapshot.Heads[Latest].ReorgEpoch != 2 || snapshot.Heads[Latest].Hash != first.Hash || snapshot.Heads[Latest].ReorgPending {
		t.Fatalf("reader did not observe completed recovery: %+v err=%v", snapshot, err)
	}
	events, err := reader.ReadEvents(ctx, "test", "0-0", time.Millisecond, 10)
	if err != nil || len(events) != 4 {
		t.Fatalf("barrier should not emit an accepted header: events=%d err=%v", len(events), err)
	}
}
