package head

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStoreFencesExpiredLeader(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	token, acquired, err := store.Acquire(ctx, "ethereum", "one", 10*time.Millisecond)
	if err != nil || !acquired {
		t.Fatalf("acquire: %v %v", acquired, err)
	}
	time.Sleep(15 * time.Millisecond)
	if _, err := store.Publish(ctx, token, Head{Chain: "ethereum", Family: "evm", Commitment: Latest, Number: 1, Hash: hash(1)}); err != ErrLeadershipLost {
		t.Fatalf("expected leadership loss, got %v", err)
	}
	second, acquired, err := store.Acquire(ctx, "ethereum", "two", time.Second)
	if err != nil || !acquired || second == token {
		t.Fatalf("second acquire: %q %v %v", second, acquired, err)
	}
}

func TestMemoryStoreDoesNotRepublishDuplicateHash(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	token, acquired, err := store.Acquire(ctx, "ethereum", "one", time.Second)
	if err != nil || !acquired {
		t.Fatalf("acquire: acquired=%v err=%v", acquired, err)
	}
	value := Head{Chain: "ethereum", Family: "evm", Commitment: Latest, Number: 1, Hash: hash(1), ObservedAt: time.Now()}
	first, err := store.Publish(ctx, token, value)
	if err != nil {
		t.Fatal(err)
	}
	value.ObservedAt = time.Now().Add(time.Second)
	second, err := store.Publish(ctx, token, value)
	if err != nil {
		t.Fatal(err)
	}
	if first.Generation != second.Generation {
		t.Fatalf("duplicate changed generation: %d != %d", first.Generation, second.Generation)
	}
	if !first.ChangedAt.Equal(second.ChangedAt) {
		t.Fatalf("duplicate changed the stuck-head timestamp: %s != %s", first.ChangedAt, second.ChangedAt)
	}
	events, err := store.ReadEvents(ctx, "ethereum", "0-0", time.Millisecond, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one stream event, got %d", len(events))
	}
}

func TestHeadStreamKeepsReturnToPreviouslyAcceptedFork(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	token, ok, err := store.Acquire(ctx, "test", "leader", time.Second)
	if err != nil || !ok {
		t.Fatalf("leadership: %v", err)
	}
	for _, fork := range []byte{1, 2, 1} {
		if _, err := store.Publish(ctx, token, Head{Chain: "test", Family: "evm", Commitment: Latest, Number: 10, Hash: hash(fork), ObservedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	events, err := store.ReadEvents(ctx, "test", "0-0", time.Millisecond, 10)
	if err != nil || len(events) != 3 || events[2].Head.Hash != hash(1) {
		t.Fatalf("fork transition lost: %+v %v", events, err)
	}
	checkpoint, err := store.StreamStart(ctx, "test")
	if err != nil || checkpoint.ID != events[2].ID || checkpoint.Head.Generation != events[2].Head.Generation {
		t.Fatalf("checkpoint mismatch: %+v %v", checkpoint, err)
	}
}

func hash(value byte) string {
	const digits = "0123456789abcdef"
	result := make([]byte, 66)
	result[0] = '0'
	result[1] = 'x'
	for i := 2; i < len(result); i++ {
		result[i] = digits[value%16]
	}
	return string(result)
}
