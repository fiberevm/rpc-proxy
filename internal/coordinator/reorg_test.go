package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/head"
)

func TestCoordinatorReorgBarrier(t *testing.T) {
	ancestor := coordinatorBlock(8, '8', '7')
	common := coordinatorBlock(9, '9', '8')
	current := coordinatorBlock(10, 'a', '9')
	next := coordinatorBlock(11, 'b', 'a')
	gap := coordinatorBlock(12, 'c', 'b')
	fork := coordinatorBlock(10, 'd', '9')
	forkNext := coordinatorBlock(11, 'e', 'd')
	for _, test := range []struct {
		name      string
		candidate head.Head
		blocks    []head.Head
		canonical head.Head
		wantHash  string
		wantEpoch uint64
		pending   bool
		wantError bool
	}{
		{name: "ordinary extension", candidate: next, blocks: []head.Head{current, next}, canonical: current, wantHash: next.Hash},
		{name: "continuous gap", candidate: gap, blocks: []head.Head{current, next, gap}, canonical: current, wantHash: gap.Hash},
		{name: "lagging ancestor", candidate: common, blocks: []head.Head{ancestor, common, current}, canonical: current, wantHash: current.Hash},
		{name: "same height replacement", candidate: fork, blocks: []head.Head{common, current, fork}, canonical: fork, wantHash: fork.Hash, wantEpoch: 1},
		{name: "longer fork", candidate: forkNext, blocks: []head.Head{common, current, fork, forkNext}, canonical: fork, wantHash: forkNext.Hash, wantEpoch: 1},
		{name: "missing same height ancestry", candidate: fork, blocks: []head.Head{current, fork}, canonical: fork, wantHash: current.Hash, wantEpoch: 1, pending: true, wantError: true},
		{name: "missing next height ancestry", candidate: forkNext, blocks: []head.Head{current, forkNext}, canonical: fork, wantHash: current.Hash, wantEpoch: 1, pending: true, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			coordinator := testCoordinator(t, 4, test.blocks, map[uint64]head.Head{10: test.canonical}, test.candidate)
			store := coordinator.store.(*head.MemoryStore)
			store.Set(current)
			token, acquired, err := store.Acquire(context.Background(), "test", "leader", 10*time.Second)
			if err != nil || !acquired {
				t.Fatalf("acquire leader: acquired=%v err=%v", acquired, err)
			}
			err = coordinator.consider(context.Background(), token, test.candidate)
			if (err != nil) != test.wantError {
				t.Fatalf("consider: %v", err)
			}
			snapshot, err := store.Snapshot(context.Background(), "test")
			accepted := snapshot.Heads[head.Latest]
			if err != nil || accepted.Hash != test.wantHash || accepted.ReorgEpoch != test.wantEpoch || accepted.ReorgPending != test.pending {
				t.Fatalf("incorrect reorg state: %+v err=%v", accepted, err)
			}
		})
	}
}

func TestCoordinatorDisagreementKeepsReadsFenced(t *testing.T) {
	common := coordinatorBlock(9, '9', '8')
	current := coordinatorBlock(10, 'a', '9')
	fork := coordinatorBlock(10, 'd', '9')
	next := coordinatorBlock(11, 'e', 'd')
	blocks := []head.Head{common, current, fork, next}
	coordinator := testCoordinator(t, 4, blocks, map[uint64]head.Head{10: current}, current)
	forkCoordinator := testCoordinator(t, 4, blocks, map[uint64]head.Head{10: fork}, fork)
	chainConfig := coordinator.runtime.Config
	chainConfig.Upstreams = []config.UpstreamConfig{
		{ID: "old", HTTPURL: chainConfig.Upstreams[0].HTTPURL, MaxConcurrency: 8},
		{ID: "fork", HTTPURL: forkCoordinator.runtime.Config.Upstreams[0].HTTPURL, MaxConcurrency: 8},
	}
	chainConfig.HeadRequestTimeout = config.Duration(time.Second)
	coordinator.runtime = chain.NewRuntime(chainConfig, coordinator.telemetry)
	if err := coordinator.runtime.Validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	store := coordinator.store.(*head.MemoryStore)
	store.Set(current)
	token, acquired, err := store.Acquire(context.Background(), "test", "leader", 10*time.Second)
	if err != nil || !acquired {
		t.Fatalf("acquire leader: acquired=%v err=%v", acquired, err)
	}
	// HTTP fallback must forward the conflicting observation even when the
	// current hash wins selectFreshest's equal-height tie break.
	observations := make(chan observation, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	coordinator.poll(ctx, observations, []string{head.Latest})
	coordinator.pollWorkers.Wait()
	close(observations)
	count := 0
	for observed := range observations {
		count++
		if err := coordinator.consider(ctx, token, observed.head); err != nil {
			t.Fatal(err)
		}
	}
	if count != 2 {
		t.Fatalf("poll hid a provider's fork: got %d observations", count)
	}
	// An old-fork refresh cannot blindly clear a persisted reorg marker.
	if err := coordinator.consider(ctx, token, current); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx, "test")
	accepted := snapshot.Heads[head.Latest]
	if err != nil || accepted.Hash != current.Hash || !accepted.ReorgPending || accepted.ReorgEpoch != 1 {
		t.Fatalf("disagreement did not keep reads fenced: %+v err=%v", accepted, err)
	}
	// A verified longer fork may win without requiring unanimity from a lagging provider.
	if err := coordinator.consider(ctx, token, next); err != nil {
		t.Fatal(err)
	}
	snapshot, err = store.Snapshot(ctx, "test")
	accepted = snapshot.Heads[head.Latest]
	if err != nil || accepted.Hash != next.Hash || accepted.ReorgPending || accepted.ReorgEpoch != 1 {
		t.Fatalf("longer fork did not recover: %+v err=%v", accepted, err)
	}
}

func TestCoordinatorCanonicalConvergenceClearsPendingReorg(t *testing.T) {
	current := coordinatorBlock(10, 'a', '9')
	coordinator := testCoordinator(t, 4, []head.Head{current}, map[uint64]head.Head{10: current}, current)
	store := coordinator.store.(*head.MemoryStore)
	store.Set(current)
	token, acquired, err := store.Acquire(context.Background(), "test", "leader", 10*time.Second)
	if err != nil || !acquired {
		t.Fatalf("acquire leader: acquired=%v err=%v", acquired, err)
	}
	if _, err := store.BeginReorg(context.Background(), "test", token); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.consider(context.Background(), token, current); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(context.Background(), "test")
	accepted := snapshot.Heads[head.Latest]
	if err != nil || accepted.ReorgPending || accepted.ReorgEpoch != 1 || accepted.Hash != current.Hash {
		t.Fatalf("canonical convergence did not clear barrier: %+v err=%v", accepted, err)
	}
}
