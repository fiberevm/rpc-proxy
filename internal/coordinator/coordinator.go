package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
	"github.com/fiberevm/rpc-proxy/internal/telemetry"
	"github.com/fiberevm/rpc-proxy/internal/upstream"
	"github.com/fiberevm/rpc-proxy/internal/utils"
)

type Coordinator struct {
	runtime        *chain.Runtime
	store          head.Store
	owner          string
	leaderTTL      time.Duration
	logger         *slog.Logger
	telemetry      *telemetry.Telemetry
	polling        sync.Map
	pollWorkers    sync.WaitGroup
	subscriptionMu sync.RWMutex
	subscriptions  map[string]time.Time
	providerMu     sync.RWMutex
	providerHeads  map[providerCommitment]uint64
}

type providerCommitment struct {
	upstreamID string
	commitment string
}

// Options contains the per-chain services and leadership settings required by a coordinator.
type Options struct {
	Runtime   *chain.Runtime
	Store     head.Store
	Owner     string
	LeaderTTL time.Duration
	Logger    *slog.Logger
	Telemetry *telemetry.Telemetry
}

type observation struct{ head head.Head }

// NewCoordinator creates a fenced per-chain head coordinator with injected storage and telemetry.
func NewCoordinator(options Options) *Coordinator {
	return &Coordinator{
		runtime:       options.Runtime,
		store:         options.Store,
		owner:         options.Owner,
		leaderTTL:     options.LeaderTTL,
		logger:        options.Logger,
		telemetry:     options.Telemetry,
		subscriptions: make(map[string]time.Time),
		providerHeads: make(map[providerCommitment]uint64),
	}
}

// Run competes for leadership and coordinates accepted heads until ctx is canceled.
func (c *Coordinator) Run(ctx context.Context) {
	backoff := time.NewTicker(c.leaderTTL / 2)
	defer backoff.Stop()
	for {
		redisCtx, finishRedis := c.telemetry.Span(ctx, "redis.acquire_leader", "chain:"+c.runtime.Config.Name, "operation:acquire_leader")
		token, acquired, err := c.store.Acquire(redisCtx, c.runtime.Config.Name, c.owner, c.leaderTTL)
		finishRedis(err)
		if err != nil {
			c.telemetry.Count("redis.failure", 1, "chain:"+c.runtime.Config.Name, "operation:acquire_leader")
			c.logger.Error("coordinator leadership acquisition failed", "chain", c.runtime.Config.Name, "error", err)
		}
		if acquired {
			c.logger.Info("head coordinator leadership acquired", "chain", c.runtime.Config.Name)
			c.telemetry.Count("coordinator.leadership_change", 1, "chain:"+c.runtime.Config.Name, "state:acquired")
			c.runLeader(ctx, token)
			releaseBase, releaseCancel := context.WithTimeout(context.Background(), time.Second)
			releaseCtx, finishRelease := c.telemetry.Span(releaseBase, "redis.release_leader", "chain:"+c.runtime.Config.Name, "operation:release_leader")
			releaseErr := c.store.Release(releaseCtx, c.runtime.Config.Name, token)
			finishRelease(releaseErr)
			releaseCancel()
			if releaseErr != nil {
				c.telemetry.Count("redis.failure", 1, "chain:"+c.runtime.Config.Name, "operation:release_leader")
			}
			c.telemetry.Count("coordinator.leadership_change", 1, "chain:"+c.runtime.Config.Name, "state:lost")
		}
		select {
		case <-ctx.Done():
			return
		case <-backoff.C:
		}
	}
}

func (c *Coordinator) runLeader(parent context.Context, token string) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	observations := make(chan observation, 128)
	var watchers sync.WaitGroup
	defer func() {
		cancel()
		watchers.Wait()
		c.pollWorkers.Wait()
	}()
	watchers.Add(1)
	go func() {
		defer watchers.Done()
		c.maintainLease(ctx, cancel, token)
	}()
	if c.runtime.Config.Family == "evm" {
		for _, candidate := range c.runtime.Upstreams {
			if candidate.WebsocketURL == "" {
				continue
			}
			candidate := candidate
			watchers.Add(1)
			go func() { defer watchers.Done(); c.watchEVM(ctx, candidate, observations) }()
		}
	}
	pollTicker := time.NewTicker(c.runtime.Config.PollInterval.Value())
	defer pollTicker.Stop()
	commitments := []string{head.Latest}
	if c.runtime.Config.Family == "solana" {
		commitments = []string{head.Processed, head.Confirmed, head.Finalized}
	}
	c.poll(ctx, observations, commitments)
	for {
		select {
		case <-ctx.Done():
			return
		case <-pollTicker.C:
			c.poll(ctx, observations, commitments)
			c.emitHeadAges(ctx)
		case candidate := <-observations:
			if err := c.consider(ctx, token, candidate.head); err != nil {
				if errors.Is(err, head.ErrLeadershipLost) {
					return
				}
				c.logger.Warn("head candidate rejected", "chain", c.runtime.Config.Name, "error", err)
			}
		}
	}
}

func (c *Coordinator) maintainLease(ctx context.Context, cancel context.CancelFunc, token string) {
	ticker := time.NewTicker(c.leaderTTL / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			leaseCtx, stop := context.WithTimeout(ctx, c.leaderTTL/3)
			redisCtx, finish := c.telemetry.Span(leaseCtx, "redis.renew_leader", "chain:"+c.runtime.Config.Name, "operation:renew_leader")
			ok, err := c.store.Renew(redisCtx, c.runtime.Config.Name, token, c.leaderTTL)
			finish(err)
			stop()
			if err != nil || !ok {
				c.telemetry.Count("redis.failure", 1, "chain:"+c.runtime.Config.Name, "operation:renew")
				cancel()
				return
			}
		}
	}
}

func (c *Coordinator) poll(ctx context.Context, output chan<- observation, commitments []string) {
	for _, commitment := range commitments {
		commitment := commitment
		if _, running := c.polling.LoadOrStore(commitment, struct{}{}); running {
			continue
		}
		c.pollWorkers.Add(1)
		go func() {
			defer c.pollWorkers.Done()
			defer c.polling.Delete(commitment)
			var wg sync.WaitGroup
			observedHeads := make(chan head.Head, len(c.runtime.Upstreams))
			for _, candidate := range c.runtime.Candidates(false) {
				candidate := candidate
				if c.runtime.Config.Family == "evm" && commitment == head.Latest && c.hasFreshSubscription(candidate.ID) {
					continue
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					queryCtx, cancel := context.WithTimeout(ctx, c.runtime.Config.HeadRequestTimeout.Value())
					defer cancel()
					if c.runtime.Config.Family == "evm" && commitment == head.Latest {
						c.telemetry.Count("coordinator.latest_poll", 1, "chain:"+c.runtime.Config.Name, "upstream:"+candidate.ID)
					}
					observedHead, err := c.pollOne(queryCtx, candidate, commitment)
					if err == nil {
						c.recordProviderHead(observedHead)
						observedHeads <- observedHead
					} else {
						c.telemetry.Count("coordinator.poll_error", 1, "chain:"+c.runtime.Config.Name, "commitment:"+commitment, "upstream:"+candidate.ID)
					}
				}()
			}
			wg.Wait()
			close(observedHeads)
			var candidates []head.Head
			for observedHead := range observedHeads {
				candidates = append(candidates, observedHead)
			}
			if len(candidates) == 0 {
				return
			}
			redisCtx, finishRedis := c.telemetry.Span(ctx, "redis.snapshot", "chain:"+c.runtime.Config.Name, "operation:snapshot")
			snapshot, err := c.store.Snapshot(redisCtx, c.runtime.Config.Name)
			finishRedis(err)
			if err != nil {
				c.telemetry.Count("redis.failure", 1, "chain:"+c.runtime.Config.Name, "operation:snapshot")
				return
			}
			current, _ := snapshot.Get(commitment)
			selected := c.selectFreshest(current, candidates)
			select {
			case output <- observation{head: selected}:
			case <-ctx.Done():
			}
		}()
	}
}

func (c *Coordinator) pollOne(ctx context.Context, candidate *upstream.Client, commitment string) (head.Head, error) {
	if c.runtime.Config.Family == "evm" {
		headCall, err := c.call(ctx, candidate, "eth_getBlockByNumber", commitment, false)
		if err != nil {
			return head.Head{}, err
		}
		if headCall.Error != nil {
			return head.Head{}, headCall.Error
		}
		observedHead, err := c.runtime.ParseEVMHead(chain.EVMHeadParseRequest{Commitment: commitment, Origin: candidate.ID, Header: headCall.Result})
		if err != nil {
			return head.Head{}, err
		}
		confirmedCall, err := c.call(ctx, candidate, "eth_getBlockByHash", observedHead.Hash, false)
		if err != nil {
			return head.Head{}, err
		}
		if confirmedCall.Error != nil {
			return head.Head{}, confirmedCall.Error
		}
		confirmedHead, err := c.runtime.ParseEVMHead(chain.EVMHeadParseRequest{Commitment: commitment, Origin: candidate.ID, Header: confirmedCall.Result})
		if err != nil || !strings.EqualFold(confirmedHead.Hash, observedHead.Hash) || confirmedHead.Number != observedHead.Number || !strings.EqualFold(confirmedHead.ParentHash, observedHead.ParentHash) {
			return head.Head{}, errors.New("block hash lookup did not match head")
		}
		return observedHead, nil
	}
	slotCall, err := c.call(ctx, candidate, "getSlot", map[string]any{"commitment": commitment})
	if err != nil {
		return head.Head{}, err
	}
	if slotCall.Error != nil {
		return head.Head{}, slotCall.Error
	}
	var slot uint64
	if err := json.Unmarshal(slotCall.Result, &slot); err != nil {
		return head.Head{}, fmt.Errorf("decode %s slot from upstream %s: %w", commitment, candidate.ID, err)
	}
	return head.Head{Chain: c.runtime.Config.Name, Family: "solana", Commitment: commitment, Number: slot, Origin: candidate.ID, ObservedAt: time.Now().UTC()}, nil
}

func (c *Coordinator) selectFreshest(current head.Head, candidates []head.Head) head.Head {
	selected := candidates[0]
	for _, candidate := range candidates[1:] {
		if candidate.Number > selected.Number {
			selected = candidate
		}
	}
	if !current.IsZero() {
		for _, candidate := range candidates {
			if candidate.Identity() == current.Identity() && candidate.Number == selected.Number {
				return candidate
			}
		}
		if selected.Number < current.Number {
			for _, candidate := range candidates {
				if candidate.Identity() == current.Identity() {
					return candidate
				}
			}
		}
	}
	return selected
}

func (c *Coordinator) consider(ctx context.Context, token string, candidate head.Head) error {
	redisCtx, finishRedis := c.telemetry.Span(ctx, "redis.snapshot", "chain:"+candidate.Chain, "operation:snapshot")
	snapshot, err := c.store.Snapshot(redisCtx, candidate.Chain)
	finishRedis(err)
	if err != nil {
		c.telemetry.Count("redis.failure", 1, "chain:"+candidate.Chain, "operation:snapshot")
		return err
	}
	current, exists := snapshot.Get(candidate.Commitment)
	if candidate.Family == "evm" && !exists && candidate.Number > 0 {
		parent, err := c.runtime.EVMBlockByHash(ctx, chain.EVMBlockByHashRequest{Hash: candidate.ParentHash, Commitment: candidate.Commitment, PreferredUpstreamID: candidate.Origin})
		if err != nil {
			return fmt.Errorf("verify initial head parent: %w", err)
		}
		if parent.Number >= candidate.Number || candidate.Number-parent.Number != 1 {
			return errors.New("initial head has invalid parent linkage")
		}
	}
	if candidate.Family == "solana" && exists && candidate.Number < current.Number {
		// A lagging observation cannot lower a previously promised slot floor.
		return nil
	}
	if candidate.Family == "evm" && exists && candidate.Hash != current.Hash {
		if candidate.Number <= current.Number {
			observed, err := c.currentStillObserved(ctx, current)
			if err != nil {
				return err
			}
			if observed {
				return nil
			}
		}
		reorg, err := c.validateEVMTransition(ctx, current, candidate)
		if err != nil {
			return err
		}
		if reorg {
			c.telemetry.Count("head.reorg", 1, "chain:"+candidate.Chain)
		}
	}
	redisCtx, finishRedis = c.telemetry.Span(ctx, "redis.publish_head", "chain:"+candidate.Chain, "operation:publish_head")
	published, err := c.store.Publish(redisCtx, token, candidate)
	finishRedis(err)
	if err != nil {
		c.telemetry.Count("redis.failure", 1, "chain:"+candidate.Chain, "operation:publish_head")
		return err
	}
	c.telemetry.Gauge("head.number", float64(published.Number), "chain:"+published.Chain, "commitment:"+published.Commitment, "family:"+published.Family)
	c.telemetry.Gauge("head.age", time.Since(published.ObservedAt).Seconds(), "chain:"+published.Chain, "commitment:"+published.Commitment)
	c.telemetry.Gauge("head.stuck_age", time.Since(published.ChangedAt).Seconds(), "chain:"+published.Chain, "commitment:"+published.Commitment)
	return nil
}

func (c *Coordinator) validateEVMTransition(ctx context.Context, current, candidate head.Head) (bool, error) {
	if candidate.Number == current.Number+1 && strings.EqualFold(candidate.ParentHash, current.Hash) {
		return false, nil
	}
	depth := c.runtime.Config.ReorgDepth
	if depth < 1 {
		return false, errors.New("reorg depth must be positive")
	}
	if candidate.Number > 0 {
		parentRequest := chain.EVMBlockByHashRequest{Hash: candidate.ParentHash, Commitment: candidate.Commitment, PreferredUpstreamID: candidate.Origin}
		parent, err := c.runtime.EVMBlockByHash(ctx, parentRequest)
		if err != nil {
			return false, fmt.Errorf("verify candidate parent: %w", err)
		}
		if parent.Number+1 != candidate.Number || !strings.EqualFold(parent.Hash, candidate.ParentHash) {
			return false, errors.New("candidate head has invalid parent linkage")
		}
	}

	candidateCursor := candidate
	for step := 0; candidateCursor.Number > current.Number; step++ {
		if step >= depth {
			return false, errors.New("candidate gap exceeds continuity window")
		}
		parent, err := c.runtime.EVMBlockByHash(ctx, chain.EVMBlockByHashRequest{Hash: candidateCursor.ParentHash, Commitment: candidate.Commitment, PreferredUpstreamID: candidate.Origin})
		if err != nil {
			return false, fmt.Errorf("verify candidate continuity: %w", err)
		}
		if parent.Number >= candidateCursor.Number || parent.Number+1 != candidateCursor.Number {
			return false, errors.New("candidate chain has invalid parent linkage")
		}
		candidateCursor = parent
	}
	if candidateCursor.Number == current.Number && strings.EqualFold(candidateCursor.Hash, current.Hash) {
		return false, nil
	}

	currentAncestors := map[string]uint64{current.Hash: current.Number}
	cursor := current
	for step := 0; step < depth && cursor.Number > 0 && cursor.ParentHash != ""; step++ {
		parentRequest := chain.EVMBlockByHashRequest{Hash: cursor.ParentHash, Commitment: current.Commitment, PreferredUpstreamID: current.Origin}
		parent, err := c.runtime.EVMBlockByHash(ctx, parentRequest)
		if err != nil {
			break
		}
		if parent.Number+1 != cursor.Number || !strings.EqualFold(parent.Hash, cursor.ParentHash) {
			return false, errors.New("current chain has invalid parent linkage")
		}
		currentAncestors[parent.Hash] = parent.Number
		cursor = parent
	}

	cursor = candidateCursor

	for step := 0; step <= depth; step++ {
		if number, ok := c.getAncestorNumber(currentAncestors, cursor.Hash); ok && number == cursor.Number {
			return true, nil
		}
		if cursor.Number == 0 || cursor.ParentHash == "" || step == depth {
			break
		}
		parentRequest := chain.EVMBlockByHashRequest{Hash: cursor.ParentHash, Commitment: candidate.Commitment, PreferredUpstreamID: candidate.Origin}
		parent, err := c.runtime.EVMBlockByHash(ctx, parentRequest)
		if err != nil {
			return false, fmt.Errorf("verify candidate ancestry: %w", err)
		}
		if parent.Number+1 != cursor.Number || !strings.EqualFold(parent.Hash, cursor.ParentHash) {
			return false, errors.New("candidate chain has invalid parent linkage")
		}
		cursor = parent
	}
	return false, fmt.Errorf("candidate has no common ancestor within reorg depth %d", depth)
}

func (c *Coordinator) emitHeadAges(ctx context.Context) {
	redisCtx, finishRedis := c.telemetry.Span(ctx, "redis.snapshot", "chain:"+c.runtime.Config.Name, "operation:snapshot")
	snapshot, err := c.store.Snapshot(redisCtx, c.runtime.Config.Name)
	finishRedis(err)
	if err != nil {
		c.telemetry.Count("redis.failure", 1, "chain:"+c.runtime.Config.Name, "operation:snapshot")
		return
	}
	for commitment, acceptedHead := range snapshot.Heads {
		if c.runtime.Config.Family == "evm" && commitment != head.Latest {
			continue
		}
		if !acceptedHead.ObservedAt.IsZero() {
			c.telemetry.Gauge("head.age", time.Since(acceptedHead.ObservedAt).Seconds(), "chain:"+acceptedHead.Chain, "family:"+acceptedHead.Family, "commitment:"+commitment)
		}
		if !acceptedHead.ChangedAt.IsZero() {
			c.telemetry.Gauge("head.stuck_age", time.Since(acceptedHead.ChangedAt).Seconds(), "chain:"+acceptedHead.Chain, "family:"+acceptedHead.Family, "commitment:"+commitment)
		}
		for _, candidate := range c.runtime.Upstreams {
			c.providerMu.RLock()
			number, observed := c.providerHeads[providerCommitment{upstreamID: candidate.ID, commitment: commitment}]
			c.providerMu.RUnlock()
			if observed {
				var lag uint64
				if acceptedHead.Number > number {
					lag = acceptedHead.Number - number
				}
				c.telemetry.Gauge("provider.head_lag", float64(lag), "chain:"+acceptedHead.Chain, "family:"+acceptedHead.Family, "commitment:"+commitment, "upstream:"+candidate.ID)
			}
		}
	}
}

func (c *Coordinator) recordProviderHead(observedHead head.Head) {
	// Retain only the latest observation per configured provider/commitment, including WebSocket heads.
	c.providerMu.Lock()
	c.providerHeads[providerCommitment{upstreamID: observedHead.Origin, commitment: observedHead.Commitment}] = observedHead.Number
	c.providerMu.Unlock()
}

func (c *Coordinator) currentStillObserved(ctx context.Context, current head.Head) (bool, error) {
	uncertain := false
	checked := 0
	for _, candidate := range c.runtime.Candidates(false) {
		queryCtx, cancel := context.WithTimeout(ctx, time.Second)
		blockCall, err := c.call(queryCtx, candidate, "eth_getBlockByNumber", utils.FormatEVMQuantity(current.Number), false)
		cancel()
		if err != nil || blockCall.Error != nil {
			uncertain = true
			continue
		}
		observedHead, err := c.runtime.ParseEVMHead(chain.EVMHeadParseRequest{Commitment: current.Commitment, Origin: candidate.ID, Header: blockCall.Result})
		if err != nil || observedHead.Number != current.Number {
			uncertain = true
			continue
		}
		checked++
		if strings.EqualFold(observedHead.Hash, current.Hash) {
			return true, nil
		}
	}
	if uncertain || checked == 0 {
		return false, errors.New("cannot prove accepted head was displaced")
	}
	return false, nil
}

func (c *Coordinator) watchEVM(ctx context.Context, candidate *upstream.Client, output chan<- observation) {
	for ctx.Err() == nil {
		if !c.runtime.IsValid(candidate.ID) || !candidate.Healthy() {
			c.waitReconnect(ctx)
			continue
		}
		c.watchEVMSession(ctx, candidate, output)
		if ctx.Err() != nil {
			return
		}
		c.telemetry.Count("ws.upstream_failover", 1, "chain:"+c.runtime.Config.Name, "upstream:"+candidate.ID)
		c.waitReconnect(ctx)
	}
}

func (c *Coordinator) waitReconnect(ctx context.Context) {
	timer := time.NewTimer(time.Second + time.Duration(rand.IntN(500))*time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func (c *Coordinator) call(ctx context.Context, candidate *upstream.Client, method string, arguments ...any) (upstream.RPCResponse, error) {
	encodedParameters, err := jsonrpc.MarshalCallParams(arguments...)
	if err != nil {
		return upstream.RPCResponse{}, fmt.Errorf("prepare %s coordinator call: %w", method, err)
	}
	return candidate.Call(ctx, method, encodedParameters)
}

func (c *Coordinator) getAncestorNumber(ancestors map[string]uint64, hashValue string) (uint64, bool) {
	for ancestorHash, number := range ancestors {
		if strings.EqualFold(ancestorHash, hashValue) {
			return number, true
		}
	}
	return 0, false
}

func (c *Coordinator) writeWebsocketJSON(ctx context.Context, conn *websocket.Conn, message any) error {
	encodedMessage, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("encode upstream websocket message: %w", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, encodedMessage); err != nil {
		return fmt.Errorf("write upstream websocket message: %w", err)
	}
	return nil
}
