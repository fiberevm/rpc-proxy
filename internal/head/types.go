package head

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	Latest    = "latest"
	Safe      = "safe"
	Finalized = "finalized"
	Processed = "processed"
	Confirmed = "confirmed"
)

var ErrLeadershipLost = errors.New("head coordinator leadership lost")

// ErrStreamGap means the consumer's checkpoint is no longer retained.
var ErrStreamGap = errors.New("accepted head stream checkpoint is no longer retained")

// ErrReorgFence means a publication did not acknowledge the current reorg epoch.
var ErrReorgFence = errors.New("accepted head reorg fence changed")

// Head is one verified EVM block or Solana slot accepted for a commitment level.
type Head struct {
	Chain      string `json:"chain"`
	Family     string `json:"family"`
	Commitment string `json:"commitment"`
	Number     uint64 `json:"number"`
	Hash       string `json:"hash,omitempty"`
	ParentHash string `json:"parent_hash,omitempty"`
	Generation uint64 `json:"generation"`
	// ReorgEpoch changes before fork recovery, not on ordinary head advancement.
	ReorgEpoch uint64 `json:"reorg_epoch"`
	// ReorgPending survives leadership changes and blocks pinned reads until verified recovery.
	ReorgPending bool            `json:"reorg_pending"`
	ObservedAt   time.Time       `json:"observed_at"`
	ChangedAt    time.Time       `json:"changed_at"`
	Origin       string          `json:"origin"`
	Header       json.RawMessage `json:"header,omitempty"`
}

// Identity returns the hash identity for EVM or the commitment-and-slot identity for Solana.
func (h Head) Identity() string {
	if h.Hash != "" {
		return h.Hash
	}
	return fmt.Sprintf("%s:%d", h.Commitment, h.Number)
}

// IsZero reports an absent head; Solana slot zero is a valid observed floor.
func (h Head) IsZero() bool { return h.Number == 0 && h.Hash == "" && h.Family != "solana" }

// Snapshot contains the independently accepted commitment heads for one chain.
type Snapshot struct {
	Chain string
	Heads map[string]Head
}

// Get returns the accepted head for commitment when the snapshot contains it.
func (s Snapshot) Get(commitment string) (Head, bool) {
	h, ok := s.Heads[commitment]
	return h, ok
}

// Event is one retained accepted-head stream item and its resumable cursor ID.
type Event struct {
	ID   string
	Head Head
}

// Store defines the fenced coordination and accepted-head operations consumed by coordinators and gateways.
type Store interface {
	Acquire(ctx context.Context, chain, owner string, ttl time.Duration) (token string, acquired bool, err error)
	Renew(ctx context.Context, chain, token string, ttl time.Duration) (bool, error)
	Release(ctx context.Context, chain, token string) error
	Publish(ctx context.Context, token string, acceptedHead Head) (Head, error)
	BeginReorg(ctx context.Context, chain, token string) (Head, error)
	Snapshot(ctx context.Context, chain string) (Snapshot, error)
	StreamCursor(ctx context.Context, chain string) (string, error)
	StreamStart(ctx context.Context, chain string) (Event, error)
	ReadEvents(ctx context.Context, chain, afterCursor string, blockTimeout time.Duration, count int64) ([]Event, error)
	Ping(ctx context.Context) error
}

// MemoryStore is deterministic and useful for tests and embedded development.
type MemoryStore struct {
	mu      sync.Mutex
	heads   map[string]map[string]Head
	leaders map[string]memoryLeader
	events  map[string][]Event
	notify  chan struct{}
	gen     map[string]uint64
}

type memoryLeader struct {
	token   string
	expires time.Time
}

// NewMemoryStore creates a deterministic in-process Store for tests and embedded development.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		heads: map[string]map[string]Head{}, leaders: map[string]memoryLeader{},
		events: map[string][]Event{}, notify: make(chan struct{}), gen: map[string]uint64{},
	}
}

// Acquire creates a fenced in-memory leadership token when chain has no active leader.
func (m *MemoryStore) Acquire(_ context.Context, chain, owner string, ttl time.Duration) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.leaders[chain]
	if current.token != "" && time.Now().Before(current.expires) {
		return "", false, nil
	}
	m.gen[chain]++
	token := fmt.Sprintf("%s:%d", owner, m.gen[chain])
	m.leaders[chain] = memoryLeader{token: token, expires: time.Now().Add(ttl)}
	return token, true, nil
}

// Renew extends an in-memory chain lease only for its current fencing token.
func (m *MemoryStore) Renew(_ context.Context, chain, token string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.leaders[chain]
	if current.token != token || time.Now().After(current.expires) {
		return false, nil
	}
	current.expires = time.Now().Add(ttl)
	m.leaders[chain] = current
	return true, nil
}

// Release removes an in-memory chain lease only for its current fencing token.
func (m *MemoryStore) Release(_ context.Context, chain, token string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.leaders[chain].token == token {
		delete(m.leaders, chain)
	}
	return nil
}

// Publish stores one accepted head when token still owns the chain lease.
func (m *MemoryStore) Publish(_ context.Context, token string, acceptedHead Head) (Head, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	leader := m.leaders[acceptedHead.Chain]
	if leader.token != token || time.Now().After(leader.expires) {
		return Head{}, ErrLeadershipLost
	}
	if m.heads[acceptedHead.Chain] == nil {
		m.heads[acceptedHead.Chain] = map[string]Head{}
	}
	previous, exists := m.heads[acceptedHead.Chain][acceptedHead.Commitment]
	if acceptedHead.Family == "evm" && acceptedHead.Commitment == Latest && (acceptedHead.ReorgPending || acceptedHead.ReorgEpoch != previous.ReorgEpoch) {
		return Head{}, ErrReorgFence
	}
	if exists && previous.Identity() == acceptedHead.Identity() {
		acceptedHead.Generation = previous.Generation
		acceptedHead.ChangedAt = previous.ChangedAt
	} else {
		if acceptedHead.ChangedAt.IsZero() {
			acceptedHead.ChangedAt = acceptedHead.ObservedAt
		}
		m.gen[acceptedHead.Chain]++
		acceptedHead.Generation = m.gen[acceptedHead.Chain]
		if acceptedHead.Family == "evm" && acceptedHead.Commitment == Latest {
			id := fmt.Sprintf("%d-0", acceptedHead.Generation)
			m.events[acceptedHead.Chain] = append(m.events[acceptedHead.Chain], Event{ID: id, Head: acceptedHead})
		}
	}
	m.heads[acceptedHead.Chain][acceptedHead.Commitment] = acceptedHead
	close(m.notify)
	m.notify = make(chan struct{})
	return acceptedHead, nil
}

// BeginReorg fences readers before fork recovery; only the current chain leader may advance the epoch.
func (m *MemoryStore) BeginReorg(_ context.Context, chain, token string) (Head, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	leader := m.leaders[chain]
	if leader.token != token || !time.Now().Before(leader.expires) {
		return Head{}, ErrLeadershipLost
	}
	current, exists := m.heads[chain][Latest]
	if !exists || current.Family != "evm" || current.IsZero() {
		return Head{}, ErrReorgFence
	}
	if !current.ReorgPending {
		current.ReorgEpoch++
		current.ReorgPending = true
		m.heads[chain][Latest] = current
	}
	return current, nil
}

// Set installs a head without fencing for deterministic test and embedded setup.
func (m *MemoryStore) Set(acceptedHead Head) Head {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.heads[acceptedHead.Chain] == nil {
		m.heads[acceptedHead.Chain] = map[string]Head{}
	}
	m.gen[acceptedHead.Chain]++
	acceptedHead.Generation = m.gen[acceptedHead.Chain]
	if acceptedHead.ChangedAt.IsZero() {
		acceptedHead.ChangedAt = acceptedHead.ObservedAt
	}
	m.heads[acceptedHead.Chain][acceptedHead.Commitment] = acceptedHead
	if acceptedHead.Family == "evm" && acceptedHead.Commitment == Latest {
		id := fmt.Sprintf("%d-0", acceptedHead.Generation)
		m.events[acceptedHead.Chain] = append(m.events[acceptedHead.Chain], Event{ID: id, Head: acceptedHead})
	}
	close(m.notify)
	m.notify = make(chan struct{})
	return acceptedHead
}

// Snapshot returns a copy of all in-memory accepted commitment heads for chain.
func (m *MemoryStore) Snapshot(_ context.Context, chain string) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	snapshot := Snapshot{Chain: chain, Heads: map[string]Head{}}
	for commitment, acceptedHead := range m.heads[chain] {
		snapshot.Heads[commitment] = acceptedHead
	}
	return snapshot, nil
}

// StreamCursor returns the last in-memory event ID for a newly connected client.
func (m *MemoryStore) StreamCursor(ctx context.Context, chain string) (string, error) {
	checkpoint, err := m.StreamStart(ctx, chain)
	return checkpoint.ID, err
}

// StreamStart atomically returns the last event and its cursor as a subscription baseline.
func (m *MemoryStore) StreamStart(_ context.Context, chain string) (Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	events := m.events[chain]
	if len(events) == 0 {
		return Event{ID: "0-0"}, nil
	}
	return events[len(events)-1], nil
}

// ReadEvents waits up to blockTimeout and returns accepted heads after afterCursor.
func (m *MemoryStore) ReadEvents(ctx context.Context, chain, afterCursor string, blockTimeout time.Duration, count int64) ([]Event, error) {
	for {
		m.mu.Lock()
		start := 0
		found := afterCursor == "0-0"
		for i, event := range m.events[chain] {
			if event.ID == afterCursor {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			m.mu.Unlock()
			return nil, ErrStreamGap
		}
		if start < len(m.events[chain]) {
			end := len(m.events[chain])
			if count > 0 && int64(end-start) > count {
				end = start + int(count)
			}
			events := append([]Event(nil), m.events[chain][start:end]...)
			m.mu.Unlock()
			return events, nil
		}
		notify := m.notify
		m.mu.Unlock()
		timer := time.NewTimer(blockTimeout)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-notify:
			timer.Stop()
		case <-timer.C:
			return nil, nil
		}
	}
}

// Ping always succeeds because the in-memory store has no external dependency.
func (m *MemoryStore) Ping(context.Context) error { return nil }
