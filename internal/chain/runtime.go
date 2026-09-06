package chain

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
	"github.com/fiberevm/rpc-proxy/internal/telemetry"
	"github.com/fiberevm/rpc-proxy/internal/upstream"
	"github.com/fiberevm/rpc-proxy/internal/utils"
	"golang.org/x/sync/singleflight"
)

type Runtime struct {
	Config         config.ChainConfig
	Upstreams      []*upstream.Client
	telemetry      *telemetry.Telemetry
	validMu        sync.RWMutex
	valid          map[string]bool
	validation     map[string]string
	availabilityMu sync.Mutex
	availability   map[string]availabilityRecord
	probes         singleflight.Group
}

type availabilityRecord struct {
	available bool
	expires   time.Time
}

type Status struct {
	Name       string            `json:"name"`
	Family     string            `json:"family"`
	Validation map[string]string `json:"validation,omitempty"`
	Upstreams  []upstream.Status `json:"upstreams"`
}

// EVMHeadParseRequest groups the origin metadata and encoded header required to construct an accepted head.
type EVMHeadParseRequest struct {
	Commitment string
	Origin     string
	Header     json.RawMessage
}

// EVMBlockByHashRequest identifies a block and preferred upstream for continuity validation.
type EVMBlockByHashRequest struct {
	Hash                string
	Commitment          string
	PreferredUpstreamID string
}

// EVMBlockByNumberRequest identifies a numbered block on one specific upstream fork.
type EVMBlockByNumberRequest struct {
	Number     uint64
	Commitment string
	UpstreamID string
}

type evmNumberTargetRequest struct {
	quantity string
	snapshot head.Snapshot
}

var (
	// ErrInvalidEVMBlockReference identifies caller-supplied block references that fail strict validation.
	ErrInvalidEVMBlockReference = errors.New("invalid EVM block reference")
	// ErrUnsupportedEVMBlockReference identifies valid references, such as pending, that cannot be pinned.
	ErrUnsupportedEVMBlockReference = errors.New("unsupported EVM block reference")
	// ErrEVMBlockUnavailable identifies valid historical references absent from every eligible upstream.
	ErrEVMBlockUnavailable = errors.New("EVM block unavailable")
)

// NewRuntime builds the per-chain service and injects an upstream client for each validated configuration entry.
func NewRuntime(configuration config.ChainConfig, telemetryService *telemetry.Telemetry) *Runtime {
	runtime := &Runtime{Config: configuration, telemetry: telemetryService, valid: map[string]bool{}, validation: map[string]string{}, availability: map[string]availabilityRecord{}}
	for _, upstreamConfig := range configuration.Upstreams {
		clientOptions := upstream.Options{Config: upstreamConfig, Chain: configuration.Name, Family: configuration.Family, Telemetry: telemetryService}
		runtime.Upstreams = append(runtime.Upstreams, upstream.NewClient(clientOptions))
	}
	return runtime
}

// Validate probes every upstream concurrently, records capabilities, and returns all probe failures.
func (r *Runtime) Validate(ctx context.Context) error {
	var wg sync.WaitGroup
	validationErrors := make(chan error, len(r.Upstreams))
	for _, candidate := range r.Upstreams {
		candidate := candidate
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := r.validateUpstream(ctx, candidate)
			r.validMu.Lock()
			defer r.validMu.Unlock()
			r.valid[candidate.ID] = err == nil
			if err != nil {
				r.validation[candidate.ID] = r.telemetry.Redact(err.Error())
				validationErrors <- fmt.Errorf("validate upstream %s: %w", candidate.ID, err)
			} else {
				delete(r.validation, candidate.ID)
			}
		}()
	}
	wg.Wait()
	close(validationErrors)

	var failures []error
	for validationErr := range validationErrors {
		failures = append(failures, validationErr)
	}
	return errors.Join(failures...)
}

func (r *Runtime) validateUpstream(ctx context.Context, candidate *upstream.Client) error {
	if r.Config.Family == "evm" {
		chainIDCall, err := r.call(ctx, candidate, "eth_chainId")
		if err != nil {
			return err
		}
		if chainIDCall.Error != nil {
			return chainIDCall.Error
		}
		var chainID string
		if err := json.Unmarshal(chainIDCall.Result, &chainID); err != nil {
			return fmt.Errorf("invalid chain id response: %w", err)
		}
		actualChainID, chainIDErr := utils.ParseEVMQuantity(chainID)
		if chainIDErr != nil {
			return fmt.Errorf("parse upstream chain id: %w", chainIDErr)
		}
		configuredChainID, configuredChainIDErr := utils.ParseEVMQuantity(r.Config.ChainID)
		if configuredChainIDErr != nil {
			return fmt.Errorf("parse configured chain id: %w", configuredChainIDErr)
		}
		if actualChainID != configuredChainID {
			return fmt.Errorf("chain id mismatch: got %s", chainID)
		}
		if r.Config.GenesisHash != "" {
			genesisCall, err := r.call(ctx, candidate, "eth_getBlockByNumber", "0x0", false)
			if err != nil {
				return err
			}
			if genesisCall.Error != nil {
				return genesisCall.Error
			}
			block, err := r.ParseEVMHead(EVMHeadParseRequest{Commitment: head.Finalized, Origin: candidate.ID, Header: genesisCall.Result})
			if err != nil {
				return err
			}
			if !strings.EqualFold(block.Hash, r.Config.GenesisHash) {
				return fmt.Errorf("genesis hash mismatch: got %s", block.Hash)
			}
		}
		latestCall, err := r.call(ctx, candidate, "eth_getBlockByNumber", "latest", false)
		if err != nil {
			return err
		}
		if latestCall.Error != nil {
			return latestCall.Error
		}
		latest, err := r.ParseEVMHead(EVMHeadParseRequest{Commitment: head.Latest, Origin: candidate.ID, Header: latestCall.Result})
		if err != nil {
			return err
		}
		r.probeEVMStateMethods(ctx, candidate, latest.Hash)
		return nil
	}
	genesisCall, err := r.call(ctx, candidate, "getGenesisHash")
	if err != nil {
		return err
	}
	if genesisCall.Error != nil {
		return genesisCall.Error
	}
	var genesis string
	if err := json.Unmarshal(genesisCall.Result, &genesis); err != nil {
		return fmt.Errorf("invalid genesis hash response: %w", err)
	}
	if genesis != r.Config.GenesisHash {
		return fmt.Errorf("genesis hash mismatch: got %s", genesis)
	}
	slotCall, err := r.call(ctx, candidate, "getSlot", map[string]any{"commitment": head.Finalized})
	if err != nil {
		return err
	}
	if slotCall.Error != nil {
		return slotCall.Error
	}
	var slot uint64
	if err := json.Unmarshal(slotCall.Result, &slot); err != nil {
		return fmt.Errorf("invalid finalized slot response: %w", err)
	}
	capabilityCall, err := r.call(ctx, candidate, "getAccountInfo", "11111111111111111111111111111111", map[string]any{"commitment": head.Finalized, "minContextSlot": slot})
	if err != nil {
		return err
	}
	if capabilityCall.Error != nil {
		return fmt.Errorf("minContextSlot capability probe failed: %w", capabilityCall.Error)
	}
	return nil
}

func (r *Runtime) probeEVMStateMethods(ctx context.Context, candidate *upstream.Client, blockHash string) {
	const zeroAddress = "0x0000000000000000000000000000000000000000"
	const missingHash = "0x0000000000000000000000000000000000000000000000000000000000000000"
	probes := []struct {
		method    string
		arguments []any
	}{
		{"eth_getBalance", []any{zeroAddress}},
		{"eth_getCode", []any{zeroAddress}},
		{"eth_getTransactionCount", []any{zeroAddress}},
		{"eth_getStorageAt", []any{zeroAddress, "0x0"}},
		{"eth_getProof", []any{zeroAddress, []string{}}},
		{"eth_call", []any{map[string]any{"to": zeroAddress, "data": "0x"}}},
	}
	var pending sync.WaitGroup
	for _, probe := range probes {
		pending.Add(1)
		go func() {
			defer pending.Done()
			arguments := append(probe.arguments, map[string]any{"blockHash": blockHash, "requireCanonical": true})
			positive, err := r.call(ctx, candidate, probe.method, arguments...)
			supported := err == nil && positive.Error == nil && len(positive.Result) > 0 && !bytes.Equal(positive.Result, []byte("null"))
			if supported {
				arguments[len(arguments)-1] = map[string]any{"blockHash": missingHash, "requireCanonical": true}
				negative, err := r.call(ctx, candidate, probe.method, arguments...)
				supported = err == nil && IsUnavailableRPCError(negative.Error)
			}
			candidate.SetPinnedMethod(probe.method, supported)
		}()
	}
	pending.Wait()
	anySupported := false
	for _, probe := range probes {
		anySupported = anySupported || candidate.SupportsPinnedMethod(probe.method)
	}
	candidate.SetEIP1898(anySupported)
}

// IsValid reports whether the upstream ID passed capability validation at startup.
func (r *Runtime) IsValid(id string) bool {
	r.validMu.RLock()
	defer r.validMu.RUnlock()
	return r.valid[id]
}

// Status returns validation and health details suitable for the private status endpoint.
func (r *Runtime) Status() Status {
	r.validMu.RLock()
	defer r.validMu.RUnlock()
	status := Status{Name: r.Config.Name, Family: r.Config.Family, Validation: map[string]string{}}
	for upstreamID, validationError := range r.validation {
		status.Validation[upstreamID] = validationError
	}
	for _, candidate := range r.Upstreams {
		status.Upstreams = append(status.Upstreams, candidate.Status())
	}
	return status
}

// Candidates returns healthy, validated upstreams ordered by rolling latency and success rate.
func (r *Runtime) Candidates(requireEIP1898 bool) []*upstream.Client {
	candidates := make([]*upstream.Client, 0, len(r.Upstreams))
	for _, candidate := range r.Upstreams {
		if !r.IsValid(candidate.ID) || !candidate.Healthy() {
			continue
		}
		if requireEIP1898 && !candidate.SupportsEIP1898() {
			continue
		}
		candidates = append(candidates, candidate)
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Score() < candidates[j].Score() })
	return candidates
}

// Upstream returns the client with id, or nil when the chain has no such configured upstream.
func (r *Runtime) Upstream(id string) *upstream.Client {
	for _, candidate := range r.Upstreams {
		if candidate.ID == id {
			return candidate
		}
	}
	return nil
}

// GetEVMBlock validates blockReference and returns the exact header used to pin an EVM request.
func (r *Runtime) GetEVMBlock(ctx context.Context, blockReference json.RawMessage, snapshot head.Snapshot) (head.Head, error) {
	if len(blockReference) == 0 || bytes.Equal(bytes.TrimSpace(blockReference), []byte("null")) {
		return r.getRequiredHead(snapshot, head.Latest)
	}

	var tagOrQuantity string
	if err := json.Unmarshal(blockReference, &tagOrQuantity); err == nil {
		switch tagOrQuantity {
		case head.Latest:
			return r.getRequiredHead(snapshot, head.Latest)
		case head.Safe, head.Finalized:
			return head.Head{}, fmt.Errorf("%w: %s is not tracked; only latest is supported as a live EVM head", ErrUnsupportedEVMBlockReference, tagOrQuantity)
		case "pending":
			return head.Head{}, fmt.Errorf("%w: pending state has no canonical block hash", ErrUnsupportedEVMBlockReference)
		case "earliest":
			tagOrQuantity = "0x0"
		}
		return r.getEVMBlockByQuantity(ctx, evmNumberTargetRequest{quantity: tagOrQuantity, snapshot: snapshot})
	}

	var object struct {
		BlockHash        string `json:"blockHash"`
		BlockNumber      string `json:"blockNumber"`
		RequireCanonical *bool  `json:"requireCanonical"`
	}
	if err := json.Unmarshal(blockReference, &object); err != nil {
		return head.Head{}, fmt.Errorf("%w: expected a string or EIP-1898 object", ErrInvalidEVMBlockReference)
	}
	if object.BlockHash != "" && object.BlockNumber != "" || object.RequireCanonical != nil && object.BlockHash == "" {
		return head.Head{}, fmt.Errorf("%w: inconsistent EIP-1898 fields", ErrInvalidEVMBlockReference)
	}
	if object.BlockHash != "" {
		if !utils.IsEVMHash(object.BlockHash) {
			return head.Head{}, fmt.Errorf("%w: invalid block hash", ErrInvalidEVMBlockReference)
		}
		return head.Head{Chain: r.Config.Name, Family: "evm", Commitment: "explicit", Hash: object.BlockHash, ObservedAt: time.Now()}, nil
	}
	if object.BlockNumber == "" {
		return head.Head{}, fmt.Errorf("%w: block identifier requires blockHash or blockNumber", ErrInvalidEVMBlockReference)
	}
	return r.getEVMBlockByQuantity(ctx, evmNumberTargetRequest{quantity: object.BlockNumber, snapshot: snapshot})
}

func (r *Runtime) getEVMBlockByQuantity(ctx context.Context, request evmNumberTargetRequest) (head.Head, error) {
	number, err := utils.ParseEVMQuantity(request.quantity)
	if err != nil {
		return head.Head{}, fmt.Errorf("%w: invalid block number", ErrInvalidEVMBlockReference)
	}

	accepted, err := r.getRequiredHead(request.snapshot, head.Latest)
	if err != nil {
		return head.Head{}, err
	}
	if accepted.ReorgPending || number > accepted.Number {
		return head.Head{}, fmt.Errorf("%w: block %s is outside the accepted snapshot", ErrEVMBlockUnavailable, request.quantity)
	}
	// A number lookup, even on the head's origin, can switch forks behind a load
	// balancer. Only the accepted header's parent links select the same branch.
	if r.Config.ReorgDepth < 0 || accepted.Number-number > uint64(r.Config.ReorgDepth) {
		return head.Head{}, fmt.Errorf("%w: block %s exceeds the ancestry window; use an explicit block hash", ErrEVMBlockUnavailable, request.quantity)
	}
	ancestryCtx, cancel := context.WithTimeout(ctx, r.Config.HeadRequestTimeout.Value())
	defer cancel()
	cursor := accepted
	for cursor.Number > number {
		parent, err := r.EVMBlockByHash(ancestryCtx, EVMBlockByHashRequest{Hash: cursor.ParentHash, Commitment: "explicit", PreferredUpstreamID: cursor.Origin})
		if err != nil {
			return head.Head{}, fmt.Errorf("%w: get ancestor of block %s: %w", ErrEVMBlockUnavailable, request.quantity, err)
		}
		if parent.Number != cursor.Number-1 {
			return head.Head{}, fmt.Errorf("%w: discontinuous ancestry for block %s", ErrEVMBlockUnavailable, request.quantity)
		}
		cursor = parent
	}
	cursor.Commitment = "explicit"
	return cursor, nil
}

func (r *Runtime) getRequiredHead(snapshot head.Snapshot, commitment string) (head.Head, error) {
	acceptedHead, ok := snapshot.Get(commitment)
	if !ok || acceptedHead.IsZero() {
		return head.Head{}, fmt.Errorf("%w: commitment %s", ErrEVMBlockUnavailable, commitment)
	}
	return acceptedHead, nil
}

// HasEVMBlock checks whether candidate can serve target and coalesces concurrent checks for the same hash.
func (r *Runtime) HasEVMBlock(ctx context.Context, candidate *upstream.Client, target head.Head) (bool, error) {
	if target.Hash == "" {
		return false, errors.New("target block hash is required")
	}
	// Hash-only checks must not satisfy stricter coordinator checks of a header's
	// height and parent. These constraints are part of the probe's identity.
	key := r.getEVMAvailabilityKey(candidate.ID, target)
	return r.probe(ctx, key, func(probeCtx context.Context) (bool, error) {
		blockCall, err := r.call(probeCtx, candidate, "eth_getBlockByHash", target.Hash, false)
		if err != nil {
			return false, fmt.Errorf("query target block from upstream %s: %w", candidate.ID, err)
		}
		if blockCall.Error != nil || string(blockCall.Result) == "null" {
			return false, nil
		}
		block, err := r.ParseEVMHead(EVMHeadParseRequest{Commitment: target.Commitment, Origin: candidate.ID, Header: blockCall.Result})
		if err != nil {
			return false, fmt.Errorf("parse target block from upstream %s: %w", candidate.ID, err)
		}
		return strings.EqualFold(block.Hash, target.Hash) && (target.Commitment == "explicit" || block.Number == target.Number && (target.ParentHash == "" || strings.EqualFold(block.ParentHash, target.ParentHash))), nil
	})
}

// EVMBlockByHash fetches and verifies a header, preferring the provider that
// observed it. It is used by the coordinator to prove parent continuity before
// moving the globally accepted head.
func (r *Runtime) EVMBlockByHash(ctx context.Context, request EVMBlockByHashRequest) (head.Head, error) {
	if !utils.IsEVMHash(request.Hash) {
		return head.Head{}, errors.New("invalid block hash")
	}
	var failures []error
	for _, candidate := range r.preferredCandidates(request.PreferredUpstreamID) {
		blockCall, err := r.call(ctx, candidate, "eth_getBlockByHash", request.Hash, false)
		if err != nil {
			failures = append(failures, fmt.Errorf("query block from upstream %s: %w", candidate.ID, err))
			continue
		}
		if blockCall.Error != nil {
			failures = append(failures, fmt.Errorf("query block from upstream %s: %w", candidate.ID, blockCall.Error))
			continue
		}
		if string(blockCall.Result) == "null" {
			continue
		}
		block, err := r.ParseEVMHead(EVMHeadParseRequest{Commitment: request.Commitment, Origin: candidate.ID, Header: blockCall.Result})
		if err == nil && strings.EqualFold(block.Hash, request.Hash) {
			return block, nil
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("parse block from upstream %s: %w", candidate.ID, err))
		}
	}
	return head.Head{}, errors.Join(fmt.Errorf("block %s is unavailable", request.Hash), errors.Join(failures...))
}

// EVMBlockByNumberFrom deliberately uses one provider. Mixing providers while
// proving a fork would make an otherwise valid set of headers look continuous.
func (r *Runtime) EVMBlockByNumberFrom(ctx context.Context, request EVMBlockByNumberRequest) (head.Head, error) {
	candidate := r.Upstream(request.UpstreamID)
	if candidate == nil || !r.IsValid(request.UpstreamID) || !candidate.Healthy() {
		return head.Head{}, fmt.Errorf("upstream %s is unavailable", request.UpstreamID)
	}
	blockCall, err := r.call(ctx, candidate, "eth_getBlockByNumber", utils.FormatEVMQuantity(request.Number), false)
	if err != nil {
		return head.Head{}, err
	}
	if blockCall.Error != nil {
		return head.Head{}, blockCall.Error
	}
	block, err := r.ParseEVMHead(EVMHeadParseRequest{Commitment: request.Commitment, Origin: candidate.ID, Header: blockCall.Result})
	if err != nil {
		return head.Head{}, err
	}
	if block.Number != request.Number {
		return head.Head{}, errors.New("upstream returned a different block number")
	}
	return block, nil
}

func (r *Runtime) preferredCandidates(preferredID string) []*upstream.Client {
	candidates := r.Candidates(false)
	if preferredID == "" {
		return candidates
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].ID == preferredID && candidates[j].ID != preferredID
	})
	return candidates
}

// HasSolanaSlot checks whether candidate has reached target and coalesces concurrent checks for the same slot.
func (r *Runtime) HasSolanaSlot(ctx context.Context, candidate *upstream.Client, target head.Head) (bool, error) {
	key := fmt.Sprintf("%s:solana:%s:%d", candidate.ID, target.Commitment, target.Number)
	return r.probe(ctx, key, func(probeCtx context.Context) (bool, error) {
		slotCall, err := r.call(probeCtx, candidate, "getSlot", map[string]any{"commitment": target.Commitment, "minContextSlot": target.Number})
		if err != nil {
			return false, fmt.Errorf("query target slot from upstream %s: %w", candidate.ID, err)
		}
		if slotCall.Error != nil {
			return false, nil
		}
		var slot uint64
		if err := json.Unmarshal(slotCall.Result, &slot); err != nil {
			return false, fmt.Errorf("decode target slot from upstream %s: %w", candidate.ID, err)
		}
		return slot >= target.Number, nil
	})
}

func (r *Runtime) probe(ctx context.Context, key string, checkAvailability func(context.Context) (bool, error)) (bool, error) {
	if available, cached := r.getCachedAvailability(key); cached {
		return available, nil
	}
	probe := r.probes.DoChan(key, func() (any, error) {
		// A prior probe may finish after the caller's cache check but before joining singleflight.
		if available, cached := r.getCachedAvailability(key); cached {
			return available, nil
		}
		// A canceled waiter must not cancel the probe shared by other requests.
		probeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 1500*time.Millisecond)
		defer cancel()
		available, err := checkAvailability(probeCtx)
		if err != nil {
			return false, err
		}
		ttl := 2 * r.Config.PollInterval.Value()
		if ttl < 250*time.Millisecond {
			ttl = 250 * time.Millisecond
		}
		if !available {
			ttl = 75 * time.Millisecond
		}
		r.availabilityMu.Lock()
		if len(r.availability) >= 4096 {
			for cachedKey, record := range r.availability {
				if time.Now().After(record.expires) {
					delete(r.availability, cachedKey)
				}
			}
			for cachedKey := range r.availability {
				if len(r.availability) < 4096 {
					break
				}
				delete(r.availability, cachedKey)
			}
		}
		r.availability[key] = availabilityRecord{available: available, expires: time.Now().Add(ttl)}
		r.availabilityMu.Unlock()
		return available, nil
	})
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case availability := <-probe:
		if availability.Err != nil {
			return false, availability.Err
		}
		available, ok := availability.Val.(bool)
		if !ok {
			return false, errors.New("availability probe returned an invalid response")
		}
		return available, nil
	}
}

func (r *Runtime) getCachedAvailability(key string) (bool, bool) {
	r.availabilityMu.Lock()
	defer r.availabilityMu.Unlock()
	record, exists := r.availability[key]
	return record.available, exists && time.Now().Before(record.expires)
}

// InvalidateAvailability discards a cached target check after an upstream rejects a pinned read.
func (r *Runtime) InvalidateAvailability(candidateID string, target head.Head) {
	r.availabilityMu.Lock()
	defer r.availabilityMu.Unlock()
	delete(r.availability, r.getEVMAvailabilityKey(candidateID, target))
	delete(r.availability, fmt.Sprintf("%s:solana:%s:%d", candidateID, target.Commitment, target.Number))
}

func (r *Runtime) getEVMAvailabilityKey(candidateID string, target head.Head) string {
	return fmt.Sprintf("%s:evm:%s:%s:%d:%s", candidateID, target.Hash, target.Commitment, target.Number, target.ParentHash)
}

// ParseEVMHead validates an upstream header and adds this runtime's chain identity and observation time.
func (r *Runtime) ParseEVMHead(request EVMHeadParseRequest) (head.Head, error) {
	if len(request.Header) == 0 || string(request.Header) == "null" {
		return head.Head{}, errors.New("block is null")
	}
	var block map[string]json.RawMessage
	if err := json.Unmarshal(request.Header, &block); err != nil {
		return head.Head{}, fmt.Errorf("decode block: %w", err)
	}
	var numberHex, hashValue, parentHash string
	if err := json.Unmarshal(block["number"], &numberHex); err != nil {
		return head.Head{}, errors.New("block has invalid number")
	}
	if err := json.Unmarshal(block["hash"], &hashValue); err != nil || !utils.IsEVMHash(hashValue) {
		return head.Head{}, errors.New("block has invalid hash")
	}
	if err := json.Unmarshal(block["parentHash"], &parentHash); err != nil || !utils.IsEVMHash(parentHash) {
		return head.Head{}, errors.New("block has invalid parent hash")
	}
	number, err := utils.ParseEVMQuantity(numberHex)
	if err != nil {
		return head.Head{}, err
	}
	delete(block, "transactions")
	header, err := json.Marshal(block)
	if err != nil {
		return head.Head{}, fmt.Errorf("encode block header: %w", err)
	}
	return head.Head{Chain: r.Config.Name, Family: "evm", Commitment: request.Commitment, Number: number, Hash: hashValue, ParentHash: parentHash, Origin: request.Origin, ObservedAt: time.Now().UTC(), Header: header}, nil
}

// IsUnavailableRPCError reports RPC errors that mean an upstream cannot serve a pinned target.
func IsUnavailableRPCError(rpcErr *jsonrpc.Error) bool {
	if rpcErr == nil || rpcErr.Code == jsonrpc.CodeMethodNotFound {
		return false
	}
	message := strings.ToLower(rpcErr.Message + " " + fmt.Sprint(rpcErr.Data))
	return rpcErr.Code == -32001 || strings.Contains(message, "not found") || strings.Contains(message, "unknown block") || strings.Contains(message, "minimum context slot") || strings.Contains(message, "not canonical")
}

func (r *Runtime) call(ctx context.Context, candidate *upstream.Client, method string, arguments ...any) (upstream.RPCResponse, error) {
	encodedParameters, err := jsonrpc.MarshalCallParams(arguments...)
	if err != nil {
		return upstream.RPCResponse{}, fmt.Errorf("prepare %s upstream call: %w", method, err)
	}
	return candidate.Call(ctx, method, encodedParameters)
}
