// Package receipts verifies receipt inclusion before allowing transaction-hash cache entries.
package receipts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/services/requestcache"
	"github.com/fiberevm/rpc-proxy/internal/utils"
)

type blockReader interface {
	EVMBlockByHash(context.Context, chain.EVMBlockByHashRequest) (head.Head, error)
}

// Options supplies the trusted block reader, shared bounded cache and maximum ancestry distance.
type Options struct {
	Blocks   blockReader
	Cache    *requestcache.Service
	MaxDepth int
}

// Service checks inclusion by following exact parent hashes from an accepted head.
type Service struct {
	blocks   blockReader
	cache    *requestcache.Service
	maxDepth int
}

// BlockRequest identifies the receipt's block and the accepted branch it must belong to.
type BlockRequest struct {
	Block    head.Head
	Accepted head.Head
}

// NewService constructs an inclusion verifier using validated limits and injected block dependencies.
func NewService(options Options) *Service {
	return &Service{blocks: options.Blocks, cache: options.Cache, maxDepth: options.MaxDepth}
}

// ContainsBlock verifies request.Block on request.Accepted's parent chain within the configured depth.
// False means the receipt must stay uncached; an unavailable or inconsistent header returns an error.
func (s *Service) ContainsBlock(ctx context.Context, request BlockRequest) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("verify receipt inclusion: %w", err)
	}
	if !utils.IsEVMHash(request.Block.Hash) || !utils.IsEVMHash(request.Accepted.Hash) {
		return false, errors.New("receipt inclusion requires valid block and accepted head hashes")
	}
	if request.Accepted.ReorgPending || request.Block.Number > request.Accepted.Number || request.Accepted.Number-request.Block.Number > uint64(s.maxDepth) {
		return false, nil
	}
	cursor := request.Accepted
	for cursor.Number > request.Block.Number {
		if err := ctx.Err(); err != nil {
			return false, fmt.Errorf("verify receipt ancestry: %w", err)
		}
		// The accepted header already proves its direct parent's identity.
		if cursor.Number-1 == request.Block.Number {
			return strings.EqualFold(cursor.ParentHash, request.Block.Hash), nil
		}
		parent, err := s.getParent(ctx, cursor)
		if err != nil {
			return false, fmt.Errorf("get receipt ancestor: %w", err)
		}
		if parent.Number != cursor.Number-1 || !strings.EqualFold(parent.Hash, cursor.ParentHash) {
			return false, errors.New("receipt ancestry contains a discontinuous parent")
		}
		cursor = parent
	}
	return strings.EqualFold(cursor.Hash, request.Block.Hash), nil
}

func (s *Service) getParent(ctx context.Context, child head.Head) (head.Head, error) {
	blockRequest := chain.EVMBlockByHashRequest{Hash: child.ParentHash, Commitment: "explicit", PreferredUpstreamID: child.Origin}
	if s.cache == nil {
		return s.blocks.EVMBlockByHash(ctx, blockRequest)
	}
	// Immutable headers can be shared across receipts and epochs. Only their verified
	// links to this request's accepted head establish canonical inclusion.
	lookup, err := s.cache.GetOrLoad(ctx, requestcache.Request{
		Key: fmt.Sprintf("receipt-header:%q:%s", child.Chain, child.ParentHash),
		Load: func(loadCtx context.Context) (requestcache.Response, error) {
			parent, err := s.blocks.EVMBlockByHash(loadCtx, blockRequest)
			if err != nil {
				return requestcache.Response{}, fmt.Errorf("fetch receipt ancestor: %w", err)
			}
			encodedParent, err := json.Marshal(parent)
			if err != nil {
				return requestcache.Response{}, fmt.Errorf("encode receipt ancestor: %w", err)
			}
			return requestcache.Response{Payload: encodedParent, Upstream: parent.Origin}, nil
		},
	})
	if err != nil {
		return head.Head{}, fmt.Errorf("load receipt ancestor: %w", err)
	}
	var parent head.Head
	if err := json.Unmarshal(lookup.Response.Payload, &parent); err != nil {
		return head.Head{}, fmt.Errorf("decode receipt ancestor: %w", err)
	}
	return parent, nil
}
